from atproto import Client
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Set, Tuple

# ================== CONFIG VIA ENV ==================

HOURS_BACK = int(os.getenv("HOURS_BACK", "3"))
POST_DELAY_SECONDS = float(os.getenv("POST_DELAY_SECONDS", "3"))
MAX_PER_RUN = int(os.getenv("MAX_PER_RUN", "100"))
MAX_PER_USER = int(os.getenv("MAX_PER_USER", "10"))
REPOST_LOG_FILE = os.getenv("REPOST_LOG_FILE", "reposted.txt")
FOLLOW_ON_REPOST = os.getenv("FOLLOW_ON_REPOST", "0") == "1"

LIST_MEMBER_LIMIT = int(os.getenv("LIST_MEMBER_LIMIT", "200"))
AUTHOR_POSTS_PER_MEMBER = int(os.getenv("AUTHOR_POSTS_PER_MEMBER", "50"))
FEED_MAX_ITEMS = int(os.getenv("FEED_MAX_ITEMS", "1000"))

# ================== HELPERS ==================

def log(msg: str):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{now}] {msg}")

def parse_time(record, post) -> Optional[datetime]:
    for attr in ["createdAt", "indexedAt", "created_at", "timestamp"]:
        val = getattr(record, attr, None) or getattr(post, attr, None)
        if val:
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            except Exception:
                continue
    return None

def load_repost_log(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}

def save_repost_log(path: str, uris: Set[str]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for uri in sorted(uris):
            f.write(uri + "\n")

def has_media(record) -> bool:
    embed = getattr(record, "embed", None)
    if not embed:
        return False
    if getattr(embed, "images", None):
        return True
    if getattr(embed, "video", None):
        return True
    return False

def is_quote_post(record) -> bool:
    embed = getattr(record, "embed", None)
    if not embed:
        return False
    return bool(getattr(embed, "record", None) or getattr(embed, "recordWithMedia", None))

# ================== URI NORMALIZERS ==================

FEED_URL_RE = re.compile(r"https://bsky.app/profile/([^/]+)/feed/([^/?#]+)", re.I)
LIST_URL_RE = re.compile(r"https://bsky.app/profile/([^/]+)/lists/([^/?#]+)", re.I)

def resolve_handle_to_did(client: Client, actor: str) -> Optional[str]:
    if actor.startswith("did:"):
        return actor
    try:
        out = client.com.atproto.identity.resolve_handle({"handle": actor})
        return getattr(out, "did", None)
    except Exception:
        return None

def normalize_feed_uri(client: Client, link: str) -> Optional[str]:
    if not link:
        return None
    if link.startswith("at://"):
        return link
    m = FEED_URL_RE.match(link)
    if not m:
        return None
    did = resolve_handle_to_did(client, m.group(1))
    if not did:
        return None
    return f"at://{did}/app.bsky.feed.generator/{m.group(2)}"

def normalize_list_uri(client: Client, link: str) -> Optional[str]:
    if not link:
        return None
    if link.startswith("at://"):
        return link
    m = LIST_URL_RE.match(link)
    if not m:
        return None
    did = resolve_handle_to_did(client, m.group(1))
    if not did:
        return None
    return f"at://{did}/app.bsky.graph.list/{m.group(2)}"

# ================== FETCHERS ==================

def fetch_feed_items(client: Client, feed_uri: str) -> List:
    items, cursor = [], None
    while True:
        params = {"feed": feed_uri, "limit": 100}
        if cursor:
            params["cursor"] = cursor
        out = client.app.bsky.feed.get_feed(params)
        batch = getattr(out, "feed", []) or []
        items.extend(batch)
        cursor = getattr(out, "cursor", None)
        if not cursor or len(items) >= FEED_MAX_ITEMS:
            break
    return items[:FEED_MAX_ITEMS]

def fetch_list_members(client: Client, list_uri: str) -> List[str]:
    members, cursor = [], None
    while True:
        params = {"list": list_uri, "limit": 100}
        if cursor:
            params["cursor"] = cursor
        out = client.app.bsky.graph.get_list(params)
        for it in getattr(out, "items", []) or []:
            subj = getattr(it, "subject", None)
            if subj and getattr(subj, "did", None):
                members.append(subj.did)
            if len(members) >= LIST_MEMBER_LIMIT:
                return members[:LIST_MEMBER_LIMIT]
        cursor = getattr(out, "cursor", None)
        if not cursor:
            break
    return members[:LIST_MEMBER_LIMIT]

def fetch_author_posts(client: Client, actor: str) -> List:
    try:
        out = client.app.bsky.feed.get_author_feed({"actor": actor, "limit": AUTHOR_POSTS_PER_MEMBER})
        return getattr(out, "feed", []) or []
    except Exception:
        return []

# ================== MAIN ==================

def main():
    username = os.getenv("BSKY_USERNAME")
    password = os.getenv("BSKY_PASSWORD")

    if not username or not password:
        log("❌ Missing login env vars")
        return

    client = Client()
    client.login(username, password)
    log("✅ Logged in")

    cutoff = datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)
    done = load_repost_log(REPOST_LOG_FILE)

    # FEEDS
    feed_links = [os.getenv(f"FEED_{i}_LINK", "") for i in range(1, 11)]
    feeds = [normalize_feed_uri(client, f) for f in feed_links if f]

    # STOPLISTS
    stop_links = [os.getenv(f"STOPLIST_{i}_LINK", "") for i in range(1, 11)]
    stop_dids = set()
    for link in stop_links:
        uri = normalize_list_uri(client, link)
        if uri:
            stop_dids.update(fetch_list_members(client, uri))

    candidates = []

    for feed_uri in feeds:
        log(f"📥 Feed: {feed_uri}")
        for item in fetch_feed_items(client, feed_uri):
            post = item.post
            record = post.record
            uri = post.uri
            cid = post.cid

            if uri in done:
                continue
            if hasattr(item, "reason") and item.reason is not None:
                continue
            if is_quote_post(record):
                continue
            if not has_media(record):
                continue
            if getattr(record, "reply", None):
                continue

            created_dt = parse_time(record, post)
            if not created_dt or created_dt < cutoff:
                continue

            author_did = getattr(post.author, "did", None)
            if author_did in stop_dids:
                continue

            candidates.append({
                "uri": uri,
                "cid": cid,
                "author": author_did,
                "created": created_dt
            })

    candidates.sort(key=lambda x: x["created"])

    reposted = 0
    per_user = {}

    for c in candidates:
        if reposted >= MAX_PER_RUN:
            break

        au = c["author"]
        per_user.setdefault(au, 0)
        if per_user[au] >= MAX_PER_USER:
            continue

        try:
            client.app.bsky.feed.repost.create(
                repo=client.me.did,
                record={
                    "subject": {"uri": c["uri"], "cid": c["cid"]},
                    "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
            )
            done.add(c["uri"])
            per_user[au] += 1
            reposted += 1

            if FOLLOW_ON_REPOST:
                try:
                    profile = client.app.bsky.actor.get_profile({"actor": au})
                    if not getattr(profile.viewer, "following", None):
                        client.app.bsky.graph.follow.create(
                            repo=client.me.did,
                            record={
                                "subject": au,
                                "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            },
                        )
                except Exception:
                    pass

            time.sleep(POST_DELAY_SECONDS)

        except Exception as e:
            log(f"⚠️ Repost error: {e}")
            time.sleep(5)

    save_repost_log(REPOST_LOG_FILE, done)
    log(f"🔥 Done — {reposted} reposts")

if __name__ == "__main__":
    main()