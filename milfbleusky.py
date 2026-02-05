from atproto import Client
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Set

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

def now_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for uri in sorted(uris):
            f.write(uri + "\n")
    os.replace(tmp, path)

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

FEED_URL_RE = re.compile(r"^https?://(www\.)?bsky\.app/profile/([^/]+)/feed/([^/?#]+)", re.I)
LIST_URL_RE = re.compile(r"^https?://(www\.)?bsky\.app/profile/([^/]+)/lists/([^/?#]+)", re.I)

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
    link = link.strip()
    if link.startswith("at://") and "/app.bsky.feed.generator/" in link:
        return link
    m = FEED_URL_RE.match(link)
    if not m:
        return None
    actor = m.group(2)
    rkey = m.group(3)
    did = resolve_handle_to_did(client, actor)
    if not did:
        return None
    return f"at://{did}/app.bsky.feed.generator/{rkey}"

def normalize_list_uri(client: Client, link: str) -> Optional[str]:
    if not link:
        return None
    link = link.strip()
    if link.startswith("at://") and "/app.bsky.graph.list/" in link:
        return link
    m = LIST_URL_RE.match(link)
    if not m:
        return None
    actor = m.group(2)
    rkey = m.group(3)
    did = resolve_handle_to_did(client, actor)
    if not did:
        return None
    return f"at://{did}/app.bsky.graph.list/{rkey}"

# ================== FETCHERS ==================

def fetch_feed_items(client: Client, feed_uri: str) -> List:
    items: List = []
    cursor = None
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

def fetch_list_member_dids(client: Client, list_uri: str) -> Set[str]:
    members: Set[str] = set()
    cursor = None
    while True:
        params = {"list": list_uri, "limit": 100}
        if cursor:
            params["cursor"] = cursor
        out = client.app.bsky.graph.get_list(params)
        items = getattr(out, "items", []) or []
        for it in items:
            subj = getattr(it, "subject", None)
            did = getattr(subj, "did", None) if subj else None
            if did:
                members.add(did)
                if len(members) >= LIST_MEMBER_LIMIT:
                    return members
        cursor = getattr(out, "cursor", None)
        if not cursor:
            break
    return members

# ================== ACTIONS ==================

def do_like(client: Client, uri: str, cid: str) -> bool:
    try:
        client.app.bsky.feed.like.create(
            repo=client.me.did,
            record={
                "subject": {"uri": uri, "cid": cid},
                "createdAt": now_z(),
            },
        )
        return True
    except Exception:
        return False

def do_follow_if_needed(client: Client, actor_did: str) -> bool:
    """
    Follow alleen als we nog niet volgen.
    Return True als we gevolgd hebben, False anders.
    """
    try:
        prof = client.app.bsky.actor.get_profile({"actor": actor_did})
        viewer = getattr(prof, "viewer", None)
        if viewer and getattr(viewer, "following", None):
            return False  # al gevolgd

        client.app.bsky.graph.follow.create(
            repo=client.me.did,
            record={"subject": actor_did, "createdAt": now_z()},
        )
        return True
    except Exception:
        return False

# ================== MAIN ==================

def main():
    username = (os.getenv("BSKY_USERNAME") or "").strip()
    password = (os.getenv("BSKY_PASSWORD") or "").strip()

    if not username or not password:
        log("❌ Missing env BSKY_USERNAME / BSKY_PASSWORD")
        return

    client = Client()
    client.login(username, password)
    log("✅ Logged in")

    cutoff = datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)
    done = load_repost_log(REPOST_LOG_FILE)

    # FEEDS (max 10 via env)
    feed_uris: List[str] = []
    for i in range(1, 11):
        link = (os.getenv(f"FEED_{i}_LINK") or "").strip()
        if not link:
            continue
        uri = normalize_feed_uri(client, link)
        if uri:
            feed_uris.append(uri)

    if not feed_uris:
        log("ℹ️ Geen FEEDS ingevuld — niets te doen.")
        return

    # STOPLISTS (max 10) -> DID set
    stop_dids: Set[str] = set()
    for i in range(1, 11):
        link = (os.getenv(f"STOPLIST_{i}_LINK") or "").strip()
        if not link:
            continue
        list_uri = normalize_list_uri(client, link)
        if list_uri:
            stop_dids |= fetch_list_member_dids(client, list_uri)

    if stop_dids:
        log(f"🛑 Stoplijst leden geladen: {len(stop_dids)}")

    # Collect candidates
    candidates: List[Dict] = []
    for feed_uri in feed_uris:
        log(f"📥 Feed ophalen: {feed_uri}")
        items = fetch_feed_items(client, feed_uri)

        for item in items:
            post = item.post
            record = post.record
            uri = post.uri
            cid = post.cid

            if uri in done:
                continue

            # boosts/reposts overslaan
            if hasattr(item, "reason") and item.reason is not None:
                continue

            # replies overslaan
            if getattr(record, "reply", None):
                continue

            # quote overslaan
            if is_quote_post(record):
                continue

            # media-only
            if not has_media(record):
                continue

            created_dt = parse_time(record, post)
            if not created_dt or created_dt < cutoff:
                continue

            author_did = getattr(post.author, "did", None)
            if not author_did:
                continue

            # stoplist filter
            if author_did in stop_dids:
                continue

            candidates.append(
                {
                    "uri": uri,
                    "cid": cid,
                    "author_did": author_did,
                    "created": created_dt,
                }
            )

    candidates.sort(key=lambda x: x["created"])
    log(f"🧩 Candidates: {len(candidates)}")

    reposted = 0
    liked = 0
    followed = 0
    per_user: Dict[str, int] = {}

    for c in candidates:
        if reposted >= MAX_PER_RUN:
            break

        au = c["author_did"]
        per_user.setdefault(au, 0)
        if per_user[au] >= MAX_PER_USER:
            continue

        try:
            # Repost
            client.app.bsky.feed.repost.create(
                repo=client.me.did,
                record={
                    "subject": {"uri": c["uri"], "cid": c["cid"]},
                    "createdAt": now_z(),
                },
            )

            done.add(c["uri"])
            per_user[au] += 1
            reposted += 1

            # Like (best effort)
            if do_like(client, c["uri"], c["cid"]):
                liked += 1

            # Follow (best effort)
            if FOLLOW_ON_REPOST:
                if do_follow_if_needed(client, au):
                    followed += 1

            time.sleep(POST_DELAY_SECONDS)

        except Exception as e:
            log(f"⚠️ Repost error: {e}")
            time.sleep(5)

    save_repost_log(REPOST_LOG_FILE, done)
    log(f"🔥 Done — {reposted} reposts, {liked} likes, {followed} follows")

if __name__ == "__main__":
    main()