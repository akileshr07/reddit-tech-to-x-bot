# bot.py — rewritten version
# Put this file next to your session_logger.py
# Requires: requests, requests_oauthlib, flask
# Environment variables:
#   X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET  -> for OAuth1 (media uploads)
#   X_BEARER_TOKEN -> for OAuth2 text-only posting
#   PORT (optional)
#
# Persisted posting history: posted_history.json (created next to this script)

import os
import time
import random
import logging
import re
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify
from requests_oauthlib import OAuth1

# session logger from your repo
from session_logger import log_session

# ========== CONFIG ==========

USER_AGENT = "AkiRedditBot/1.0 (by u/WayOk4302)"

SUBREDDITS: Dict[str, Dict[str, Any]] = {
    "developersIndia": {
        "url": "https://www.reddit.com/r/developersIndia/",
        "post_time": "09:00",  # IST
        "hashtags": "#TechTwitter #Programming #Coding #WebDevelopment #DeveloperLife #100DaysOfCode #Tech",
    },
    "ProgrammerHumor": {
        "url": "https://www.reddit.com/r/ProgrammerHumor/",
        "post_time": "18:00",  # IST
        "hashtags": "#Funny #Humor #FunnyTweets #Memes #DankMemes #Comedy #LOL",
    },
    "technology": {
        "url": "https://www.reddit.com/r/technology/",
        "post_time": "06:00",  # IST
        "hashtags": "#TechNews #TechnologyNews #AI #Innovation #Gadgets #Cybersecurity #TechTrends #NewTech",
    },
    "oddlysatisfying": {
        "url": "https://www.reddit.com/r/oddlysatisfying/",
        "post_time": "12:00",  # IST
        "hashtags": "#OddlySatisfying #ASMR #Satisfying #Relaxing #SatisfyingVideos #Relaxation #ASMRCommunity",
    },
    "IndiaCricket": {
        "url": "https://www.reddit.com/r/IndiaCricket/hot/",
        "post_time": "15:00",  # IST
        "hashtags": "#Cricket #IPL #WorldCup #Sports #CricketLovers #TeamIndia #CricketFever",
    },
}

REDDIT_LIMIT = 80
RETRY_LIMIT = 3
WAIT_SECONDS = 2

PRIMARY_WINDOW_HOURS = 10
FALLBACK_WINDOW_HOURS = 24

BODY_CHAR_LIMIT = 220
TWEET_MAX_LEN = 280

IMAGE_SCORE_BONUS = 10

JOIN_STYLES = [
    "{body}\n\n{hashtags}",
    "{body} {hashtags}",
    "{body}\n{hashtags}",
]

# How far from slot time we still accept a post (in minutes)
SLOT_TOLERANCE_MINUTES = 20

MEDIA_UPLOAD_URL = "https://upload.twitter.com/1.1/media/upload.json"
TWEET_POST_URL_V2 = "https://api.twitter.com/2/tweets"

# posting history file (to avoid reposts)
HISTORY_PATH = Path(__file__).parent / "posted_history.json"
POST_HISTORY_TTL_HOURS = 48  # don't repost the same reddit post within 48 hours

# media chunk size for Twitter chunked upload (5MB recommended)
CHUNK_SIZE = 5 * 1024 * 1024

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reddit-x-bot")

app = Flask(__name__)

# ========== TIME HELPERS ==========


def now_utc() -> datetime:
    return datetime.utcnow()


def now_ist() -> datetime:
    # UTC + 5:30
    return now_utc() + timedelta(hours=5, minutes=30)


def hhmm_ist(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = now_ist()
    return dt.strftime("%H:%M")


def minutes_since_midnight(dt: datetime) -> int:
    return dt.hour * 60 + dt.minute


def parse_hhmm_to_minutes(hhmm: str) -> int:
    h_str, m_str = hhmm.split(":")
    return int(h_str) * 60 + int(m_str)


# ========== SMALL UTIL ==========


def read_history() -> Dict[str, Any]:
    if not HISTORY_PATH.exists():
        return {}
    try:
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def write_history(data: Dict[str, Any]) -> None:
    tmp = HISTORY_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    tmp.replace(HISTORY_PATH)


def clean_history() -> None:
    """Remove old entries beyond TTL to keep file small."""
    data = read_history()
    cutoff = time.time() - POST_HISTORY_TTL_HOURS * 3600
    changed = False
    for sub, entries in list(data.items()):
        new_entries = [e for e in entries if e.get("ts", 0) >= cutoff]
        if len(new_entries) != len(entries):
            data[sub] = new_entries
            changed = True
    if changed:
        write_history(data)


def record_posted(subreddit: str, reddit_id: str, tweet_url: Optional[str]) -> None:
    data = read_history()
    arr = data.get(subreddit) or []
    arr.append({"id": reddit_id, "ts": int(time.time()), "tweet_url": tweet_url})
    data[subreddit] = arr
    write_history(data)


def was_posted_recently(subreddit: str, reddit_id: str) -> bool:
    data = read_history()
    arr = data.get(subreddit) or []
    cutoff = time.time() - POST_HISTORY_TTL_HOURS * 3600
    for e in arr:
        if e.get("id") == reddit_id and e.get("ts", 0) >= cutoff:
            return True
    return False


# ========== REDDIT FETCH (HARDENED) ==========


def reddit_fetch_json(url: str) -> Optional[Dict[str, Any]]:
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code in (429, 500, 502, 503, 504):
                logger.warning("Reddit HTTP %s, attempt %s/%s", resp.status_code, attempt, RETRY_LIMIT)
                time.sleep(WAIT_SECONDS * attempt)
                continue

            text = resp.text
            if text.lstrip().startswith("<"):
                logger.warning("Reddit returned HTML, attempt %s/%s", attempt, RETRY_LIMIT)
                time.sleep(WAIT_SECONDS * attempt)
                continue

            return resp.json()

        except Exception as e:
            logger.warning("Reddit fetch error attempt %s/%s: %s", attempt, RETRY_LIMIT, e)
            time.sleep(WAIT_SECONDS * attempt)

    logger.error("Reddit fetch failed after %s attempts", RETRY_LIMIT)
    return None


def fetch_subreddit_posts(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    base = cfg["url"].rstrip("/")
    url = f"{base}/top/.json?t=day&limit={REDDIT_LIMIT}"

    data = reddit_fetch_json(url)
    if not data:
        return []

    children = data.get("data", {}).get("children", [])
    posts = [c.get("data", {}) for c in children if isinstance(c, dict)]
    logger.info("Fetched %d raw posts from %s", len(posts), base)
    return posts


# ========== FILTERS ==========


def clean_body(text: Optional[str]) -> str:
    text = text or ""
    text = re.sub(r"EDIT:.*", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"\[.*?\]\(.*?\)", "", text)
    text = text.replace("\n", " ")
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def filter_posts_by_window(posts: List[Dict[str, Any]], hours: int) -> List[Dict[str, Any]]:
    cutoff_ts = now_utc().timestamp() - hours * 3600
    out: List[Dict[str, Any]] = []

    for p in posts:
        created = p.get("created_utc", 0)
        if created < cutoff_ts:
            continue

        if p.get("over_18"):
            continue

        body = clean_body(p.get("selftext"))
        title = (p.get("title") or "").strip()

        if not body and not title:
            continue

        if body and len(body) > BODY_CHAR_LIMIT:
            continue

        out.append(p)

    logger.info("Filtered to %d posts in last %d hours", len(out), hours)
    return out


# ========== ENGAGEMENT SCORE ==========


def post_has_image(post: Dict[str, Any]) -> bool:
    url = (post.get("url") or "").lower()
    if any(url.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp")):
        return True

    preview = post.get("preview")
    if isinstance(preview, dict) and preview.get("images"):
        return True

    media_meta = post.get("media_metadata")
    if isinstance(media_meta, dict) and media_meta:
        return True

    return False


def post_has_reddit_video(post: Dict[str, Any]) -> bool:
    # check secure_media.reddit_video or preview variants
    if post.get("secure_media") and isinstance(post["secure_media"], dict):
        if post["secure_media"].get("reddit_video"):
            return True
    # sometimes in preview->reddit_video_preview
    preview = post.get("preview")
    if isinstance(preview, dict) and preview.get("reddit_video_preview"):
        return True
    # url
    url = (post.get("url") or "").lower()
    if "v.redd.it" in url or url.endswith(".mp4"):
        return True
    return False


def score_post(p: Dict[str, Any]) -> float:
    score = (
        (p.get("ups") or 0) * 0.6 +
        (p.get("num_comments") or 0) * 0.4 +
        (p.get("upvote_ratio") or 0) * 8 +
        (p.get("total_awards_received") or 0) * 4
    )

    if post_has_image(p):
        score += IMAGE_SCORE_BONUS

    # small bonus for video posts
    if post_has_reddit_video(p):
        score += IMAGE_SCORE_BONUS / 2

    return float(score)


def sort_posts_by_score(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for p in posts:
        p["_engagement_score"] = score_post(p)
    posts_sorted = sorted(posts, key=lambda x: x.get("_engagement_score", 0), reverse=True)
    return posts_sorted


# ========== TWEET BUILDER ==========


def is_reddit_url(url: Optional[str]) -> bool:
    if not url:
        return False
    u = url.lower()
    return ("reddit.com" in u) or ("redd.it" in u)


def is_external_url(url: Optional[str]) -> bool:
    return bool(url) and not is_reddit_url(url)


def _split_hashtags(hashtags: str) -> List[str]:
    if not hashtags:
        return []
    return [tok for tok in hashtags.split() if tok.strip()]


def _build_tweet_text(body: str, ext: str, hashtags_tokens: List[str], external: bool) -> str:
    hashtags_str = " ".join(hashtags_tokens) if hashtags_tokens else ""

    if external:
        parts: List[str] = [body.strip()]
        if ext:
            parts.append(ext.strip())
        if hashtags_str:
            parts.append(hashtags_str.strip())
        return " ".join(p for p in parts if p).strip()

    if hashtags_str:
        template = random.choice(JOIN_STYLES)
        txt = template.format(body=body.strip(), hashtags=hashtags_str.strip())
        return txt.strip()

    return body.strip()


def build_tweet(post: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    raw_body = clean_body(post.get("selftext"))
    title = (post.get("title") or "").strip()

    body = raw_body if raw_body else title
    if not body:
        return None

    ext = post.get("url") or ""
    external = is_external_url(ext)
    hashtags = cfg.get("hashtags", "").strip()
    hashtag_tokens = _split_hashtags(hashtags)

    tweet = _build_tweet_text(body, ext, hashtag_tokens, external)

    tokens = list(hashtag_tokens)
    while len(tweet) > TWEET_MAX_LEN and tokens:
        tokens.pop()
        tweet = _build_tweet_text(body, ext, tokens, external)

    if len(tweet) > TWEET_MAX_LEN:
        tweet = tweet[: TWEET_MAX_LEN - 1].rstrip() + "…"

    return tweet


# ========== MEDIA EXTRACTION ==========


def extract_image_urls(post: Dict[str, Any]) -> List[str]:
    urls: List[str] = []

    url = post.get("url") or ""
    if any(url.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp")):
        urls.append(url)

    preview = post.get("preview")
    if isinstance(preview, dict):
        images = preview.get("images") or []
        for img in images:
            if not isinstance(img, dict):
                continue
            src = img.get("source", {}) or {}
            u = src.get("url")
            if u:
                urls.append(u.replace("&amp;", "&"))

    media_meta = post.get("media_metadata")
    if isinstance(media_meta, dict):
        for meta in media_meta.values():
            if not isinstance(meta, dict):
                continue
            s = meta.get("s") or {}
            u = s.get("u") or s.get("url")
            if u:
                urls.append(u.replace("&amp;", "&"))

    deduped: List[str] = []
    for u in urls:
        if u not in deduped:
            deduped.append(u)
        if len(deduped) >= 4:
            break
    return deduped


def extract_reddit_video_url(post: Dict[str, Any]) -> Optional[str]:
    # Common spots: secure_media.reddit_video.fallback_url, preview.reddit_video_preview.fallback_url, url if endswith .mp4
    if post.get("secure_media") and isinstance(post["secure_media"], dict):
        rv = post["secure_media"].get("reddit_video")
        if rv and rv.get("fallback_url"):
            return rv["fallback_url"].replace("&amp;", "&")

    preview = post.get("preview")
    if isinstance(preview, dict):
        rv = preview.get("reddit_video_preview")
        if rv and rv.get("fallback_url"):
            return rv["fallback_url"].replace("&amp;", "&")

    url = post.get("url") or ""
    if url and url.endswith(".mp4"):
        return url

    # sometimes media metadata has variants
    media_meta = post.get("media_metadata") or {}
    for meta in media_meta.values():
        if not isinstance(meta, dict):
            continue
        # sometimes 'p' list contains mp4 variants (rare)
        for key in ("s", "p"):
            s = meta.get(key) or {}
            u = s.get("u") or s.get("url")
            if u and u.endswith(".mp4"):
                return u.replace("&amp;", "&")

    return None


# ========== TWITTER AUTH HELPERS ==========


def get_oauth1_client() -> Optional[OAuth1]:
    api_key = os.getenv("X_API_KEY")
    api_secret = os.getenv("X_API_SECRET")
    access_token = os.getenv("X_ACCESS_TOKEN")
    access_secret = os.getenv("X_ACCESS_SECRET")

    if not all([api_key, api_secret, access_token, access_secret]):
        return None

    return OAuth1(
        api_key,
        api_secret,
        access_token,
        access_secret,
        signature_type="auth_header",
    )


def get_twitter_auth(prefer_oauth1: bool = False) -> Tuple[str, Any]:
    oauth1 = get_oauth1_client()
    bearer = os.getenv("X_BEARER_TOKEN")

    if prefer_oauth1 and oauth1:
        return "oauth1", oauth1

    if bearer:
        headers = {
            "Authorization": f"Bearer {bearer}",
            "Content-Type": "application/json",
        }
        return "oauth2", headers

    if oauth1:
        return "oauth1", oauth1

    raise RuntimeError("No valid Twitter credentials: set X_BEARER_TOKEN or OAuth1 keys.")


# ========== TWITTER MEDIA UPLOAD (supports images + chunked video upload) ==========


def _twitter_media_init_total_bytes(oauth1: OAuth1, total_bytes: int, media_type: str, media_category: str) -> Optional[str]:
    data = {
        "command": "INIT",
        "total_bytes": str(total_bytes),
        "media_type": media_type,
        "media_category": media_category,
    }
    resp = requests.post(MEDIA_UPLOAD_URL, data=data, auth=oauth1, timeout=30)
    if resp.status_code >= 300:
        logger.warning("INIT failed %s: %s", resp.status_code, resp.text)
        return None
    return resp.json().get("media_id_string") or resp.json().get("media_id")


def _twitter_media_append(oauth1: OAuth1, media_id: str, segment_index: int, chunk: bytes) -> bool:
    files = {"media": chunk}
    data = {"command": "APPEND", "media_id": str(media_id), "segment_index": str(segment_index)}
    # requests wants files mapping; using data+files is correct
    resp = requests.post(MEDIA_UPLOAD_URL, data=data, files=files, auth=oauth1, timeout=60)
    if resp.status_code >= 300:
        logger.warning("APPEND failed %s: %s", resp.status_code, resp.text)
        return False
    return True


def _twitter_media_finalize(oauth1: OAuth1, media_id: str) -> bool:
    data = {"command": "FINALIZE", "media_id": str(media_id)}
    resp = requests.post(MEDIA_UPLOAD_URL, data=data, auth=oauth1, timeout=60)
    if resp.status_code >= 300:
        logger.warning("FINALIZE failed %s: %s", resp.status_code, resp.text)
        return False
    return True


def upload_media_to_twitter(media_urls: List[str]) -> List[str]:
    """
    Uploads given image/video urls to Twitter (v1.1 media/upload).
    - If OAuth1 is missing -> returns [] (no media).
    - Detects videos and performs chunked upload (INIT/APPEND/FINAL).
    Returns list of media_id strings.
    """
    oauth1 = get_oauth1_client()
    if not oauth1:
        logger.info("No OAuth1 keys present; posting without media.")
        return []

    media_ids: List[str] = []
    for url in media_urls:
        try:
            logger.info("Downloading media %s", url)
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30, stream=True)
            r.raise_for_status()
            content_type = r.headers.get("Content-Type", "") or ""
            # read all bytes (we need length)
            content = r.content
            total_bytes = len(content)

            # Decide media_type and category
            if "video" in content_type or url.lower().endswith(".mp4") or total_bytes > 200 * 1024:
                # consider as video
                media_type = content_type or "video/mp4"
                media_category = "tweet_video"
                logger.info("Uploading as video, size=%s bytes", total_bytes)

                # INIT
                mid = _twitter_media_init_total_bytes(oauth1, total_bytes, media_type, media_category)
                if not mid:
                    logger.warning("INIT failed for %s, skipping media", url)
                    continue

                # APPEND in chunks
                segment = 0
                offset = 0
                while offset < total_bytes:
                    chunk = content[offset: offset + CHUNK_SIZE]
                    ok = _twitter_media_append(oauth1, mid, segment, chunk)
                    if not ok:
                        logger.warning("Append failed for segment %s of %s", segment, url)
                        break
                    segment += 1
                    offset += len(chunk)
                    # small sleep to be polite
                    time.sleep(0.2)
                else:
                    # FINALIZE
                    okf = _twitter_media_finalize(oauth1, mid)
                    if okf:
                        media_ids.append(str(mid))
                    else:
                        logger.warning("Finalize failed for %s", url)
                continue

            # else treat as image (small)
            # requests.post with files for small images is fine
            logger.info("Uploading image, size=%s bytes", total_bytes)
            resp = requests.post(MEDIA_UPLOAD_URL, files={"media": content}, auth=oauth1, timeout=30)
            if resp.status_code >= 300:
                logger.warning("Image upload failed (%s): %s", resp.status_code, resp.text)
                continue
            data = resp.json()
            mid = data.get("media_id_string") or data.get("media_id")
            if mid:
                media_ids.append(str(mid))

        except Exception as e:
            logger.warning("Error uploading media from %s: %s", url, e)

    return media_ids


# ========== TWITTER TWEET POSTING ==========


def twitter_post_tweet(text: str, media_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Hybrid posting:
    - If media_ids present -> prefer OAuth1 (user context).
      If OAuth1 missing but Bearer exists, tweet without media.
    - If no media_ids -> OAuth2 Bearer if present, else OAuth1.
    """
    if media_ids:
        oauth1 = get_oauth1_client()
        if oauth1:
            payload: Dict[str, Any] = {"text": text, "media": {"media_ids": media_ids}}
            resp = requests.post(TWEET_POST_URL_V2, json=payload, auth=oauth1, timeout=20)
        else:
            logger.info("No OAuth1 for media; tweeting text-only via OAuth2 fallback.")
            auth_type, auth_obj = get_twitter_auth(prefer_oauth1=False)
            headers = auth_obj if isinstance(auth_obj, dict) else {}
            payload = {"text": text}
            resp = requests.post(TWEET_POST_URL_V2, json=payload, headers=headers, timeout=20)
    else:
        auth_type, auth_obj = get_twitter_auth(prefer_oauth1=False)
        payload = {"text": text}
        if auth_type == "oauth2":
            headers = auth_obj  # type: ignore
            resp = requests.post(TWEET_POST_URL_V2, json=payload, headers=headers, timeout=20)
        else:
            resp = requests.post(TWEET_POST_URL_V2, json=payload, auth=auth_obj, timeout=20)

    if resp.status_code >= 300:
        raise RuntimeError(f"Twitter error {resp.status_code}: {resp.text}")

    return resp.json().get("data", {})


# ========== CORE LOGIC ==========


def pick_best_post_and_tweet_text(posts: List[Dict[str, Any]], cfg: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    sorted_posts = sort_posts_by_score(posts)
    for p in sorted_posts:
        # skip if posted recently
        subreddit_key = cfg.get("url", "").split("/")[-2] if "/" in cfg.get("url", "") else "unknown"
        reddit_id = p.get("id")
        if reddit_id and was_posted_recently(subreddit_key, reddit_id):
            logger.info("Skipping reddit id %s because posted recently", reddit_id)
            continue
        tweet = build_tweet(p, cfg)
        if tweet:
            return p, tweet
    return None, None


def handle_slot_for_subreddit(name: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    integrated logging: logs failures at each meaningful step with reason codes
    and success on completion.
    """
    slot_id = f"slot_{name}"

    # Step 1: Fetch posts
    posts = fetch_subreddit_posts(cfg)
    if not posts:
        log_session(slot_id, "fail", "reddit_fetch_empty", {"subreddit": name})
        return {"subreddit": name, "status": "reddit_fetch_empty"}

    # Step 2: Filter windows (primary -> fallback)
    primary = filter_posts_by_window(posts, PRIMARY_WINDOW_HOURS)
    candidates = primary
    if not candidates:
        fallback = filter_posts_by_window(posts, FALLBACK_WINDOW_HOURS)
        candidates = fallback

    if not candidates:
        log_session(slot_id, "fail", "no_candidates", {"subreddit": name})
        return {"subreddit": name, "status": "no_candidates"}

    # Step 3: Build tweet and pick post
    best_post, tweet_text = pick_best_post_and_tweet_text(candidates, cfg)
    if not best_post or not tweet_text:
        log_session(slot_id, "fail", "no_tweetable_post", {"subreddit": name})
        return {"subreddit": name, "status": "no_tweetable_post"}

    reddit_id = best_post.get("id")
    subreddit_key = cfg.get("url", "").split("/")[-2] if "/" in cfg.get("url", "") else name

    # double-check posted history one more time (race-safety)
    if reddit_id and was_posted_recently(subreddit_key, reddit_id):
        log_session(slot_id, "fail", "already_posted_recently", {"subreddit": name, "post_id": reddit_id})
        return {"subreddit": name, "status": "already_posted_recently", "post_id": reddit_id}

    # Step 4: Media extraction & upload (images + videos)
    media_urls: List[str] = []
    # video first (prefer uploading the reddit video)
    rv = extract_reddit_video_url(best_post)
    if rv:
        media_urls.append(rv)
    else:
        # fallback to images if any
        imgs = extract_image_urls(best_post)
        media_urls.extend(imgs)

    media_ids: List[str] = []
    if media_urls:
        try:
            media_ids = upload_media_to_twitter(media_urls)
            if not media_ids and media_urls:
                # media urls were found but upload failed
                log_session(slot_id, "fail", "media_upload_partial_or_failed", {"image_urls": media_urls})
                # proceed: tweet without media (we choose to continue without media)
        except Exception as e:
            log_session(slot_id, "fail", "media_upload_failed", {"error": str(e)})
            media_ids = []

    # Step 5: Post tweet
    try:
        data = twitter_post_tweet(tweet_text, media_ids=media_ids or None)
        tweet_id = data.get("id")
        tweet_url = f"https://x.com/i/web/status/{tweet_id}" if tweet_id else None

        # record posted to history to prevent duplicate reposts
        record_posted(subreddit_key, reddit_id, tweet_url)

        log_session(slot_id, "success", None, {"post_id": reddit_id, "tweet_url": tweet_url})
        return {
            "subreddit": name,
            "status": "tweeted",
            "tweet_url": tweet_url,
            "post_id": reddit_id,
        }

    except Exception as e:
        logger.error("Failed to post tweet for %s: %s", name, e)
        log_session(slot_id, "fail", "tweet_post_failed", {"error": str(e)})
        return {"subreddit": name, "status": "error", "error": str(e)}


def find_due_subreddits(now: datetime) -> List[Tuple[str, Dict[str, Any]]]:
    now_min = minutes_since_midnight(now)
    slot_now = hhmm_ist(now)
    logger.info("Current IST time %s (%s minutes)", slot_now, now_min)

    targets: List[Tuple[str, Dict[str, Any]]] = []

    for name, cfg in SUBREDDITS.items():
        slot_str = cfg["post_time"]
        slot_min = parse_hhmm_to_minutes(slot_str)
        delta = abs(now_min - slot_min)
        logger.info("Subreddit %s slot %s (%s minutes), delta=%s", name, slot_str, slot_min, delta)

        if delta <= SLOT_TOLERANCE_MINUTES:
            targets.append((name, cfg))

    return targets


def handle_awake() -> Dict[str, Any]:
    # cleanup old history entries occasionally
    try:
        clean_history()
    except Exception:
        pass

    ist = now_ist()
    logger.info("AWAKE at IST: %s", ist.isoformat())

    targets = find_due_subreddits(ist)

    if not targets:
        logger.info("No subreddit within ±%s minutes of any slot.", SLOT_TOLERANCE_MINUTES)
        log_session("slot_timing", "fail", "no_slot_due", {"time": ist.isoformat()})
        return {
            "status": "no_slot",
            "time_ist": ist.isoformat(),
            "tolerance_minutes": SLOT_TOLERANCE_MINUTES,
        }

    results: List[Dict[str, Any]] = []
    for name, cfg in targets:
        res = handle_slot_for_subreddit(name, cfg)
        results.append(res)

    return {
        "status": "done",
        "time_ist": ist.isoformat(),
        "tolerance_minutes": SLOT_TOLERANCE_MINUTES,
        "results": results,
    }


# ========== FLASK ENDPOINTS ==========


@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "running", "service": "reddit→x bot"})


@app.route("/awake", methods=["GET", "POST"])
def awake():
    result = handle_awake()
    return jsonify(result)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
