import os
import time
import random
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify
from requests_oauthlib import OAuth1

# <-- session logger (make sure session_logger.py is in the same folder) -->
from session_logger import log_session

# =========================================
# CONFIG
# =========================================

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
SLOT_TOLERANCE_MINUTES = 10

MEDIA_UPLOAD_URL = "https://upload.twitter.com/1.1/media/upload.json"
TWEET_POST_URL_V2 = "https://api.twitter.com/2/tweets"

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reddit-x-bot")

app = Flask(__name__)


# =========================================
# TIME HELPERS
# =========================================

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


# =========================================
# REDDIT FETCH (HARDENED)
# =========================================

def reddit_fetch_json(url: str) -> Optional[Dict[str, Any]]:
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            # Rate limit / transient
            if resp.status_code in (429, 500, 502, 503, 504):
                logger.warning(
                    "Reddit HTTP %s, attempt %s/%s",
                    resp.status_code,
                    attempt,
                    RETRY_LIMIT,
                )
                time.sleep(WAIT_SECONDS * attempt)
                continue

            text = resp.text
            # HTML fallback = blocked / not JSON
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
    # top/day so 24h window matches our primary/fallback logic
    url = f"{base}/top/.json?t=day&limit={REDDIT_LIMIT}"

    data = reddit_fetch_json(url)
    if not data:
        return []

    children = data.get("data", {}).get("children", [])
    posts = [c.get("data", {}) for c in children if isinstance(c, dict)]
    logger.info("Fetched %d raw posts from %s", len(posts), base)
    return posts


# =========================================
# FILTERS
# =========================================

def clean_body(text: Optional[str]) -> str:
    text = text or ""
    # Remove EDIT sections
    text = re.sub(r"EDIT:.*", "", text, flags=re.IGNORECASE | re.DOTALL)
    # Remove markdown links [text](url)
    text = re.sub(r"\[.*?\]\(.*?\)", "", text)
    # Newlines to spaces
    text = text.replace("\n", " ")
    # Collapse multiple spaces
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def filter_posts_by_window(posts: List[Dict[str, Any]], hours: int) -> List[Dict[str, Any]]:
    cutoff_ts = now_utc().timestamp() - hours * 3600
    out: List[Dict[str, Any]] = []

    for p in posts:
        created = p.get("created_utc", 0)
        if created < cutoff_ts:
            continue

        # NSFW filter
        if p.get("over_18"):
            continue

        body = clean_body(p.get("selftext"))
        title = (p.get("title") or "").strip()

        # Require at least body or title
        if not body and not title:
            continue

        # Only enforce body length limit (title can be long; we'll trim later)
        if body and len(body) > BODY_CHAR_LIMIT:
            continue

        out.append(p)

    logger.info("Filtered to %d posts in last %d hours", len(out), hours)
    return out


# =========================================
# ENGAGEMENT SCORE
# =========================================

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


def score_post(p: Dict[str, Any]) -> float:
    score = (
        (p.get("ups") or 0) * 0.6 +
        (p.get("num_comments") or 0) * 0.4 +
        (p.get("upvote_ratio") or 0) * 8 +
        (p.get("total_awards_received") or 0) * 4
    )

    if post_has_image(p):
        score += IMAGE_SCORE_BONUS

    return float(score)


def sort_posts_by_score(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for p in posts:
        p["_engagement_score"] = score_post(p)
    posts_sorted = sorted(posts, key=lambda x: x.get("_engagement_score", 0), reverse=True)
    return posts_sorted


# =========================================
# TWEET BUILDER
# =========================================

def is_reddit_url(url: Optional[str]) -> bool:
    if not url:
        return False
    u = url.lower()
    return ("reddit.com" in u) or ("redd.it" in u)


def is_external_url(url: Optional[str]) -> bool:
    return bool(url) and not is_reddit_url(url)


def _split_hashtags(hashtags: str) -> List[str]:
    """
    Split hashtag string into tokens, preserving order.
    """
    if not hashtags:
        return []
    return [tok for tok in hashtags.split() if tok.strip()]


def _build_tweet_text(body: str, ext: str, hashtags_tokens: List[str], external: bool) -> str:
    """
    Build tweet text from components without enforcing length.
    Hashtags are always appended at the end (if present).
    For non-external, we still use random JOIN_STYLES to change spacing/newlines.
    """
    hashtags_str = " ".join(hashtags_tokens) if hashtags_tokens else ""

    if external:
        # Deterministic: body + ext + hashtags
        parts: List[str] = [body.strip()]
        if ext:
            parts.append(ext.strip())
        if hashtags_str:
            parts.append(hashtags_str.strip())
        return " ".join(p for p in parts if p).strip()

    # Non-external: random join style for body + hashtags
    if hashtags_str:
        template = random.choice(JOIN_STYLES)
        txt = template.format(body=body.strip(), hashtags=hashtags_str.strip())
        return txt.strip()

    # No hashtags at all -> just body
    return body.strip()


def build_tweet(post: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    """
    body-first, title-fallback tweet builder with:
    - external link support
    - hashtags always appended
    - hashtag-stripping loop if >280 chars
    """
    raw_body = clean_body(post.get("selftext"))
    title = (post.get("title") or "").strip()

    # 1. Body first, title if no body
    body = raw_body if raw_body else title
    if not body:
        return None  # nothing tweetable

    ext = post.get("url") or ""
    external = is_external_url(ext)
    hashtags = cfg.get("hashtags", "").strip()
    hashtag_tokens = _split_hashtags(hashtags)

    # 1) Build with all hashtags
    tweet = _build_tweet_text(body, ext, hashtag_tokens, external)

    # 2) If too long, drop hashtags one by one from the END
    tokens = list(hashtag_tokens)
    while len(tweet) > TWEET_MAX_LEN and tokens:
        tokens.pop()  # drop last hashtag
        tweet = _build_tweet_text(body, ext, tokens, external)

    # 3) If STILL too long (e.g., gigantic title), hard-trim with ellipsis.
    if len(tweet) > TWEET_MAX_LEN:
        tweet = tweet[: TWEET_MAX_LEN - 1].rstrip() + "…"

    return tweet


# =========================================
# MEDIA EXTRACTION & UPLOAD
# =========================================

def extract_image_urls(post: Dict[str, Any]) -> List[str]:
    urls: List[str] = []

    # Direct image link
    url = post.get("url") or ""
    if any(url.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp")):
        urls.append(url)

    # Preview images
    preview = post.get("preview")
    if isinstance(preview, dict):
        images = preview.get("images") or []
        for img in images:
            if not isinstance(img, dict):
                continue
            src = img.get("source", {}) or {}
            u = src.get("url")
            if u:
                # reddit escapes some chars -> normalize
                urls.append(u.replace("&amp;", "&"))

    # Galleries (media_metadata)
    media_meta = post.get("media_metadata")
    if isinstance(media_meta, dict):
        for meta in media_meta.values():
            if not isinstance(meta, dict):
                continue
            s = meta.get("s") or {}  # 's' = source
            u = s.get("u") or s.get("url")
            if u:
                urls.append(u.replace("&amp;", "&"))

    # Dedup & cap at 4
    deduped: List[str] = []
    for u in urls:
        if u not in deduped:
            deduped.append(u)
        if len(deduped) >= 4:
            break

    return deduped


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


def upload_images_to_twitter(image_urls: List[str]) -> List[str]:
    """
    Upload images via v1.1 media/upload.
    Requires OAuth1 user context.
    Returns list of media_ids.
    """
    oauth1 = get_oauth1_client()
    if not oauth1:
        logger.info("No OAuth1 keys present; posting without media.")
        return []

    media_ids: List[str] = []

    for img_url in image_urls:
        try:
            img_resp = requests.get(img_url, timeout=20)
            img_resp.raise_for_status()
            # requests.post for files expects file-tuple; keep simple: pass bytes
            resp = requests.post(MEDIA_UPLOAD_URL, files={"media": img_resp.content}, auth=oauth1, timeout=30)
            if resp.status_code >= 300:
                logger.warning("Media upload failed (%s): %s", resp.status_code, resp.text)
                continue

            data = resp.json()
            mid = data.get("media_id_string") or data.get("media_id")
            if mid:
                media_ids.append(str(mid))

        except Exception as e:
            logger.warning("Error uploading media from %s: %s", img_url, e)

    return media_ids


# =========================================
# TWITTER POSTING (HYBRID AUTH)
# =========================================

def get_twitter_auth(prefer_oauth1: bool = False) -> Tuple[str, Any]:
    """
    Returns (auth_type, auth_obj_or_headers)
    auth_type: "oauth2" or "oauth1"
    - If prefer_oauth1=True and OAuth1 keys exist, choose OAuth1 first.
    - Otherwise, if Bearer exists, use oauth2; fallback to oauth1.
    """
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


def twitter_post_tweet(text: str, media_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Hybrid posting:
    - If media_ids present -> prefer OAuth1 (user context).
      If OAuth1 missing but Bearer exists, tweet without media.
    - If no media_ids -> OAuth2 Bearer if present, else OAuth1.
    """
    if media_ids:
        # Need OAuth1 for media; if not present, drop media_ids.
        oauth1 = get_oauth1_client()
        if oauth1:
            auth_type, auth_obj = "oauth1", oauth1
            payload: Dict[str, Any] = {"text": text, "media": {"media_ids": media_ids}}
        else:
            logger.info("No OAuth1 for media; tweeting text-only via OAuth2/OAuth1 fallback.")
            auth_type, auth_obj = get_twitter_auth(prefer_oauth1=False)
            payload = {"text": text}
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


# =========================================
# CORE SLOT LOGIC
# =========================================

def pick_best_post_and_tweet_text(
    posts: List[Dict[str, Any]],
    cfg: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Sort posts by engagement and pick the first one that can build a tweet.
    Body-first, title-fallback.
    """
    sorted_posts = sort_posts_by_score(posts)
    for p in sorted_posts:
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

    # Step 3: Build tweet
    best_post, tweet_text = pick_best_post_and_tweet_text(candidates, cfg)
    if not best_post or not tweet_text:
        log_session(slot_id, "fail", "no_tweetable_post", {"subreddit": name})
        return {"subreddit": name, "status": "no_tweetable_post"}

    # Step 4: Media extraction & upload
    image_urls = extract_image_urls(best_post)
    media_ids: List[str] = []
    if image_urls:
        try:
            media_ids = upload_images_to_twitter(image_urls)
            if not media_ids and image_urls:
                # media urls were found but upload failed
                log_session(slot_id, "fail", "media_upload_partial_or_failed", {"image_urls": image_urls})
                # proceed: tweet without media (optional) — we choose to continue without media
        except Exception as e:
            log_session(slot_id, "fail", "media_upload_failed", {"error": str(e)})
            media_ids = []

    # Step 5: Post tweet
    try:
        data = twitter_post_tweet(tweet_text, media_ids=media_ids or None)
        tweet_id = data.get("id")
        tweet_url = f"https://x.com/i/web/status/{tweet_id}" if tweet_id else None

        log_session(slot_id, "success", None, {"post_id": best_post.get("id"), "tweet_url": tweet_url})
        return {
            "subreddit": name,
            "status": "tweeted",
            "tweet_url": tweet_url,
            "post_id": best_post.get("id"),
        }

    except Exception as e:
        logger.error("Failed to post tweet for %s: %s", name, e)
        log_session(slot_id, "fail", "tweet_post_failed", {"error": str(e)})
        return {"subreddit": name, "status": "error", "error": str(e)}


def find_due_subreddits(now: datetime) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Return all (name, cfg) where current IST time is within ±SLOT_TOLERANCE_MINUTES
    of the configured post_time.
    """
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
    ist = now_ist()
    logger.info("AWAKE at IST: %s", ist.isoformat())

    targets = find_due_subreddits(ist)

    if not targets:
        logger.info("No subreddit within ±%s minutes of any slot.", SLOT_TOLERANCE_MINUTES)
        # Log timing miss — useful when scheduler fired at wrong time
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


# =========================================
# FLASK ENDPOINTS
# =========================================

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
