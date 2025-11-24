 #!/usr/bin/env python3
import os
import time
import random
import logging
import re
import json
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
        "url": "https://www.reddit.com/r/IndiaTech/",
        "post_time": "12:00",  # IST
        "hashtags": "#TechTwitter #Programming #Coding #WebDevelopment",
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

# Sliding windows in hours
PRIMARY_WINDOW_HOURS = 10
FALLBACK_WINDOW_HOURS = 24
MAX_WINDOW_HOURS = 48
WINDOW_SEQUENCE_HOURS = [PRIMARY_WINDOW_HOURS, FALLBACK_WINDOW_HOURS, MAX_WINDOW_HOURS]

BODY_CHAR_LIMIT = 220
TWEET_MAX_LEN = 280

# Kept for possible future tuning (not used directly in priority groups now)
IMAGE_SCORE_BONUS = 10
SINGLE_IMAGE_PRIORITY_BONUS = 25

# How far from slot time we still accept a post (in minutes)
SLOT_TOLERANCE_MINUTES = 20

# Avoid reposting same reddit post within this cooldown (hours)
POST_COOLDOWN_HOURS = 24

MEDIA_UPLOAD_URL = "https://upload.twitter.com/1.1/media/upload.json"
TWEET_POST_URL_V2 = "https://api.twitter.com/2/tweets"

# Emergency: retweet recent tweet from @HumansNoContext
HUMANS_NO_CONTEXT_HANDLE = "HumansNoContext"
TWITTER_USER_TIMELINE_URL_V1 = "https://api.twitter.com/1.1/statuses/user_timeline.json"
TWITTER_RETWEET_URL_V1 = "https://api.twitter.com/1.1/statuses/retweet/{tweet_id}.json"

# Local persisted posted-history file (prevents repeated posting across restarts)
POST_HISTORY_FILE = "post_history.json"

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reddit-x-bot")

app = Flask(__name__)


# =========================================
# PERSISTENT POST HISTORY (simple JSON)
# =========================================

def load_post_history() -> Dict[str, Any]:
    try:
        if os.path.exists(POST_HISTORY_FILE):
            with open(POST_HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.warning("Failed to load post history: %s", e)
    # Structure:
    # { "posted_ids": { "<reddit_id>": "<iso timestamp>" }, "subreddit_last": { "developersIndia": "t3_abc..." } }
    return {"posted_ids": {}, "subreddit_last": {}}


def save_post_history(history: Dict[str, Any]) -> None:
    try:
        with open(POST_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f)
    except Exception as e:
        logger.warning("Failed to save post history: %s", e)


POST_HISTORY = load_post_history()


def mark_post_as_posted(reddit_id: str, subreddit_name: str) -> None:
    now_iso = datetime.utcnow().isoformat()
    POST_HISTORY.setdefault("posted_ids", {})[reddit_id] = now_iso
    POST_HISTORY.setdefault("subreddit_last", {})[subreddit_name] = reddit_id
    save_post_history(POST_HISTORY)


def was_post_recently_posted(reddit_id: str) -> bool:
    posted = POST_HISTORY.get("posted_ids", {})
    ts = posted.get(reddit_id)
    if not ts:
        return False
    try:
        dt = datetime.fromisoformat(ts)
        if datetime.utcnow() - dt < timedelta(hours=POST_COOLDOWN_HOURS):
            return True
    except Exception:
        return False
    return False


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
    # Use /new so we can apply real sliding time windows (10h/24h/48h)
    url = f"{base}/new/.json?limit={REDDIT_LIMIT}"

    data = reddit_fetch_json(url)
    if not data:
        return []

    children = data.get("data", {}).get("children", [])
    posts = [c.get("data", {}) for c in children if isinstance(c, dict)]
    logger.info("Fetched %d raw posts from %s", len(posts), base)
    return posts


# =========================================
# POST TYPE CHECKS AND FILTERS
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


def post_is_video(post: Dict[str, Any]) -> bool:
    if post.get("is_video"):
        return True

    secure_media = post.get("secure_media") or post.get("media")
    if isinstance(secure_media, dict):
        if secure_media.get("reddit_video") or str(secure_media.get("type", "")).startswith("video"):
            return True

    preview = post.get("preview")
    if isinstance(preview, dict):
        images = preview.get("images") or []
        for img in images:
            if not isinstance(img, dict):
                continue
            variants = img.get("variants", {})
            if "mp4" in variants or "gif" in variants:
                return True

    url = (post.get("url") or "").lower()
    if any(url.endswith(ext) for ext in (".mp4", ".mov", ".webm")):
        return True

    return False


def is_external_video_url(url: Optional[str]) -> bool:
    if not url:
        return False
    u = url.lower()
    video_domains = [
        "youtube.com",
        "youtu.be",
        "tiktok.com",
        "vimeo.com",
        "dailymotion.com",
        "streamable.com",
        "facebook.com",
        "fb.watch",
        "instagram.com",
        "x.com",
        "twitter.com",
    ]
    return any(d in u for d in video_domains)


def post_is_gif_or_animated(post: Dict[str, Any]) -> bool:
    url = (post.get("url") or "").lower()
    if any(url.endswith(ext) for ext in (".gif", ".gifv", ".webp")):
        return True

    preview = post.get("preview")
    if isinstance(preview, dict):
        images = preview.get("images") or []
        for img in images:
            if not isinstance(img, dict):
                continue
            variants = img.get("variants", {})
            if "gif" in variants or "mp4" in variants:
                return True

    return False


def post_has_image(post: Dict[str, Any]) -> bool:
    url = (post.get("url") or "").lower()
    if any(url.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp")):
        return True

    preview = post.get("preview")
    if isinstance(preview, dict) and preview.get("images"):
        return True

    media_meta = post.get("media_metadata")
    if isinstance(media_meta, dict) and media_meta:
        return True

    return False


def is_gallery_post(post: Dict[str, Any]) -> bool:
    if post.get("is_gallery"):
        return True
    if isinstance(post.get("gallery_data"), dict):
        return True
    if isinstance(post.get("media_metadata"), dict) and post.get("media_metadata"):
        return True
    return False


def is_text_post(post: Dict[str, Any]) -> bool:
    return bool(post.get("is_self"))


def is_link_post(post: Dict[str, Any]) -> bool:
    if post.get("is_self"):
        return False
    post_hint = str(post.get("post_hint") or "").lower()
    return post_hint in ("link", "rich:video")


def is_text_or_link_post(post: Dict[str, Any]) -> bool:
    return is_text_post(post) or is_link_post(post)


def post_is_crosspost(post: Dict[str, Any]) -> bool:
    if post.get("crosspost_parent"):
        return True
    cpl = post.get("crosspost_parent_list")
    if isinstance(cpl, list) and cpl:
        return True
    return False


def post_is_poll(post: Dict[str, Any]) -> bool:
    if post.get("poll_data"):
        return True
    if isinstance(post.get("polls"), list) and post.get("polls"):
        return True
    return False


def post_is_deleted_or_removed(post: Dict[str, Any]) -> bool:
    title = (post.get("title") or "").strip().lower()
    body = (post.get("selftext") or "").strip().lower()
    if title in ("[deleted]", "[removed]"):
        return True
    if body in ("[deleted]", "[removed]"):
        return True
    if post.get("removed_by_category"):
        return True
    return False


def post_is_promoted_or_sponsored(post: Dict[str, Any]) -> bool:
    if post.get("is_created_from_ads_ui"):
        return True
    if post.get("promoted"):
        return True
    if post.get("is_ad"):
        return True
    if post.get("adserver_click_url"):
        return True
    return False


def passes_hard_filters(post: Dict[str, Any]) -> bool:
    """
    STEP 2 — Hard Filters (Auto-Reject — No Scoring)
    Reject if:
    - Video / external video / GIF/animated
    - Poll
    - Crosspost
    - Spoiler / NSFW
    - Stickied / distinguished
    - Deleted / removed
    - Promoted / sponsored
    - Contest-mode
    """
    url = post.get("url") or ""

    # Post type restrictions
    if post_is_video(post):
        return False
    if is_external_video_url(url):
        return False
    if post_is_gif_or_animated(post):
        return False
    if post_is_poll(post):
        return False
    if post_is_crosspost(post):
        return False

    # Spoiler / NSFW
    if post.get("spoiler"):
        return False
    if post.get("over_18"):
        return False

    # Stickied / distinguished
    if post.get("stickied"):
        return False
    if post.get("distinguished") not in (None, "", "null"):
        return False

    # Deleted / removed
    if post_is_deleted_or_removed(post):
        return False

    # Promoted / sponsored
    if post_is_promoted_or_sponsored(post):
        return False

    # Contest-mode
    if post.get("contest_mode"):
        return False

    return True


# =========================================
# ENGAGEMENT SCORE & PRIORITY
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


def compute_priority_group(post: Dict[str, Any], body: str) -> int:
    """
    STEP 4 — Prioritization Rules (Before Scoring)

    TOP PRIORITY (3):
      - No body text AND is Gallery or Image

    SECOND PRIORITY (2):
      - Other Gallery and Image posts

    LOWEST PRIORITY (1):
      - Text or Link posts with selftext ≤ 200 chars
    """
    gallery = is_gallery_post(post)
    image = post_has_image(post)
    has_body = bool(body)

    if (gallery or image) and not has_body:
        return 3  # top priority

    if gallery or image:
        return 2  # second priority

    # Remaining candidates are short text/link posts (≤ 200 chars enforced earlier)
    return 1  # lowest priority


def compute_engagement_score(post: Dict[str, Any]) -> float:
    """
    STEP 5 — Engagement Score Calculation

    score = (upvotes * 0.65)
          + (comments * 0.35)
          + (upvote_ratio * 10)
    """
    ups = post.get("ups") or 0
    comments = post.get("num_comments") or 0
    upr = post.get("upvote_ratio") or 0.0

    score = (ups * 0.65) + (comments * 0.35) + (upr * 10.0)
    return float(score)


# =========================================
# TWEET BUILDER (NEW RULES)
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


def build_tweet(post: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    """
    TWEET TEXT RULES:

    RULE 1 — Determine Base Tweet Text
      CASE A — Only title (no body):           use title
      CASE B — Title + body:                   use body
      CASE C — Title + body + images/gallery:  use title  (body ignored)

    RULE 2 — Append Subreddit-Specific Hashtags
      {base_text} {#tag1 #tag2 ...}   (no external URL)
      OR, if external URL:
      {base_text}
      {external_url}
      {hashtags}

    RULE 3 — Enforce 280 characters:
      1) Build full tweet
      2) If > 280, drop hashtags from END one-by-one
      3) If still > 280 with 0 hashtags, trim base_text by single characters
         from the end (no word-based trimming).
    """
    raw_body = clean_body(post.get("selftext"))
    title = (post.get("title") or "").strip()

    has_title = bool(title)
    has_body = bool(raw_body)
    has_images = is_gallery_post(post) or post_has_image(post)

    # RULE 1 — Base text selection
    if has_title and not has_body:
        # CASE A
        base_text = title
    elif has_title and has_body and has_images:
        # CASE C
        base_text = title
    elif has_title and has_body:
        # CASE B
        base_text = raw_body
    elif has_body:
        base_text = raw_body
    elif has_title:
        base_text = title
    else:
        return None  # nothing tweetable

    base_text = base_text.strip()
    if not base_text:
        return None

    # External URL logic: If URL is external → include it; If URL is Reddit → ignore it.
    url = post.get("url") or ""
    external_url = url if is_external_url(url) else ""

    # RULE 2 — Append subreddit-specific hashtags
    hashtags = cfg.get("hashtags", "").strip()
    hashtag_tokens = _split_hashtags(hashtags)

    def assemble_tweet(current_base: str, tokens: List[str]) -> str:
        hashtags_str = " ".join(tokens) if tokens else ""
        current_base = current_base.strip()

        if external_url:
            if hashtags_str:
                txt = f"{current_base}\n{external_url}\n{hashtags_str}"
            else:
                txt = f"{current_base}\n{external_url}"
        else:
            if hashtags_str:
                txt = f"{current_base} {hashtags_str}"
            else:
                txt = current_base

        return txt.strip()

    # Start with full hashtags
    tweet = assemble_tweet(base_text, hashtag_tokens)

    # If already within limit, done
    if len(tweet) <= TWEET_MAX_LEN:
        return tweet

    # RULE 3 — Trim hashtags from the END
    tokens = list(hashtag_tokens)
    while len(tweet) > TWEET_MAX_LEN and tokens:
        tokens.pop()  # drop last hashtag
        tweet = assemble_tweet(base_text, tokens)

    if len(tweet) <= TWEET_MAX_LEN:
        return tweet

    # No hashtags left; now trim base_text character-by-character
    tokens = []  # no hashtags
    while len(tweet) > TWEET_MAX_LEN and base_text:
        base_text = base_text[:-1]  # trim one character
        tweet = assemble_tweet(base_text, tokens)

    # Extremely defensive: if base_text empty and still over limit (e.g., absurdly long URL),
    # truncate the final tweet string safely by characters (still character-level trimming).
    if len(tweet) > TWEET_MAX_LEN:
        tweet = tweet[:TWEET_MAX_LEN]

    return tweet if tweet.strip() else None


# =========================================
# MEDIA UPLOAD
# =========================================

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
            resp = requests.post(
                MEDIA_UPLOAD_URL,
                files={"media": img_resp.content},
                auth=oauth1,
                timeout=30,
            )
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
# EMERGENCY MODE — RETWEET @HumansNoContext
# =========================================

def retweet_recent_from_humans_no_context(slot_id: str) -> Optional[str]:
    """
    EMERGENCY MODE: If no Reddit post qualifies, retweet the most recent
    non-reply, non-retweet tweet from @HumansNoContext.

    Uses Twitter API v1.1 user_timeline + statuses/retweet/:id with OAuth1.
    """
    oauth1 = get_oauth1_client()
    if not oauth1:
        logger.error("Emergency retweet failed: no OAuth1 credentials.")
        log_session(slot_id, "fail", "emergency_retweet_no_oauth1", {})
        return None

    try:
        params = {
            "screen_name": HUMANS_NO_CONTEXT_HANDLE,
            "count": 5,
            "exclude_replies": "true",
            "include_rts": "false",
            "tweet_mode": "extended",
        }
        resp = requests.get(TWITTER_USER_TIMELINE_URL_V1, params=params, auth=oauth1, timeout=20)
        if resp.status_code >= 300:
            logger.error("Failed to fetch HumansNoContext timeline: %s %s", resp.status_code, resp.text)
            log_session(
                slot_id,
                "fail",
                "emergency_timeline_fetch_failed",
                {"status_code": resp.status_code, "response": resp.text[:300]},
            )
            return None

        timeline = resp.json()
        if not isinstance(timeline, list) or not timeline:
            logger.error("Empty or invalid timeline from HumansNoContext")
            log_session(slot_id, "fail", "emergency_timeline_empty", {})
            return None

        tweet = timeline[0]
        tweet_id = tweet.get("id_str") or str(tweet.get("id"))
        if not tweet_id:
            logger.error("No tweet id found for HumansNoContext latest tweet")
            log_session(slot_id, "fail", "emergency_no_tweet_id", {})
            return None

        rt_url = TWITTER_RETWEET_URL_V1.format(tweet_id=tweet_id)
        rt_resp = requests.post(rt_url, auth=oauth1, timeout=20)

        if rt_resp.status_code >= 300:
            logger.error("Retweet failed: %s %s", rt_resp.status_code, rt_resp.text)
            log_session(
                slot_id,
                "fail",
                "emergency_retweet_failed",
                {"status_code": rt_resp.status_code, "response": rt_resp.text[:300]},
            )
            return None

        logger.info("Emergency retweet successful: %s", tweet_id)
        return tweet_id

    except Exception as e:
        logger.error("Exception during emergency retweet: %s", e)
        log_session(slot_id, "fail", "emergency_retweet_exception", {"error": str(e)})
        return None


# =========================================
# CORE SLOT LOGIC — REDDIT SELECTION PIPELINE
# =========================================

def pick_best_post_and_tweet_text(
    posts: List[Dict[str, Any]],
    cfg: Dict[str, Any],
    subreddit_name: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Implements the full selection pipeline:

    STEP 1 — Sliding time windows:
      - 10h, then 24h, then 48h (stop as soon as one window yields a valid tweet)

    STEP 2 — Hard Filters:
      - Apply passes_hard_filters()

    STEP 3 — Text Length Filter:
      - If Text/Link post AND len(selftext) > 200 => reject

    STEP 4 — Prioritization Groups:
      - Priority 3: no body AND (gallery or image)
      - Priority 2: other gallery/image posts
      - Priority 1: short text/link (≤ 200 chars)

    STEP 5 — Engagement Score:
      - score = upvotes*0.65 + comments*0.35 + upvote_ratio*10

    STEP 6 — Selection:
      - Sort by priority DESC, score DESC
      - Build tweet; first tweetable candidate wins
    """
    now_ts = now_utc().timestamp()

    for hours in WINDOW_SEQUENCE_HOURS:
        cutoff_ts = now_ts - hours * 3600
        candidates: List[Dict[str, Any]] = []

        for p in posts:
            created = p.get("created_utc") or 0
            try:
                created_ts = float(created)
            except Exception:
                continue

            # STEP 1 — time window
            if created_ts < cutoff_ts:
                continue

            # Hard filters
            if not passes_hard_filters(p):
                continue

            # Skip already posted
            reddit_id = p.get("id")
            if reddit_id and was_post_recently_posted(reddit_id):
                continue

            # Text length filter for text/link posts
            body = clean_body(p.get("selftext"))
            if is_text_or_link_post(p) and len(body) > 200:
                continue

            # If there is neither body nor title, skip
            title = (p.get("title") or "").strip()
            if not body and not title:
                continue

            # Compute priority and score
            priority = compute_priority_group(p, body)
            score = compute_engagement_score(p)

            candidates.append(
                {
                    "post": p,
                    "priority": priority,
                    "score": score,
                }
            )

        logger.info("Window %sh -> %d candidates for %s", hours, len(candidates), subreddit_name)

        if not candidates:
            # No candidates in this window; expand to next window
            continue

        # STEP 6 — select top post within this window
        candidates_sorted = sorted(
            candidates,
            key=lambda c: (-c["priority"], -c["score"]),
        )

        for item in candidates_sorted:
            post = item["post"]
            tweet = build_tweet(post, cfg)
            if tweet:
                logger.info(
                    "Selected post from %s window=%sh priority=%s score=%.2f id=%s",
                    subreddit_name,
                    hours,
                    item["priority"],
                    item["score"],
                    post.get("id"),
                )
                return post, tweet

        # If we had candidates but none tweetable, try next (larger) window.
        logger.info(
            "Window %sh had %d candidates but none tweetable for %s; expanding window.",
            hours,
            len(candidates),
            subreddit_name,
        )

    # After 10h/24h/48h windows, no tweetable post
    return None, None


def handle_slot_for_subreddit(name: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Integrated logging with selection pipeline + Emergency Mode.

    If no Reddit post qualifies after all windows and filters:
    - EMERGENCY MODE: retweet the most recent tweet from @HumansNoContext
    """
    slot_id = f"slot_{name}"

    # STEP 1: Fetch posts
    posts = fetch_subreddit_posts(cfg)
    if not posts:
        log_session(slot_id, "fail", "reddit_fetch_empty", {"subreddit": name})
        return {"subreddit": name, "status": "reddit_fetch_empty"}

    # STEPS 1–6: Sliding windows + filters + scoring + tweet build
    best_post, tweet_text = pick_best_post_and_tweet_text(posts, cfg, name)

    if not best_post or not tweet_text:
        # EMERGENCY MODE — NEVER BREAK THE BOT
        logger.info("No qualifying Reddit post for %s. Entering EMERGENCY MODE.", name)
        rt_id = retweet_recent_from_humans_no_context(slot_id)
        if rt_id:
            log_session(
                slot_id,
                "success",
                "emergency_retweet",
                {"retweeted_tweet_id": rt_id, "source_handle": HUMANS_NO_CONTEXT_HANDLE},
            )
            return {
                "subreddit": name,
                "status": "emergency_retweet",
                "retweeted_tweet_id": rt_id,
                "source_handle": HUMANS_NO_CONTEXT_HANDLE,
            }
        else:
            log_session(
                slot_id,
                "fail",
                "no_tweetable_post_and_emergency_failed",
                {"subreddit": name},
            )
            return {
                "subreddit": name,
                "status": "no_tweetable_post_emergency_failed",
            }

    # Media extraction & upload (if any)
    image_urls = extract_image_urls(best_post)
    media_ids: List[str] = []
    if image_urls:
        try:
            media_ids = upload_images_to_twitter(image_urls)
            if not media_ids and image_urls:
                # media urls were found but upload failed
                log_session(
                    slot_id,
                    "fail",
                    "media_upload_partial_or_failed",
                    {"image_urls": image_urls},
                )
                # proceed: tweet without media
        except Exception as e:
            log_session(slot_id, "fail", "media_upload_failed", {"error": str(e)})
            media_ids = []

    # Post tweet
    try:
        data = twitter_post_tweet(tweet_text, media_ids=media_ids or None)
        tweet_id = data.get("id")
        tweet_url = f"https://x.com/i/web/status/{tweet_id}" if tweet_id else None

        # Mark as posted to avoid repeats
        reddit_id = best_post.get("id")
        if reddit_id:
            mark_post_as_posted(reddit_id, name)

        log_session(
            slot_id,
            "success",
            None,
            {"post_id": best_post.get("id"), "tweet_url": tweet_url},
        )
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
            # Avoid re-triggering same subreddit if we've already posted
            last_id = POST_HISTORY.get("subreddit_last", {}).get(name)
            if last_id and was_post_recently_posted(last_id):
                logger.info(
                    "Skipping subreddit %s because last post %s was recent",
                    name,
                    last_id,
                )
                continue
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
