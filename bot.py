#!/usr/bin/env python3
"""
Async-optimized rewrite of your reddit -> x bot.

Key features:
- asyncio + aiohttp for network-heavy operations (Reddit fetch, image download)
- ThreadPool for blocking operations (requests + OAuth1 media uploads / Twitter calls)
- In-memory analysis cache to avoid duplicate work per run
- Compiled regex objects for fast body cleaning
- Reduced default reddit fetch size (configurable)
- Avoid full sorts: pick best candidate via max() with comparator
- Flask endpoints kept sync; they call asyncio.run for the async pipeline
- Preserves original behavior/selection rules & emergency retweet fallback
"""

import os
import asyncio
import logging
import json
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

import aiohttp
import requests  # used in ThreadPool for OAuth1 / Twitter v2 calls
from flask import Flask, jsonify
from requests_oauthlib import OAuth1

# session logger (user must provide)
from session_logger import log_session

# =========================
# CONFIG (tweak as needed)
# =========================
USER_AGENT = "AkiRedditBot/1.0 (by u/WayOk4302)"
# Default reduced fetch size -- you can set REDDIT_LIMIT env var to override
REDDIT_LIMIT = int(os.getenv("REDDIT_LIMIT", "30"))

# Subreddits config (same shape as your original)
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

RETRY_LIMIT = 3
WAIT_SECONDS = 2

# Sliding windows in hours (same semantic as original)
PRIMARY_WINDOW_HOURS = 10
FALLBACK_WINDOW_HOURS = 24
MAX_WINDOW_HOURS = 48
WINDOW_SEQUENCE_HOURS = [PRIMARY_WINDOW_HOURS, FALLBACK_WINDOW_HOURS, MAX_WINDOW_HOURS]

BODY_CHAR_LIMIT = 220
TWEET_MAX_LEN = 280

SLOT_TOLERANCE_MINUTES = 20
POST_COOLDOWN_HOURS = 24

MEDIA_UPLOAD_URL = "https://upload.twitter.com/1.1/media/upload.json"
TWEET_POST_URL_V2 = "https://api.twitter.com/2/tweets"

HUMANS_NO_CONTEXT_HANDLE = "HumansNoContext"
TWITTER_USER_TIMELINE_URL_V1 = "https://api.twitter.com/1.1/statuses/user_timeline.json"
TWITTER_RETWEET_URL_V1 = "https://api.twitter.com/1.1/statuses/retweet/{tweet_id}.json"

POST_HISTORY_FILE = "post_history.json"

# concurrency tuning
ASYNC_HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)
MAX_CONCURRENT_REDDIT_FETCH = 6  # parallel subreddit fetches
IMAGE_DOWNLOAD_CONCURRENCY = 4
THREADPOOL_WORKERS = int(os.getenv("THREADPOOL_WORKERS", "6"))

# =========================
# logging
# =========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reddit-x-bot-async")

# Flask app (sync)
app = Flask(__name__)

# =========================
# compiled regexes / caches
# =========================
EDIT_RE = re.compile(r"EDIT:.*", re.IGNORECASE | re.DOTALL)
LINK_RE = re.compile(r"\[.*?\]\(.*?\)")
SPACE_RE = re.compile(r"\s{2,}")
HASHTAG_SPLIT_RE = re.compile(r"\s+")

# Analysis cache for the lifetime of a run (cleared each awake call)
_analysis_cache: Dict[str, Dict[str, Any]] = {}

# POST_HISTORY loaded once at startup and updated to disk
def load_post_history() -> Dict[str, Any]:
    try:
        if os.path.exists(POST_HISTORY_FILE):
            with open(POST_HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.warning("Failed to load post history: %s", e)
    return {"posted_ids": {}, "subreddit_last": {}}


def save_post_history(history: Dict[str, Any]) -> None:
    try:
        with open(POST_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f)
    except Exception as e:
        logger.warning("Failed to save post history: %s", e)


POST_HISTORY = load_post_history()
# cached_recent_post_ids for O(1) membership test
_POSTED_IDS_CACHE = set(POST_HISTORY.get("posted_ids", {}).keys())

def mark_post_as_posted(reddit_id: str, subreddit_name: str) -> None:
    now_iso = datetime.utcnow().isoformat()
    POST_HISTORY.setdefault("posted_ids", {})[reddit_id] = now_iso
    POST_HISTORY.setdefault("subreddit_last", {})[subreddit_name] = reddit_id
    _POSTED_IDS_CACHE.add(reddit_id)
    save_post_history(POST_HISTORY)


def was_post_recently_posted(reddit_id: str) -> bool:
    # fast membership check using cached set
    if reddit_id in _POSTED_IDS_CACHE:
        try:
            ts = POST_HISTORY.get("posted_ids", {}).get(reddit_id)
            if not ts:
                return False
            dt = datetime.fromisoformat(ts)
            return (datetime.utcnow() - dt) < timedelta(hours=POST_COOLDOWN_HOURS)
        except Exception:
            return False
    return False


# =========================
# time helpers
# =========================
def now_utc() -> datetime:
    return datetime.utcnow()


def now_ist() -> datetime:
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


# =========================
# Body cleaning / helpers
# =========================
def clean_body(text: Optional[str]) -> str:
    text = text or ""
    text = EDIT_RE.sub("", text)
    text = LINK_RE.sub("", text)
    text = text.replace("\n", " ")
    text = SPACE_RE.sub(" ", text)
    return text.strip()


def _split_hashtags(hashtags: str) -> List[str]:
    if not hashtags:
        return []
    return [tok for tok in HASHTAG_SPLIT_RE.split(hashtags.strip()) if tok]


def is_reddit_url(url: Optional[str]) -> bool:
    if not url:
        return False
    u = url.lower()
    return ("reddit.com" in u) or ("redd.it" in u)


def is_external_url(url: Optional[str]) -> bool:
    return bool(url) and not is_reddit_url(url)


# =========================
# Post-type checks (kept synchronous / cheap)
# =========================
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
    url = post.get("url") or ""
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
    if post.get("spoiler"):
        return False
    if post.get("over_18"):
        return False
    if post.get("stickied"):
        return False
    if post.get("distinguished") not in (None, "", "null"):
        return False
    if post_is_deleted_or_removed(post):
        return False
    if post_is_promoted_or_sponsored(post):
        return False
    if post.get("contest_mode"):
        return False
    return True


# =========================
# Scoring / priority (cached per-run)
# =========================
def compute_engagement_score(post: Dict[str, Any]) -> float:
    ups = post.get("ups") or 0
    comments = post.get("num_comments") or 0
    upr = post.get("upvote_ratio") or 0.0
    return float((ups * 0.65) + (comments * 0.35) + (upr * 10.0))


def compute_priority_group(post: Dict[str, Any], body: str) -> int:
    gallery = is_gallery_post(post)
    image = post_has_image(post)
    has_body = bool(body)
    if (gallery or image) and not has_body:
        return 3
    if gallery or image:
        return 2
    return 1


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


# =========================
# Tweet builder (keeps original rules)
# =========================
def build_tweet(post: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    raw_body = clean_body(post.get("selftext"))
    title = (post.get("title") or "").strip()

    has_title = bool(title)
    has_body = bool(raw_body)
    has_images = is_gallery_post(post) or post_has_image(post)

    if has_title and not has_body:
        base_text = title
    elif has_title and has_body and has_images:
        base_text = title
    elif has_title and has_body:
        base_text = raw_body
    elif has_body:
        base_text = raw_body
    elif has_title:
        base_text = title
    else:
        return None

    base_text = base_text.strip()
    if not base_text:
        return None

    url = post.get("url") or ""
    external_url = url if is_external_url(url) else ""

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

    tweet = assemble_tweet(base_text, hashtag_tokens)
    if len(tweet) <= TWEET_MAX_LEN:
        return tweet

    tokens = list(hashtag_tokens)
    while len(tweet) > TWEET_MAX_LEN and tokens:
        tokens.pop()
        tweet = assemble_tweet(base_text, tokens)
    if len(tweet) <= TWEET_MAX_LEN:
        return tweet

    tokens = []
    while len(tweet) > TWEET_MAX_LEN and base_text:
        base_text = base_text[:-1]
        tweet = assemble_tweet(base_text, tokens)
    if len(tweet) > TWEET_MAX_LEN:
        tweet = tweet[:TWEET_MAX_LEN]
    return tweet if tweet.strip() else None


# =========================
# OAuth1 / Twitter helpers (blocking) - run in ThreadPool
# =========================
def get_oauth1_client() -> Optional[OAuth1]:
    api_key = os.getenv("X_API_KEY")
    api_secret = os.getenv("X_API_SECRET")
    access_token = os.getenv("X_ACCESS_TOKEN")
    access_secret = os.getenv("X_ACCESS_SECRET")
    if not all([api_key, api_secret, access_token, access_secret]):
        return None
    return OAuth1(api_key, api_secret, access_token, access_secret, signature_type="auth_header")


def _upload_single_image_blocking(oauth1: OAuth1, img_url: str) -> Optional[str]:
    try:
        resp = requests.get(img_url, timeout=20)
        resp.raise_for_status()
        post_resp = requests.post(MEDIA_UPLOAD_URL, files={"media": resp.content}, auth=oauth1, timeout=30)
        if post_resp.status_code >= 300:
            logger.warning("Media upload failed (%s): %s", post_resp.status_code, post_resp.text)
            return None
        data = post_resp.json()
        mid = data.get("media_id_string") or data.get("media_id")
        if mid:
            return str(mid)
    except Exception as e:
        logger.warning("Error uploading media from %s: %s", img_url, e)
    return None


def upload_images_to_twitter(image_urls: List[str]) -> List[str]:
    """
    Blocking wrapper that uses ThreadPoolExecutor to upload images via OAuth1.
    Called from async code via run_in_executor.
    """
    oauth1 = get_oauth1_client()
    if not oauth1:
        logger.info("No OAuth1 keys present; posting without media.")
        return []
    media_ids: List[str] = []
    with ThreadPoolExecutor(max_workers=min(len(image_urls) or 1, THREADPOOL_WORKERS)) as ex:
        futures = [ex.submit(_upload_single_image_blocking, oauth1, u) for u in image_urls]
        for f in futures:
            try:
                mid = f.result()
                if mid:
                    media_ids.append(mid)
            except Exception as e:
                logger.warning("Media upload exception: %s", e)
    return media_ids


def _twitter_post_blocking(payload: Dict[str, Any], prefer_oauth1: bool = False) -> Dict[str, Any]:
    """
    Blocking call to post a tweet. Uses OAuth1 for media or bearer for pure text if available.
    """
    oauth1 = get_oauth1_client()
    bearer = os.getenv("X_BEARER_TOKEN")
    if payload.get("media") and oauth1:
        # prefer oauth1 if media present
        try:
            resp = requests.post(TWEET_POST_URL_V2, json=payload, auth=oauth1, timeout=20)
            resp.raise_for_status()
            return resp.json().get("data", {})
        except Exception as e:
            raise RuntimeError(f"Twitter post error (oauth1): {e}")
    # fallback to bearer token if present
    if bearer:
        headers = {"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"}
        try:
            resp = requests.post(TWEET_POST_URL_V2, json=payload, headers=headers, timeout=20)
            resp.raise_for_status()
            return resp.json().get("data", {})
        except Exception as e:
            raise RuntimeError(f"Twitter post error (oauth2): {e}")
    if oauth1:
        try:
            resp = requests.post(TWEET_POST_URL_V2, json=payload, auth=oauth1, timeout=20)
            resp.raise_for_status()
            return resp.json().get("data", {})
        except Exception as e:
            raise RuntimeError(f"Twitter post error (oauth1-fallback): {e}")
    raise RuntimeError("No valid Twitter credentials: set X_BEARER_TOKEN or OAuth1 keys.")


# =========================
# Emergency retweet blocking wrapper
# =========================
def _retweet_recent_from_humans_blocking() -> Optional[str]:
    oauth1 = get_oauth1_client()
    if not oauth1:
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
            return None
        timeline = resp.json()
        if not isinstance(timeline, list) or not timeline:
            return None
        tweet = timeline[0]
        tweet_id = tweet.get("id_str") or str(tweet.get("id"))
        if not tweet_id:
            return None
        rt_url = TWITTER_RETWEET_URL_V1.format(tweet_id=tweet_id)
        rt_resp = requests.post(rt_url, auth=oauth1, timeout=20)
        if rt_resp.status_code >= 300:
            return None
        return tweet_id
    except Exception:
        return None


# =========================
# Async network functions
# =========================
async def reddit_fetch_json(session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
    headers = {"User-Agent": USER_AGENT}
    backoff = 1
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            async with session.get(url, headers=headers, timeout=ASYNC_HTTP_TIMEOUT) as resp:
                if resp.status in (429, 500, 502, 503, 504):
                    logger.warning("Reddit HTTP %s attempt %s/%s", resp.status, attempt, RETRY_LIMIT)
                    await asyncio.sleep(WAIT_SECONDS * backoff)
                    backoff += 1
                    continue
                text = await resp.text()
                if text.lstrip().startswith("<"):
                    logger.warning("Reddit returned HTML, attempt %s/%s", attempt, RETRY_LIMIT)
                    await asyncio.sleep(WAIT_SECONDS * backoff)
                    backoff += 1
                    continue
                return await resp.json()
        except asyncio.TimeoutError:
            logger.warning("Reddit fetch timeout attempt %s/%s: %s", attempt, RETRY_LIMIT, url)
            await asyncio.sleep(WAIT_SECONDS * backoff)
            backoff += 1
        except Exception as e:
            logger.warning("Reddit fetch error attempt %s/%s: %s", attempt, RETRY_LIMIT, e)
            await asyncio.sleep(WAIT_SECONDS * backoff)
            backoff += 1
    logger.error("Reddit fetch failed after %s attempts: %s", RETRY_LIMIT, url)
    return None


async def fetch_subreddit_posts_async(session: aiohttp.ClientSession, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    base = cfg["url"].rstrip("/")
    url = f"{base}/new/.json?limit={REDDIT_LIMIT}"
    data = await reddit_fetch_json(session, url)
    if not data:
        return []
    children = data.get("data", {}).get("children", [])
    posts = [c.get("data", {}) for c in children if isinstance(c, dict)]
    logger.info("Fetched %d raw posts from %s", len(posts), base)
    return posts


async def download_image_bytes(session: aiohttp.ClientSession, url: str) -> Optional[bytes]:
    try:
        async with session.get(url, timeout=ASYNC_HTTP_TIMEOUT) as resp:
            resp.raise_for_status()
            return await resp.read()
    except Exception as e:
        logger.warning("Image download failed for %s: %s", url, e)
        return None


# =========================
# Selection pipeline (async orchestration; heavy-lifting cached)
# =========================
def _analyze_post_cached(post: Dict[str, Any]) -> Dict[str, Any]:
    pid = post.get("id") or post.get("name") or str(id(post))
    if pid in _analysis_cache:
        return _analysis_cache[pid]

    body = clean_body(post.get("selftext"))
    gallery = is_gallery_post(post)
    has_img = post_has_image(post)
    priority = compute_priority_group(post, body)
    score = compute_engagement_score(post)
    title = (post.get("title") or "").strip()
    created_utc = float(post.get("created_utc") or 0)
    analysis = {
        "id": pid,
        "body": body,
        "gallery": gallery,
        "has_img": has_img,
        "priority": priority,
        "score": score,
        "title": title,
        "created_utc": created_utc,
        "post": post,
    }
    _analysis_cache[pid] = analysis
    return analysis


async def pick_best_post_and_tweet_text_async(
    posts: List[Dict[str, Any]],
    cfg: Dict[str, Any],
    subreddit_name: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    # current UTC timestamp once
    now_ts = now_utc().timestamp()
    for hours in WINDOW_SEQUENCE_HOURS:
        cutoff_ts = now_ts - hours * 3600
        candidates: List[Dict[str, Any]] = []

        # First pass: filter cheaply and build analysis entries
        for p in posts:
            created = p.get("created_utc") or 0
            try:
                created_ts = float(created)
            except Exception:
                continue
            if created_ts < cutoff_ts:
                continue
            if not passes_hard_filters(p):
                continue
            reddit_id = p.get("id")
            if reddit_id and was_post_recently_posted(reddit_id):
                continue

            # do analysis (cached)
            a = _analyze_post_cached(p)

            # text length filter for text/link posts
            if is_text_or_link_post(p) and len(a["body"]) > 200:
                continue
            if not a["body"] and not a["title"]:
                continue

            candidates.append(a)

        logger.info("Window %sh -> %d candidates for %s", hours, len(candidates), subreddit_name)
        if not candidates:
            continue

        # pick best by priority desc, score desc using max() with key
        def key_fn(a: Dict[str, Any]):
            return (a["priority"], a["score"])

        # find best candidate (highest priority and score)
        best = max(candidates, key=key_fn)

        # But we must prefer candidates with no body & gallery over others within same window.
        # If multiple tie on (priority, score) we'd previously sorted; using max returns one with max key.
        # Build tweet for best; if not tweetable, try removing it and picking next best, etc.
        # We'll iterate through descending order without sorting full list (we'll repeatedly pop best).
        tried = 0
        # Convert to a heap-like loop using repeated max calls, but limited attempts to avoid heavy work
        remaining = candidates.copy()
        while remaining and tried < 6:  # limit attempts for performance
            cur_best = max(remaining, key=key_fn)
            post = cur_best["post"]
            tweet = build_tweet(post, cfg)
            if tweet:
                logger.info(
                    "Selected post from %s window=%sh priority=%s score=%.2f id=%s",
                    subreddit_name,
                    hours,
                    cur_best["priority"],
                    cur_best["score"],
                    post.get("id"),
                )
                return post, tweet
            # remove current best and try next
            remaining.remove(cur_best)
            tried += 1

        logger.info(
            "Window %sh had %d candidates but none tweetable for %s; expanding window.",
            hours,
            len(candidates),
            subreddit_name,
        )
    return None, None


async def handle_slot_for_subreddit_async(name: str, cfg: Dict[str, Any], session: aiohttp.ClientSession, executor: ThreadPoolExecutor) -> Dict[str, Any]:
    slot_id = f"slot_{name}"
    posts = await fetch_subreddit_posts_async(session, cfg)
    if not posts:
        log_session(slot_id, "fail", "reddit_fetch_empty", {"subreddit": name})
        return {"subreddit": name, "status": "reddit_fetch_empty"}

    best_post, tweet_text = await pick_best_post_and_tweet_text_async(posts, cfg, name)
    if not best_post or not tweet_text:
        # Emergency mode via threadpool blocking call
        logger.info("No qualifying Reddit post for %s. Entering EMERGENCY MODE.", name)
        rt_id = await asyncio.get_event_loop().run_in_executor(executor, _retweet_recent_from_humans_blocking)
        if rt_id:
            log_session(slot_id, "success", "emergency_retweet", {"retweeted_tweet_id": rt_id, "source_handle": HUMANS_NO_CONTEXT_HANDLE})
            return {"subreddit": name, "status": "emergency_retweet", "retweeted_tweet_id": rt_id, "source_handle": HUMANS_NO_CONTEXT_HANDLE}
        else:
            log_session(slot_id, "fail", "no_tweetable_post_and_emergency_failed", {"subreddit": name})
            return {"subreddit": name, "status": "no_tweetable_post_emergency_failed"}

    # Extract image URLs (cheap) and upload in executor
    image_urls = extract_image_urls(best_post)
    media_ids: List[str] = []
    if image_urls:
        try:
            media_ids = await asyncio.get_event_loop().run_in_executor(executor, upload_images_to_twitter, image_urls)
            if not media_ids and image_urls:
                log_session(slot_id, "fail", "media_upload_partial_or_failed", {"image_urls": image_urls})
        except Exception as e:
            log_session(slot_id, "fail", "media_upload_failed", {"error": str(e)})
            media_ids = []

    # Post tweet (blocking) in executor
    try:
        payload: Dict[str, Any]
        if media_ids:
            payload = {"text": tweet_text, "media": {"media_ids": media_ids}}
        else:
            payload = {"text": tweet_text}
        data = await asyncio.get_event_loop().run_in_executor(executor, _twitter_post_blocking, payload)
        tweet_id = data.get("id")
        tweet_url = f"https://x.com/i/web/status/{tweet_id}" if tweet_id else None

        reddit_id = best_post.get("id")
        if reddit_id:
            mark_post_as_posted(reddit_id, name)

        log_session(slot_id, "success", None, {"post_id": best_post.get("id"), "tweet_url": tweet_url})
        return {"subreddit": name, "status": "tweeted", "tweet_url": tweet_url, "post_id": best_post.get("id")}
    except Exception as e:
        logger.error("Failed to post tweet for %s: %s", name, e)
        log_session(slot_id, "fail", "tweet_post_failed", {"error": str(e)})
        return {"subreddit": name, "status": "error", "error": str(e)}


async def find_due_subreddits_async(now: datetime) -> List[Tuple[str, Dict[str, Any]]]:
    now_min = minutes_since_midnight(now)
    slot_now = hhmm_ist(now)
    logger.info("Current IST time %s (%s minutes)", slot_now, now_min)
    targets: List[Tuple[str, Dict[str, Any]]] = []
    for name, cfg in SUBREDDITS.items():
        slot_str = cfg["post_time"]
        slot_min = parse_hhmm_to_minutes(slot_str)
        delta = abs(now_min - slot_min)
        if delta <= SLOT_TOLERANCE_MINUTES:
            last_id = POST_HISTORY.get("subreddit_last", {}).get(name)
            if last_id and was_post_recently_posted(last_id):
                logger.info("Skipping subreddit %s because last post %s was recent", name, last_id)
                continue
            targets.append((name, cfg))
    return targets


async def handle_awake_async() -> Dict[str, Any]:
    # Clear per-run analysis cache for memory control
    _analysis_cache.clear()
    ist = now_ist()
    logger.info("AWAKE at IST: %s", ist.isoformat())

    targets = await find_due_subreddits_async(ist)
    if not targets:
        logger.info("No subreddit within ±%s minutes of any slot.", SLOT_TOLERANCE_MINUTES)
        log_session("slot_timing", "fail", "no_slot_due", {"time": ist.isoformat()})
        return {"status": "no_slot", "time_ist": ist.isoformat(), "tolerance_minutes": SLOT_TOLERANCE_MINUTES}

    results: List[Dict[str, Any]] = []
    executor = ThreadPoolExecutor(max_workers=THREADPOOL_WORKERS)
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Parallelize subreddit handling, bounded by semaphore to avoid too many concurrent fetches
        sem = asyncio.Semaphore(MAX_CONCURRENT_REDDIT_FETCH)

        async def worker(name_cfg):
            name, cfg = name_cfg
            async with sem:
                return await handle_slot_for_subreddit_async(name, cfg, session, executor)

        coros = [worker(t) for t in targets]
        # Gather results concurrently
        gathered = await asyncio.gather(*coros, return_exceptions=False)
        for r in gathered:
            results.append(r)

    return {"status": "done", "time_ist": ist.isoformat(), "tolerance_minutes": SLOT_TOLERANCE_MINUTES, "results": results}


# =========================
# Flask endpoints (sync) calling async pipeline
# =========================
@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "running", "service": "reddit→x bot (async)" })


@app.route("/awake", methods=["GET", "POST"])
def awake():
    # run the async pipeline synchronously
    try:
        result = asyncio.run(handle_awake_async())
        return jsonify(result)
    except Exception as e:
        logger.exception("Error running handle_awake_async: %s", e)
        return jsonify({"status": "error", "error": str(e)}), 500


# =========================
# CLI entrypoint
# =========================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    logger.info("Starting reddit->x bot (async) on port %s", port)
    app.run(host="0.0.0.0", port=port)
