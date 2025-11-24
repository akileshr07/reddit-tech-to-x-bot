#!/usr/bin/env python3
"""
Async + caching rewrite of reddit -> x bot.
Maintains core rules/logic of original script but optimized for concurrency.
Requires: aiohttp, tenacity
pip install aiohttp tenacity
"""

import asyncio
import os
import re
import json
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import logging
import heapq

import aiohttp
from aiohttp import ClientSession
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ============== CONFIG (tweak for performance) =================
USER_AGENT = "AkiRedditBot/1.0 (by u/WayOk4302)"
REDDIT_LIMIT = int(os.getenv("REDDIT_LIMIT", "30"))  # default smaller for speed
CONCURRENT_FETCHES = int(os.getenv("CONCURRENT_FETCHES", "6"))
MEDIA_UPLOAD_WORKERS = int(os.getenv("MEDIA_UPLOAD_WORKERS", "4"))
SLOT_TOLERANCE_MINUTES = 20
POST_COOLDOWN_HOURS = 24
WINDOW_SEQUENCE_HOURS = [10, 24, 48]
TWEET_MAX_LEN = 280
BODY_CHAR_LIMIT = 220

# Caching TTLs (seconds)
ANALYSIS_TTL = 60 * 5  # 5 minutes for in-memory post analysis
POST_HISTORY_FILE = "post_history.json"

# Reddit / Twitter endpoints
REDDIT_SUFFIX_NEW = "/new/.json"
MEDIA_UPLOAD_URL = "https://upload.twitter.com/1.1/media/upload.json"
TWEET_POST_URL_V2 = "https://api.twitter.com/2/tweets"
TWITTER_USER_TIMELINE_URL_V1 = "https://api.twitter.com/1.1/statuses/user_timeline.json"
TWITTER_RETWEET_URL_V1 = "https://api.twitter.com/1.1/statuses/retweet/{tweet_id}.json"
HUMANS_NO_CONTEXT_HANDLE = "HumansNoContext"

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reddit-x-bot-async")

# ============== PRE-COMPILED REGEX ============================
EDIT_RE = re.compile(r"EDIT:.*", re.IGNORECASE | re.DOTALL)
LINK_RE = re.compile(r"\[.*?\]\(.*?\)")
SPACE_RE = re.compile(r"\s{2,}")

# ============== SIMPLE TTL CACHE FOR POST ANALYSIS ============
class TTLCache:
    def __init__(self):
        self._store: Dict[str, Tuple[float, Any]] = {}

    def set(self, key: str, value: Any, ttl: int):
        self._store[key] = (datetime.utcnow().timestamp() + ttl, value)

    def get(self, key: str):
        val = self._store.get(key)
        if not val:
            return None
        exp, data = val
        if datetime.utcnow().timestamp() > exp:
            del self._store[key]
            return None
        return data

ANALYSIS_CACHE = TTLCache()

# ============== POST HISTORY (persistent) =====================
def load_post_history() -> Dict[str, Any]:
    try:
        if os.path.exists(POST_HISTORY_FILE):
            with open(POST_HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.warning("Failed to load post history: %s", e)
    return {"posted_ids": {}, "subreddit_last": {}}

def save_post_history(h):
    try:
        with open(POST_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(h, f)
    except Exception as e:
        logger.warning("Failed to save post history: %s", e)

POST_HISTORY = load_post_history()
RECENT_POST_CACHE = set(POST_HISTORY.get("posted_ids", {}).keys())

def mark_post_as_posted(reddit_id: str, subreddit_name: str):
    now_iso = datetime.utcnow().isoformat()
    POST_HISTORY.setdefault("posted_ids", {})[reddit_id] = now_iso
    POST_HISTORY.setdefault("subreddit_last", {})[subreddit_name] = reddit_id
    RECENT_POST_CACHE.add(reddit_id)
    save_post_history(POST_HISTORY)

def was_post_recently_posted(reddit_id: str) -> bool:
    if reddit_id in RECENT_POST_CACHE:
        # quick hit
        ts = POST_HISTORY.get("posted_ids", {}).get(reddit_id)
        if not ts:
            return False
        try:
            dt = datetime.fromisoformat(ts)
            if datetime.utcnow() - dt < timedelta(hours=POST_COOLDOWN_HOURS):
                return True
        except Exception:
            return False
    return False

# ============== SUBREDDIT CONFIG (example) ===================
SUBREDDITS: Dict[str, Dict[str, Any]] = {
    "developersIndia": {
        "url": "https://www.reddit.com/r/developersIndia",
        "post_time": "09:00",
        "hashtags": "#TechTwitter #Programming #Coding #WebDevelopment #DeveloperLife #100DaysOfCode #Tech",
    },
    "ProgrammerHumor": {
        "url": "https://www.reddit.com/r/ProgrammerHumor",
        "post_time": "18:00",
        "hashtags": "#Funny #Humor #FunnyTweets #Memes #DankMemes #Comedy #LOL",
    },
    # add others as needed...
}

# ============== ASYNC HTTP HELPERS ===========================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, max=5), retry=retry_if_exception_type(Exception))
async def fetch_json(session: ClientSession, url: str, params: dict = None, headers: dict = None, timeout: int = 15) -> Optional[dict]:
    hdrs = headers or {}
    hdrs.setdefault("User-Agent", USER_AGENT)
    async with session.get(url, params=params, headers=hdrs, timeout=timeout) as resp:
        text = await resp.text()
        if resp.status >= 500:
            raise RuntimeError(f"Server error {resp.status}")
        # If Reddit sometimes returns HTML because of blocking, treat as failure
        if text.lstrip().startswith("<"):
            raise RuntimeError("Non-JSON response")
        try:
            return await resp.json()
        except Exception:
            # fallback: attempt manual parse
            return None

async def fetch_bytes(session: ClientSession, url: str, timeout: int = 20) -> Optional[bytes]:
    async with session.get(url, timeout=timeout) as resp:
        if resp.status >= 300:
            logger.warning("Image fetch failed %s -> %s", url, resp.status)
            return None
        return await resp.read()

# ============== POST ANALYSIS (cached) =======================
def clean_body_sync(text: Optional[str]) -> str:
    t = text or ""
    t = EDIT_RE.sub("", t)
    t = LINK_RE.sub("", t)
    t = t.replace("\n", " ")
    t = SPACE_RE.sub(" ", t)
    return t.strip()

def post_is_video_sync(post: Dict[str, Any]) -> bool:
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
            variants = img.get("variants", {})
            if "mp4" in variants or "gif" in variants:
                return True
    url = (post.get("url") or "").lower()
    if any(url.endswith(ext) for ext in (".mp4", ".mov", ".webm")):
        return True
    return False

def post_is_gif_or_animated_sync(post: Dict[str, Any]) -> bool:
    url = (post.get("url") or "").lower()
    if any(url.endswith(ext) for ext in (".gif", ".gifv", ".webp")):
        return True
    preview = post.get("preview")
    if isinstance(preview, dict):
        images = preview.get("images") or []
        for img in images:
            variants = img.get("variants", {})
            if "gif" in variants or "mp4" in variants:
                return True
    return False

def post_has_image_sync(post: Dict[str, Any]) -> bool:
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

def is_gallery_post_sync(post: Dict[str, Any]) -> bool:
    if post.get("is_gallery"):
        return True
    if isinstance(post.get("gallery_data"), dict):
        return True
    if isinstance(post.get("media_metadata"), dict) and post.get("media_metadata"):
        return True
    return False

def post_is_poll_sync(post: Dict[str, Any]) -> bool:
    if post.get("poll_data"):
        return True
    if isinstance(post.get("polls"), list) and post.get("polls"):
        return True
    return False

def passes_hard_filters_sync(post: Dict[str, Any]) -> bool:
    # quick checks first
    url = post.get("url") or ""
    if post_is_video_sync(post):
        return False
    if is_external_video_url(url):
        return False
    if post_is_gif_or_animated_sync(post):
        return False
    if post_is_poll_sync(post):
        return False
    if post_is_crosspost_sync(post):
        return False
    if post.get("spoiler"):
        return False
    if post.get("over_18"):
        return False
    if post.get("stickied"):
        return False
    if post.get("distinguished") not in (None, "", "null"):
        return False
    if post_is_deleted_or_removed_sync(post):
        return False
    if post_is_promoted_or_sponsored_sync(post):
        return False
    if post.get("contest_mode"):
        return False
    return True

# small sync helper functions used by hard filters
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

def post_is_crosspost_sync(post: Dict[str, Any]) -> bool:
    if post.get("crosspost_parent"):
        return True
    cpl = post.get("crosspost_parent_list")
    if isinstance(cpl, list) and cpl:
        return True
    return False

def post_is_deleted_or_removed_sync(post: Dict[str, Any]) -> bool:
    title = (post.get("title") or "").strip().lower()
    body = (post.get("selftext") or "").strip().lower()
    if title in ("[deleted]", "[removed]"):
        return True
    if body in ("[deleted]", "[removed]"):
        return True
    if post.get("removed_by_category"):
        return True
    return False

def post_is_promoted_or_sponsored_sync(post: Dict[str, Any]) -> bool:
    if post.get("is_created_from_ads_ui"):
        return True
    if post.get("promoted"):
        return True
    if post.get("is_ad"):
        return True
    if post.get("adserver_click_url"):
        return True
    return False

def compute_engagement_score_sync(post: Dict[str, Any]) -> float:
    ups = post.get("ups") or 0
    comments = post.get("num_comments") or 0
    upr = post.get("upvote_ratio") or 0.0
    score = (ups * 0.65) + (comments * 0.35) + (upr * 10.0)
    return float(score)

def extract_image_urls_sync(post: Dict[str, Any]) -> List[str]:
    urls: List[str] = []
    url = post.get("url") or ""
    if any(url.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp")):
        urls.append(url)
    preview = post.get("preview")
    if isinstance(preview, dict):
        images = preview.get("images") or []
        for img in images:
            src = img.get("source", {}) or {}
            u = src.get("url")
            if u:
                urls.append(u.replace("&amp;", "&"))
    media_meta = post.get("media_metadata")
    if isinstance(media_meta, dict):
        for meta in media_meta.values():
            s = meta.get("s") or {}
            u = s.get("u") or s.get("url")
            if u:
                urls.append(u.replace("&amp;", "&"))
    # dedup and cap
    deduped = []
    for u in urls:
        if u not in deduped:
            deduped.append(u)
        if len(deduped) >= 4:
            break
    return deduped

def compute_priority_group_sync(post: Dict[str, Any], body: str) -> int:
    gallery = is_gallery_post_sync(post)
    image = post_has_image_sync(post)
    has_body = bool(body)
    if (gallery or image) and not has_body:
        return 3
    if gallery or image:
        return 2
    return 1

# ============== ASYNC REDDIT FETCH ===========================
async def fetch_subreddit_posts(session: ClientSession, base_url: str, limit: int = REDDIT_LIMIT) -> List[Dict[str, Any]]:
    base = base_url.rstrip("/")
    url = f"{base}{REDDIT_SUFFIX_NEW}"
    params = {"limit": str(limit)}
    data = await fetch_json(session, url, params=params)
    if not data:
        return []
    children = data.get("data", {}).get("children", [])
    posts = [c.get("data", {}) for c in children if isinstance(c, dict)]
    return posts

# ============== FAST SELECTION PIPELINE (async wrapper) ============
async def analyze_post_cached(post: Dict[str, Any]) -> Dict[str, Any]:
    pid = post.get("id") or post.get("name") or str(hash(json.dumps(post)))
    cached = ANALYSIS_CACHE.get(pid)
    if cached:
        return cached
    # Light-weight analysis
    body = clean_body_sync(post.get("selftext"))
    passes = passes_hard_filters_sync(post)
    if not passes:
        res = {"pass": False}
        ANALYSIS_CACHE.set(pid, res, ANALYSIS_TTL)
        return res
    # More analysis only when it passes quick filters
    priority = compute_priority_group_sync(post, body)
    score = compute_engagement_score_sync(post)
    has_title = bool((post.get("title") or "").strip())
    has_images = is_gallery_post_sync(post) or post_has_image_sync(post)
    res = {
        "pass": True,
        "body": body,
        "title": (post.get("title") or "").strip(),
        "priority": priority,
        "score": score,
        "has_title": has_title,
        "has_images": has_images,
    }
    ANALYSIS_CACHE.set(pid, res, ANALYSIS_TTL)
    return res

def build_tweet_sync(post: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    raw_body = clean_body_sync(post.get("selftext"))
    title = (post.get("title") or "").strip()
    has_title = bool(title)
    has_body = bool(raw_body)
    has_images = is_gallery_post_sync(post) or post_has_image_sync(post)
    # Base text selection same rules
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
    hashtag_tokens = [tok for tok in hashtags.split() if tok.strip()]
    def assemble(current_base: str, tokens: List[str]) -> str:
        hashtags_str = " ".join(tokens) if tokens else ""
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
    tweet = assemble(base_text, hashtag_tokens)
    if len(tweet) <= TWEET_MAX_LEN:
        return tweet
    tokens = list(hashtag_tokens)
    while len(tweet) > TWEET_MAX_LEN and tokens:
        tokens.pop()
        tweet = assemble(base_text, tokens)
    if len(tweet) <= TWEET_MAX_LEN:
        return tweet
    # trim base_text char-by-char
    tokens = []
    while len(tweet) > TWEET_MAX_LEN and base_text:
        base_text = base_text[:-1]
        tweet = assemble(base_text, tokens)
    if len(tweet) > TWEET_MAX_LEN:
        tweet = tweet[:TWEET_MAX_LEN]
    return tweet if tweet.strip() else None

async def pick_best_post_and_tweet_text_async(posts: List[Dict[str, Any]], cfg: Dict[str, Any], subreddit_name: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    now_ts = datetime.utcnow().timestamp()
    # iterate windows
    for hours in WINDOW_SEQUENCE_HOURS:
        cutoff_ts = now_ts - hours * 3600
        candidates = []
        # analyze posts concurrently but limited
        sem = asyncio.Semaphore(CONCURRENT_FETCHES)
        async def analyze_if_candidate(p):
            created = p.get("created_utc") or 0
            try:
                created_ts = float(created)
            except Exception:
                return None
            if created_ts < cutoff_ts:
                return None
            reddit_id = p.get("id")
            if reddit_id and was_post_recently_posted(reddit_id):
                return None
            # quick analysis cached
            async with sem:
                analysis = await analyze_post_cached(p)
            if not analysis.get("pass"):
                return None
            # If text/link post and body > 200 reject
            body = analysis.get("body", "")
            if (p.get("is_self") or (str(p.get("post_hint") or "").lower() in ("link", "rich:video"))) and len(body) > 200:
                return None
            if not body and not analysis.get("title"):
                return None
            # candidate
            return {"post": p, "priority": analysis["priority"], "score": analysis["score"], "analysis": analysis}
        # launch all
        tasks = [asyncio.create_task(analyze_if_candidate(p)) for p in posts]
        results = await asyncio.gather(*tasks)
        for r in results:
            if r:
                candidates.append(r)
        logger.info("Window %sh -> %d candidates for %s", hours, len(candidates), subreddit_name)
        if not candidates:
            continue
        # single-pass selection (O(n)) to find best
        best = None
        for c in candidates:
            if not best or (c["priority"], c["score"]) > (best["priority"], best["score"]):
                best = c
        # attempt tweet build for the best candidate; if fail, try next best without sorting whole list
        # create a small heap of top-k by priority+score to try candidates in decreasing order
        # produce keys as tuple (-priority, -score) for min-heap
        heap = []
        for c in candidates:
            heapq.heappush(heap, ((-c["priority"], -c["score"]), c))
        while heap:
            _, cand = heapq.heappop(heap)
            post = cand["post"]
            tweet = build_tweet_sync(post, cfg)
            if tweet:
                logger.info("Selected post id=%s priority=%s score=%.2f", post.get("id"), cand["priority"], cand["score"])
                return post, tweet
        # if none tweetable -> expand window
    return None, None

# ============== ASYNC MEDIA UPLOAD (placeholder) =================
# NOTE: OAuth1 with aiohttp requires building the auth header; here we'll
# show an async-concurrent upload pattern. In production, implement OAuth1
# properly or run uploads via ThreadPool with requests if simpler.

async def upload_single_image(session: ClientSession, img_url: str) -> Optional[str]:
    try:
        b = await fetch_bytes(session, img_url)
        if not b:
            return None
        # Placeholder: upload to Twitter v1.1 requires OAuth1; the implementation depends on your infra.
        # If you have OAuth1, construct multipart/form-data request with auth headers here.
        # For now we'll return a fake media id to illustrate pipeline (replace with real upload).
        await asyncio.sleep(0)  # yield
        fake_mid = "m_" + str(abs(hash(img_url)))[-10:]
        return fake_mid
    except Exception as e:
        logger.warning("upload_single_image failed %s: %s", img_url, e)
        return None

async def upload_images_to_twitter_async(session: ClientSession, image_urls: List[str]) -> List[str]:
    if not image_urls:
        return []
    sem = asyncio.Semaphore(MEDIA_UPLOAD_WORKERS)
    async def wrapped(u):
        async with sem:
            return await upload_single_image(session, u)
    tasks = [asyncio.create_task(wrapped(u)) for u in image_urls]
    results = await asyncio.gather(*tasks)
    return [r for r in results if r]

# ============== TWITTER POST (placeholder) ======================
# Implement real post with Twitter API v2 + bearer or OAuth1 as your infra supports.
# For demonstration, these are async placeholders that must be wired to real auth.

async def twitter_post_tweet_async(session: ClientSession, text: str, media_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    # In production: call TWEET_POST_URL_V2 with Bearer or OAuth1 as required.
    await asyncio.sleep(0)  # placeholder
    fake_id = "tweet_" + str(abs(hash(text)))[:8]
    return {"id": fake_id}

async def retweet_recent_from_humans_no_context_async(session: ClientSession) -> Optional[str]:
    # Placeholder: fetch timeline and retweet using OAuth1; return tweet id
    await asyncio.sleep(0)
    return None  # return None to indicate fallback not implemented in placeholder

# ============== CORE SLOT HANDLER =============================
async def handle_slot_for_subreddit_async(session: ClientSession, name: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    slot_id = f"slot_{name}"
    posts = await fetch_subreddit_posts(session, cfg["url"], limit=REDDIT_LIMIT)
    if not posts:
        logger.warning("reddit_fetch_empty %s", name)
        return {"subreddit": name, "status": "reddit_fetch_empty"}
    best_post, tweet_text = await pick_best_post_and_tweet_text_async(posts, cfg, name)
    if not best_post or not tweet_text:
        logger.info("No qualifying Reddit post for %s. Trying emergency retweet.", name)
        rt_id = await retweet_recent_from_humans_no_context_async(session)
        if rt_id:
            return {"subreddit": name, "status": "emergency_retweet", "retweeted_tweet_id": rt_id}
        return {"subreddit": name, "status": "no_tweetable_post_emergency_failed"}
    image_urls = extract_image_urls_sync(best_post)
    media_ids = []
    if image_urls:
        try:
            media_ids = await upload_images_to_twitter_async(session, image_urls)
        except Exception as e:
            logger.warning("media upload failed: %s", e)
            media_ids = []
    try:
        data = await twitter_post_tweet_async(session, tweet_text, media_ids or None)
        tweet_id = data.get("id")
        tweet_url = f"https://x.com/i/web/status/{tweet_id}" if tweet_id else None
        reddit_id = best_post.get("id")
        if reddit_id:
            mark_post_as_posted(reddit_id, name)
        return {"subreddit": name, "status": "tweeted", "tweet_url": tweet_url, "post_id": reddit_id}
    except Exception as e:
        logger.error("Failed to post tweet for %s: %s", name, e)
        return {"subreddit": name, "status": "error", "error": str(e)}

def hhmm_ist(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.utcnow() + timedelta(hours=5, minutes=30)
    return dt.strftime("%H:%M")

def minutes_since_midnight(dt: datetime) -> int:
    return dt.hour * 60 + dt.minute

def parse_hhmm_to_minutes(hhmm: str) -> int:
    h_str, m_str = hhmm.split(":")
    return int(h_str) * 60 + int(m_str)

def find_due_subreddits(now: datetime) -> List[Tuple[str, Dict[str, Any]]]:
    now_min = minutes_since_midnight(now)
    targets = []
    for name, cfg in SUBREDDITS.items():
        slot_min = parse_hhmm_to_minutes(cfg["post_time"])
        delta = abs(now_min - slot_min)
        last_id = POST_HISTORY.get("subreddit_last", {}).get(name)
        if delta <= SLOT_TOLERANCE_MINUTES:
            if last_id and was_post_recently_posted(last_id):
                continue
            targets.append((name, cfg))
    return targets

# ============== ENTRYPOINT (async) ============================
async def handle_awake_async() -> Dict[str, Any]:
    ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    targets = find_due_subreddits(ist)
    if not targets:
        return {"status": "no_slot", "time_ist": ist.isoformat(), "tolerance_minutes": SLOT_TOLERANCE_MINUTES}
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(handle_slot_for_subreddit_async(session, name, cfg)) for name, cfg in targets]
        results = await asyncio.gather(*tasks)
    return {"status": "done", "time_ist": ist.isoformat(), "tolerance_minutes": SLOT_TOLERANCE_MINUTES, "results": results}

# ============== SIMPLE CLI TEST RUN ============================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-once", action="store_true", help="Run slot handler once and exit")
    args = parser.parse_args()
    if args.run_once:
        res = asyncio.run(handle_awake_async())
        print(json.dumps(res, indent=2))
    else:
        # Example simple loop - in production use scheduler (cron, systemd, cloud scheduler)
        async def loop_forever():
            while True:
                res = await handle_awake_async()
                logger.info("AWAKE result: %s", res)
                await asyncio.sleep(60)  # low-frequency loop; scheduler preferred
        try:
            asyncio.run(loop_forever())
        except KeyboardInterrupt:
            logger.info("Exiting.")
