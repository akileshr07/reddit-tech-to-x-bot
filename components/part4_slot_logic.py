"""
Part 4 — Slot Logic (Async)
This file orchestrates:
  - Time calculations
  - Subreddit scheduling
  - Async Reddit fetch (from Part 1)
  - Async post selection pipeline (from Part 3)
  - Tweet posting
  - Emergency retweet fallback
  - Logging & history
"""

from __future__ import annotations

import os
import time
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import requests
from requests_oauthlib import OAuth1

# Imports from previous parts
from components.part1_async_http import create_aiohttp_session
from components.part3_media_pipeline import pick_best_post_and_tweet_text_async
from utils.session_logger import log_session

# -----------------------------------------
# CONFIG
# -----------------------------------------
USER_AGENT = "AkiRedditBot/1.0 (by u/WayOk4302)"

SUBREDDITS: Dict[str, Dict[str, Any]] = {
    "developersIndia": {
        "url": "https://www.reddit.com/r/developersIndia/",
        "post_time": "09:00",
        "hashtags": "#TechTwitter #Programming #Coding #WebDevelopment #DeveloperLife #100DaysOfCode #Tech",
    },
    "ProgrammerHumor": {
        "url": "https://www.reddit.com/r/ProgrammerHumor/",
        "post_time": "18:00",
        "hashtags": "#Funny #Humor #FunnyTweets #Memes #DankMemes #Comedy #LOL",
    },
    "technology": {
        "url": "https://www.reddit.com/r/technology/",
        "post_time": "06:00",
        "hashtags": "#TechNews #TechnologyNews #AI #Innovation #Gadgets #Cybersecurity #TechTrends #NewTech",
    },
    "oddlysatisfying": {
        "url": "https://www.reddit.com/r/IndiaTech/",
        "post_time": "12:00",
        "hashtags": "#TechTwitter #Programming #Coding #WebDevelopment",
    },
    "IndiaCricket": {
        "url": "https://www.reddit.com/r/IndiaCricket/hot/",
        "post_time": "15:00",
        "hashtags": "#Cricket #IPL #WorldCup #Sports #CricketLovers #TeamIndia #CricketFever",
    },
}

SLOT_TOLERANCE_MINUTES = 20
POST_HISTORY_FILE = "post_history.json"
POST_COOLDOWN_HOURS = 24
TWITTER_POST_URL = "https://api.twitter.com/2/tweets"

# Emergency fallback tweeting
HUMANS_NO_CONTEXT = "HumansNoContext"
HNC_TIMELINE = "https://api.twitter.com/1.1/statuses/user_timeline.json"
HNC_RETWEET = "https://api.twitter.com/1.1/statuses/retweet/{id}.json"

logger = logging.getLogger("reddit-x-bot.slot")
logger.setLevel(logging.INFO)

# -----------------------------------------
# POST HISTORY
# -----------------------------------------
def load_post_history() -> Dict[str, Any]:
    if not os.path.exists(POST_HISTORY_FILE):
        return {"posted_ids": {}, "subreddit_last": {}}
    try:
        import json
        with open(POST_HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"posted_ids": {}, "subreddit_last": {}}

def save_post_history(data: Dict[str, Any]):
    import json
    with open(POST_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f)

POST_HISTORY = load_post_history()

def mark_posted(pid: str, sub: str):
    POST_HISTORY["posted_ids"][pid] = datetime.utcnow().isoformat()
    POST_HISTORY["subreddit_last"][sub] = pid
    save_post_history(POST_HISTORY)

def was_recent(pid: str) -> bool:
    ts = POST_HISTORY["posted_ids"].get(pid)
    if not ts:
        return False
    try:
        dt = datetime.fromisoformat(ts)
        return datetime.utcnow() - dt < timedelta(hours=POST_COOLDOWN_HOURS)
    except:
        return False

# -----------------------------------------
# TIME HELPERS
# -----------------------------------------
def now_ist() -> datetime:
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def minutes_since_midnight(dt: datetime) -> int:
    return dt.hour * 60 + dt.minute

def parse_hhmm(s: str) -> int:
    h, m = s.split(":")
    return int(h) * 60 + int(m)

# -----------------------------------------
# TWITTER POST (SYNC)
# -----------------------------------------
def get_oauth2_headers() -> Optional[Dict[str, str]]:
    bearer = os.getenv("X_BEARER_TOKEN")
    if not bearer:
        return None
    return {
        "Authorization": f"Bearer {bearer}",
        "Content-Type": "application/json",
    }

def post_tweet_sync(text: str, media_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    headers = get_oauth2_headers()
    if not headers:
        raise RuntimeError("Missing X_BEARER_TOKEN")

    payload: Dict[str, Any] = {"text": text}
    if media_ids:
        payload["media"] = {"media_ids": media_ids}

    import json
    resp = requests.post(TWITTER_POST_URL, headers=headers, json=payload, timeout=20)
    if resp.status_code >= 300:
        raise RuntimeError(f"Twitter error: {resp.status_code} {resp.text}")

    return resp.json().get("data", {})

# -----------------------------------------
# EMERGENCY MODE  (SYNC)
# -----------------------------------------
def retweet_humans_no_context() -> Optional[str]:
    oauth1 = OAuth1(
        os.getenv("X_API_KEY"),
        os.getenv("X_API_SECRET"),
        os.getenv("X_ACCESS_TOKEN"),
        os.getenv("X_ACCESS_SECRET"),
        signature_type="auth_header",
    )

    params = {
        "screen_name": HUMANS_NO_CONTEXT,
        "count": 5,
        "exclude_replies": "true",
        "include_rts": "false",
        "tweet_mode": "extended",
    }

    r = requests.get(HNC_TIMELINE, params=params, auth=oauth1)
    if r.status_code >= 300:
        return None

    timeline = r.json()
    if not timeline:
        return None

    tid = timeline[0].get("id_str")
    if not tid:
        return None

    ret = requests.post(HNC_RETWEET.format(id=tid), auth=oauth1)
    if ret.status_code >= 300:
        return None

    return tid

# -----------------------------------------
# FIND DUE SUBREDDITS
# -----------------------------------------
def find_due_subreddits(now: datetime) -> List[Tuple[str, Dict[str, Any]]]:
    now_min = minutes_since_midnight(now)
    targets = []

    for name, cfg in SUBREDDITS.items():
        slot_min = parse_hhmm(cfg["post_time"])
        if abs(now_min - slot_min) <= SLOT_TOLERANCE_MINUTES:

            last = POST_HISTORY["subreddit_last"].get(name)
            if last and was_recent(last):
                continue

            targets.append((name, cfg))

    return targets

# -----------------------------------------
# CORE SLOT HANDLER (ASYNC)
# -----------------------------------------
async def handle_slot_async(name: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    slot_id = f"slot_{name}"

    async_http = create_aiohttp_session()
    async with async_http as session:

        # Fetch posts async
        from components.part1_async_http import fetch_subreddit_posts_async
        posts = await fetch_subreddit_posts_async(cfg, session)
        if not posts:
            log_session(slot_id, "fail", "reddit_fetch_empty", {"subreddit": name})
            return {"subreddit": name, "status": "reddit_fetch_empty"}

        # Selection pipeline async
        post, tweet, media_ids = await pick_best_post_and_tweet_text_async(
            posts,
            cfg,
            session_for_download=session,
        )

    if not post or not tweet:
        # Emergency fallback
        tid = retweet_humans_no_context()
        if tid:
            log_session(slot_id, "success", "emergency_retweet", {"tweet_id": tid})
            return {"subreddit": name, "status": "emergency_retweet", "tweet_id": tid}
        else:
            log_session(slot_id, "fail", "emergency_failed", {})
            return {"subreddit": name, "status": "emergency_failed"}

    # Post tweet (sync)
    try:
        tdata = post_tweet_sync(tweet, media_ids)
        tweet_id = tdata.get("id")
        tweet_url = f"https://x.com/i/web/status/{tweet_id}"

        mark_posted(post["id"], name)

        log_session(
            slot_id,
            "success",
            None,
            {"post_id": post["id"], "tweet_url": tweet_url},
        )

        return {"subreddit": name, "status": "tweeted", "tweet_url": tweet_url}

    except Exception as e:
        logger.error("Tweet post failed: %s", e)
        log_session(slot_id, "fail", "tweet_post_failed", {"error": str(e)})
        return {"subreddit": name, "status": "error", "error": str(e)}

# -----------------------------------------
# MAIN AWAKE HANDLER (ASYNC)
# -----------------------------------------
async def handle_awake_async() -> Dict[str, Any]:
    now = now_ist()
    targets = find_due_subreddits(now)

    if not targets:
        return {"status": "no_slot", "time": now.isoformat()}

    results = []
    for name, cfg in targets:
        res = await handle_slot_async(name, cfg)
        results.append(res)

    return {"status": "done", "time": now.isoformat(), "results": results}
