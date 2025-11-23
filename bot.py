import os
import time
import math
import logging
from datetime import datetime, timedelta

import requests
from requests_oauthlib import OAuth1
from flask import Flask, jsonify, request

# ==========================
# Config
# ==========================

REDDIT_URL = "https://www.reddit.com/r/technology/new.json?limit=100"
USER_AGENT = "AkiRedditBot/1.0 (by u/WayOk4302)"

POSTING_START_HOUR_IST = 9   # 09:00 IST
POSTING_END_HOUR_IST = 21    # 21:00 IST
WINDOW_HOURS = 8             # last 8 hours
MAX_POSTS = 5                # top 5 engaging

REDDIT_MAX_RETRIES = 3
REDDIT_BACKOFF_SECONDS = 5

# basic logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reddit-to-x-bot")

app = Flask(__name__)


# ==========================
# Time Helpers
# ==========================

def now_ist():
    """
    Compute current time in IST (UTC+5:30) without extra deps.
    """
    return datetime.utcnow() + timedelta(hours=5, minutes=30)


def is_within_posting_window(ist_dt: datetime) -> bool:
    """
    Only tweet between 09:00 and 21:00 IST (inclusive).
    """
    hour = ist_dt.hour
    return POSTING_START_HOUR_IST <= hour <= POSTING_END_HOUR_IST


def current_slot_index(ist_dt: datetime) -> int:
    """
    Map current IST time to a slot index 0..4 (for 9,12,15,18,21)
    Slot size = 3 hours.
    09:00-11:59 -> slot 0
    12:00-14:59 -> slot 1
    15:00-17:59 -> slot 2
    18:00-20:59 -> slot 3
    21:00-23:59 -> slot 4 (we’ll only be called up to 21:00 ideally)
    """
    hours_since_start = ist_dt.hour - POSTING_START_HOUR_IST
    if hours_since_start < 0:
        return -1
    return hours_since_start // 3


# ==========================
# Reddit Fetch
# ==========================

def fetch_reddit_json_with_retry():
    """
    Fetch r/technology JSON with a small retry for rate limits / transient errors.
    """
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(1, REDDIT_MAX_RETRIES + 1):
        try:
            resp = requests.get(REDDIT_URL, headers=headers, timeout=15)
            # If rate-limited or server error, retry
            if resp.status_code in (429, 500, 502, 503, 504):
                logger.warning(
                    "Reddit returned %s. Attempt %s/%s.",
                    resp.status_code,
                    attempt,
                    REDDIT_MAX_RETRIES,
                )
                if attempt < REDDIT_MAX_RETRIES:
                    time.sleep(REDDIT_BACKOFF_SECONDS * attempt)
                    continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning("Reddit fetch error on attempt %s/%s: %s",
                           attempt, REDDIT_MAX_RETRIES, e)
            if attempt < REDDIT_MAX_RETRIES:
                time.sleep(REDDIT_BACKOFF_SECONDS * attempt)
            else:
                raise

    raise RuntimeError("Failed to fetch Reddit JSON after retries")


def fetch_top_reddit_posts(window_hours=WINDOW_HOURS, limit=MAX_POSTS):
    """
    Fetch top posts from r/technology in the last `window_hours`,
    sorted by simple engagement score (score + 2 * comments).
    Returns up to `limit` posts.
    """
    data = fetch_reddit_json_with_retry()

    now_ts = time.time()
    cutoff = now_ts - window_hours * 3600

    posts = []
    for child in data.get("data", {}).get("children", []):
        d = child.get("data", {})
        created = d.get("created_utc", 0)

        # only last N hours
        if created < cutoff:
            continue

        # skip stickied or NSFW
        if d.get("stickied") or d.get("over_18"):
            continue

        score = d.get("score", 0)
        comments = d.get("num_comments", 0)
        engagement = score + comments * 2

        title = d.get("title", "").strip()
        url = "https://www.reddit.com" + d.get("permalink", "")

        posts.append(
            {
                "id": d.get("id"),
                "title": title,
                "url": url,
                "score": score,
                "comments": comments,
                "engagement": engagement,
            }
        )

    # sort by engagement desc
    posts.sort(key=lambda p: p["engagement"], reverse=True)
    return posts[:limit]


# ==========================
# Tweet Formatting & Posting
# ==========================

def make_tweet_text(post):
    """
    Format tweet as: Title + link, but keep under 280 chars.
    """
    title = post["title"]
    url = post["url"]

    base = f"{title} {url}"
    if len(base) <= 280:
        return base

    # leave space for link + 1 space
    max_title_len = 280 - (len(url) + 1)
    truncated_title = title[: max_title_len - 1] + "…"
    return f"{truncated_title} {url}"


def post_to_x(text):
    """
    Post a tweet using OAuth1 and return tweet URL.
    Env vars (GitHub or Render):
        X_API_KEY
        X_API_SECRET
        X_ACCESS_TOKEN
        X_ACCESS_SECRET
    """
    api_key = os.environ["X_API_KEY"]
    api_secret = os.environ["X_API_SECRET"]
    access_token = os.environ["X_ACCESS_TOKEN"]
    access_secret = os.environ["X_ACCESS_SECRET"]

    auth = OAuth1(
        api_key,
        api_secret,
        access_token,
        access_secret,
        signature_type="auth_header",
    )

    url = "https://api.twitter.com/2/tweets"
    resp = requests.post(
        url,
        json={"text": text},
        auth=auth,
        timeout=15,
    )

    if resp.status_code >= 300:
        logger.error("X API error %s: %s", resp.status_code, resp.text)
        raise RuntimeError(f"X API error {resp.status_code}: {resp.text}")

    data = resp.json()
    tweet_id = data.get("data", {}).get("id")
    tweet_url = f"https://x.com/i/web/status/{tweet_id}"
    logger.info("Tweeted: %s", tweet_url)
    return tweet_url


# ==========================
# Core "wake and run" logic
# ==========================

def handle_awake():
    """
    Main flow:
    - Check IST time and posting window.
    - Fetch top 5 posts from last 8h.
    - Pick one post based on time slot (stateless).
    - Tweet it.
    """
    ist = now_ist()
    logger.info("Awake call at IST %s", ist.isoformat())

    if not is_within_posting_window(ist):
        logger.info("Outside posting window, skipping tweet.")
        return {
            "status": "outside_posting_window",
            "now_ist": ist.isoformat(),
        }

    try:
        posts = fetch_top_reddit_posts(window_hours=WINDOW_HOURS, limit=MAX_POSTS)
    except Exception as e:
        logger.error("Failed to fetch Reddit posts: %s", e)
        return {
            "status": "reddit_error",
            "error": str(e),
            "now_ist": ist.isoformat(),
        }

    if not posts:
        logger.info("No eligible posts found in the last %s hours.", WINDOW_HOURS)
        return {
            "status": "no_posts_found",
            "now_ist": ist.isoformat(),
        }

    slot = current_slot_index(ist)
    if slot < 0:
        logger.info("Slot index negative (%s), skipping tweet.", slot)
        return {
            "status": "not_scheduled_for_this_time",
            "slot": slot,
            "now_ist": ist.isoformat(),
        }

    # Choose post based on slot (stateless schedule)
    index = min(slot, len(posts) - 1)
    chosen = posts[index]

    tweet_text = make_tweet_text(chosen)

    try:
        tweet_url = post_to_x(tweet_text)
    except Exception as e:
        logger.error("Failed to post tweet: %s", e)
        return {
            "status": "x_error",
            "error": str(e),
            "post": chosen,
            "now_ist": ist.isoformat(),
        }

    return {
        "status": "tweeted",
        "tweet_url": tweet_url,
        "slot": slot,
        "post": chosen,
        "now_ist": ist.isoformat(),
    }


# ==========================
# Flask Endpoints
# ==========================

@app.route("/", methods=["GET"])
def index():
    """
    Simple health check / ping endpoint.
    """
    return jsonify({"status": "ok", "message": "reddit → X bot is alive"})


@app.route("/awake", methods=["GET", "POST"])
def awake():
    """
    This is what your uptime monitor / external trigger calls every 3 hours.
    """
    result = handle_awake()
    return jsonify(result)


# ==========================
# Local Dev Entry Point
# ==========================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
