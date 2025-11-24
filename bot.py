# bot.py
"""
Reddit -> X bot (full rewrite)
- Robust Reddit fetch
- Video handling with ffmpeg re-encode fallback
- Twitter v1.1 chunked media upload (INIT/APPEND/FINALIZE/STATUS)
- Wait for video processing before posting
- Dedupe with posted_history.json (per-subreddit TTL)
- Uses session_logger.log_session for audit logging
"""

import os
import time
import random
import logging
import re
import json
import tempfile
import shutil
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify
from requests_oauthlib import OAuth1

# make sure session_logger.py exists in the same folder
from session_logger import log_session

# ========= CONFIG =========

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

SLOT_TOLERANCE_MINUTES = 20

MEDIA_UPLOAD_URL = "https://upload.twitter.com/1.1/media/upload.json"
TWEET_POST_URL_V2 = "https://api.twitter.com/2/tweets"

HISTORY_PATH = Path(__file__).parent / "posted_history.json"
POST_HISTORY_TTL_HOURS = 48

CHUNK_SIZE = 5 * 1024 * 1024  # 5MB per chunk

# ffmpeg detection
FFMPEG_PATH = shutil.which("ffmpeg")

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reddit-x-bot")

app = Flask(__name__)

# ========= TIME HELPERS =========


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


# ========= HISTORY HELPERS =========


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


# ========= REDDIT FETCH =========


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


# ========= FILTERS =========


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


# ========= ENGAGEMENT SCORE =========


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
    if post.get("secure_media") and isinstance(post["secure_media"], dict):
        if post["secure_media"].get("reddit_video"):
            return True
    preview = post.get("preview")
    if isinstance(preview, dict) and preview.get("reddit_video_preview"):
        return True
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

    if post_has_reddit_video(p):
        score += IMAGE_SCORE_BONUS / 2

    return float(score)


def sort_posts_by_score(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for p in posts:
        p["_engagement_score"] = score_post(p)
    posts_sorted = sorted(posts, key=lambda x: x.get("_engagement_score", 0), reverse=True)
    return posts_sorted


# ========= TWEET BUILDER =========


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


# ========= MEDIA EXTRACTION =========


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

    media_meta = post.get("media_metadata") or {}
    for meta in media_meta.values():
        if not isinstance(meta, dict):
            continue
        for key in ("s", "p"):
            s = meta.get(key) or {}
            u = s.get("u") or s.get("url")
            if u and u.endswith(".mp4"):
                return u.replace("&amp;", "&")

    return None


# ========= TWITTER AUTH =========


def get_oauth1_client() -> Optional[OAuth1]:
    api_key = os.getenv("X_API_KEY")
    api_secret = os.getenv("X_API_SECRET")
    access_token = os.getenv("X_ACCESS_TOKEN")
    access_secret = os.getenv("X_ACCESS_SECRET")

    if not all([api_key, api_secret, access_token, access_secret]):
        return None

    return OAuth1(api_key, api_secret, access_token, access_secret, signature_type="auth_header")


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


# ========= TWITTER MEDIA CHUNKED UPLOAD =========


def _twitter_media_init(oauth1: OAuth1, total_bytes: int, media_type: str, media_category: str) -> Optional[str]:
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


def _twitter_media_status(oauth1: OAuth1, media_id: str) -> Dict[str, Any]:
    params = {"command": "STATUS", "media_id": str(media_id)}
    resp = requests.get(MEDIA_UPLOAD_URL, params=params, auth=oauth1, timeout=30)
    try:
        return resp.json()
    except Exception:
        return {}


def wait_for_video_processing(oauth1: OAuth1, media_id: str, timeout_sec: int = 120) -> bool:
    start = time.time()
    while time.time() - start < timeout_sec:
        status = _twitter_media_status(oauth1, media_id)
        processing = status.get("processing_info")
        if not processing:
            return True
        state = processing.get("state")
        if state == "succeeded":
            return True
        if state == "failed":
            logger.error("Video processing failed: %s", status)
            return False
        check_after = processing.get("check_after_secs", 1)
        time.sleep(check_after)
    logger.error("Video processing timed out for media_id=%s", media_id)
    return False


def _download_url_to_tempfile(url: str, suffix: str = "") -> Optional[str]:
    try:
        headers = {"User-Agent": USER_AGENT}
        r = requests.get(url, headers=headers, timeout=40, stream=True)
        r.raise_for_status()
        tf = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        with open(tf.name, "wb") as fh:
            for chunk in r.iter_content(1024 * 64):
                if not chunk:
                    break
                fh.write(chunk)
        return tf.name
    except Exception as e:
        logger.warning("Failed to download %s: %s", url, e)
        return None


def ffmpeg_reencode_to_mp4(in_path: str, out_path: str) -> bool:
    if not FFMPEG_PATH:
        logger.info("ffmpeg not available, cannot re-encode")
        return False
    # Re-encode to h264 baseline + aac audio, fast preset, reasonable bitrate.
    cmd = [
        FFMPEG_PATH,
        "-y",
        "-i",
        in_path,
        "-c:v",
        "libx264",
        "-profile:v",
        "baseline",
        "-level",
        "3.1",
        "-preset",
        "veryfast",
        "-crf",
        "23",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        out_path,
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError as e:
        logger.warning("ffmpeg re-encode failed: %s", e)
        return False


def upload_media_to_twitter(media_urls: List[str]) -> List[str]:
    oauth1 = get_oauth1_client()
    if not oauth1:
        logger.info("No OAuth1 keys present; posting without media.")
        return []

    media_ids: List[str] = []
    for url in media_urls:
        try:
            logger.info("Downloading media %s", url)
            tmp_in = _download_url_to_tempfile(url, suffix=os.path.splitext(url)[1] if "." in url else "")
            if not tmp_in:
                logger.warning("Download failed for %s", url)
                continue

            # determine if likely video
            is_video = tmp_in.lower().endswith((".mp4", ".mov", ".mkv")) or True if "v.redd.it" in url else False
            # Quick MIME sniff
            try:
                import mimetypes
                mtype = mimetypes.guess_type(tmp_in)[0] or ""
            except Exception:
                mtype = ""

            # If ext suggests video or size large, treat as video
            total_bytes = Path(tmp_in).stat().st_size

            treat_as_video = False
            if url.lower().endswith((".mp4", ".mov")) or "v.redd.it" in url or total_bytes > 200 * 1024:
                treat_as_video = True

            if treat_as_video:
                # Ensure mp4 and codec compatibility: re-encode into safe mp4 if ffmpeg available
                tmp_fixed = None
                if FFMPEG_PATH:
                    tmp_fixed = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name
                    ok = ffmpeg_reencode_to_mp4(tmp_in, tmp_fixed)
                    if ok:
                        os.unlink(tmp_in)
                        tmp_in = tmp_fixed
                    else:
                        # reencode failed -> keep original file (may still work)
                        if tmp_fixed and os.path.exists(tmp_fixed):
                            try:
                                os.unlink(tmp_fixed)
                            except Exception:
                                pass

                # Now upload with chunked flow
                media_type = "video/mp4"
                media_category = "tweet_video"
                with open(tmp_in, "rb") as fh:
                    content = fh.read()
                total_bytes = len(content)

                mid = _twitter_media_init(oauth1, total_bytes, media_type, media_category)
                if not mid:
                    logger.warning("INIT failed for %s", url)
                    try:
                        os.unlink(tmp_in)
                    except Exception:
                        pass
                    continue

                segment = 0
                offset = 0
                success_append = True
                while offset < total_bytes:
                    chunk = content[offset: offset + CHUNK_SIZE]
                    ok = _twitter_media_append(oauth1, mid, segment, chunk)
                    if not ok:
                        success_append = False
                        break
                    segment += 1
                    offset += len(chunk)
                    time.sleep(0.1)
                if not success_append:
                    logger.warning("Append failed for media %s", url)
                    try:
                        os.unlink(tmp_in)
                    except Exception:
                        pass
                    continue

                okf = _twitter_media_finalize(oauth1, mid)
                if not okf:
                    logger.warning("Finalize failed for media_id=%s", mid)
                    try:
                        os.unlink(tmp_in)
                    except Exception:
                        pass
                    continue

                # WAIT for processing
                okp = wait_for_video_processing(oauth1, mid, timeout_sec=180)
                if okp:
                    media_ids.append(str(mid))
                else:
                    logger.warning("Video processing failed/timed out for %s", url)
                    # Try re-encode (if not already re-encoded) and re-upload once
                    if FFMPEG_PATH:
                        # if we already re-encoded, don't loop forever
                        tmp_try = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name
                        ok2 = ffmpeg_reencode_to_mp4(tmp_in, tmp_try)
                        if ok2:
                            try:
                                os.unlink(tmp_in)
                            except Exception:
                                pass
                            # attempt again: re-init/upload
                            with open(tmp_try, "rb") as fh2:
                                c2 = fh2.read()
                            total_bytes2 = len(c2)
                            mid2 = _twitter_media_init(oauth1, total_bytes2, "video/mp4", "tweet_video")
                            if mid2:
                                seg2 = 0
                                of2 = 0
                                ok_append2 = True
                                while of2 < total_bytes2:
                                    ch = c2[of2: of2 + CHUNK_SIZE]
                                    if not _twitter_media_append(oauth1, mid2, seg2, ch):
                                        ok_append2 = False
                                        break
                                    seg2 += 1
                                    of2 += len(ch)
                                if ok_append2 and _twitter_media_finalize(oauth1, mid2) and wait_for_video_processing(oauth1, mid2, timeout_sec=180):
                                    media_ids.append(str(mid2))
                                else:
                                    logger.warning("Re-upload after ffmpeg still failed for %s", url)
                            try:
                                os.unlink(tmp_try)
                            except Exception:
                                pass
                    try:
                        os.unlink(tmp_in)
                    except Exception:
                        pass
                continue  # done with this media entry

            # else treat as image (small)
            with open(tmp_in, "rb") as fh:
                content = fh.read()
            resp = requests.post(MEDIA_UPLOAD_URL, files={"media": content}, auth=oauth1, timeout=30)
            if resp.status_code >= 300:
                logger.warning("Image upload failed (%s): %s", resp.status_code, resp.text)
                try:
                    os.unlink(tmp_in)
                except Exception:
                    pass
                continue
            data = resp.json()
            mid = data.get("media_id_string") or data.get("media_id")
            if mid:
                media_ids.append(str(mid))
            try:
                os.unlink(tmp_in)
            except Exception:
                pass

        except Exception as e:
            logger.warning("Error uploading media from %s: %s", url, e)
            continue

    return media_ids


# ========= POST TWEET =========


def twitter_post_tweet(text: str, media_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    if media_ids:
        oauth1 = get_oauth1_client()
        if oauth1:
            payload: Dict[str, Any] = {"text": text, "media": {"media_ids": media_ids}}
            resp = requests.post(TWEET_POST_URL_V2, json=payload, auth=oauth1, timeout=30)
        else:
            logger.info("No OAuth1 for media; tweeting text-only via OAuth2 fallback.")
            auth_type, auth_obj = get_twitter_auth(prefer_oauth1=False)
            headers = auth_obj if isinstance(auth_obj, dict) else {}
            payload = {"text": text}
            resp = requests.post(TWEET_POST_URL_V2, json=payload, headers=headers, timeout=30)
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


# ========= CORE SLOT LOGIC =========


def pick_best_post_and_tweet_text(posts: List[Dict[str, Any]], cfg: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    sorted_posts = sort_posts_by_score(posts)
    for p in sorted_posts:
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
    slot_id = f"slot_{name}"

    posts = fetch_subreddit_posts(cfg)
    if not posts:
        log_session(slot_id, "fail", "reddit_fetch_empty", {"subreddit": name})
        return {"subreddit": name, "status": "reddit_fetch_empty"}

    primary = filter_posts_by_window(posts, PRIMARY_WINDOW_HOURS)
    candidates = primary
    if not candidates:
        fallback = filter_posts_by_window(posts, FALLBACK_WINDOW_HOURS)
        candidates = fallback

    if not candidates:
        log_session(slot_id, "fail", "no_candidates", {"subreddit": name})
        return {"subreddit": name, "status": "no_candidates"}

    best_post, tweet_text = pick_best_post_and_tweet_text(candidates, cfg)
    if not best_post or not tweet_text:
        log_session(slot_id, "fail", "no_tweetable_post", {"subreddit": name})
        return {"subreddit": name, "status": "no_tweetable_post"}

    reddit_id = best_post.get("id")
    subreddit_key = cfg.get("url", "").split("/")[-2] if "/" in cfg.get("url", "") else name
    if reddit_id and was_posted_recently(subreddit_key, reddit_id):
        log_session(slot_id, "fail", "already_posted_recently", {"subreddit": name, "post_id": reddit_id})
        return {"subreddit": name, "status": "already_posted_recently", "post_id": reddit_id}

    media_urls: List[str] = []
    rv = extract_reddit_video_url(best_post)
    if rv:
        media_urls.append(rv)
    else:
        imgs = extract_image_urls(best_post)
        media_urls.extend(imgs)

    media_ids: List[str] = []
    if media_urls:
        try:
            media_ids = upload_media_to_twitter(media_urls)
            if not media_ids and media_urls:
                log_session(slot_id, "fail", "media_upload_partial_or_failed", {"image_urls": media_urls})
        except Exception as e:
            log_session(slot_id, "fail", "media_upload_failed", {"error": str(e)})
            media_ids = []

    try:
        data = twitter_post_tweet(tweet_text, media_ids=media_ids or None)
        tweet_id = data.get("id")
        tweet_url = f"https://x.com/i/web/status/{tweet_id}" if tweet_id else None

        record_posted(subreddit_key, reddit_id, tweet_url)
        log_session(slot_id, "success", None, {"post_id": reddit_id, "tweet_url": tweet_url})
        return {"subreddit": name, "status": "tweeted", "tweet_url": tweet_url, "post_id": reddit_id}
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
        return {"status": "no_slot", "time_ist": ist.isoformat(), "tolerance_minutes": SLOT_TOLERANCE_MINUTES}

    results: List[Dict[str, Any]] = []
    for name, cfg in targets:
        res = handle_slot_for_subreddit(name, cfg)
        results.append(res)

    return {"status": "done", "time_ist": ist.isoformat(), "tolerance_minutes": SLOT_TOLERANCE_MINUTES, "results": results}


# ========= FLASK =========


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
