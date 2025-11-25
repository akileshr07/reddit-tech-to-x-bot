#!/usr/bin/env python3
"""
part3_media_pipeline.py

Async image downloader, threaded media uploader, optimized tweet builder,
and async selection pipeline.

Integrates with:
  - components.part2_post_analysis: analyze_post_async, extract_images_cached, clean_body_cached
  - Uses aiohttp for async downloads, requests + ThreadPoolExecutor for safe uploads
"""

from __future__ import annotations

import asyncio
import async_timeout
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import requests
from requests_oauthlib import OAuth1

# Local analysis module (Part 2)
from components.part2_post_analysis import analyze_post_async, extract_images_cached, clean_body_cached

# -------------------------
# Config / Tunables
# -------------------------
IMAGE_DOWNLOAD_TIMEOUT = 20
IMAGE_DOWNLOAD_RETRIES = 2
IMAGE_DOWNLOAD_CONCURRENCY = 4
UPLOAD_THREADPOOL_MAX = 4
TWEET_MAX_LEN = 280
BODY_CHAR_LIMIT = 220

MEDIA_UPLOAD_URL = "https://upload.twitter.com/1.1/media/upload.json"

logger = logging.getLogger("reddit-x-bot.part3")
logger.setLevel(logging.INFO)


# -------------------------
# OAuth1 helper (for media upload)
# -------------------------
def get_oauth1_from_env() -> Optional[OAuth1]:
    api_key = os.getenv("X_API_KEY")
    api_secret = os.getenv("X_API_SECRET")
    access_token = os.getenv("X_ACCESS_TOKEN")
    access_secret = os.getenv("X_ACCESS_SECRET")
    if not all([api_key, api_secret, access_token, access_secret]):
        return None
    return OAuth1(api_key, api_secret, access_token, access_secret, signature_type="auth_header")


# -------------------------
# Async image downloader
# -------------------------
async def _download_single_image(session: aiohttp.ClientSession, url: str, timeout: int = IMAGE_DOWNLOAD_TIMEOUT) -> Optional[bytes]:
    if not url:
        return None

    last_exc = None
    for attempt in range(1, IMAGE_DOWNLOAD_RETRIES + 2):
        try:
            async with async_timeout.timeout(timeout):
                async with session.get(url) as resp:
                    if resp.status >= 400:
                        last_exc = Exception(f"status {resp.status}")
                        logger.warning("download image: bad status %s for %s", resp.status, url)
                        # retry
                        await asyncio.sleep(0.1 * attempt)
                        continue
                    data = await resp.read()
                    if not data:
                        last_exc = Exception("empty content")
                        await asyncio.sleep(0.1 * attempt)
                        continue
                    return data
        except asyncio.TimeoutError:
            last_exc = asyncio.TimeoutError()
            logger.warning("download image: timeout for %s attempt %s", url, attempt)
            await asyncio.sleep(0.2 * attempt)
        except aiohttp.ClientError as ce:
            last_exc = ce
            logger.warning("download image: client error %s for %s attempt %s", ce, url, attempt)
            await asyncio.sleep(0.2 * attempt)
        except Exception as e:
            last_exc = e
            logger.exception("download image: unexpected error for %s: %s", url, e)
            await asyncio.sleep(0.2 * attempt)

    logger.info("download image failed for %s after retries: %s", url, last_exc)
    return None


async def download_images_async(urls: List[str], session: aiohttp.ClientSession, *, concurrency: int = IMAGE_DOWNLOAD_CONCURRENCY) -> List[bytes]:
    """
    Concurrently download image URLs. Returns list of image bytes (order preserved for successful downloads).
    Failed downloads are dropped.
    """
    if not urls:
        return []

    sem = asyncio.Semaphore(concurrency)
    results: List[Optional[bytes]] = [None] * len(urls)

    async def _worker(i: int, u: str):
        async with sem:
            results[i] = await _download_single_image(session, u)

    tasks = [asyncio.create_task(_worker(i, u)) for i, u in enumerate(urls)]
    await asyncio.gather(*tasks)
    downloaded = [b for b in results if b]
    logger.info("download_images_async: %d/%d succeeded", len(downloaded), len(urls))
    return downloaded


# -------------------------
# Threaded media uploader (sync) — safe to call from event loop using run_in_executor
# -------------------------
def _upload_single_media(oauth1: OAuth1, media_bytes: bytes, filename_hint: str = "img.jpg") -> Optional[str]:
    """
    Upload a single media blob synchronously using requests. Returns media_id_string on success.
    """
    if not oauth1:
        return None
    try:
        files = {"media": (filename_hint, media_bytes)}
        resp = requests.post(MEDIA_UPLOAD_URL, files=files, auth=oauth1, timeout=30)
        if resp.status_code >= 300:
            logger.warning("_upload_single_media: failed status %s body %.200s", resp.status_code, resp.text)
            return None
        j = resp.json()
        mid = j.get("media_id_string") or j.get("media_id")
        return str(mid) if mid else None
    except Exception as e:
        logger.exception("_upload_single_media: exception uploading media: %s", e)
        return None


def upload_images_to_twitter_threaded(media_blobs: List[bytes], *, max_workers: int = UPLOAD_THREADPOOL_MAX) -> List[str]:
    """
    Upload a list of image bytes to Twitter in parallel using threads.
    Returns list of media_id strings for successfully uploaded images.
    If OAuth1 env vars are missing, returns [].
    """
    if not media_blobs:
        return []

    oauth1 = get_oauth1_from_env()
    if not oauth1:
        logger.info("upload_images_to_twitter_threaded: no OAuth1 credentials found; skipping media upload")
        return []

    media_ids: List[str] = []
    workers = min(max_workers, len(media_blobs))
    with ThreadPoolExecutor(max_workers=workers) as exe:
        futures = [exe.submit(_upload_single_media, oauth1, blob, f"img_{i}.jpg") for i, blob in enumerate(media_blobs)]
        for f in futures:
            try:
                mid = f.result()
                if mid:
                    media_ids.append(mid)
            except Exception as e:
                logger.exception("upload_images_to_twitter_threaded: upload failed: %s", e)

    logger.info("upload_images_to_twitter_threaded: uploaded %d/%d", len(media_ids), len(media_blobs))
    return media_ids


# -------------------------
# Tweet builder (optimized + cached hashtag split)
# -------------------------
_hashtag_cache: Dict[str, List[str]] = {}


def _split_hashtags_cached(tags: str) -> List[str]:
    if tags in _hashtag_cache:
        return _hashtag_cache[tags]
    toks = [t.strip() for t in (tags or "").split() if t.strip()]
    _hashtag_cache[tags] = toks
    return toks


def _is_reddit_url(url: Optional[str]) -> bool:
    if not url:
        return False
    u = url.lower()
    return ("reddit.com" in u) or ("redd.it" in u)


def _is_external_url(url: Optional[str]) -> bool:
    return bool(url) and not _is_reddit_url(url)


def build_tweet_optimized(post: Dict[str, Any], cfg: Dict[str, Any], *, max_len: int = TWEET_MAX_LEN) -> Optional[str]:
    """
    Build tweet text following original bot rules but optimized:
    - Base text selection respects title/body/images rules
    - External URL placed on a separate line when present
    - Hashtags appended or placed on last line with external URL
    - Trimming: drop hashtags from end, then char-trim base_text
    """
    title = (post.get("title") or "").strip()
    raw_body = (post.get("selftext") or "")
    # Prefer cleaned body if available from analysis cache (clean_body_cached)
    try:
        cleaned = clean_body_cached(raw_body)
    except Exception:
        cleaned = raw_body.strip()

    has_title = bool(title)
    has_body = bool(cleaned)
    has_images = bool(post.get("is_gallery") or post.get("media_metadata") or post.get("preview"))

    if has_title and not has_body:
        base = title
    elif has_title and has_body and has_images:
        base = title
    elif has_title and has_body:
        base = cleaned
    elif has_body:
        base = cleaned
    elif has_title:
        base = title
    else:
        return None

    base = base.strip()
    if not base:
        return None

    url = post.get("url") or ""
    external_url = url if _is_external_url(url) else ""

    hashtags_raw = (cfg.get("hashtags") if isinstance(cfg, dict) else "") or ""
    hashtag_tokens = _split_hashtags_cached(hashtags_raw)

    def assemble(btxt: str, tags: List[str]) -> str:
        tags_str = " ".join(tags) if tags else ""
        if external_url:
            if tags_str:
                return f"{btxt}\n{external_url}\n{tags_str}".strip()
            else:
                return f"{btxt}\n{external_url}".strip()
        else:
            if tags_str:
                return f"{btxt} {tags_str}".strip()
            else:
                return btxt.strip()

    tweet = assemble(base, hashtag_tokens)
    if len(tweet) <= max_len:
        return tweet

    # Drop hashtags from end until fits
    tokens = list(hashtag_tokens)
    while tokens and len(assemble(base, tokens)) > max_len:
        tokens.pop()
    tweet = assemble(base, tokens)
    if len(tweet) <= max_len:
        return tweet

    # Char-by-char trim base
    b = base
    while len(assemble(b, [])) > max_len and b:
        b = b[:-1]
    tweet = assemble(b, [])
    if len(tweet) > max_len:
        tweet = tweet[:max_len]
    return tweet if tweet.strip() else None


# -------------------------
# Async selection pipeline (O(n) per window)
# -------------------------
async def pick_best_post_and_tweet_text_async(
    posts: List[Dict[str, Any]],
    cfg: Dict[str, Any],
    *,
    window_sequence_hours: List[int] = (10, 24, 48),
    now_ts: Optional[float] = None,
    session_for_download: Optional[aiohttp.ClientSession] = None,
    download_concurrency: int = IMAGE_DOWNLOAD_CONCURRENCY,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[List[str]]]:
    """
    Select the best post according to priority & score and build tweet text.
    Returns (post, tweet_text, media_ids). media_ids may be None or [].
    """
    import time
    import json

    if now_ts is None:
        now_ts = time.time()

    # Normalize created timestamps; drop posts without valid created_utc
    normalized = []
    for p in posts:
        try:
            ct = float(p.get("created_utc", 0))
        except Exception:
            continue
        p["_created_ts"] = ct
        normalized.append(p)

    if not normalized:
        return None, None, None

    # For later cached image extraction: extract_images_cached expects a JSON string (per part2)
    def post_to_json_str(post: Dict[str, Any]) -> str:
        return json.dumps(post, sort_keys=True)

    for hours in window_sequence_hours:
        cutoff = now_ts - hours * 3600
        best = None
        best_priority = -1
        best_score = float("-inf")

        # single scan to find best candidate in window
        for p in normalized:
            if p.get("_created_ts", 0) < cutoff:
                continue

            pid = p.get("id")
            if not pid:
                continue

            # analyze_post_async (cached heavy lifting)
            try:
                analysis = await analyze_post_async(p)
            except Exception as e:
                logger.exception("analyze_post_async failed for %s: %s", pid, e)
                continue

            if not analysis:
                continue

            # Text/link posts longer than BODY_CHAR_LIMIT rejected
            is_text_or_link = bool(p.get("is_self")) or (str(p.get("post_hint") or "").lower() in ("link", "rich:video"))
            if is_text_or_link and len(analysis.get("clean_body", "")) > BODY_CHAR_LIMIT:
                continue

            priority = analysis.get("priority", 1)
            score = analysis.get("score", 0.0)

            if (priority > best_priority) or (priority == best_priority and score > best_score):
                best = p
                best_priority = priority
                best_score = score

        if not best:
            continue  # expand window

        # Build tweet for best candidate only
        tweet_text = build_tweet_optimized(best, cfg)
        if not tweet_text:
            logger.info("pick_best: tweet build failed for candidate %s; expanding window", best.get("id"))
            continue

        # If there are images and session provided, download & upload; otherwise return tweet-only
        images_urls = []
        try:
            jstr = post_to_json_str(best)
            images_urls = extract_images_cached(jstr)
        except Exception as e:
            logger.exception("extract_images_cached failed: %s", e)
            images_urls = []

        media_ids: Optional[List[str]] = None
        if images_urls and session_for_download is not None:
            try:
                blobs = await download_images_async(images_urls, session_for_download, concurrency=download_concurrency)
                if blobs:
                    # Upload in threadpool (blocking) — safe for hybrid architecture
                    loop = asyncio.get_running_loop()
                    media_ids = await loop.run_in_executor(None, upload_images_to_twitter_threaded, blobs, UPLOAD_THREADPOOL_MAX)
                else:
                    media_ids = []
            except Exception as e:
                logger.exception("media download/upload failed for %s: %s", best.get("id"), e)
                media_ids = []

        return best, tweet_text, media_ids

    # nothing found
    return None, None, None
