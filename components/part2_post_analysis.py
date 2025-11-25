#!/usr/bin/env python3
"""
part2_post_analysis.py

Post-cleaning, filtering, scoring and image-extraction logic.
Designed for high-performance async pipelines with aggressive caching.

Exports:
  - clean_body_cached(text)
  - passes_hard_filters_cached(post)
  - compute_priority_group(post, body)
  - compute_engagement_score(post)
  - extract_images_cached(post)
  - analyze_post_async(post)
"""

from __future__ import annotations

import re
import logging
from typing import Any, Dict, List, Optional
from functools import lru_cache

logger = logging.getLogger("reddit-x-bot.part2")
logger.setLevel(logging.INFO)

# -----------------------------------------------------------
# 1. BODY CLEANING (CACHED)
# -----------------------------------------------------------

@lru_cache(maxsize=2000)
def clean_body_cached(text: Optional[str]) -> str:
    """
    Clean selftext/body:
      - Remove EDIT: ... sections
      - Remove markdown links
      - Normalize spacing
      - Strip newlines
    Fully cached for speed.
    """
    if not text:
        return ""

    t = text

    # Remove EDIT: sections
    t = re.sub(r"EDIT:.*", "", t, flags=re.IGNORECASE | re.DOTALL)

    # Remove markdown links [text](url)
    t = re.sub(r"\[.*?\]\(.*?\)", "", t)

    # Replace newlines with space
    t = t.replace("\n", " ")

    # Collapse multiple spaces
    t = re.sub(r"\s{2,}", " ", t)

    return t.strip()


# -----------------------------------------------------------
# 2. HARD FILTERS (CACHED + OPTIMIZED)
# -----------------------------------------------------------

VIDEO_EXTS = (".mp4", ".mov", ".webm")
IMG_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".gifv")
GIF_LIKE = (".gif", ".gifv", ".webp")

VIDEO_DOMAINS = [
    "youtube.com", "youtu.be", "tiktok.com", "vimeo.com",
    "dailymotion.com", "streamable.com",
    "facebook.com", "fb.watch",
    "instagram.com", "x.com", "twitter.com",
]


def _post_is_video(post: Dict[str, Any]) -> bool:
    url = (post.get("url") or "").lower()

    # Direct extensions
    if any(url.endswith(ext) for ext in VIDEO_EXTS):
        return True

    if post.get("is_video"):
        return True

    # secure_media or media may contain video
    media = post.get("secure_media") or post.get("media")
    if isinstance(media, dict):
        if media.get("reddit_video"):
            return True
        if str(media.get("type", "")).startswith("video"):
            return True

    # preview-based detection
    preview = post.get("preview")
    if isinstance(preview, dict):
        images = preview.get("images") or []
        for img in images:
            if isinstance(img, dict):
                variants = img.get("variants", {})
                if "mp4" in variants:
                    return True

    return False


def _is_external_video_url(url: str) -> bool:
    u = url.lower()
    return any(d in u for d in VIDEO_DOMAINS)


def _post_is_gif_like(post: Dict[str, Any]) -> bool:
    url = (post.get("url") or "").lower()
    if any(url.endswith(ext) for ext in GIF_LIKE):
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


def _post_has_image(post: Dict[str, Any]) -> bool:
    url = (post.get("url") or "").lower()
    if any(url.endswith(ext) for ext in IMG_EXTS):
        return True

    preview = post.get("preview")
    if isinstance(preview, dict) and preview.get("images"):
        return True

    media_meta = post.get("media_metadata")
    if isinstance(media_meta, dict) and media_meta:
        return True

    return False


def _is_gallery_post(post: Dict[str, Any]) -> bool:
    if post.get("is_gallery"):
        return True
    if isinstance(post.get("gallery_data"), dict):
        return True
    if isinstance(post.get("media_metadata"), dict) and post.get("media_metadata"):
        return True
    return False


def _post_is_crosspost(post: Dict[str, Any]) -> bool:
    if post.get("crosspost_parent"):
        return True
    cpl = post.get("crosspost_parent_list")
    return isinstance(cpl, list) and len(cpl) > 0


def _post_is_poll(post: Dict[str, Any]) -> bool:
    if post.get("poll_data"):
        return True
    polls = post.get("polls")
    return isinstance(polls, list) and polls


def _post_deleted(post: Dict[str, Any]) -> bool:
    title = (post.get("title") or "").strip().lower()
    body = (post.get("selftext") or "").strip().lower()

    if title in ("[deleted]", "[removed]"):
        return True
    if body in ("[deleted]", "[removed]"):
        return True
    if post.get("removed_by_category"):
        return True

    return False


def _post_is_promoted(post: Dict[str, Any]) -> bool:
    if post.get("is_created_from_ads_ui"):
        return True
    if post.get("promoted"):
        return True
    if post.get("is_ad"):
        return True
    if post.get("adserver_click_url"):
        return True
    return False


@lru_cache(maxsize=5000)
def passes_hard_filters_cached(post_json: str) -> bool:
    """
    Cached version of hard filter evaluation.

    NOTE: Reddit posts are dicts. To cache, we convert them to a stable string key.
    The calling code MUST pass post_json = json.dumps(post, sort_keys=True)
    """
    import json
    post = json.loads(post_json)

    url = post.get("url") or ""

    if _post_is_video(post):
        return False
    if _is_external_video_url(url):
        return False
    if _post_is_gif_like(post):
        return False
    if _post_is_poll(post):
        return False
    if _post_is_crosspost(post):
        return False

    if post.get("spoiler"):
        return False
    if post.get("over_18"):
        return False

    if post.get("stickied"):
        return False
    if post.get("distinguished") not in (None, "", "null"):
        return False

    if _post_deleted(post):
        return False
    if _post_is_promoted(post):
        return False
    if post.get("contest_mode"):
        return False

    return True


# -----------------------------------------------------------
# 3. IMAGE EXTRACTION (CACHED)
# -----------------------------------------------------------

@lru_cache(maxsize=5000)
def extract_images_cached(post_json: str) -> List[str]:
    """
    Return up to 4 deduplicated image URLs from the post.
    Cached for speed.
    """
    import json
    post = json.loads(post_json)

    urls: List[str] = []
    url = post.get("url") or ""

    # direct image
    if any(url.lower().endswith(ext) for ext in IMG_EXTS):
        urls.append(url)

    # preview
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

    # gallery
    media_meta = post.get("media_metadata")
    if isinstance(media_meta, dict):
        for meta in media_meta.values():
            if not isinstance(meta, dict):
                continue
            s = meta.get("s") or {}
            u = s.get("u") or s.get("url")
            if u:
                urls.append(u.replace("&amp;", "&"))

    deduped = []
    for u in urls:
        if u not in deduped:
            deduped.append(u)
        if len(deduped) >= 4:
            break

    return deduped


# -----------------------------------------------------------
# 4. PRIORITY + ENGAGEMENT SCORE
# -----------------------------------------------------------

def compute_priority_group(post: Dict[str, Any], body: str) -> int:
    """
    Priority rules:
      3: Image/Gallery with NO body
      2: Image/Gallery with body
      1: Short text/link (body ≤ 200 chars)
    """
    has_gallery = _is_gallery_post(post)
    has_image = _post_has_image(post)
    has_body = bool(body)

    if (has_gallery or has_image) and not has_body:
        return 3
    if has_gallery or has_image:
        return 2

    return 1


def compute_engagement_score(post: Dict[str, Any]) -> float:
    ups = post.get("ups") or 0
    cm = post.get("num_comments") or 0
    upr = post.get("upvote_ratio") or 0.0
    return (ups * 0.65) + (cm * 0.35) + (upr * 10.0)


# -----------------------------------------------------------
# 5. UNIFIED POST ANALYZER (ASYNC-FRIENDLY)
# -----------------------------------------------------------

async def analyze_post_async(post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Async-friendly wrapper for all cached operations.

    Returns structure:
      {
        "post": post,
        "clean_body": <str>,
        "passes_filters": <bool>,
        "priority": <int>,
        "score": <float>,
        "images": <list>
      }
    """
    import json

    # Cached body cleanup
    body = clean_body_cached(post.get("selftext"))

    # Hard filters (cached)
    post_json = json.dumps(post, sort_keys=True)
    if not passes_hard_filters_cached(post_json):
        return None

    # Priority group
    priority = compute_priority_group(post, body)

    # Engagement score
    score = compute_engagement_score(post)

    # Images (cached)
    images = extract_images_cached(post_json)

    return {
        "post": post,
        "clean_body": body,
        "priority": priority,
        "score": score,
        "images": images,
    }
