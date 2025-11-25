from __future__ import annotations

#!/usr/bin/env python3
"""
part1_async_http.py

Async HTTP engine and async Reddit fetch for the hybrid architecture.

Provides:
  - AsyncHTTP (reusable aiohttp session manager)
  - async_fetch_json(url, session)
  - fetch_subreddit_posts_async(cfg, session, limit=30)
  - create_aiohttp_session(...)
  - run_sync(coro)  # safe helper to run async from sync (Flask)
"""


import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp
import async_timeout

# ----------------------
# Config / Tunables
# ----------------------
USER_AGENT = "AkiRedditBot/1.0 (by u/WayOk4302)"
REDDIT_LIMIT_DEFAULT = 30
ASYNC_RETRY_LIMIT = 3
ASYNC_BASE_WAIT_SECONDS = 1.0
ASYNC_TIMEOUT_SECONDS = 12
DEFAULT_CONCURRENCY = 6

logger = logging.getLogger("reddit-x-bot.part1")
logger.setLevel(logging.INFO)


# ----------------------
# aiohttp session manager
# ----------------------
class AsyncHTTP:
    """
    aiohttp ClientSession manager with a bounded connector.
    Use like:

      async_http = create_aiohttp_session()
      async with async_http as session:
          posts = await fetch_subreddit_posts_async(cfg, session)

    Note: the context manager does not auto-close the session on exit (to allow reuse).
          Call async_http.close() when you want to fully close the session.
    """

    def __init__(self, *, concurrency: int = DEFAULT_CONCURRENCY, user_agent: str = USER_AGENT):
        self._connector = aiohttp.TCPConnector(limit=concurrency)
        self._user_agent = user_agent
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            headers = {"User-Agent": self._user_agent}
            self._session = aiohttp.ClientSession(connector=self._connector, headers=headers)
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        # Keep session open for reuse; caller should call close() if needed.
        return False

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ----------------------
# Helper to run async from sync (safe)
# ----------------------
def run_sync(coro):
    """
    Run an async coroutine from synchronous code.

    Primary strategy: asyncio.run(coro)
    Fallback: if an event loop is already running, schedule the coroutine on a new thread
    and return its result (safe for typical Flask use).
    """
    try:
        return asyncio.run(coro)
    except RuntimeError:
        # If there's an active event loop (rare in Flask), run the coroutine in a new thread's loop.
        try:
            import threading

            result_container: Dict[str, Any] = {}

            def _target():
                try:
                    res = asyncio.new_event_loop().run_until_complete(coro)
                    result_container["result"] = res
                except Exception as e:
                    result_container["error"] = e

            t = threading.Thread(target=_target, daemon=True)
            t.start()
            t.join()
            if "error" in result_container:
                raise result_container["error"]
            return result_container.get("result")
        except Exception as e:
            # As a last resort, re-raise
            raise RuntimeError("run_sync fallback failed") from e


# ----------------------
# Async fetch with retries + simple backoff + HTML detection
# ----------------------
async def async_fetch_json(
    url: str,
    session: aiohttp.ClientSession,
    *,
    retry_limit: int = ASYNC_RETRY_LIMIT,
    timeout_seconds: int = ASYNC_TIMEOUT_SECONDS,
) -> Optional[Dict[str, Any]]:
    """
    Fetch JSON from URL with retry/backoff. Returns parsed JSON or None.

    Notes:
      - Detects HTML responses (Cloudflare/login pages) and treats them as transient failures.
      - Uses a simple exponential backoff between retries.
    """
    if not url:
        return None

    for attempt in range(1, retry_limit + 1):
        wait = ASYNC_BASE_WAIT_SECONDS * (2 ** (attempt - 1))
        try:
            async with async_timeout.timeout(timeout_seconds):
                async with session.get(url) as resp:
                    text = await resp.text()
                    if text.lstrip().startswith("<"):
                        # Likely HTML (blocked or Cloudflare). Retry unless final attempt.
                        logger.warning("async_fetch_json: HTML returned for %s (status=%s) attempt=%s/%s", url, resp.status, attempt, retry_limit)
                        if attempt < retry_limit:
                            await asyncio.sleep(wait)
                            continue
                        return None

                    # Parse JSON (let aiohttp infer content-type)
                    try:
                        js = await resp.json(content_type=None)
                        return js
                    except Exception as e:
                        logger.warning("async_fetch_json: json parse error for %s: %s", url, e)
                        if attempt < retry_limit:
                            await asyncio.sleep(wait)
                            continue
                        return None

        except asyncio.TimeoutError:
            logger.warning("async_fetch_json: timeout for %s attempt=%s/%s", url, attempt, retry_limit)
            if attempt < retry_limit:
                await asyncio.sleep(wait)
                continue
        except aiohttp.ClientError as ce:
            logger.warning("async_fetch_json: client error for %s: %s", url, ce)
            if attempt < retry_limit:
                await asyncio.sleep(wait)
                continue
        except Exception as e:
            logger.exception("async_fetch_json: unexpected error for %s: %s", url, e)
            return None

    logger.error("async_fetch_json: failed after %s attempts for %s", retry_limit, url)
    return None


# ----------------------
# Async fetch subreddit posts (/new/.json?limit=N)
# ----------------------
async def fetch_subreddit_posts_async(cfg: Dict[str, Any], session: aiohttp.ClientSession, *, limit: int = REDDIT_LIMIT_DEFAULT) -> List[Dict[str, Any]]:
    """
    Fetch /new/.json for a subreddit configured in `cfg` (expects 'url' key).
    Returns list of post data dicts (same shape as Reddit API /new children[][data]).
    """
    base = (cfg.get("url") or "").rstrip("/")
    if not base:
        logger.warning("fetch_subreddit_posts_async: missing url in cfg: %s", cfg)
        return []

    url = f"{base}/new/.json?limit={limit}"

    raw = await async_fetch_json(url, session)
    if not raw:
        logger.info("fetch_subreddit_posts_async: empty response for %s", base)
        return []

    try:
        children = raw.get("data", {}).get("children", [])
        posts: List[Dict[str, Any]] = []
        if isinstance(children, list):
            for item in children:
                if isinstance(item, dict):
                    data = item.get("data")
                    if isinstance(data, dict):
                        posts.append(data)
        logger.info("fetch_subreddit_posts_async: fetched %d posts from %s", len(posts), base)
        return posts
    except Exception as e:
        logger.exception("fetch_subreddit_posts_async: error parsing response for %s: %s", base, e)
        return []


# ----------------------
# Convenience factory
# ----------------------
def create_aiohttp_session(concurrency: int = DEFAULT_CONCURRENCY, user_agent: str = USER_AGENT) -> AsyncHTTP:
    """
    Create and return an AsyncHTTP manager.
    Use as:
      async_http = create_aiohttp_session()
      async with async_http as session:
          posts = await fetch_subreddit_posts_async(cfg, session)
    """
    return AsyncHTTP(concurrency=concurrency, user_agent=user_agent)


# ----------------------
# Demo (sanity check)
# ----------------------
if __name__ == "__main__":
    import time

    async def _demo():
        cfg = {"url": "https://www.reddit.com/r/python/"}
        async_http = create_aiohttp_session()
        async with async_http as session:
            posts = await fetch_subreddit_posts_async(cfg, session)
            print(f"Demo: fetched {len(posts)} posts")
            for p in posts[:5]:
                print(" -", (p.get("title") or "")[:120])
        await async_http.close()

    run_sync(_demo())
#!/usr/bin/env python3
"""
part1_async_http.py

Async HTTP engine and async Reddit fetch for the hybrid architecture.

Provides:
  - AsyncHTTP (reusable aiohttp session manager)
  - async_fetch_json(url, session)
  - fetch_subreddit_posts_async(cfg, session, limit=30)
  - create_aiohttp_session(...)
  - run_sync(coro)  # safe helper to run async from sync (Flask)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp
import async_timeout

# ----------------------
# Config / Tunables
# ----------------------
USER_AGENT = "AkiRedditBot/1.0 (by u/WayOk4302)"
REDDIT_LIMIT_DEFAULT = 30
ASYNC_RETRY_LIMIT = 3
ASYNC_BASE_WAIT_SECONDS = 1.0
ASYNC_TIMEOUT_SECONDS = 12
DEFAULT_CONCURRENCY = 6

logger = logging.getLogger("reddit-x-bot.part1")
logger.setLevel(logging.INFO)


# ----------------------
# aiohttp session manager
# ----------------------
class AsyncHTTP:
    """
    aiohttp ClientSession manager with a bounded connector.
    Use like:

      async_http = create_aiohttp_session()
      async with async_http as session:
          posts = await fetch_subreddit_posts_async(cfg, session)

    Note: the context manager does not auto-close the session on exit (to allow reuse).
          Call async_http.close() when you want to fully close the session.
    """

    def __init__(self, *, concurrency: int = DEFAULT_CONCURRENCY, user_agent: str = USER_AGENT):
        self._connector = aiohttp.TCPConnector(limit=concurrency)
        self._user_agent = user_agent
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            headers = {"User-Agent": self._user_agent}
            self._session = aiohttp.ClientSession(connector=self._connector, headers=headers)
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        # Keep session open for reuse; caller should call close() if needed.
        return False

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ----------------------
# Helper to run async from sync (safe)
# ----------------------
def run_sync(coro):
    """
    Run an async coroutine from synchronous code.

    Primary strategy: asyncio.run(coro)
    Fallback: if an event loop is already running, schedule the coroutine on a new thread
    and return its result (safe for typical Flask use).
    """
    try:
        return asyncio.run(coro)
    except RuntimeError:
        # If there's an active event loop (rare in Flask), run the coroutine in a new thread's loop.
        try:
            import threading

            result_container: Dict[str, Any] = {}

            def _target():
                try:
                    res = asyncio.new_event_loop().run_until_complete(coro)
                    result_container["result"] = res
                except Exception as e:
                    result_container["error"] = e

            t = threading.Thread(target=_target, daemon=True)
            t.start()
            t.join()
            if "error" in result_container:
                raise result_container["error"]
            return result_container.get("result")
        except Exception as e:
            # As a last resort, re-raise
            raise RuntimeError("run_sync fallback failed") from e


# ----------------------
# Async fetch with retries + simple backoff + HTML detection
# ----------------------
async def async_fetch_json(
    url: str,
    session: aiohttp.ClientSession,
    *,
    retry_limit: int = ASYNC_RETRY_LIMIT,
    timeout_seconds: int = ASYNC_TIMEOUT_SECONDS,
) -> Optional[Dict[str, Any]]:
    """
    Fetch JSON from URL with retry/backoff. Returns parsed JSON or None.

    Notes:
      - Detects HTML responses (Cloudflare/login pages) and treats them as transient failures.
      - Uses a simple exponential backoff between retries.
    """
    if not url:
        return None

    for attempt in range(1, retry_limit + 1):
        wait = ASYNC_BASE_WAIT_SECONDS * (2 ** (attempt - 1))
        try:
            async with async_timeout.timeout(timeout_seconds):
                async with session.get(url) as resp:
                    text = await resp.text()
                    if text.lstrip().startswith("<"):
                        # Likely HTML (blocked or Cloudflare). Retry unless final attempt.
                        logger.warning("async_fetch_json: HTML returned for %s (status=%s) attempt=%s/%s", url, resp.status, attempt, retry_limit)
                        if attempt < retry_limit:
                            await asyncio.sleep(wait)
                            continue
                        return None

                    # Parse JSON (let aiohttp infer content-type)
                    try:
                        js = await resp.json(content_type=None)
                        return js
                    except Exception as e:
                        logger.warning("async_fetch_json: json parse error for %s: %s", url, e)
                        if attempt < retry_limit:
                            await asyncio.sleep(wait)
                            continue
                        return None

        except asyncio.TimeoutError:
            logger.warning("async_fetch_json: timeout for %s attempt=%s/%s", url, attempt, retry_limit)
            if attempt < retry_limit:
                await asyncio.sleep(wait)
                continue
        except aiohttp.ClientError as ce:
            logger.warning("async_fetch_json: client error for %s: %s", url, ce)
            if attempt < retry_limit:
                await asyncio.sleep(wait)
                continue
        except Exception as e:
            logger.exception("async_fetch_json: unexpected error for %s: %s", url, e)
            return None

    logger.error("async_fetch_json: failed after %s attempts for %s", retry_limit, url)
    return None


# ----------------------
# Async fetch subreddit posts (/new/.json?limit=N)
# ----------------------
async def fetch_subreddit_posts_async(cfg: Dict[str, Any], session: aiohttp.ClientSession, *, limit: int = REDDIT_LIMIT_DEFAULT) -> List[Dict[str, Any]]:
    """
    Fetch /new/.json for a subreddit configured in `cfg` (expects 'url' key).
    Returns list of post data dicts (same shape as Reddit API /new children[][data]).
    """
    base = (cfg.get("url") or "").rstrip("/")
    if not base:
        logger.warning("fetch_subreddit_posts_async: missing url in cfg: %s", cfg)
        return []

    url = f"{base}/new/.json?limit={limit}"

    raw = await async_fetch_json(url, session)
    if not raw:
        logger.info("fetch_subreddit_posts_async: empty response for %s", base)
        return []

    try:
        children = raw.get("data", {}).get("children", [])
        posts: List[Dict[str, Any]] = []
        if isinstance(children, list):
            for item in children:
                if isinstance(item, dict):
                    data = item.get("data")
                    if isinstance(data, dict):
                        posts.append(data)
        logger.info("fetch_subreddit_posts_async: fetched %d posts from %s", len(posts), base)
        return posts
    except Exception as e:
        logger.exception("fetch_subreddit_posts_async: error parsing response for %s: %s", base, e)
        return []


# ----------------------
# Convenience factory
# ----------------------
def create_aiohttp_session(concurrency: int = DEFAULT_CONCURRENCY, user_agent: str = USER_AGENT) -> AsyncHTTP:
    """
    Create and return an AsyncHTTP manager.
    Use as:
      async_http = create_aiohttp_session()
      async with async_http as session:
          posts = await fetch_subreddit_posts_async(cfg, session)
    """
    return AsyncHTTP(concurrency=concurrency, user_agent=user_agent)


# ----------------------
# Demo (sanity check)
# ----------------------
if __name__ == "__main__":
    import time

    async def _demo():
        cfg = {"url": "https://www.reddit.com/r/python/"}
        async_http = create_aiohttp_session()
        async with async_http as session:
            posts = await fetch_subreddit_posts_async(cfg, session)
            print(f"Demo: fetched {len(posts)} posts")
            for p in posts[:5]:
                print(" -", (p.get("title") or "")[:120])
        await async_http.close()

    run_sync(_demo())
