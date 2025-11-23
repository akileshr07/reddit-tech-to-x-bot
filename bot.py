import os
import time
import requests
from requests_oauthlib import OAuth1

REDDIT_URL = "https://www.reddit.com/r/technology/new.json?limit=100"
USER_AGENT = "AkiRedditBot/1.0 (by u/WayOk4302)"


# -----------------------------
# Retry helper
# -----------------------------
def try_request(method, url, max_tries=3, delay=3, **kwargs):
    """
    Safe request with small retry.
    Useful for GitHub Actions where IP sometimes gets rate-limited.
    """
    for attempt in range(1, max_tries + 1):
        try:
            resp = method(url, timeout=15, **kwargs)
            if resp.status_code == 429:
                print(f"Rate limited (429). Retry {attempt}/{max_tries}...")
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            print(f"Request failed: {e}. Retry {attempt}/{max_tries}...")
            time.sleep(delay)

    raise RuntimeError("Request failed after retries.")


# -----------------------------
# Fetch + filter + rank Reddit posts
# -----------------------------
def fetch_top_reddit_posts(window_hours=8, limit=5):
    resp = try_request(
        requests.get,
        REDDIT_URL,
        headers={"User-Agent": USER_AGENT},
    )
    data = resp.json()

    now = time.time()
    cutoff = now - (window_hours * 3600)

    posts = []

    for child in data.get("data", {}).get("children", []):
        d = child.get("data", {})
        created = d.get("created_utc", 0)

        # Only recent posts
        if created < cutoff:
            continue

        # Skip stickied, NSFW, ads
        if d.get("stickied") or d.get("over_18"):
            continue

        score = d.get("score", 0)
        comments = d.get("num_comments", 0)

        engagement = score + (comments * 2)

        posts.append({
            "id": d.get("id"),
            "title": d.get("title", "").strip(),
            "url": "https://www.reddit.com" + d.get("permalink", ""),
            "score": score,
            "comments": comments,
            "engagement": engagement,
        })

    # Sort by engagement
    posts.sort(key=lambda p: p["engagement"], reverse=True)

    return posts[:limit]


# -----------------------------
# Prepare tweet text
# -----------------------------
def make_tweet_text(post):
    title = post["title"]
    url = post["url"]

    combined = f"{title} {url}"

    if len(combined) <= 280:
        return combined

    max_title_len = 280 - (len(url) + 1)
    truncated = title[: max_title_len - 1] + "…"
    return f"{truncated} {url}"


# -----------------------------
# Tweet using X API v2
# -----------------------------
def post_to_x(text):
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

    resp = try_request(
        requests.post,
        url,
        json={"text": text},
        auth=auth,
    )

    data = resp.json()
    tweet_id = data.get("data", {}).get("id")
    print(f"Tweeted successfully → https://x.com/i/web/status/{tweet_id}")


# -----------------------------
# Main logic
# -----------------------------
def main():
    print("Fetching r/technology posts...")
    posts = fetch_top_reddit_posts(window_hours=8, limit=5)

    if not posts:
        print("No posts found in last 8 hours.")
        return

    # Select the #1 most engaging post
    best = posts[0]

    print("Selected top post:")
    print(best)

    tweet_text = make_tweet_text(best)
    print("Tweet text:", tweet_text)

    print("Posting to X...")
    post_to_x(tweet_text)


# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    main()
