import os
import time
import requests
from requests_oauthlib import OAuth1


# ---------------------------------
# CONFIG
# ---------------------------------

PUSHSHIFT_URL = "https://api.pushshift.io/reddit/search/submission/"
SUBREDDIT = "technology"
WINDOW_HOURS = 8
TOP_LIMIT = 5


# ---------------------------------
# Retry helper (for Pushshift & X)
# ---------------------------------
def try_request(method, url, max_tries=3, delay=3, **kwargs):
    for attempt in range(1, max_tries + 1):
        try:
            resp = method(url, timeout=20, **kwargs)
            if resp.status_code >= 500:
                print(f"Server error {resp.status_code}. Retry {attempt}/{max_tries}...")
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            print(f"Request failed: {e}. Retry {attempt}/{max_tries}...")
            time.sleep(delay)

    raise RuntimeError("Request failed after retries.")


# ---------------------------------
# Fetch from Pushshift (no auth)
# ---------------------------------
def fetch_top_posts(window_hours=8, limit=5):
    now = int(time.time())
    cutoff = now - window_hours * 3600

    params = {
        "subreddit": SUBREDDIT,
        "after": cutoff,
        "sort": "desc",
        "sort_type": "created_utc",
        "size": 100
    }

    print("Fetching from Pushshift...")
    resp = try_request(requests.get, PUSHSHIFT_URL, params=params)
    data = resp.json()

    posts_raw = data.get("data", [])
    posts = []

    for p in posts_raw:
        title = p.get("title", "").strip()
        url = p.get("full_link") or f"https://www.reddit.com{p.get('permalink', '')}"

        score = p.get("score", 0)
        comments = p.get("num_comments", 0)

        engagement = score + comments * 2

        posts.append({
            "id": p.get("id"),
            "title": title,
            "url": url,
            "score": score,
            "comments": comments,
            "engagement": engagement,
        })

    posts.sort(key=lambda x: x["engagement"], reverse=True)
    return posts[:limit]


# ---------------------------------
# Create tweet text
# ---------------------------------
def make_tweet_text(post):
    title = post["title"]
    url = post["url"]
    combined = f"{title} {url}"

    if len(combined) <= 280:
        return combined

    max_title_len = 280 - len(url) - 1
    return f"{title[:max_title_len]}… {url}"


# ---------------------------------
# Send tweet
# ---------------------------------
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
        auth=auth
    )

    data = resp.json()
    tweet_id = data.get("data", {}).get("id")
    print(f"Tweeted → https://x.com/i/web/status/{tweet_id}")


# ---------------------------------
# Main bot logic
# ---------------------------------
def main():
    print("Pulling top posts from r/technology (Pushshift)...")
    posts = fetch_top_posts(WINDOW_HOURS, TOP_LIMIT)

    if not posts:
        print("No posts found in last 8 hours.")
        return

    best = posts[0]
    print("Selected post:", best)

    tweet_text = make_tweet_text(best)
    print("Tweet:", tweet_text)

    print("Posting to X...")
    post_to_x(tweet_text)


if __name__ == "__main__":
    main()
