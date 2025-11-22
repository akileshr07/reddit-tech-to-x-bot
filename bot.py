import os
import time
import math
import requests
from requests_oauthlib import OAuth1

REDDIT_URL = "https://www.reddit.com/r/technology/new.json?limit=100"
USER_AGENT = "Mozilla/5.0 (reddit-tech-to-x-bot by u_yourname)"

def fetch_top_reddit_posts(window_hours=8, limit=5):
    resp = requests.get(
        REDDIT_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    now = time.time()
    cutoff = now - window_hours * 3600

    posts = []
    for child in data.get("data", {}).get("children", []):
        d = child.get("data", {})
        created = d.get("created_utc", 0)

        # only last N hours
        if created < cutoff:
            continue

        # skip stickied/ads
        if d.get("stickied") or d.get("over_18"):
            continue

        score = d.get("score", 0)
        comments = d.get("num_comments", 0)

        # simple engagement formula (tweak if you want)
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


def make_tweet_text(post):
    """
    Format: Title + link, but keep under 280 chars.
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
        raise RuntimeError(
            f"X API error {resp.status_code}: {resp.text}"
        )

    data = resp.json()
    tweet_id = data.get("data", {}).get("id")
    print(f"Tweeted: https://x.com/i/web/status/{tweet_id}")


def main():
    posts = fetch_top_reddit_posts(window_hours=8, limit=5)

    if not posts:
        print("No posts found in the last 8 hours.")
        return

    # YOU ASKED: "top 5 engaging" → we computed them
    # but to stay within free X tier, we tweet ONLY the #1.
    best = posts[0]
    tweet_text = make_tweet_text(best)
    print("Selected post:", best)
    print("Tweet text:", tweet_text)

    post_to_x(tweet_text)


if __name__ == "__main__":
    main()
