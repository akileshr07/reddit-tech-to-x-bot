import requests
import json
import os
from requests_oauthlib import OAuth1
import time

RAPID_URL = "https://reddapi.p.rapidapi.com/api/scrape/new"
HEADERS = {
    "x-rapidapi-key": os.environ["RAPIDAPI_KEY"],
    "x-rapidapi-host": "reddapi.p.rapidapi.com"
}

POSTED_FILE = "posted_ids.txt"
WINDOW_HOURS = 8
LIMIT = 50


# Load already-posted IDs
def load_ids():
    if not os.path.exists(POSTED_FILE):
        return set()
    return set(open(POSTED_FILE).read().splitlines())


def save_id(pid):
    with open(POSTED_FILE, "a") as f:
        f.write(pid + "\n")


def fetch_reddit():
    params = {"subreddit": "technology", "limit": LIMIT}
    res = requests.get(RAPID_URL, headers=HEADERS, params=params)
    res.raise_for_status()
    return res.json()


def filter_and_rank(posts):
    now = time.time()
    cutoff = now - WINDOW_HOURS * 3600

    clean = []
    for p in posts:
        created = p.get("created_utc")
        if not created or created < cutoff:
            continue

        score = p.get("score", 0)
        comments = p.get("num_comments", 0)
        title = p.get("title")
        url = p.get("full_link")

        engagement = score + comments * 2

        clean.append({
            "id": p.get("id"),
            "title": title,
            "url": url,
            "engagement": engagement
        })

    clean.sort(key=lambda x: x["engagement"], reverse=True)
    return clean


def make_tweet(p):
    base = f"{p['title']} {p['url']}"
    if len(base) <= 280:
        return base
    max_len = 280 - len(p['url']) - 1
    return f"{p['title'][:max_len]}… {p['url']}"


def tweet(text):
    auth = OAuth1(
        os.environ["X_API_KEY"],
        os.environ["X_API_SECRET"],
        os.environ["X_ACCESS_TOKEN"],
        os.environ["X_ACCESS_SECRET"],
        signature_type="auth_header"
    )

    res = requests.post(
        "https://api.twitter.com/2/tweets",
        json={"text": text},
        auth=auth
    )
    res.raise_for_status()
    print("Tweeted:", res.json())


def main():
    posted = load_ids()
    print("Already posted:", posted)

    raw = fetch_reddit()
    posts = filter_and_rank(raw)

    fresh = None
    for p in posts:
        if p["id"] not in posted:
            fresh = p
            break

    if not fresh:
        print("No new posts found.")
        return

    print("Selected:", fresh)

    text = make_tweet(fresh)
    tweet(text)
    save_id(fresh["id"])


if __name__ == "__main__":
    main()
