import os
import time
import requests
from requests_oauthlib import OAuth1

PUSHSHIFT_URL = "https://api.pushshift.io/reddit/search/submission/"
SUBREDDIT = "technology"
WINDOW_HOURS = 8
TOP_LIMIT = 5
POSTED_FILE = "posted_ids.txt"


# -----------------------------
# Retry helper
# -----------------------------
def try_request(method, url, max_tries=3, delay=3, **kwargs):
    for attempt in range(1, max_tries + 1):
        try:
            resp = method(url, timeout=20, **kwargs)
            if resp.status_code >= 500:
                print(f"Server error {resp.status_code}, retry {attempt}/{max_tries}...")
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            print(f"Request failed: {e}, retry {attempt}/{max_tries}...")
            time.sleep(delay)
    raise RuntimeError("Request failed after retries.")


# -----------------------------
# Load posted IDs
# -----------------------------
def load_posted_ids():
    if not os.path.exists(POSTED_FILE):
        return set()
    with open(POSTED_FILE, "r") as f:
        return set(line.strip() for line in f.readlines())


# -----------------------------
# Save posted ID
# -----------------------------
def save_posted_id(post_id):
    with open(POSTED_FILE, "a") as f:
        f.write(post_id + "\n")


# -----------------------------
# Fetch posts from Pushshift
# -----------------------------
def fetch_top_posts(window_hours=8, limit=5):
    now = int(time.time())
    cutoff = now - window_hours * 3600

    params = {
        "subreddit": SUBREDDIT,
        "after": cutoff,
        "sort": "desc",
        "sort_type": "score",
        "size": 100,
    }

    print("Fetching posts from Pushshift...")
    resp = try_request(requests.get, PUSHSHIFT_URL, params=params)
    data = resp.json()

    list_raw = data.get("data", [])
    posts = []

    for p in list_raw:
        title = p.get("title", "").strip()
        url = p.get("full_link") or f"https://www.reddit.com{p.get('permalink', '')}"

        score = p.get("score", 0)
        comments = p.get("num_comments", 0)
        engagement = score + comments * 2

        posts.append({
            "id": p.get("id"),
            "title": title,
            "url": url,
            "engagement": engagement,
        })

    posts.sort(key=lambda x: x["engagement"], reverse=True)
    return posts[:limit]


# -----------------------------
# Prepare tweet text
# -----------------------------
def make_tweet_text(post):
    txt = f"{post['title']} {post['url']}"
    if len(txt) <= 280:
        return txt

    max_len = 280 - len(post["url"]) - 1
    return f"{post['title'][:max_len]}… {post['url']}"


# -----------------------------
# Tweet to X
# -----------------------------
def post_to_x(text):
    auth = OAuth1(
        os.environ["X_API_KEY"],
        os.environ["X_API_SECRET"],
        os.environ["X_ACCESS_TOKEN"],
        os.environ["X_ACCESS_SECRET"],
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
    print("Tweeted:", f"https://x.com/i/web/status/{data['data']['id']}")


# -----------------------------
# Main logic
# -----------------------------
def main():
    posted_ids = load_posted_ids()
    print("Already posted IDs:", posted_ids)

    posts = fetch_top_posts(WINDOW_HOURS, TOP_LIMIT)

    fresh_post = None
    for post in posts:
        if post["id"] not in posted_ids:
            fresh_post = post
            break

    if not fresh_post:
        print("No fresh posts available.")
        return

    print("Selected new post:", fresh_post)

    tweet = make_tweet_text(fresh_post)
    print("Tweet text:", tweet)

    post_to_x(tweet)
    save_posted_id(fresh_post["id"])


if __name__ == "__main__":
    main()
