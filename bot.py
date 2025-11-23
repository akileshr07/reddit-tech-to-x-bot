import os
import time
import requests
import xml.etree.ElementTree as ET
from requests_oauthlib import OAuth1


RSS_URL = "https://www.reddit.com/r/technology/.rss"
POSTED_FILE = "posted_ids.txt"
TOP_LIMIT = 5
WINDOW_HOURS = 8


# -----------------------------------------------
# Safe HTTP helper with retry
# -----------------------------------------------
def try_request(method, url, max_tries=3, delay=3, **kwargs):
    for attempt in range(1, max_tries + 1):
        try:
            resp = method(url, timeout=15, **kwargs)
            if resp.status_code in [429, 500, 502, 503]:
                print(f"Retry {attempt}/{max_tries} for {resp.status_code}")
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            print(f"Error: {e}, retry {attempt}/{max_tries}")
            time.sleep(delay)
    raise RuntimeError("Failed after retries.")


# -----------------------------------------------
# Load posted IDs
# -----------------------------------------------
def load_posted_ids():
    if not os.path.exists(POSTED_FILE):
        return set()
    return set(i.strip() for i in open(POSTED_FILE).readlines())


def save_posted_id(pid):
    with open(POSTED_FILE, "a") as f:
        f.write(pid + "\n")


# -----------------------------------------------
# Fetch RSS posts
# -----------------------------------------------
def fetch_rss_posts():
    print("Fetching RSS...")
    resp = try_request(requests.get, RSS_URL, headers={"User-Agent": "Mozilla/5.0"})
    root = ET.fromstring(resp.text)

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    entries = root.findall("atom:entry", ns)

    posts = []

    for entry in entries:
        title = entry.find("atom:title", ns).text
        link = entry.find("atom:link", ns).attrib["href"]
        updated = entry.find("atom:updated", ns).text

        post_id = link.split("/")[-3]  # reddit.com/r/subreddit/comments/ID/title

        posts.append({
            "id": post_id,
            "title": title,
            "url": link,
            "updated": updated
        })

    return posts


# -----------------------------------------------
# Get score & comments via Reddit JSON
# -----------------------------------------------
def fetch_post_metrics(post_id):
    json_url = f"https://www.reddit.com/comments/{post_id}.json"
    resp = try_request(requests.get, json_url, headers={"User-Agent": "Mozilla/5.0"})
    data = resp.json()

    info = data[0]["data"]["children"][0]["data"]

    return {
        "score": info.get("score", 0),
        "comments": info.get("num_comments", 0),
        "created": info.get("created_utc", 0)
    }


# -----------------------------------------------
# Rank posts by engagement
# -----------------------------------------------
def select_top_posts():
    rss_posts = fetch_rss_posts()
    now = time.time()
    cutoff = now - WINDOW_HOURS * 3600

    enriched = []
    for p in rss_posts:
        metrics = fetch_post_metrics(p["id"])
        if metrics["created"] < cutoff:
            continue

        engagement = metrics["score"] + metrics["comments"] * 2

        enriched.append({
            "id": p["id"],
            "title": p["title"],
            "url": p["url"],
            "engagement": engagement
        })

    enriched.sort(key=lambda x: x["engagement"], reverse=True)
    return enriched[:TOP_LIMIT]


# -----------------------------------------------
# X Posting
# -----------------------------------------------
def post_to_x(text):
    auth = OAuth1(
        os.environ["X_API_KEY"],
        os.environ["X_API_SECRET"],
        os.environ["X_ACCESS_TOKEN"],
        os.environ["X_ACCESS_SECRET"],
        signature_type="auth_header",
    )
    resp = try_request(
        requests.post,
        "https://api.twitter.com/2/tweets",
        json={"text": text},
        auth=auth
    )
    tid = resp.json()["data"]["id"]
    print("Tweeted:", f"https://x.com/i/web/status/{tid}")


# -----------------------------------------------
# Tweet selection
# -----------------------------------------------
def make_tweet(post):
    text = f"{post['title']} {post['url']}"
    if len(text) <= 280:
        return text
    max_len = 280 - len(post['url']) - 1
    return f"{post['title'][:max_len]}… {post['url']}"


# -----------------------------------------------
# Main
# -----------------------------------------------
def main():
    posted = load_posted_ids()
    print("Posted IDs:", posted)

    posts = select_top_posts()

    fresh = None
    for p in posts:
        if p["id"] not in posted:
            fresh = p
            break

    if not fresh:
        print("No fresh post")
        return

    print("Selected:", fresh)
    tweet = make_tweet(fresh)

    post_to_x(tweet)
    save_posted_id(fresh["id"])


if __name__ == "__main__":
    main()
