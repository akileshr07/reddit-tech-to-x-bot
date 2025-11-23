import os
import time
import requests
from bs4 import BeautifulSoup
from requests_oauthlib import OAuth1


SUBREDDIT = "technology"
URL = f"https://old.reddit.com/r/{SUBREDDIT}/new/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AkiBot/1.0)"}

POSTED_FILE = "posted_ids.txt"
WINDOW_HOURS = 8
TOP_LIMIT = 5


# -----------------------------------------------
# Retry helper
# -----------------------------------------------
def try_request(method, url, max_tries=3, delay=3, **kwargs):
    for attempt in range(1, max_tries + 1):
        try:
            r = method(url, timeout=20, **kwargs)
            if r.status_code in [429, 500, 503]:
                print(f"Retry {attempt}/{max_tries} for {r.status_code}")
                time.sleep(delay)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"Error: {e} retry {attempt}/{max_tries}")
            time.sleep(delay)

    raise RuntimeError("Failed after retries")


# -----------------------------------------------
# Load + Save posted IDs
# -----------------------------------------------
def load_posted_ids():
    if not os.path.exists(POSTED_FILE):
        return set()
    return set(i.strip() for i in open(POSTED_FILE).readlines())


def save_posted_id(pid):
    with open(POSTED_FILE, "a") as f:
        f.write(pid + "\n")


# -----------------------------------------------
# Scrape old Reddit HTML
# -----------------------------------------------
def scrape_old_reddit():
    print("Scraping old Reddit HTML...")
    resp = try_request(requests.get, URL, headers=HEADERS)

    soup = BeautifulSoup(resp.text, "lxml")
    things = soup.find_all("div", class_="thing")

    posts = []
    now = time.time()
    cutoff = now - WINDOW_HOURS * 3600

    for post in things:
        post_id = post.get("data-fullname", "")  # t3_xxxxx

        title_tag = post.find("a", class_="title")
        if not title_tag:
            continue

        title = title_tag.text.strip()
        url = title_tag.get("href")

        # Fix relative URLs
        if url.startswith("/r/"):
            url = "https://old.reddit.com" + url

        time_tag = post.find("time")
        if not time_tag:
            continue

        created_str = time_tag.get("datetime")
        try:
            # Parse ISO time
            created_ts = int(time.mktime(time.strptime(created_str, "%Y-%m-%dT%H:%M:%S+00:00")))
        except:
            continue

        if created_ts < cutoff:
            continue

        # Score
        score_tag = post.find("div", class_="score likes")
        score = 0
        if score_tag:
            if score_tag.text == "•":
                score = 0
            elif "k" in score_tag.text:
                score = int(float(score_tag.text.replace("k", "")) * 1000)
            else:
                try:
                    score = int(score_tag.text)
                except:
                    score = 0

        # Comment count
        com_tag = post.find("a", class_="comments")
        comments = 0
        if com_tag:
            try:
                comments = int(com_tag.text.split()[0])
            except:
                comments = 0

        engagement = score + comments * 2

        posts.append({
            "id": post_id,
            "title": title,
            "url": url,
            "engagement": engagement
        })

    # Sort posts by engagement
    posts.sort(key=lambda x: x["engagement"], reverse=True)
    return posts[:TOP_LIMIT]


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
# Tweet text
# -----------------------------------------------
def make_tweet(post):
    t = f"{post['title']} {post['url']}"
    if len(t) <= 280:
        return t
    max_len = 280 - len(post["url"]) - 1
    return f"{post['title'][:max_len]}… {post['url']}"


# -----------------------------------------------
# Main
# -----------------------------------------------
def main():
    posted = load_posted_ids()
    print("Posted IDs:", posted)

    posts = scrape_old_reddit()

    fresh = None
    for p in posts:
        if p["id"] not in posted:
            fresh = p
            break

    if not fresh:
        print("No new posts")
        return

    print("Selected:", fresh)
    tweet = make_tweet(fresh)

    post_to_x(tweet)
    save_posted_id(fresh["id"])


if __name__ == "__main__":
    main()
