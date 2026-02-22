import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import re 

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

# ALL Madison-related subreddits
SUBREDDITS = [
    # Primary Madison subreddits
    "madisonwi",
    "UWMadison",
    
    # Campus & Student Life
    "UWMadisonHousing",
    "UWMadisonJobs",
    "UWMadisonTickets",
    
    # Madison Area suburbs/neighborhoods
    "Middleton_WI",
    "SunPrairie", 
    "FitchburgWI",
    "VeronaWI",
    "OregonWI",
    "CrossPlains",
    "DeForestWI",
    "WaunakeeWI",
    "StoughtonWI",
    
    # Wisconsin general (will filter for Madison content)
    "wisconsin",
    "WisconsinBadgers",
    "WisconsinSports",
    
    # Niche Madison communities
    "MadisonFood",
    "MadtownBeer",
    "MadisonBikeMN",
    "MadisonCraft",
    "MadisonJobs",
    "MadisonClassifieds",
    "MadisonSoccer",
    "MadisonMusic",
    "MadisonEvents",
]

# Regex pattern to match Madison-related content
MADISON_PATTERN = re.compile(r'(?i)(madisonwi|madison\s*wi|madison,?\s*wisconsin|madison\s+area|dane\s+county|uw[\s-]*madison|isthmus|state\s+street|capitol\s+square)')

KEYWORDS = [
    # Demand signals
    "wish there was",
    "need a",
    "why is there no",
    "miss having",
    "there should be",
    "madison needs",
    "why isn't there",
    "nowhere to",
    "no place to",
    # Neighborhoods
    "State Street",
    "Willy Street",
    "East Washington",
    "Monroe Street",
    "Atwood",
    "downtown",
    "capitol square",
    "west side",
    "east side",
    # Business types
    "coffee",
    "pharmacy",
    "grocery",
    "late night food",
    "restaurant",
    "bar",
    "gym",
    "boba",
    "bakery",
    "convenience store",
]

NEIGHBORHOODS = [
    "State Street",
    "Willy Street",
    "East Washington",
    "Monroe Street",
    "Atwood",
    "downtown madison",
    "capitol square",
    "west side",
    "east side",
]

BASE_URL = "https://api.pullpush.io/reddit"

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def get_location_tag(text):
    text_lower = text.lower()
    for neighborhood in NEIGHBORHOODS:
        if neighborhood.lower() in text_lower:
            return neighborhood
    return "general madison"

def unix_to_date(unix_ts):
    try:
        return datetime.utcfromtimestamp(int(unix_ts)).strftime("%Y-%m-%d")
    except:
        return ""

def two_years_ago_unix():
    dt = datetime.utcnow() - timedelta(days=730)
    return int(dt.timestamp())

# ─────────────────────────────────────────
# SCRAPE POSTS
# ─────────────────────────────────────────

def fetch_posts(subreddit, keyword, limit=100):
    posts = []
    params = {
        "subreddit": subreddit,
        "q": keyword,
        "size": limit,
        "after": two_years_ago_unix(),
        "sort": "desc",
        "sort_type": "score"
    }

    try:
        response = requests.get(
            f"{BASE_URL}/search/submission/",
            params=params,
            timeout=15
        )

        if response.status_code != 200:
            print(f"  [!] Status {response.status_code} for '{keyword}' in r/{subreddit}")
            return posts

        data = response.json()
        items = data.get("data", [])

        for post in items:
            title = post.get("title", "")
            body = post.get("selftext", "")
            full_text = f"{title} {body}".strip()

            if len(full_text) < 10:
                continue

            posts.append({
                "text": full_text,
                "source": "reddit",
                "subreddit": subreddit,
                "upvote_score": post.get("score", 0),
                "created_date": unix_to_date(post.get("created_utc", 0)),
                "location_tag": get_location_tag(full_text),
                "type": "post",
                "keyword_trigger": keyword,
                "post_id": post.get("id", "")
            })

    except requests.exceptions.Timeout:
        print(f"  [!] Timeout for '{keyword}' in r/{subreddit}")
    except Exception as e:
        print(f"  [!] Error for '{keyword}' in r/{subreddit}: {e}")

    return posts

# ─────────────────────────────────────────
# SCRAPE COMMENTS
# ─────────────────────────────────────────

def fetch_comments(subreddit, keyword, limit=100):
    comments = []
    params = {
        "subreddit": subreddit,
        "q": keyword,
        "size": limit,
        "after": two_years_ago_unix(),
        "sort": "desc",
        "sort_type": "score"
    }

    try:
        response = requests.get(
            f"{BASE_URL}/search/comment/",
            params=params,
            timeout=15
        )

        if response.status_code != 200:
            print(f"  [!] Status {response.status_code} for comments '{keyword}' in r/{subreddit}")
            return comments

        data = response.json()
        items = data.get("data", [])

        for comment in items:
            body = comment.get("body", "").strip()

            if len(body) < 10:
                continue
            if body in ["[deleted]", "[removed]"]:
                continue

            comments.append({
                "text": body,
                "source": "reddit",
                "subreddit": subreddit,
                "upvote_score": comment.get("score", 0),
                "created_date": unix_to_date(comment.get("created_utc", 0)),
                "location_tag": get_location_tag(body),
                "type": "comment",
                "keyword_trigger": keyword,
                "post_id": comment.get("link_id", "")
            })

    except requests.exceptions.Timeout:
        print(f"  [!] Timeout for comments '{keyword}' in r/{subreddit}")
    except Exception as e:
        print(f"  [!] Error for comments '{keyword}' in r/{subreddit}: {e}")

    return comments

# ─────────────────────────────────────────
# MAIN SCRAPE LOOP
# ─────────────────────────────────────────

def run_full_scrape():
    import json
    
    all_entries = []
    seen_texts = set()
    
    # Output files
    csv_file = "reddit_raw.csv"
    jsonl_file = "reddit_raw.jsonl"  # JSON Lines - great for NLP
    
    total_combos = len(SUBREDDITS) * len(KEYWORDS)
    count = 0
    
    # Clear/create output files
    open(jsonl_file, 'w').close()
    
    print(f"Scraping {total_combos} keyword/subreddit combinations...")
    print(f"Saving to: {csv_file} and {jsonl_file}")
    print("-" * 50)

    for subreddit in SUBREDDITS:
        for keyword in KEYWORDS:
            count += 1
            
            # Minimal progress indicator
            print(f"[{count}/{total_combos}] r/{subreddit} — '{keyword}'", end=" ", flush=True)

            # Fetch posts
            posts = fetch_posts(subreddit, keyword)

            # Fetch comments
            comments = fetch_comments(subreddit, keyword)
            
            new_entries = 0

            # Deduplicate and add
            for entry in posts + comments:
                text_key = entry["text"][:120].lower().strip()
                if text_key not in seen_texts:
                    seen_texts.add(text_key)
                    all_entries.append(entry)
                    new_entries += 1
                    
                    # Append to JSONL file immediately (progressive save)
                    with open(jsonl_file, 'a', encoding='utf-8') as f:
                        f.write(json.dumps(entry, ensure_ascii=False) + '\n')
            
            print(f"→ +{new_entries} (total: {len(all_entries)})")

            time.sleep(1)  # be respectful to the API
        
        # Save CSV after each subreddit (checkpoint)
        if all_entries:
            df = pd.DataFrame(all_entries)
            df.to_csv(csv_file, index=False, encoding='utf-8')

    # ─────────────────────────────────────
    # FINAL SAVE & CLEANUP
    # ─────────────────────────────────────

    if not all_entries:
        print("\nNo entries collected.")
        return

    df = pd.DataFrame(all_entries)

    # Clean up
    df = df[df["text"].str.len() > 10]
    df = df.drop_duplicates(subset=["text"])
    df = df.sort_values("upvote_score", ascending=False)
    df = df.reset_index(drop=True)

    # Save final CSV
    df.to_csv(csv_file, index=False, encoding='utf-8')
    
    # Save clean JSONL (overwrite with deduplicated version)
    with open(jsonl_file, 'w', encoding='utf-8') as f:
        for _, row in df.iterrows():
            f.write(json.dumps(row.to_dict(), ensure_ascii=False) + '\n')

    # ─────────────────────────────────────
    # SUMMARY (minimal)
    # ─────────────────────────────────────

    print(f"\n{'='*50}")
    print(f"SCRAPE COMPLETE")
    print(f"{'='*50}")
    print(f"Total entries: {len(df)}")
    print(f"Posts: {len(df[df['type']=='post'])} | Comments: {len(df[df['type']=='comment'])}")
    print(f"Subreddits: {df['subreddit'].nunique()}")
    print(f"Date range: {df['created_date'].min()} to {df['created_date'].max()}")
    print(f"\nFiles saved:")
    print(f"  • {csv_file} (CSV for spreadsheets)")
    print(f"  • {jsonl_file} (JSON Lines for NLP)")

if __name__ == "__main__":
    run_full_scrape()