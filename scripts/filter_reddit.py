import pandas as pd
import json

# Load raw data
df = pd.read_csv("data/raw/reddit_raw.csv")

print(f"Loaded {len(df)} entries from data/raw/reddit_raw.csv")

# Keywords that signal business demand or neighborhood sentiment
RELEVANT_KEYWORDS = [
    # Direct demand signals
    "wish there was",
    "need a",
    "why is there no",
    "miss having",
    "there should be",
    "madison needs",
    "why isn't there",
    "nowhere to",
    "no place to",
    "should open",
    "we need",
    "would love a",
    "looking for a",
    "anyone know",
    "does madison have",
    "is there a",

    # Business types
    "coffee",
    "pharmacy",
    "grocery",
    "late night",
    "restaurant",
    "bar ",
    "gym",
    "boba",
    "bakery",
    "convenience",
    "coworking",
    "food",
    "shop",
    "store",

    # Neighborhoods
    "state street",
    "willy street",
    "east washington",
    "monroe street",
    "atwood",
    "downtown",
    "capitol square",
    "west side",
    "east side",
    "williamson",
    "langdon",
    "near campus",
    "near the capitol",

    # Sentiment signals
    "love this place",
    "hate that",
    "so good",
    "terrible",
    "awful",
    "amazing",
    "wish it was open",
    "closed down",
    "went out of business",
    "new place",
    "just opened",
    "recommend",
    "avoid",
    "overpriced",
    "underrated",
    "hidden gem",
    "miss it",
]

def is_relevant(text):
    text_lower = str(text).lower()
    return any(kw.lower() in text_lower for kw in RELEVANT_KEYWORDS)

# Filter
df["is_relevant"] = df["text"].apply(is_relevant)
relevant_df = df[df["is_relevant"] == True].copy()
relevant_df = relevant_df.drop(columns=["is_relevant"])

# Sort by upvote score (most relevant first)
relevant_df = relevant_df.sort_values("upvote_score", ascending=False).reset_index(drop=True)

# Save as CSV
relevant_df.to_csv("data/raw/reddit_filtered.csv", index=False)

# Save as JSON (for NLP)
relevant_df.to_json("data/raw/reddit_filtered.json", orient="records", indent=2)

# Save as JSONL (best for NLP pipelines)
with open("data/raw/reddit_filtered.jsonl", "w", encoding="utf-8") as f:
    for _, row in relevant_df.iterrows():
        f.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")

# Summary
print(f"\n{'='*50}")
print("FILTERING COMPLETE")
print(f"{'='*50}")
print(f"Original entries:  {len(df)}")
print(f"Relevant entries:  {len(relevant_df)}")
print(f"Dropped:           {len(df) - len(relevant_df)}")
print(f"\nBy location tag:")
print(relevant_df["location_tag"].value_counts().to_string())
print(f"\nBy subreddit:")
print(relevant_df["subreddit"].value_counts().to_string())
print(f"\nTop keyword triggers:")
print(relevant_df["keyword_trigger"].value_counts().head(10).to_string())
print(f"\nFiles saved:")
print("  • reddit_filtered.csv")
print("  • reddit_filtered.json")
print("  • reddit_filtered.jsonl")