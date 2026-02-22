#!/usr/bin/env python3
"""
Filter Reddit data to keep only RELEVANT entries for sentiment analysis.
Keeps entries that mention:
- Business types (restaurant, coffee, etc.)
- Business demand signals ("wish there was", "need a", etc.)
- Madison locations/neighborhoods
"""

import pandas as pd
import json
import re

df = pd.read_csv("data/raw/reddit_filtered.csv")
print(f"[OK] Loaded {len(df)} entries from data/raw/reddit_filtered.csv")

DEMAND_SIGNALS = [
    "wish there was",
    "wish we had",
    "need a",
    "needs a",
    "madison needs",
    "why is there no",
    "why isn't there",
    "why don't we have",
    "should open",
    "someone should open",
    "would love a",
    "would kill for",
    "miss having",
    "we need more",
    "nowhere to get",
    "no place to",
    "hard to find",
    "looking for a",
    "anyone know where",
    "recommend a",
    "suggestions for",
    
    "just opened",
    "new place",
    "closed down",
    "went out of business",
    "shutting down",
    "opening soon",
    "coming soon",
    "grand opening",
    
    "best place for",
    "worst place",
    "overpriced",
    "underrated",
    "hidden gem",
    "avoid",
    "love this place",
    "hate this place",
]

BUSINESS_TYPES = [
    "restaurant", "cafe", "coffee", "coffee shop", "bakery", "bar", "pub",
    "brewery", "grocery", "grocery store", "supermarket", "convenience store",
    "pharmacy", "drugstore", "gym", "fitness", "yoga", "salon", "barbershop",
    "laundromat", "dry cleaner", "bank", "atm", "gas station",
    "boba", "bubble tea", "ice cream", "pizza", "sushi", "thai", "mexican",
    "chinese", "indian", "vietnamese", "korean", "ramen", "pho",
    "breakfast", "brunch", "lunch", "dinner", "late night", "24 hour",
    "food truck", "deli", "sandwich", "burger", "wings", "tacos",
    "coworking", "office space", "daycare", "childcare", "vet", "veterinarian",
    "auto shop", "mechanic", "car wash", "parking",
    "bookstore", "thrift store", "vintage", "antique",
    "dispensary", "smoke shop", "vape",
    "hotel", "airbnb", "motel",
]

# =============================================================================
# MADISON NEIGHBORHOODS (for location extraction)
# =============================================================================

NEIGHBORHOODS = {
    "downtown": ["downtown", "capitol square", "capitol", "state street", "king street"],
    "state street": ["state street", "state st"],
    "capitol square": ["capitol square", "the square"],
    
    # Near East
    "willy street": ["willy street", "willy st", "williamson", "williamson street"],
    "atwood": ["atwood", "schenk-atwood", "schenk atwood"],
    "east washington": ["east wash", "east washington", "e washington", "e wash"],
    "marquette": ["marquette"],
    "tenney": ["tenney", "tenney park"],
    
    "monroe street": ["monroe street", "monroe st"],
    "camp randall": ["camp randall", "regent", "regent street"],
    "vilas": ["vilas", "vilas park"],
    "hilldale": ["hilldale"],
    
    "campus": ["campus", "uw campus", "university", "bascom", "library mall", "langdon"],
    "state street": ["state street"],
    
    # Isthmus
    "isthmus": ["isthmus"],
    
    "west side": ["west side", "westside", "west madison"],
    "middleton": ["middleton"],
    "fitchburg": ["fitchburg"],
    "verona": ["verona"],
    "junction": ["junction", "west towne"],
    
    "east side": ["east side", "eastside", "east madison"],
    "sun prairie": ["sun prairie"],
    "cottage grove": ["cottage grove"],
    "monona": ["monona"],
    
    "north side": ["north side", "northside", "north madison"],
    "waunakee": ["waunakee"],
    "deforest": ["deforest", "de forest"],
    
    # South
    "south side": ["south side", "southside", "south madison"],
    "fish hatchery": ["fish hatchery"],
    "park street": ["park street", "park st"],
}

def has_demand_signal(text):
    """Check if text contains a business demand signal."""
    text_lower = str(text).lower()
    for signal in DEMAND_SIGNALS:
        if signal in text_lower:
            return True
    return False

def has_business_type(text):
    """Check if text mentions a business type."""
    text_lower = str(text).lower()
    return any(btype in text_lower for btype in BUSINESS_TYPES)

def extract_location(text):
    """Extract the most specific neighborhood mentioned."""
    text_lower = str(text).lower()
    
    for neighborhood, keywords in NEIGHBORHOODS.items():
        for kw in keywords:
            if kw in text_lower:
                return neighborhood
    
    return "general madison"

def is_relevant(text):
    """
    Keep entry if it has:
    - A business type mentioned, OR
    - A demand signal, OR  
    - A specific location (not just general madison)
    """
    has_business = has_business_type(text)
    has_demand = has_demand_signal(text)
    has_location = extract_location(text) != "general madison"
    
    return has_business or has_demand or has_location

print("\n[PROCESSING] Filtering to keep relevant entries...")

df["location"] = df["text"].apply(extract_location)

df["is_relevant"] = df["text"].apply(is_relevant)
output_df = df[df["is_relevant"]].copy()

output_df = output_df[[
    "text",
    "source", 
    "subreddit",
    "upvote_score",
    "created_date",
    "location",
    "type",
    "keyword_trigger",
    "post_id"
]].copy()

output_df = output_df.sort_values("upvote_score", ascending=False).reset_index(drop=True)

output_df.to_csv("data/raw/reddit_filtered_final.csv", index=False)

output_df.to_json("data/raw/reddit_filtered_final.json", orient="records", indent=2)

# JSONL for NLP/HuggingFace
with open("data/raw/reddit_filtered_final.jsonl", "w", encoding="utf-8") as f:
    for _, row in output_df.iterrows():
        f.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")

print(f"\n{'='*60}")
print("FILTERING COMPLETE"))
print(f"{'='*60}")
print(f"Original entries:  {len(df)}")
print(f"Kept (relevant):   {len(output_df)}")
print(f"Removed:           {len(df) - len(output_df)}")

print(f"\n[LOCATIONS]")
print(output_df["location"].value_counts().head(15).to_string())

print(f"\n[SUBREDDIT]")
print(output_df["subreddit"].value_counts().to_string())

print(f"\n[FILES SAVED]")
print("  • reddit_filtered_final.csv")
print("  • reddit_filtered_final.json")
print("  • reddit_filtered_final.jsonl")
