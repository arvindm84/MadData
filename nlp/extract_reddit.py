#!/usr/bin/env python3
"""
Extract and clean Reddit data for NLP pipeline.
Input: reddit_filtered_final.jsonl
Output: data/processed/reddit_clean.csv
"""

import json
import csv
from pathlib import Path

# Paths
DATA_DIR = Path(__file__).parent.parent
INPUT_FILE = DATA_DIR / "reddit_filtered_final.jsonl"
OUTPUT_DIR = DATA_DIR / "data" / "processed"
OUTPUT_FILE = OUTPUT_DIR / "reddit_clean.csv"


def is_valid_text(text):
    """Check if text is valid for NLP."""
    if text is None:
        return False
    if not isinstance(text, str):
        return False
    text = text.strip()
    if len(text) < 10:
        return False
    if text.lower() in ["deleted", "removed", "[deleted]", "[removed]"]:
        return False
    return True


def main():
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    rows = []
    skipped = 0
    
    print(f"Loading: {INPUT_FILE}")
    
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            
            obj = json.loads(line)
            text = obj.get("text", "")
            
            if not is_valid_text(text):
                skipped += 1
                continue
            
            # Note: the file uses "location" not "location_tag"
            location_tag = obj.get("location", obj.get("location_tag", ""))
            
            rows.append({
                "text": text.strip(),
                "source": "reddit",
                "location_tag": location_tag
            })
    
    # Save to CSV
    print(f"Saving: {OUTPUT_FILE}")
    
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["text", "source", "location_tag"])
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"\n✅ Total rows saved: {len(rows)}")
    print(f"   Skipped (invalid): {skipped}")
    
    print(f"\n📋 Sample rows (first 3):")
    for i, row in enumerate(rows[:3]):
        text_preview = row["text"][:60] + "..." if len(row["text"]) > 60 else row["text"]
        print(f"   {i+1}. [{row['location_tag']}] {text_preview}")


if __name__ == "__main__":
    main()
