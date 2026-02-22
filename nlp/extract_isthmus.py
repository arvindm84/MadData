#!/usr/bin/env python3
"""
Extract and clean Isthmus articles for NLP pipeline.
Input: isthmus_articles.jsonl
Output: data/processed/isthmus_clean.csv

Extracts first 3 sentences to fit RoBERTa's 512 token limit.
"""

import json
import csv
import re
from pathlib import Path

# Paths
DATA_DIR = Path(__file__).parent.parent
INPUT_FILE = DATA_DIR / "isthmus_articles.jsonl"
OUTPUT_DIR = DATA_DIR / "data" / "processed"
OUTPUT_FILE = OUTPUT_DIR / "isthmus_clean.csv"


def extract_first_sentences(text, n=3):
    """Extract first n sentences from text."""
    if not text:
        return ""
    
    # Split on sentence endings (. ! ?) followed by space or end
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    
    # Take first n sentences
    first_n = sentences[:n]
    
    return " ".join(first_n).strip()


def is_valid_text(text):
    """Check if text is valid for NLP."""
    if text is None:
        return False
    if not isinstance(text, str):
        return False
    text = text.strip()
    if len(text) < 20:
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
            
            # Find main text column (could be "text", "body", "content")
            text = obj.get("text", obj.get("body", obj.get("content", "")))
            
            if not is_valid_text(text):
                skipped += 1
                continue
            
            # Extract first 3 sentences for RoBERTa token limit
            text_trimmed = extract_first_sentences(text, n=3)
            
            if not is_valid_text(text_trimmed):
                skipped += 1
                continue
            
            rows.append({
                "text": text_trimmed,
                "source": "isthmus",
                "location_tag": "general madison"
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
        text_preview = row["text"][:80] + "..." if len(row["text"]) > 80 else row["text"]
        print(f"   {i+1}. {text_preview}")


if __name__ == "__main__":
    main()
