#!/usr/bin/env python3
"""
Merge all cleaned text sources into one combined dataset.
Input: data/processed/reddit_clean.csv, data/processed/isthmus_clean.csv
Output: data/processed/all_text_combined.csv
"""

import pandas as pd
from pathlib import Path

# Paths
DATA_DIR = Path(__file__).parent.parent
PROCESSED_DIR = DATA_DIR / "data" / "processed"

INPUT_FILES = [
    PROCESSED_DIR / "reddit_clean.csv",
    PROCESSED_DIR / "isthmus_clean.csv",
]

OUTPUT_FILE = PROCESSED_DIR / "all_text_combined.csv"


def is_valid_text(text):
    """Check if text is valid."""
    if pd.isna(text):
        return False
    if not isinstance(text, str):
        return False
    if len(text.strip()) < 10:
        return False
    return True


def main():
    dfs = []
    
    for filepath in INPUT_FILES:
        print(f"Loading: {filepath.name}")
        if filepath.exists():
            df = pd.read_csv(filepath)
            print(f"   → {len(df)} rows")
            dfs.append(df)
        else:
            print(f"   ⚠️  File not found: {filepath}")
    
    if not dfs:
        print("❌ No files loaded!")
        return
    
    # Stack vertically
    print(f"\nMerging {len(dfs)} files...")
    combined = pd.concat(dfs, ignore_index=True)
    print(f"   Combined: {len(combined)} rows")
    
    # Drop duplicates by text
    before = len(combined)
    combined = combined.drop_duplicates(subset=["text"])
    print(f"   After dedup: {len(combined)} rows (removed {before - len(combined)} duplicates)")
    
    # Drop invalid text
    before = len(combined)
    combined = combined[combined["text"].apply(is_valid_text)]
    print(f"   After validation: {len(combined)} rows (removed {before - len(combined)} invalid)")
    
    # Reset index
    combined = combined.reset_index(drop=True)
    
    # Save
    print(f"\nSaving: {OUTPUT_FILE}")
    combined.to_csv(OUTPUT_FILE, index=False)
    
    # Summary
    print(f"\n{'='*50}")
    print(f"✅ MERGE COMPLETE")
    print(f"{'='*50}")
    print(f"Total rows: {len(combined)}")
    
    print(f"\n📊 BY SOURCE:")
    print(combined["source"].value_counts().to_string())
    
    print(f"\n📍 BY LOCATION_TAG (top 10):")
    print(combined["location_tag"].value_counts().head(10).to_string())


if __name__ == "__main__":
    main()
