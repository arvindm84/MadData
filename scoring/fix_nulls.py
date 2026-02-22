#!/usr/bin/env python3
"""
Fix null values in final_scores.csv

Fills null base_business_score with 50.0 and recalculates final_probability.
"""

import pandas as pd
import json
from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "processed"

INPUT_FILE = DATA_DIR / "final_scores.csv"
OUTPUT_CSV = DATA_DIR / "final_scores.csv"
OUTPUT_JSON = DATA_DIR / "final_scores.json"


def main():
    print("=" * 60)
    print("FIX NULL VALUES IN FINAL_SCORES")
    print("=" * 60)
    
    # Load data
    print(f"\n[LOADING] Loading: {INPUT_FILE.name}")
    df = pd.read_csv(INPUT_FILE)
    print(f"   Total rows: {len(df)}")
    
    null_mask = df["final_probability"].isna() | df["base_business_score"].isna()
    null_rows = df[null_mask]
    
    print(f"\n[SCANNING] Found {len(null_rows)} rows with null values:")
    print("-" * 60)
    print(null_rows[["id", "lat", "lon", "business_type"]].to_string(index=False))
    print("-" * 60)
    
    if len(null_rows) == 0:
        print("\n[OK] No null values found. Nothing to fix.")
        return
    
    print(f"\n[FIXING] Fixing {len(null_rows)} null rows...")
    
    fixed_count = 0
    for idx in df[null_mask].index:
        if pd.isna(df.loc[idx, "base_business_score"]):
            df.loc[idx, "base_business_score"] = 50.0
        
        base_business = df.loc[idx, "base_business_score"]
        sentiment = df.loc[idx, "sentiment_score"] if pd.notna(df.loc[idx, "sentiment_score"]) else 50.0
        trends = df.loc[idx, "trends_demand_score"] if pd.notna(df.loc[idx, "trends_demand_score"]) else 25.0
        
        raw = (0.40 * base_business / 100) + (0.40 * sentiment / 100) + (0.20 * trends / 100)
        final_prob = round(20 + (raw * 72), 1)
        
        df.loc[idx, "final_probability"] = final_prob
        fixed_count += 1
    
    remaining_nulls = df["final_probability"].isna().sum() + df["base_business_score"].isna().sum()
    
    print(f"\n💾 Saving: {OUTPUT_CSV.name}")
    df.to_csv(OUTPUT_CSV, index=False)
    
    print(f"   Saving: {OUTPUT_JSON.name}")
    records = df.to_dict(orient="records")
    with open(OUTPUT_JSON, "w") as f:
        json.dump(records, f, indent=2)
    
    print(f"\n[OK] Fixed {fixed_count} null rows")
    print(f"   Remaining nulls in final_probability: {df['final_probability'].isna().sum()}")
    print(f"   Remaining nulls in base_business_score: {df['base_business_score'].isna().sum()}")
    
    if remaining_nulls == 0:
        print("\n[SUCCESS] Zero nulls remain!")
    else:
        print(f"\n[WARNING] {remaining_nulls} nulls still remain")


if __name__ == "__main__":
    main()
