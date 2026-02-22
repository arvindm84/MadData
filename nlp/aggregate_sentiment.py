#!/usr/bin/env python3
"""
Aggregate sentiment scores by location and business type.
Input: data/processed/sentiment_scores_raw.csv
Output: 
  - data/processed/sentiment_by_area_business.csv
  - data/processed/sentiment_by_area.csv
"""

import pandas as pd
from pathlib import Path

# Paths
DATA_DIR = Path(__file__).parent.parent
INPUT_FILE = DATA_DIR / "data" / "processed" / "sentiment_scores_raw.csv"
OUTPUT_AREA_BIZ = DATA_DIR / "data" / "processed" / "sentiment_by_area_business.csv"
OUTPUT_AREA = DATA_DIR / "data" / "processed" / "sentiment_by_area.csv"


def calculate_aggregations(df, group_cols):
    """Calculate sentiment aggregations for given grouping columns."""
    
    def agg_func(group):
        total = len(group)
        positive_count = (group["sentiment_label"] == "positive").sum()
        negative_count = (group["sentiment_label"] == "negative").sum()
        neutral_count = (group["sentiment_label"] == "neutral").sum()
        
        return pd.Series({
            "positive_ratio": positive_count / total,
            "negative_ratio": negative_count / total,
            "neutral_ratio": neutral_count / total,
            "overall_sentiment": (group["positive_score"] - group["negative_score"]).mean(),
            "avg_confidence": group["sentiment_confidence"].mean(),
            "total_entries": total,
            "low_confidence": total < 10
        })
    
    result = df.groupby(group_cols).apply(agg_func, include_groups=False).reset_index()
    return result


def main():
    # Load data
    print(f"📂 Loading: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    print(f"   Loaded {len(df)} rows")
    
    # =========================================================================
    # AGGREGATION 1: by location_tag AND business_type
    # =========================================================================
    print(f"\n{'='*60}")
    print("AGGREGATION 1: by location_tag + business_type")
    print(f"{'='*60}")
    
    agg_area_biz = calculate_aggregations(df, ["location_tag", "business_type"])
    agg_area_biz = agg_area_biz.sort_values("overall_sentiment", ascending=False)
    
    print(f"Saving: {OUTPUT_AREA_BIZ}")
    agg_area_biz.to_csv(OUTPUT_AREA_BIZ, index=False)
    print(f"   {len(agg_area_biz)} groups")
    
    # =========================================================================
    # AGGREGATION 2: by location_tag only
    # =========================================================================
    print(f"\n{'='*60}")
    print("AGGREGATION 2: by location_tag only")
    print(f"{'='*60}")
    
    agg_area = calculate_aggregations(df, ["location_tag"])
    agg_area = agg_area.sort_values("overall_sentiment", ascending=False)
    
    print(f"Saving: {OUTPUT_AREA}")
    agg_area.to_csv(OUTPUT_AREA, index=False)
    print(f"   {len(agg_area)} locations")
    
    # =========================================================================
    # PRINT RESULTS
    # =========================================================================
    print(f"\n{'='*60}")
    print("📊 SENTIMENT BY AREA + BUSINESS (sorted by overall_sentiment)")
    print(f"{'='*60}")
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 50)
    
    # Show top 20 by overall sentiment
    print("\nTop 20 location+business combos by overall_sentiment:")
    print(agg_area_biz.head(20).to_string(index=False))
    
    print(f"\n{'='*60}")
    print("🏆 BEST BUSINESS TYPE PER LOCATION (highest positive_ratio)")
    print(f"{'='*60}")
    
    # For each location, find business_type with highest positive_ratio
    best_per_location = agg_area_biz.loc[
        agg_area_biz.groupby("location_tag")["positive_ratio"].idxmax()
    ][["location_tag", "business_type", "positive_ratio", "total_entries"]]
    
    best_per_location = best_per_location.sort_values("positive_ratio", ascending=False)
    
    print()
    for _, row in best_per_location.iterrows():
        flag = "⚠️" if row["total_entries"] < 10 else "✅"
        print(f"{flag} {row['location_tag']:20} → {row['business_type']:20} "
              f"(positive: {row['positive_ratio']:.1%}, n={int(row['total_entries'])})")
    
    print(f"\n{'='*60}")
    print("📍 SENTIMENT BY AREA ONLY")
    print(f"{'='*60}")
    print(agg_area.to_string(index=False))
    
    print(f"\n✅ AGGREGATION COMPLETE!")
    print(f"   • {OUTPUT_AREA_BIZ.name}: {len(agg_area_biz)} groups")
    print(f"   • {OUTPUT_AREA.name}: {len(agg_area)} locations")


if __name__ == "__main__":
    main()
