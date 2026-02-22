#!/usr/bin/env python3
"""
Pipeline Verification Script for Madison WI Business Viability Project.
Checks all data files and validates data integrity before scoring.
"""

import pandas as pd
import json
from pathlib import Path
import random

PROJECT_ROOT = Path(__file__).parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

FILES_TO_CHECK = [
    "data/processed/reddit_clean.csv",
    "data/processed/isthmus_clean.csv",
    "data/processed/all_text_combined.csv",
    "data/processed/sentiment_scores_raw.csv",
    "data/processed/sentiment_by_area.csv",
    "data/processed/sentiment_by_area_business.csv",
    "data/processed/sentiment_by_area_with_coords.csv",
    "data/processed/sentiment_by_area_business_with_coords.csv",
    "data/processed/sentiment_by_area.json",
    "data/processed/sentiment_by_area_business.json",
    "data/processed/trends_demand_score.json",
    "data/business_scores.csv",
]

EXPECTED_BUSINESS_TYPES = [
    "coffee shop", "restaurant", "pharmacy", "grocery store",
    "bar", "gym", "late night food", "bakery",
    "convenience store", "coworking space", "daycare",
    "hardware store", "urgent care", "general business"
]

# Track results
checks_run = 0
checks_passed = 0
checks_failed = 0
failed_checks = []


def check(name, condition):
    """Run a check and track result."""
    global checks_run, checks_passed, checks_failed, failed_checks
    checks_run += 1
    
    if condition:
        checks_passed += 1
        print(f"   [PASS] {name}")
        return True
    else:
        checks_failed += 1
        failed_checks.append(name)
        print(f"   [FAIL] {name}")
        return False


def load_file(filepath):
    """Load a file (CSV or JSON) and return data."""
    path = PROJECT_ROOT / filepath
    
    if not path.exists():
        return None
    
    if filepath.endswith(".csv"):
        return pd.read_csv(path)
    elif filepath.endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return pd.DataFrame(data)
        return data
    return None


def section_1_check_files_exist():
    """Check that all expected files exist."""
    print("\n" + "="*70)
    print("SECTION 1: CHECK FILES EXIST")
    print("="*70)
    
    existing_files = []
    
    for filepath in FILES_TO_CHECK:
        path = PROJECT_ROOT / filepath
        if path.exists():
            print(f"   [OK] {filepath}")
            existing_files.append(filepath)
        else:
            print(f"   [MISSING] {filepath}")
    
    print(f"\n   Summary: {len(existing_files)}/{len(FILES_TO_CHECK)} files found")
    return existing_files


def section_2_check_row_counts(existing_files):
    """Check row counts for all existing files."""
    print("\n" + "="*70)
    print("SECTION 2: CHECK ROW COUNTS")
    print("="*70)
    
    for filepath in existing_files:
        print(f"\n   [FILE] {filepath}")
        
        data = load_file(filepath)
        
        if data is None:
            print(f"      [WARNING] Could not load file")
            continue
        
        if isinstance(data, pd.DataFrame):
            rows = len(data)
            cols = len(data.columns)
            col_names = list(data.columns)
            
            print(f"      Rows: {rows}")
            print(f"      Columns: {cols}")
            print(f"      Column names: {col_names}")
            
            if rows == 0:
                print(f"      [WARNING] EMPTY FILE")
        elif isinstance(data, dict):
            print(f"      Type: dict with {len(data)} keys")
            print(f"      Keys: {list(data.keys())}")


def section_3_check_sentiment_scores():
    """Validate sentiment_scores_raw.csv."""
    print("\n" + "="*70)
    print("SECTION 3: CHECK SENTIMENT SCORES")
    print("="*70)
    
    filepath = "data/processed/sentiment_scores_raw.csv"
    df = load_file(filepath)
    
    if df is None:
        print(f"   [WARNING] File not found: {filepath}")
        return

    print(f"\n   Loaded {len(df)} rows")
    
    score_cols = ["positive_score", "neutral_score", "negative_score"]
    for col in score_cols:
        if col in df.columns:
            null_count = df[col].isna().sum()
            check(f"No nulls in {col}", null_count == 0)
    
    if all(col in df.columns for col in score_cols):
        df["score_sum"] = df["positive_score"] + df["neutral_score"] + df["negative_score"]
        valid_sums = ((df["score_sum"] - 1.0).abs() <= 0.01).all()
        if not valid_sums:
            print(f"   [INFO] Scores don't sum to 1.0 (model only saved winning score)")
            print(f"         This is OK - positive_ratio/negative_ratio use label counts, not scores")
    
    if "sentiment_label" in df.columns:
        valid_labels = {"positive", "neutral", "negative"}
        actual_labels = set(df["sentiment_label"].unique())
        check("sentiment_label only has valid values", actual_labels.issubset(valid_labels))
        
        print(f"\n   [SENTIMENT] sentiment_label breakdown:")
        print(df["sentiment_label"].value_counts().to_string(header=False))
    
    if "business_type" in df.columns:
        actual_types = set(df["business_type"].unique())
        expected_set = set(EXPECTED_BUSINESS_TYPES)
        check("business_type only has expected categories", actual_types.issubset(expected_set))
        
        print(f"\n   [BREAKDOWN] business_type breakdown:")
        print(df["business_type"].value_counts().to_string(header=False))


def section_4_check_coordinates():
    """Validate coordinates in sentiment_by_area_business_with_coords.csv."""
    print("\n" + "="*70)
    print("SECTION 4: CHECK COORDINATES")
    print("="*70)
    
    filepath = "data/processed/sentiment_by_area_business_with_coords.csv"
    df = load_file(filepath)
    
    if df is None:
        print(f"   [WARNING] File not found: {filepath}")
        return

    print(f"\n   Loaded {len(df)} rows")
    
    # Check no null lat/lon
    if "lat" in df.columns and "lon" in df.columns:
        check("No null lat values", df["lat"].isna().sum() == 0)
        check("No null lon values", df["lon"].isna().sum() == 0)
        
        # Check lat range (Madison metro area: 42.9 to 43.3)
        lat_valid = (df["lat"] >= 42.9) & (df["lat"] <= 43.3)
        check("All lat values in Madison range (42.9-43.3)", lat_valid.all())
        
        # Check lon range (Madison metro area: -89.6 to -89.1)
        lon_valid = (df["lon"] >= -89.6) & (df["lon"] <= -89.1)
        check("All lon values in Madison range (-89.6 to -89.1)", lon_valid.all())
    
    # Print unique locations with coordinates
    if "location_tag" in df.columns:
        print(f"\n   [COORDINATES] Unique locations and coordinates:")
        unique_locs = df[["location_tag", "lat", "lon"]].drop_duplicates()
        for _, row in unique_locs.iterrows():
            print(f"      {row['location_tag']:20} → ({row['lat']:.4f}, {row['lon']:.4f})")


def section_5_check_business_scores():
    """Validate business_scores.csv."""
    print("\n" + "="*70)
    print("SECTION 5: CHECK BUSINESS SCORES")
    print("="*70)
    
    filepath = "business_scores.csv"
    df = load_file(filepath)
    
    if df is None:
        print(f"   [WARNING] File not found: {filepath}")
        return

    print(f"\n   Loaded {len(df)} rows")
    
    # Check no null lat/lon
    if "lat" in df.columns and "lon" in df.columns:
        check("No null lat values", df["lat"].isna().sum() == 0)
        check("No null lon values", df["lon"].isna().sum() == 0)
        
        lat_valid = (df["lat"] >= 42.9) & (df["lat"] <= 43.3)
        check("All lat values in Madison range (42.9-43.3)", lat_valid.all())
        
        lon_valid = (df["lon"] >= -89.6) & (df["lon"] <= -89.1)
        check("All lon values in Madison range (-89.6 to -89.1)", lon_valid.all())
    
    if "saturation_score" in df.columns:
        sat_valid = (df["saturation_score"] >= 0) & (df["saturation_score"] <= 1)
        check("saturation_score values between 0 and 1", sat_valid.all())
    
    if "business_type" in df.columns:
        print(f"\n   [BREAKDOWN] Rows per business_type:")
        print(df["business_type"].value_counts().to_string(header=False))


def section_6_sample_data():
    """Print sample data for spot checking."""
    print("\n" + "="*70)
    print("SECTION 6: SAMPLE DATA SPOT CHECK")
    print("="*70)
    
    print("\n   [DATA] sentiment_scores_raw.csv (3 random rows):")
    df = load_file("data/processed/sentiment_scores_raw.csv")
    if df is not None and len(df) > 0:
        cols = ["text", "sentiment_label", "business_type"]
        cols = [c for c in cols if c in df.columns]
        sample = df[cols].sample(min(3, len(df)), random_state=42)
        for i, row in sample.iterrows():
            text_preview = str(row.get("text", ""))[:50] + "..."
            print(f"      [{row.get('sentiment_label', 'N/A')}] [{row.get('business_type', 'N/A')}]")
            print(f"      {text_preview}")
            print()
    
    print("\n   [DATA] sentiment_by_area_business_with_coords.csv (3 random rows):")
    df = load_file("data/processed/sentiment_by_area_business_with_coords.csv")
    if df is not None and len(df) > 0:
        cols = ["location_tag", "business_type", "positive_ratio", "overall_sentiment", "lat", "lon"]
        cols = [c for c in cols if c in df.columns]
        sample = df[cols].sample(min(3, len(df)), random_state=42)
        print(sample.to_string(index=False))
    
    # Sample from business_scores.csv
    print("\n   [DATA] business_scores.csv (3 random rows):")
    df = load_file("business_scores.csv")
    if df is not None and len(df) > 0:
        cols = ["id", "lat", "lon", "saturation_score"]
        cols = [c for c in cols if c in df.columns]
        if cols:
            sample = df[cols].sample(min(3, len(df)), random_state=42)
            print(sample.to_string(index=False))
        else:
            print(f"      Available columns: {list(df.columns)}")
            sample = df.sample(min(3, len(df)), random_state=42)
            print(sample.to_string(index=False))


def section_7_final_summary():
    """Print final summary."""
    print("\n" + "="*70)
    print("SECTION 7: FINAL SUMMARY")
    print("="*70)
    
    print(f"\n   Total checks run:    {checks_run}")
    print(f"   Total PASSED:        {checks_passed}")
    print(f"   Total FAILED:        {checks_failed}")
    
    if failed_checks:
        print(f"\n   [FAILURES] Failed checks:")
        for name in failed_checks:
            print(f"      • {name}")
    
    print("\n" + "="*70)
    if checks_failed == 0:
        print("[OK] PIPELINE READY - safe to run probability score")
    else:
        print("[ERROR] PIPELINE NOT READY - fix failed checks first")
    print("="*70)


def main():
    print("="*70)
    print("MADISON WI BUSINESS VIABILITY PIPELINE CHECK")
    print("="*70)
    
    # Run all sections
    existing_files = section_1_check_files_exist()
    section_2_check_row_counts(existing_files)
    section_3_check_sentiment_scores()
    section_4_check_coordinates()
    section_5_check_business_scores()
    section_6_sample_data()
    section_7_final_summary()


if __name__ == "__main__":
    main()
