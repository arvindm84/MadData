#!/usr/bin/env python3
"""
Attach geographic coordinates to sentiment aggregation files.
Input: 
  - data/processed/sentiment_by_area_business.csv
  - data/processed/sentiment_by_area.csv
Output:
  - data/processed/sentiment_by_area_with_coords.csv
  - data/processed/sentiment_by_area.json
  - data/processed/sentiment_by_area_business_with_coords.csv
  - data/processed/sentiment_by_area_business.json
"""

import pandas as pd
import json
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent
PROCESSED_DIR = DATA_DIR / "data" / "processed"

COORDINATES = {
    "state street":     {"lat": 43.0731, "lon": -89.4012},
    "willy street":     {"lat": 43.0669, "lon": -89.3704},
    "east washington":  {"lat": 43.0789, "lon": -89.3532},
    "monroe street":    {"lat": 43.0505, "lon": -89.4076},
    "atwood":           {"lat": 43.0652, "lon": -89.3621},
    "downtown madison": {"lat": 43.0740, "lon": -89.3840},
    "downtown":         {"lat": 43.0740, "lon": -89.3840},
    "capitol square":   {"lat": 43.0748, "lon": -89.3839},
    "west side":        {"lat": 43.0650, "lon": -89.4800},
    "east side":        {"lat": 43.0750, "lon": -89.3300},
    "general madison":  {"lat": 43.0731, "lon": -89.4012},
    
    "campus":           {"lat": 43.0766, "lon": -89.4125},
    "middleton":        {"lat": 43.0972, "lon": -89.5043},
    "fitchburg":        {"lat": 42.9609, "lon": -89.4237},
    "verona":           {"lat": 42.9919, "lon": -89.5332},
    "sun prairie":      {"lat": 43.1836, "lon": -89.2137},
    "monona":           {"lat": 43.0620, "lon": -89.3340},
    "cottage grove":    {"lat": 43.0761, "lon": -89.1996},
    "deforest":         {"lat": 43.2478, "lon": -89.3473},
    "waunakee":         {"lat": 43.1919, "lon": -89.4554},
    "north side":       {"lat": 43.1100, "lon": -89.3800},
    "south side":       {"lat": 43.0300, "lon": -89.3800},
    "isthmus":          {"lat": 43.0750, "lon": -89.3900},
    "camp randall":     {"lat": 43.0700, "lon": -89.4128},
    "hilldale":         {"lat": 43.0730, "lon": -89.4500},
    "junction":         {"lat": 43.0640, "lon": -89.5100},
    "fish hatchery":    {"lat": 43.0400, "lon": -89.3900},
    "park street":      {"lat": 43.0550, "lon": -89.3900},
    "tenney":           {"lat": 43.0850, "lon": -89.3700},
    "marquette":        {"lat": 43.0800, "lon": -89.3650},
    "vilas":            {"lat": 43.0550, "lon": -89.4100},
}

# Default coordinates (general madison)
DEFAULT_COORDS = {"lat": 43.0731, "lon": -89.4012}


def get_coordinates(location_tag):
    """Get coordinates for a location_tag, using default if not found."""
    if pd.isna(location_tag):
        return DEFAULT_COORDS["lat"], DEFAULT_COORDS["lon"], True
    
    location_lower = str(location_tag).lower().strip()
    
    if location_lower in COORDINATES:
        coords = COORDINATES[location_lower]
        return coords["lat"], coords["lon"], False
    else:
        return DEFAULT_COORDS["lat"], DEFAULT_COORDS["lon"], True


def process_file(input_path, output_csv, output_json):
    """Process a single CSV file and add coordinates."""
    print(f"\n[LOADING] Loading: {input_path.name}")
    df = pd.read_csv(input_path)
    print(f"   {len(df)} rows")
    
    unmatched = set()
    
    lats = []
    lons = []
    
    for _, row in df.iterrows():
        lat, lon, is_default = get_coordinates(row["location_tag"])
        lats.append(lat)
        lons.append(lon)
        
        if is_default and pd.notna(row["location_tag"]):
            unmatched.add(row["location_tag"])
    
    df["lat"] = lats
    df["lon"] = lons
    
    print(f"💾 Saving: {output_csv.name}")
    df.to_csv(output_csv, index=False)
    
    print(f"💾 Saving: {output_json.name}")
    records = df.to_dict(orient="records")
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    
    return unmatched


def main():
    print("="*60)
    print("ATTACHING COORDINATES TO SENTIMENT FILES")
    print("="*60)
    
    all_unmatched = set()
    
    unmatched = process_file(
        PROCESSED_DIR / "sentiment_by_area.csv",
        PROCESSED_DIR / "sentiment_by_area_with_coords.csv",
        PROCESSED_DIR / "sentiment_by_area.json"
    )
    all_unmatched.update(unmatched)
    
    unmatched = process_file(
        PROCESSED_DIR / "sentiment_by_area_business.csv",
        PROCESSED_DIR / "sentiment_by_area_business_with_coords.csv",
        PROCESSED_DIR / "sentiment_by_area_business.json"
    )
    all_unmatched.update(unmatched)
    
    print(f"\n{'='*60}")
    print("[WARNING] UNMATCHED LOCATIONS (used default coordinates)")
    print("="*60)
    
    if all_unmatched:
        for loc in sorted(all_unmatched):
            print(f"   • {loc}")
        print(f"\n   Total: {len(all_unmatched)} unmatched locations")
    else:
        print("   None! All locations matched.")
    
    print(f"\n[OK] COMPLETE!")
    print(f"   Created 4 output files with lat/lon coordinates")


if __name__ == "__main__":
    main()
