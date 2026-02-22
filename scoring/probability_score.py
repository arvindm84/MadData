#!/usr/bin/env python3
"""
Final Probability Scoring for Madison WI Business Viability.

Combines:
- business_scores.csv (saturation, business score by OSM location)
- sentiment_by_area_business.json (sentiment by location_tag + business type)
- transcript_sentiment.json (additional sentiment from transcripts)
- trends_demand_score.json (demand score by business type)

Approach:
- For each OSM location (lat/lon) × each business type from trends
- Find closest sentiment match for that business type (from combined sentiment sources)
- Combine scores into final probability

Output: final_scores.csv with calibrated probability scores
"""

import pandas as pd
import json
import math
from pathlib import Path
from tqdm import tqdm

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "processed"

BUSINESS_SCORES_FILE = DATA_DIR / "business_scores.csv"
SENTIMENT_FILE = DATA_DIR / "sentiment_by_area_business.json"
TRANSCRIPT_SENTIMENT_FILE = PROJECT_ROOT / "transcript_sentiment.json"
TRENDS_FILE = DATA_DIR / "trends_demand_score.json"

OUTPUT_CSV = DATA_DIR / "final_scores.csv"
OUTPUT_JSON = DATA_DIR / "final_scores.json"


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points 
    on Earth using the Haversine formula.
    Returns distance in kilometers.
    """
    R = 6371  # Earth's radius in km
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = (math.sin(delta_lat / 2) ** 2 + 
         math.cos(lat1_rad) * math.cos(lat2_rad) * 
         math.sin(delta_lon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c


def find_closest_sentiment(lat, lon, business_type, sentiment_df, max_distance_km=15.0):
    """
    Find the closest sentiment location that matches the business_type.
    Uses positive_ratio (0-1) as sentiment indicator (more reliable than overall_sentiment).
    Returns (location_tag, positive_ratio, distance_km)
    """
    # Filter to matching business type
    matches = sentiment_df[sentiment_df["business_type"] == business_type]
    
    if len(matches) == 0:
        # No sentiment data for this business type - return neutral
        return "no_data", 0.5, 999.0
    
    best_match = None
    best_distance = float("inf")
    best_sentiment = 0.5  # Neutral default
    
    for _, row in matches.iterrows():
        dist = haversine_distance(lat, lon, row["lat"], row["lon"])
        if dist < best_distance:
            best_distance = dist
            best_match = row["location_tag"]
            # Use positive_ratio (0-1) which is reliable
            best_sentiment = row.get("positive_ratio", 0.5)
    
    # If too far, discount the sentiment
    if best_distance > max_distance_km:
        # Blend toward neutral (0.5) as distance increases
        blend_factor = min(1.0, (best_distance - max_distance_km) / 20.0)
        best_sentiment = best_sentiment * (1 - blend_factor) + 0.5 * blend_factor
    
    return best_match, best_sentiment, best_distance


def main():
    print("="*60)
    print("FINAL PROBABILITY SCORING")
    print("="*60)
    
    # =========================================================================
    # LOAD DATA
    # =========================================================================
    print("\n📂 Loading data...")
    
    # Business scores (OSM locations)
    print(f"   Loading: {BUSINESS_SCORES_FILE.name}")
    business_df = pd.read_csv(BUSINESS_SCORES_FILE)
    print(f"      {len(business_df)} OSM locations")
    
    # Sentiment by area + business (Reddit/Isthmus)
    print(f"   Loading: {SENTIMENT_FILE.name}")
    with open(SENTIMENT_FILE, "r") as f:
        sentiment_data = json.load(f)
    sentiment_df = pd.DataFrame(sentiment_data)
    print(f"      {len(sentiment_df)} location×business groups from Reddit/Isthmus")
    
    # Transcript sentiment (separate source with its own weight)
    transcript_df = None
    if TRANSCRIPT_SENTIMENT_FILE.exists():
        print(f"   Loading: {TRANSCRIPT_SENTIMENT_FILE.name}")
        with open(TRANSCRIPT_SENTIMENT_FILE, "r") as f:
            transcript_data = json.load(f)
        transcript_df = pd.DataFrame(transcript_data)
        print(f"      {len(transcript_df)} location×business groups from transcripts")
    else:
        print(f"   ⚠️ {TRANSCRIPT_SENTIMENT_FILE.name} not found")
    
    # Trends demand scores
    print(f"   Loading: {TRENDS_FILE.name}")
    with open(TRENDS_FILE, "r") as f:
        trends_list = json.load(f)
    print(f"      {len(trends_list)} business types")
    
    # Convert trends list to dict for lookup
    trends_data = {item["business_type"]: item["demand_score"] for item in trends_list}
    
    # Get business types from trends (excluding "general business" with 0 score)
    business_types = [bt for bt, score in trends_data.items() if score > 0]
    print(f"   Using {len(business_types)} business types for scoring")
    
    # =========================================================================
    # CALCULATE SCORES FOR EACH LOCATION × BUSINESS TYPE
    # =========================================================================
    print(f"\n🔄 Calculating probability scores ({len(business_df)} locations × {len(business_types)} business types)...")
    
    results = []
    
    for idx, row in tqdm(business_df.iterrows(), total=len(business_df), desc="Scoring"):
        lot_id = row.get("id", idx)
        lat = row["lat"]
        lon = row["lon"]
        
        # Base business score from OSM analysis (0-1)
        base_business_score = row.get("business_score", 0.5)
        saturation_score = row.get("saturation_score", 0.5)
        traffic_score = row.get("traffic_score", 0.5)
        demo_score = row.get("demo_score", 0.5)
        
        # For each business type, calculate a score
        for business_type in business_types:
            # Find closest Reddit/Isthmus sentiment match for this business type
            matched_location, positive_ratio, distance_km = find_closest_sentiment(
                lat, lon, business_type, sentiment_df
            )
            
            # Find closest transcript sentiment match (if available)
            transcript_sentiment = 0.5  # Default neutral
            transcript_location = "no_data"
            if transcript_df is not None and len(transcript_df) > 0:
                t_loc, t_ratio, t_dist = find_closest_sentiment(
                    lat, lon, business_type, transcript_df
                )
                if t_loc != "no_data":
                    transcript_sentiment = t_ratio
                    transcript_location = t_loc
            
            # Get trends demand score (0-100 -> 0-1)
            trends_demand = trends_data.get(business_type, 25) / 100.0
            
            # =====================================================================
            # CALCULATE FINAL PROBABILITY
            # New weights:
            #   - Business Score (OSM): 30%
            #   - Reddit/Isthmus Sentiment: 25%
            #   - Transcript Sentiment: 25%
            #   - Google Trends Demand: 20%
            # =====================================================================
            
            # Sentiment components (positive_ratio is already 0-1)
            reddit_sentiment = positive_ratio
            
            # Weighted combination (all in 0-1 range)
            raw_probability = (
                0.30 * base_business_score +
                0.25 * reddit_sentiment +
                0.25 * transcript_sentiment +
                0.20 * trends_demand
            )
            
            # Calibrate to realistic range (25% - 90%)
            calibrated_probability = 25 + (raw_probability * 65)
            calibrated_probability = round(calibrated_probability, 1)
            
            results.append({
                "id": lot_id,
                "lat": lat,
                "lon": lon,
                "business_type": business_type,
                "final_probability": calibrated_probability,
                "base_business_score": round(base_business_score * 100, 1),
                "reddit_sentiment_score": round(reddit_sentiment * 100, 1),
                "transcript_sentiment_score": round(transcript_sentiment * 100, 1),
                "trends_demand_score": round(trends_demand * 100, 1),
                "saturation_score": round(saturation_score, 3),
                "matched_reddit_location": matched_location,
                "matched_transcript_location": transcript_location,
                "distance_to_sentiment_km": round(distance_km, 2)
            })
    
    # Create output dataframe
    output_df = pd.DataFrame(results)
    
    # Sort by final_probability descending
    output_df = output_df.sort_values("final_probability", ascending=False).reset_index(drop=True)
    
    print(f"\n   Generated {len(output_df)} opportunity scores")
    
    # =========================================================================
    # SAVE OUTPUTS
    # =========================================================================
    print("\n💾 Saving outputs...")
    
    # Ensure output directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Save CSV
    print(f"   Saving: {OUTPUT_CSV.name}")
    output_df.to_csv(OUTPUT_CSV, index=False)
    
    # Save JSON
    print(f"   Saving: {OUTPUT_JSON.name}")
    records = output_df.to_dict(orient="records")
    with open(OUTPUT_JSON, "w") as f:
        json.dump(records, f, indent=2)
    
    # =========================================================================
    # PRINT RESULTS
    # =========================================================================
    print("\n" + "="*60)
    print("📊 TOP 10 HIGHEST PROBABILITY OPPORTUNITIES")
    print("="*60)
    
    top_10 = output_df.head(10)[["id", "business_type", "final_probability", "matched_reddit_location"]]
    for i, row in top_10.iterrows():
        print(f"{i+1:2}. {row['final_probability']:5.1f}% | {row['business_type']:20s} | {row['matched_reddit_location']}")
    
    # Best by each business type
    print("\n" + "="*60)
    print("🏆 BEST LOCATION FOR EACH BUSINESS TYPE")
    print("="*60)
    
    for bt in sorted(business_types):
        bt_df = output_df[output_df["business_type"] == bt]
        if len(bt_df) > 0:
            best = bt_df.iloc[0]
            print(f"{bt:25s}: {best['final_probability']:5.1f}% @ ({best['lat']:.4f}, {best['lon']:.4f})")
    
    # Summary stats
    print("\n" + "="*60)
    print("📈 SUMMARY STATISTICS")
    print("="*60)
    print(f"Total opportunities scored: {len(output_df)}")
    print(f"Unique locations: {business_df['id'].nunique()}")
    print(f"Business types evaluated: {len(business_types)}")
    print(f"Probability range: {output_df['final_probability'].min()}% - {output_df['final_probability'].max()}%")
    print(f"Mean probability: {output_df['final_probability'].mean():.1f}%")
    print(f"Median probability: {output_df['final_probability'].median():.1f}%")
    
    print(f"\n✅ COMPLETE!")
    print(f"   Output: {OUTPUT_CSV}")
    print(f"   Output: {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
