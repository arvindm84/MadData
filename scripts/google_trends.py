"""
Step 5: Google Trends Analysis for Madison, WI
===============================================
Searches 14 store categories using their keyword lists.
Each keyword is searched individually, then results are
averaged together to get one score per category.

Install deps:
    pip install pytrends pandas
"""

import time
import json
import csv
from pathlib import Path
from pytrends.request import TrendReq

GEO       = "US-WI-669"
TIMEFRAME = "today 5-y"

OUTPUT_CSV     = Path("data/raw/google_trends_results.csv")
OUTPUT_SUMMARY = Path("data/raw/google_trends_summary.json")

CATEGORIES = {
    "coffee shop":        ["coffee", "cafe", "espresso", "latte", "cappuccino"],
    "restaurant":         ["restaurant", "food", "dining", "lunch", "dinner", "brunch"],
    "pharmacy":           ["pharmacy", "drugstore", "prescriptions", "CVS", "Walgreens", "medicine"],
    "grocery store":      ["grocery", "groceries", "supermarket", "produce", "Whole Foods", "Aldi"],
    "bar":                ["bar", "drinks", "beer", "cocktails", "nightlife", "brewery", "pub"],
    "gym":                ["gym", "fitness", "workout", "exercise", "yoga", "crossfit"],
    "late night food":    ["late night", "2am", "midnight", "after hours", "open late"],
    "bakery":             ["bakery", "bread", "pastry", "donuts", "croissant", "baked goods"],
    "convenience store":  ["convenience store", "corner store", "bodega", "7-eleven"],
    "coworking space":    ["coworking", "cowork", "workspace", "WeWork", "shared office"],
    "daycare":            ["daycare", "childcare", "nursery", "preschool", "kids"],
    "hardware store":     ["hardware", "tools", "Home Depot", "lumber", "plumbing"],
    "urgent care":        ["urgent care", "clinic", "walk-in", "emergency", "doctor"],
    "general business":   [],
}


def batch(lst, size=5):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def fetch_batch(pytrends, terms):
    """
    Fetch up to 5 terms in a single request — the correct way to use pytrends.
    Returns a dict of {term: stats} for all terms in the batch.
    """
    try:
        pytrends.build_payload(terms, geo=GEO, timeframe=TIMEFRAME)
        df = pytrends.interest_over_time()
        if df.empty:
            return {}
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])

        results = {}
        for term in terms:
            if term not in df.columns:
                continue
            series     = df[term]
            avg        = round(float(series.mean()), 1)
            recent_avg = round(float(series.iloc[-13:].mean()), 1)
            trend_diff = round(recent_avg - avg, 1)
            trend_dir  = "rising" if trend_diff > 2 else "falling" if trend_diff < -2 else "stable"
            peak_val   = int(series.max())
            peak_date  = str(series.idxmax().date()) if peak_val > 0 else "N/A"
            results[term] = {
                "avg_interest":    avg,
                "recent_avg":      recent_avg,
                "trend_vs_avg":    trend_diff,
                "trend_direction": trend_dir,
                "peak_interest":   peak_val,
                "peak_date":       peak_date,
            }
        return results
    except Exception as e:
        print(f"    [!] Batch fetch error: {e}")
        return {}


# ── Main ──────────────────────────────────────────────────────────────────────

def run_trends_analysis():
    print("Initializing pytrends...")
    pytrends = TrendReq(hl="en-US", tz=360)

    category_summary = {}   # Final per-category scores
    term_detail      = {}   # Raw per-term data for reference
    all_csv_rows     = []

    for category, keywords in CATEGORIES.items():
        print(f"\n── {category} ({len(keywords)} keywords) ────────────────────────")

        if not keywords:
            # general business fallback
            category_summary[category] = {
                "category_avg_interest":    0.0,
                "category_trend_direction": "stable",
                "category_trend_vs_avg":    0.0,
                "keywords_searched":        [],
                "keyword_scores":           {},
            }
            print("  (no keywords — fallback category, score = 0)")
            continue

        keyword_scores = {}

        # Fetch all keywords in batches of 5 (one request per batch)
        for i in range(0, len(keywords), 5):
            batch_terms = keywords[i:i+5]
            print(f"  Fetching batch: {batch_terms}")
            results = fetch_batch(pytrends, batch_terms)

            for keyword in batch_terms:
                result = results.get(keyword)
                if result and result["avg_interest"] > 0:
                    keyword_scores[keyword] = result
                    print(f"    {keyword:<20} avg={result['avg_interest']:5.1f}  "
                          f"trend={result['trend_direction']:<8}  "
                          f"peak={result['peak_interest']}")
                    all_csv_rows.append({
                        "category":        category,
                        "keyword":         keyword,
                        "avg_interest":    result["avg_interest"],
                        "recent_avg":      result["recent_avg"],
                        "trend_direction": result["trend_direction"],
                        "trend_vs_avg":    result["trend_vs_avg"],
                        "peak_interest":   result["peak_interest"],
                        "peak_date":       result["peak_date"],
                    })
                else:
                    keyword_scores[keyword] = {"avg_interest": 0.0, "trend_direction": "stable", "trend_vs_avg": 0.0}
                    print(f"    {keyword:<20} no data")

            time.sleep(5)   # One pause per batch, not per keyword

        # ── Aggregate keyword scores into one category score ──────────────────
        valid_scores = [v["avg_interest"] for v in keyword_scores.values() if v["avg_interest"] > 0]
        avg_interest = round(sum(valid_scores) / len(valid_scores), 1) if valid_scores else 0.0

        # Trend direction = majority vote across keywords
        trend_votes = [v["trend_direction"] for v in keyword_scores.values()]
        trend_dir   = max(set(trend_votes), key=trend_votes.count)
        trend_avg   = round(sum(v["trend_vs_avg"] for v in keyword_scores.values()) / len(keyword_scores), 1)

        category_summary[category] = {
            "category_avg_interest":    avg_interest,
            "category_trend_direction": trend_dir,
            "category_trend_vs_avg":    trend_avg,
            "keywords_searched":        list(keyword_scores.keys()),
            "keyword_scores":           keyword_scores,
        }

        print(f"  → CATEGORY SCORE: avg={avg_interest}  trend={trend_dir}  ({len(valid_scores)}/{len(keywords)} keywords had data)")

    # ── Save outputs ──────────────────────────────────────────────────────────

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "category", "keyword", "avg_interest", "recent_avg",
            "trend_direction", "trend_vs_avg", "peak_interest", "peak_date"
        ])
        writer.writeheader()
        writer.writerows(all_csv_rows)
    print(f"\n✓ Keyword detail saved → {OUTPUT_CSV}")

    output = {"geo": GEO, "timeframe": TIMEFRAME, "categories": category_summary}
    OUTPUT_SUMMARY.write_text(json.dumps(output, indent=2))
    print(f"✓ Category summary saved → {OUTPUT_SUMMARY}")

    # ── Final ranked report ───────────────────────────────────────────────────
    print("\n" + "="*60)
    print("  MADISON STORE DEMAND — CATEGORY RANKINGS")
    print("="*60)
    print(f"\n  {'Category':<22} {'Avg Interest':>13}  {'Trend':<10}")
    print("  " + "-"*48)

    ranked = sorted(
        category_summary.items(),
        key=lambda x: x[1]["category_avg_interest"],
        reverse=True
    )
    for cat, data in ranked:
        print(f"  {cat:<22} {data['category_avg_interest']:>13.1f}  {data['category_trend_direction']:<10}")

    print("\n── Rising Categories ─────────────────────────────────")
    for cat, data in ranked:
        if data["category_trend_direction"] == "rising":
            print(f"  {cat:<22} +{data['category_trend_vs_avg']:.1f} vs avg")

    # ── Final Trends Score (0-100) per category ──────────────────────────────
    # Step 1: multiply raw avg by trend multiplier
    # Step 2: normalize so the highest result = 100 (not capped)
    # This preserves differences between categories instead of flattening them

    TREND_MULTIPLIERS = {"rising": 1.25, "stable": 1.00, "falling": 0.75}

    raw_scores = {
        cat: data["category_avg_interest"] * TREND_MULTIPLIERS[data["category_trend_direction"]]
        for cat, data in category_summary.items()
    }
    max_raw = max(raw_scores.values()) if any(v > 0 for v in raw_scores.values()) else 1

    print("\n" + "="*60)
    print("  FINAL TRENDS SCORES (0-100) - use in combined scorer")
    print("="*60)
    print(f"\n  Category               Avg     Trend       x Mult   Final Score")
    print("  " + "-"*62)

    trends_scores = {}
    for cat, data in ranked:
        avg       = data["category_avg_interest"]
        trend_dir = data["category_trend_direction"]
        mult      = TREND_MULTIPLIERS[trend_dir]
        raw       = raw_scores[cat]
        final     = round((raw / max_raw) * 100, 1)
        trends_scores[cat] = final
        data["trends_final_score"] = final
        print(f"  {cat:<22}  {avg:>5.1f}  {trend_dir:<10}  x{mult:.2f}  {final:>8.1f}")

    output = {"geo": GEO, "timeframe": TIMEFRAME, "categories": category_summary}
    OUTPUT_SUMMARY.write_text(json.dumps(output, indent=2))
    print(f"\n Scores saved to {OUTPUT_SUMMARY}")

    return category_summary


if __name__ == "__main__":
    run_trends_analysis()