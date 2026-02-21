"""
Step 5: Google Trends Analysis for Madison, WI
===============================================
Tracks search interest for store types, unmet needs,
neighborhood-specific searches, and brand searches.

pytrends limits 5 terms per request, so we batch automatically.
Results saved to google_trends_results.csv and a summary report.

Install deps:
    pip install pytrends pandas
"""

import time
import json
import csv
import pandas as pd
from pathlib import Path
from pytrends.request import TrendReq

# ── Madison DMA geo code ──────────────────────────────────────────────────────
GEO       = "US-WI-669"   # Madison, WI Designated Market Area
TIMEFRAME = "today 5-y"   # Last 5 years

OUTPUT_CSV     = Path("google_trends_results.csv")
OUTPUT_SUMMARY = Path("google_trends_summary.json")

# ── Search term groups ────────────────────────────────────────────────────────

STORE_TYPES = [
    "grocery store",
    "affordable grocery",
    "fresh produce",
    "food desert",
    "pharmacy",
    "laundromat",
    "coffee shop",
    "restaurant",
    "hardware store",
    "urgent care",
    "daycare",
    "gym",
]

UNMET_NEED = [
    "nearest grocery store",
    "grocery store near me",
    "cheap groceries",
    "healthy food options",
    "late night food",
    "open sunday",
]

NEIGHBORHOOD_SPECIFIC = [
    "south madison grocery",
    "allied drive food",
    "park street restaurants",
    "east side madison food",
    "near east side madison",
]

BRAND_SEARCHES = [
    "trader joes madison",
    "whole foods madison",
    "aldi madison",
    "costco madison",
    "target madison",
]

# Combine all into one list with category labels
ALL_TERMS = (
    [(t, "store_type")        for t in STORE_TYPES] +
    [(t, "unmet_need")        for t in UNMET_NEED] +
    [(t, "neighborhood")      for t in NEIGHBORHOOD_SPECIFIC] +
    [(t, "brand")             for t in BRAND_SEARCHES]
)


# ── Core functions ────────────────────────────────────────────────────────────

def batch(lst, size=5):
    """Split a list into chunks of `size`."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def fetch_trends(pytrends, terms, geo, timeframe):
    """
    Fetch interest_over_time for a list of up to 5 terms.
    Returns a DataFrame or None on failure.
    """
    try:
        pytrends.build_payload(terms, geo=geo, timeframe=timeframe)
        df = pytrends.interest_over_time()
        if df.empty:
            return None
        # Drop the isPartial column
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])
        return df
    except Exception as e:
        print(f"  [!] Error fetching {terms}: {e}")
        return None


def fetch_related_queries(pytrends, term, geo, timeframe):
    """
    Get the top related queries for a single term.
    Useful for discovering what else people search alongside store terms.
    """
    try:
        pytrends.build_payload([term], geo=geo, timeframe=timeframe)
        related = pytrends.related_queries()
        top = related.get(term, {}).get("top")
        if top is not None and not top.empty:
            return top.head(5).to_dict("records")
    except Exception as e:
        print(f"  [!] Related queries error for '{term}': {e}")
    return []


def summarize_df(df):
    """
    For each term in a DataFrame, compute:
    - average interest (0-100)
    - peak interest
    - peak date
    - recent trend (last 3 months avg vs overall avg)
    """
    summary = {}
    for col in df.columns:
        series     = df[col]
        avg        = round(series.mean(), 1)
        peak_val   = int(series.max())
        peak_date  = str(series.idxmax().date()) if peak_val > 0 else "N/A"
        recent_avg = round(series.iloc[-13:].mean(), 1)  # last ~3 months
        trend      = round(recent_avg - avg, 1)          # positive = rising

        summary[col] = {
            "avg_interest":    avg,
            "peak_interest":   peak_val,
            "peak_date":       peak_date,
            "recent_avg":      recent_avg,
            "trend_vs_avg":    trend,
            "trend_direction": "rising" if trend > 2 else "falling" if trend < -2 else "stable",
        }
    return summary


# ── Main ──────────────────────────────────────────────────────────────────────

def run_trends_analysis():
    print("Initializing pytrends...")
    pytrends = TrendReq(hl="en-US", tz=360)  # tz=360 = US Central

    all_rows    = []   # For CSV output
    all_summary = {}   # For JSON summary
    related_out = {}   # Related queries

    # Group terms by category for batching
    categories = {}
    for term, cat in ALL_TERMS:
        categories.setdefault(cat, []).append(term)

    for category, terms in categories.items():
        print(f"\n── Category: {category} ({len(terms)} terms) ──────────────────")

        for term_batch in batch(terms, 5):
            print(f"  Fetching: {term_batch}")
            df = fetch_trends(pytrends, term_batch, GEO, TIMEFRAME)

            if df is None:
                print(f"  [!] No data returned for this batch")
                time.sleep(5)
                continue

            # Summarize
            summary = summarize_df(df)
            for term, stats in summary.items():
                stats["category"] = category
                all_summary[term] = stats
                print(f"    {term:<35} avg={stats['avg_interest']:5.1f}  "
                      f"peak={stats['peak_interest']:3d} ({stats['peak_date']})  "
                      f"trend={stats['trend_direction']}")

            # Flatten to rows for CSV
            for date, row in df.iterrows():
                for term in df.columns:
                    all_rows.append({
                        "date":     str(date.date()),
                        "term":     term,
                        "category": category,
                        "interest": int(row[term]),
                    })

            time.sleep(3)  # Respect rate limits — pytrends gets blocked if too fast

        # Fetch related queries for top terms in this category
        # (just the first term per category to avoid too many requests)
        top_term = terms[0]
        print(f"  Fetching related queries for: '{top_term}'")
        related  = fetch_related_queries(pytrends, top_term, GEO, TIMEFRAME)
        if related:
            related_out[top_term] = related
            print(f"    Top related: {[r['query'] for r in related[:3]]}")
        time.sleep(3)

    # ── Save outputs ──────────────────────────────────────────────────────────

    # CSV — full time series
    if all_rows:
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["date", "term", "category", "interest"])
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\n✓ Time series saved → {OUTPUT_CSV} ({len(all_rows):,} rows)")

    # JSON — summary stats + related queries
    output = {
        "geo":          GEO,
        "timeframe":    TIMEFRAME,
        "summary":      all_summary,
        "related_queries": related_out,
    }
    OUTPUT_SUMMARY.write_text(json.dumps(output, indent=2))
    print(f"✓ Summary saved → {OUTPUT_SUMMARY}")

    # ── Print final ranked report ─────────────────────────────────────────────
    print("\n" + "="*60)
    print("  MADISON SEARCH DEMAND RANKING")
    print("="*60)

    # Sort all terms by avg interest descending
    ranked = sorted(all_summary.items(), key=lambda x: x[1]["avg_interest"], reverse=True)

    print(f"\n{'Term':<35} {'Avg':>5}  {'Peak':>5}  {'Trend':<8}  Category")
    print("-"*75)
    for term, stats in ranked:
        print(f"  {term:<33} {stats['avg_interest']:>5.1f}  "
              f"{stats['peak_interest']:>5d}  "
              f"{stats['trend_direction']:<8}  "
              f"{stats['category']}")

    # Brand gap analysis — these are stores Madison doesn't have
    print("\n── Brand Gap Analysis (stores people search for but may not exist) ──")
    brand_terms = {k: v for k, v in all_summary.items() if v["category"] == "brand"}
    for term, stats in sorted(brand_terms.items(), key=lambda x: x[1]["avg_interest"], reverse=True):
        print(f"  {term:<30} avg interest: {stats['avg_interest']:>5.1f}  {stats['trend_direction']}")

    print("\n── Rising Trends (searches growing recently) ──────────────────────")
    rising = [(t, s) for t, s in all_summary.items() if s["trend_direction"] == "rising"]
    for term, stats in sorted(rising, key=lambda x: x[1]["trend_vs_avg"], reverse=True):
        print(f"  {term:<35} +{stats['trend_vs_avg']:.1f} vs avg")

    return all_summary


if __name__ == "__main__":
    run_trends_analysis()