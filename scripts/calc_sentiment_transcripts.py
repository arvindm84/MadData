"""
Transcript Sentiment Analyzer — Madison, WI
============================================
Reads Whisper transcripts, extracts sentences mentioning
business types, runs sentiment analysis, and outputs
one record per (location, business_type) pair.

Output format:
{
  "location_tag":      "monroe street",
  "business_type":     "coffee shop",
  "positive_ratio":    0.75,
  "negative_ratio":    0.10,
  "neutral_ratio":     0.15,
  "overall_sentiment": 0.62,
  "avg_confidence":    0.62,
  "total_entries":     8,
  "low_confidence":    false,
  "lat":               43.0505,
  "lon":              -89.4076
}

Install deps:
    pip install vaderSentiment
"""

import json
import re
from pathlib import Path
from collections import defaultdict
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer       = SentimentIntensityAnalyzer()
TRANSCRIPT_DIR = Path("transcripts")
OUTPUT_JSON    = Path("data/raw/transcript_sentiment.json")

# ── 14 Business categories (matches Google Trends categories) ─────────────────

BUSINESS_CATEGORIES = {
    "coffee shop":       ["coffee", "cafe", "espresso", "latte", "cappuccino"],
    "restaurant":        ["restaurant", "food", "eat", "dining", "lunch", "dinner", "brunch"],
    "pharmacy":          ["pharmacy", "drugstore", "prescriptions", "cvs", "walgreens", "medicine"],
    "grocery store":     ["grocery", "groceries", "supermarket", "produce", "whole foods", "aldi"],
    "bar":               ["bar", "drinks", "beer", "cocktails", "nightlife", "brewery", "pub"],
    "gym":               ["gym", "fitness", "workout", "exercise", "yoga", "crossfit"],
    "late night food":   ["late night", "2am", "midnight", "after hours", "open late"],
    "bakery":            ["bakery", "bread", "pastry", "donuts", "croissant", "baked goods"],
    "convenience store": ["convenience store", "corner store", "bodega", "7-eleven"],
    "coworking space":   ["coworking", "cowork", "workspace", "wework", "shared office"],
    "daycare":           ["daycare", "childcare", "nursery", "preschool", "kids"],
    "hardware store":    ["hardware", "tools", "home depot", "lumber", "plumbing"],
    "urgent care":       ["urgent care", "clinic", "walk-in", "emergency", "doctor"],
    "general business":  [],
}

LOCATIONS = {
    "monroe street":      (43.0505, -89.4076),
    "willy street":       (43.0886, -89.3762),
    "williamson street":  (43.0886, -89.3762),
    "atwood":             (43.0893, -89.3607),
    "south madison":      (43.0489, -89.3902),
    "allied drive":       (43.0385, -89.4347),
    "park street":        (43.0629, -89.3957),
    "east washington":    (43.0943, -89.3542),
    "downtown":           (43.0731, -89.3837),
    "state street":       (43.0762, -89.3895),
    "isthmus":            (43.0762, -89.3837),
    "near east side":     (43.0893, -89.3607),
    "near west side":     (43.0731, -89.4200),
    "east side":          (43.1100, -89.3200),
    "west side":          (43.0731, -89.4800),
    "north side":         (43.1200, -89.3700),
    "south side":         (43.0300, -89.3900),
    "campus":             (43.0766, -89.4125),
    "university avenue":  (43.0766, -89.4125),
    "meadowood":          (43.0300, -89.4347),
    "fitchburg":          (43.0200, -89.4200),
    "middleton":          (43.0990, -89.5000),
    "hilldale":           (43.0731, -89.4500),
    "odana":              (43.0600, -89.4600),
    "oscar mayer":        (43.1100, -89.3500),
    "bimbo bakery":       (43.0800, -89.3300),
    "winnebago":          (43.0900, -89.3500),
    "camp randall":       (43.0700, -89.4128),
    "stoughton road":     (43.0500, -89.3200),
    "badger road":        (43.0450, -89.4100),
}

PUBLIC_COMMENT_SIGNALS = [
    r"\bmy name is\b",
    r"\bi('m| am) a resident\b",
    r"\bi live (on|in|near|at)\b",
    r"\bour neighborhood\b",
    r"\bour community\b",
    r"\bwe (need|want|don't have|lack)\b",
    r"\bi('d| would) like to (see|have|comment)\b",
    r"\bi('m| am) speaking (on behalf|for|as)\b",
]
PUBLIC_COMMENT_RE = re.compile("|".join(PUBLIC_COMMENT_SIGNALS), re.IGNORECASE)


# ── Helpers ───────────────────────────────────────────────────────────────────

def split_sentences(text):
    text = re.sub(r"\s+", " ", text)
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if len(s.strip()) > 10]


def detect_business_types(sentence):
    lower = sentence.lower()
    return [
        btype for btype, keywords in BUSINESS_CATEGORIES.items()
        if keywords and any(kw in lower for kw in keywords)
    ]


def detect_location(sentence, meeting_title):
    """
    Find location tag from sentence text first,
    then fall back to meeting title.
    """
    combined = (sentence + " " + meeting_title).lower()
    for loc in LOCATIONS:
        if loc in combined:
            return loc

    # Broader fallbacks from meeting title
    title_lower = meeting_title.lower()
    if "south madison" in title_lower:
        return "south madison"
    if "allied" in title_lower:
        return "allied drive"
    if "east washington" in title_lower:
        return "east washington"
    if "willy" in title_lower or "williamson" in title_lower:
        return "willy street"
    if "hilldale" in title_lower:
        return "hilldale"
    if "oscar mayer" in title_lower:
        return "oscar mayer"
    if "university" in title_lower:
        return "university avenue"
    if "odana" in title_lower:
        return "odana"
    if "winnebago" in title_lower:
        return "winnebago"
    if "bimbo" in title_lower:
        return "bimbo bakery"
    if "badger" in title_lower:
        return "badger road"

    # Generic fallback — use "madison" as location
    return "downtown"


def classify_sentiment(compound):
    """Convert VADER compound score to pos/neg/neutral label."""
    if compound >= 0.05:
        return "positive"
    elif compound <= -0.05:
        return "negative"
    else:
        return "neutral"


def is_public_comment(sentence):
    return bool(PUBLIC_COMMENT_RE.search(sentence))


# ── Main analysis ─────────────────────────────────────────────────────────────

def analyze_transcripts():
    transcript_files = list(TRANSCRIPT_DIR.glob("*.txt"))
    if not transcript_files:
        print(f"[!] No transcripts found in {TRANSCRIPT_DIR}/")
        return []

    print(f"Analyzing {len(transcript_files)} transcripts...\n")

    # Accumulator: keyed by (location_tag, business_type)
    # Stores list of (compound_score, sentiment_label, is_public_comment)
    accumulator = defaultdict(list)

    for tf in transcript_files:
        raw   = tf.read_text(encoding="utf-8")
        lines = raw.split("\n")

        # Extract meeting title from first line
        title = lines[0].replace("MEETING: ", "").strip() if lines else tf.stem

        # Find transcript text (after === separator)
        sep_idx = next((i for i, l in enumerate(lines) if l.startswith("===")), 4)
        text    = "\n".join(lines[sep_idx + 1:])

        sentences  = split_sentences(text)
        hits       = 0

        for sentence in sentences:
            btypes = detect_business_types(sentence)
            if not btypes:
                continue

            scores   = analyzer.polarity_scores(sentence)
            compound = scores["compound"]
            label    = classify_sentiment(compound)
            location = detect_location(sentence, title)
            is_pub   = is_public_comment(sentence)

            for btype in btypes:
                accumulator[(location, btype)].append({
                    "compound":   compound,
                    "label":      label,
                    "is_public":  is_pub,
                })
                hits += 1

        print(f"  {title[:60]:<60}  {hits} hits")

    # ── Build output records ──────────────────────────────────────────────────
    results = []

    for (location, btype), entries in accumulator.items():
        total    = len(entries)
        pos      = sum(1 for e in entries if e["label"] == "positive")
        neg      = sum(1 for e in entries if e["label"] == "negative")
        neu      = sum(1 for e in entries if e["label"] == "neutral")

        # Weighted avg compound — public comment sentences count 1.5x
        weighted_sum   = sum(
            e["compound"] * (1.5 if e["is_public"] else 1.0)
            for e in entries
        )
        weight_total   = sum(1.5 if e["is_public"] else 1.0 for e in entries)
        overall        = round(weighted_sum / weight_total, 4) if weight_total > 0 else 0.0

        lat, lon = LOCATIONS.get(location, (43.0731, -89.3837))  # default = downtown

        results.append({
            "location_tag":      location,
            "business_type":     btype,
            "positive_ratio":    round(pos / total, 4),
            "negative_ratio":    round(neg / total, 4),
            "neutral_ratio":     round(neu / total, 4),
            "overall_sentiment": overall,
            "avg_confidence":    overall,   # VADER compound used as confidence proxy
            "total_entries":     total,
            "low_confidence":    total < 5, # flag thin data
            "lat":               lat,
            "lon":               lon,
        })

    # Sort by overall_sentiment descending
    results.sort(key=lambda x: x["overall_sentiment"], reverse=True)

    results = [r for r in results if not r["low_confidence"]]

    # ── Save ──────────────────────────────────────────────────────────────────
    OUTPUT_JSON.write_text(json.dumps(results, indent=2))
    print(f"\n✓ {len(results)} (location, business_type) pairs saved → {OUTPUT_JSON}")

    # ── Print summary ─────────────────────────────────────────────────────────
    print("\n" + "="*70)
    print("  TOP RESULTS (sorted by sentiment)")
    print("="*70)
    print(f"\n  {'Location':<22} {'Business Type':<20} {'Sentiment':>10}  {'Entries':>8}  {'Low Conf'}")
    print("  " + "-"*72)

    for r in results[:20]:
        flag = "[LOW]" if r["low_confidence"] else "[OK]  "
        print(f"  {r['location_tag']:<22} {r['business_type']:<20} "
              f"{r['overall_sentiment']:>10.4f}  {r['total_entries']:>8}  {flag}")

    low_conf = sum(1 for r in results if r["low_confidence"])
    print(f"\n  [WARNING] {low_conf}/{len(results)} results flagged low confidence (< 5 sentences)")

    return results


if __name__ == "__main__":
    analyze_transcripts()