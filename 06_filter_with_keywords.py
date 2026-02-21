"""
Step 2 (Free Version): Filter Relevant Meetings Using Keywords
=============================================================
Replaces the Claude API filter with simple keyword matching.
Works just as well for meeting titles since they're descriptive.
Completely free, no API key needed, runs instantly.
"""

import json
import re
from pathlib import Path

INPUT_JSON  = Path("all_meetings.json")
OUTPUT_JSON = Path("all_meetings.json")  # Update in place

# ── Keywords that strongly suggest relevant content ───────────────────────────

RELEVANT_KEYWORDS = [
    # Development & business proposals
    "development", "redevelopment", "proposed development",
    "mixed use", "mixed-use", "commercial", "retail",
    "conditional use", "zoning", "rezoning",

    # Specific store/business types
    "grocery", "food", "restaurant", "cafe", "coffee",
    "pharmacy", "clinic", "market", "store", "shop",
    "bakery", "brewery", "bar", "tavern", "nightclub",
    "laundromat", "hardware", "childcare", "daycare",

    # Neighborhood planning — these often include business discussions
    "neighborhood plan", "area plan", "focus area",
    "corridor", "master plan", "comprehensive plan",
    "south madison", "allied drive", "park street",
    "east washington", "williamson", "willy street",
    "meadowood", "badger road", "odana",

    # Explicit community need signals
    "market study", "economic", "business district",
    "affordable", "underserved", "food desert",
    "community input", "town hall", "community conversation",

    # TID = Tax Increment District, almost always commercial development
    "tid", "tax increment",
]

# ── Keywords that strongly suggest NOT relevant ───────────────────────────────

SKIP_KEYWORDS = [
    "redistricting", "election", "voting",
    "traffic", "parking", "road", "street lighting", "sidewalk",
    "water tower", "utility", "sewer",
    "school board", "library board",
    "fire station", "police", "public safety",
    "symphony", "orchestra", "arts center",
    "park renovation", "playground",
    "budget overview",   # generic budget meetings without neighborhood angle
]

# ── Patterns that are almost always relevant ─────────────────────────────────

ALWAYS_RELEVANT_PATTERNS = [
    r"proposed (development|redevelopment)",
    r"(neighborhood|community) meeting",
    r"(cond|conditional) use",
    r"south madison (plan|focus|market|strategy)",
    r"(phase \d|parcel) (redevelopment|development)",
    r"liquor licen",          # liquor license meetings = new business opening
]

# ── Patterns that are almost always irrelevant ────────────────────────────────

ALWAYS_SKIP_PATTERNS = [
    r"redistricting",
    r"traffic (safety|calming|management)",
    r"parking (on|at|near|management)",
    r"gun violence",
    r"budget (conversation|overview|presentation)",
]


def score_title(title, description=""):
    """
    Score a meeting title for relevance.
    Returns (is_relevant: bool, reason: str)
    """
    text = (title + " " + (description or "")).lower()

    # Hard skip patterns first
    for pattern in ALWAYS_SKIP_PATTERNS:
        if re.search(pattern, text):
            return False, f"skip pattern: {pattern}"

    # Hard relevant patterns
    for pattern in ALWAYS_RELEVANT_PATTERNS:
        if re.search(pattern, text):
            return True, f"strong match: {pattern}"

    # Count relevant keyword hits
    relevant_hits = [kw for kw in RELEVANT_KEYWORDS if kw in text]
    skip_hits     = [kw for kw in SKIP_KEYWORDS if kw in text]

    # Relevant if 1+ relevant keywords and no skip keywords
    if relevant_hits and not skip_hits:
        return True, f"keywords: {', '.join(relevant_hits[:3])}"

    # Relevant keywords outweigh skip keywords
    if len(relevant_hits) > len(skip_hits):
        return True, f"keywords: {', '.join(relevant_hits[:3])}"

    return False, f"no relevant keywords (skip: {', '.join(skip_hits[:2])})"


def run_filter():
    if not INPUT_JSON.exists():
        print("[!] Run 05_scrape_meeting_titles.py first")
        return

    meetings = json.loads(INPUT_JSON.read_text())
    print(f"Filtering {len(meetings)} meetings by keyword...\n")

    relevant_count = 0
    skipped_count  = 0

    for meeting in meetings:
        is_relevant, reason = score_title(
            meeting["title"],
            meeting.get("description", "")
        )
        meeting["relevant"] = is_relevant
        meeting["filter_reason"] = reason

        if is_relevant:
            relevant_count += 1
        else:
            skipped_count += 1

    OUTPUT_JSON.write_text(json.dumps(meetings, indent=2))

    # Print results
    relevant = [m for m in meetings if m["relevant"]]
    skipped  = [m for m in meetings if not m["relevant"]]

    print(f"── RELEVANT ({relevant_count}) ── flagged for transcription ────────")
    for m in relevant:
        print(f"  [{m['date']}] {m['title']}")
        print(f"           → {m['filter_reason']}")

    print(f"\n── SKIPPED ({skipped_count}) ─────────────────────────────────────")
    for m in skipped[:15]:  # Show first 15 so you can sanity check
        print(f"  [{m['date']}] {m['title']}")
        print(f"           → {m['filter_reason']}")
    if len(skipped) > 15:
        print(f"  ... and {len(skipped) - 15} more")

    print(f"\n✓ Saved to {OUTPUT_JSON}")
    print(f"\nTIP: Review the SKIPPED list above — if anything looks")
    print(f"     wrongly filtered, add its keywords to RELEVANT_KEYWORDS")
    print(f"     and re-run. Takes 2 seconds.")

    return meetings


if __name__ == "__main__":
    run_filter()
