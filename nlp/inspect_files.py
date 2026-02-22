#!/usr/bin/env python3
"""
Inspect data files for NLP sentiment analysis project.
Shows: first 2 rows, column names, row counts, null/empty values.
"""

import json
import pandas as pd
from pathlib import Path

# Data files to inspect
DATA_DIR = Path(__file__).parent.parent
FILES = {
    "reddit_filtered_final.jsonl": "jsonl",
    "isthmus_articles.jsonl": "jsonl",
    "google_trends_summary.json": "json",
    "google_trends_results.csv": "csv",
}


def load_jsonl(filepath):
    """Load JSONL file into list of dicts."""
    data = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    return pd.DataFrame(data)


def load_json(filepath):
    """Load JSON file."""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Handle both list and dict formats
    if isinstance(data, list):
        return pd.DataFrame(data)
    elif isinstance(data, dict):
        # If dict, show keys and try to convert
        return pd.DataFrame([data]) if not any(isinstance(v, (list, dict)) for v in data.values()) else data
    return data


def load_csv(filepath):
    """Load CSV file."""
    return pd.read_csv(filepath)


def count_nulls(df):
    """Count null and empty values per column."""
    if not isinstance(df, pd.DataFrame):
        return "N/A (not a DataFrame)"
    
    null_counts = {}
    for col in df.columns:
        null_count = df[col].isna().sum()
        empty_count = (df[col].astype(str).str.strip() == "").sum()
        if null_count > 0 or empty_count > 0:
            null_counts[col] = {"null": int(null_count), "empty": int(empty_count)}
    return null_counts if null_counts else "No nulls or empty values"


def inspect_file(filename, filetype):
    """Inspect a single file."""
    filepath = DATA_DIR / filename
    
    print(f"\n{'='*70}")
    print(f"📄 {filename}")
    print(f"{'='*70}")
    
    if not filepath.exists():
        print(f"   ❌ FILE NOT FOUND: {filepath}")
        return
    
    # Load file
    try:
        if filetype == "jsonl":
            df = load_jsonl(filepath)
        elif filetype == "json":
            df = load_json(filepath)
        elif filetype == "csv":
            df = load_csv(filepath)
        else:
            print(f"   ❌ Unknown file type: {filetype}")
            return
    except Exception as e:
        print(f"   ❌ Error loading file: {e}")
        return
    
    # Handle dict (for nested JSON)
    if isinstance(df, dict):
        print(f"\n📊 TYPE: Nested JSON/Dict")
        print(f"\n🔑 TOP-LEVEL KEYS ({len(df)}):")
        for key in df.keys():
            val = df[key]
            if isinstance(val, list):
                print(f"   • {key}: list with {len(val)} items")
            elif isinstance(val, dict):
                print(f"   • {key}: dict with {len(val)} keys")
            else:
                print(f"   • {key}: {type(val).__name__}")
        
        print(f"\n👀 PREVIEW:")
        preview = json.dumps(df, indent=2, default=str)[:1000]
        print(preview + ("..." if len(json.dumps(df)) > 1000 else ""))
        return
    
    # DataFrame inspection
    print(f"\n📊 TOTAL ROWS: {len(df)}")
    
    print(f"\n🔑 COLUMNS ({len(df.columns)}):")
    for col in df.columns:
        dtype = df[col].dtype
        print(f"   • {col} ({dtype})")
    
    print(f"\n👀 FIRST 2 ROWS:")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 50)
    print(df.head(2).to_string())
    
    print(f"\n⚠️  NULL/EMPTY VALUES:")
    nulls = count_nulls(df)
    if isinstance(nulls, str):
        print(f"   {nulls}")
    else:
        for col, counts in nulls.items():
            print(f"   • {col}: {counts['null']} nulls, {counts['empty']} empty")


def main():
    print("="*70)
    print("🔍 DATA FILE INSPECTION FOR NLP SENTIMENT ANALYSIS")
    print("="*70)
    print(f"Data directory: {DATA_DIR}")
    
    for filename, filetype in FILES.items():
        inspect_file(filename, filetype)
    
    print(f"\n{'='*70}")
    print("✅ INSPECTION COMPLETE")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
