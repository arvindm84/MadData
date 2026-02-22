#!/usr/bin/env python3
"""
Sentiment Analysis + Business Type Classification for Madison WI data.
Input: data/processed/all_text_combined.csv
Output: data/processed/sentiment_scores_raw.csv

Uses:
- cardiffnlp/twitter-roberta-base-sentiment-latest for sentiment
- facebook/bart-large-mnli for zero-shot business classification
"""

import pandas as pd
import torch
from transformers import pipeline
from tqdm import tqdm
from pathlib import Path

# Paths
DATA_DIR = Path(__file__).parent.parent
INPUT_FILE = DATA_DIR / "data" / "processed" / "all_text_combined.csv"
OUTPUT_FILE = DATA_DIR / "data" / "processed" / "sentiment_scores_raw.csv"

# Device setup - use GPU if available
device = 0 if torch.cuda.is_available() else -1
print(f"🖥️  Using device: {'GPU (CUDA)' if device == 0 else 'CPU'}")

# Batch size
BATCH_SIZE = 32


def run_sentiment_analysis(texts, sentiment_pipe):
    """Run sentiment analysis in batches."""
    results = []
    
    for i in tqdm(range(0, len(texts), BATCH_SIZE), desc="Sentiment Analysis"):
        batch = texts[i:i + BATCH_SIZE]
        
        # Run inference with truncation
        batch_results = sentiment_pipe(
            batch,
            truncation=True,
            max_length=512,
            return_all_scores=True
        )
        
        for scores in batch_results:
            # Map labels: LABEL_0=negative, LABEL_1=neutral, LABEL_2=positive
            score_dict = {s["label"]: s["score"] for s in scores}
            
            negative_score = score_dict.get("negative", score_dict.get("LABEL_0", 0))
            neutral_score = score_dict.get("neutral", score_dict.get("LABEL_1", 0))
            positive_score = score_dict.get("positive", score_dict.get("LABEL_2", 0))
            
            # Find winning label
            label_scores = {
                "negative": negative_score,
                "neutral": neutral_score,
                "positive": positive_score
            }
            sentiment_label = max(label_scores, key=label_scores.get)
            sentiment_confidence = label_scores[sentiment_label]
            
            results.append({
                "positive_score": positive_score,
                "neutral_score": neutral_score,
                "negative_score": negative_score,
                "sentiment_label": sentiment_label,
                "sentiment_confidence": sentiment_confidence
            })
    
    return results


def run_business_classification(texts, classifier):
    """Run zero-shot business type classification in batches."""
    candidate_labels = [
        "coffee shop", "restaurant", "pharmacy", "grocery store",
        "bar", "gym", "late night food", "bakery",
        "convenience store", "coworking space", "daycare",
        "hardware store", "urgent care", "general business"
    ]
    
    results = []
    
    for i in tqdm(range(0, len(texts), BATCH_SIZE), desc="Business Type Classification"):
        batch = texts[i:i + BATCH_SIZE]
        
        for text in batch:
            # Run zero-shot classification
            result = classifier(
                text,
                candidate_labels,
                truncation=True,
                max_length=512
            )
            
            results.append({
                "business_type": result["labels"][0],
                "business_type_confidence": result["scores"][0]
            })
    
    return results


def main():
    # Load data
    print(f"\n📂 Loading: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    print(f"   Loaded {len(df)} rows")
    
    texts = df["text"].tolist()
    
    # =========================================================================
    # PART 1: SENTIMENT ANALYSIS
    # =========================================================================
    print(f"\n{'='*60}")
    print("PART 1: SENTIMENT ANALYSIS")
    print(f"{'='*60}")
    print("Loading model: cardiffnlp/twitter-roberta-base-sentiment-latest")
    
    sentiment_pipe = pipeline(
        "sentiment-analysis",
        model="cardiffnlp/twitter-roberta-base-sentiment-latest",
        device=device,
        truncation=True,
        max_length=512
    )
    
    sentiment_results = run_sentiment_analysis(texts, sentiment_pipe)
    
    # Add sentiment columns to dataframe
    for key in ["positive_score", "neutral_score", "negative_score", 
                "sentiment_label", "sentiment_confidence"]:
        df[key] = [r[key] for r in sentiment_results]
    
    # Free memory
    del sentiment_pipe
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    
    # =========================================================================
    # PART 2: BUSINESS TYPE CLASSIFICATION
    # =========================================================================
    print(f"\n{'='*60}")
    print("PART 2: BUSINESS TYPE CLASSIFICATION")
    print(f"{'='*60}")
    print("Loading model: facebook/bart-large-mnli")
    
    classifier = pipeline(
        "zero-shot-classification",
        model="facebook/bart-large-mnli",
        device=device
    )
    
    business_results = run_business_classification(texts, classifier)
    
    # Add business columns to dataframe
    df["business_type"] = [r["business_type"] for r in business_results]
    df["business_type_confidence"] = [r["business_type_confidence"] for r in business_results]
    
    # Free memory
    del classifier
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    
    # =========================================================================
    # PART 3: SAVE AND SUMMARY
    # =========================================================================
    print(f"\n{'='*60}")
    print("PART 3: SAVE AND SUMMARY")
    print(f"{'='*60}")
    
    # Save
    print(f"\n💾 Saving: {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False)
    
    # Sentiment breakdown
    print(f"\n📊 SENTIMENT LABEL BREAKDOWN:")
    print(df["sentiment_label"].value_counts().to_string())
    
    # Business type breakdown
    print(f"\n🏪 BUSINESS TYPE BREAKDOWN:")
    print(df["business_type"].value_counts().to_string())
    
    # Sample rows
    print(f"\n📋 SAMPLE ROWS (5):")
    print("-" * 100)
    sample = df[["text", "sentiment_label", "business_type"]].head(5)
    for i, row in sample.iterrows():
        text_preview = row["text"][:60] + "..." if len(str(row["text"])) > 60 else row["text"]
        print(f"{i+1}. [{row['sentiment_label']}] [{row['business_type']}]")
        print(f"   {text_preview}")
        print()
    
    print(f"\n✅ COMPLETE! Saved {len(df)} rows with 7 new columns.")


if __name__ == "__main__":
    main()
