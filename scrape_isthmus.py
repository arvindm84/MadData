#!/usr/bin/env python3
"""
Isthmus Scraper - Madison local news and business articles
Scrapes isthmus.com for business-related content, openings, closings, reviews.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime

# =============================================================================
# CONFIG
# =============================================================================

BASE_URL = "https://isthmus.com"

# Sections to scrape
SECTIONS = [
    "/food-drink",
    "/food-drink/restaurant-news",
    "/food-drink/beer",
    "/food-drink/dining",
    "/opinion",
    "/music",
    "/arts",
    "/news",
]

# Keywords to filter for business-relevant articles
BUSINESS_KEYWORDS = [
    "opening", "open", "opened", "opens",
    "closing", "closed", "closes", "shutdown",
    "new restaurant", "new bar", "new cafe", "new shop",
    "downtown", "state street", "willy street", "east side", "west side",
    "madison", "coffee", "restaurant", "bar", "brewery", "bakery",
    "food", "dining", "retail", "store", "business",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

# =============================================================================
# SCRAPER FUNCTIONS
# =============================================================================

def get_soup(url):
    """Fetch and parse a URL."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code == 200:
            return BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"  Error fetching {url}: {e}")
    return None

def scrape_section(section_url, max_pages=5):
    """Scrape articles from a section."""
    articles = []
    
    for page in range(1, max_pages + 1):
        url = f"{BASE_URL}{section_url}" if page == 1 else f"{BASE_URL}{section_url}/page/{page}"
        
        soup = get_soup(url)
        if not soup:
            break
        
        # Find article links (adjust selectors based on actual site structure)
        article_links = soup.select("article a, .post-title a, h2 a, h3 a")
        
        for link in article_links:
            href = link.get("href", "")
            title = link.get_text(strip=True)
            
            if href and title and len(title) > 10:
                # Make absolute URL
                if href.startswith("/"):
                    href = BASE_URL + href
                
                # Check if business-relevant
                title_lower = title.lower()
                if any(kw in title_lower for kw in BUSINESS_KEYWORDS):
                    articles.append({
                        "title": title,
                        "url": href,
                        "section": section_url.strip("/"),
                    })
        
        time.sleep(1)
    
    return articles

def scrape_article(url):
    """Scrape full content from an article page."""
    soup = get_soup(url)
    if not soup:
        return None
    
    data = {
        "url": url,
        "title": "",
        "text": "",
        "date": "",
        "author": "",
    }
    
    # Title
    title_tag = soup.select_one("h1, .entry-title, .post-title")
    if title_tag:
        data["title"] = title_tag.get_text(strip=True)
    
    # Date
    date_tag = soup.select_one("time, .date, .post-date, .entry-date")
    if date_tag:
        data["date"] = date_tag.get_text(strip=True)
        # Try to get datetime attribute
        if date_tag.get("datetime"):
            data["date"] = date_tag.get("datetime")[:10]
    
    # Author
    author_tag = soup.select_one(".author, .byline, .entry-author")
    if author_tag:
        data["author"] = author_tag.get_text(strip=True)
    
    # Main content
    content_tag = soup.select_one("article, .entry-content, .post-content, .article-body")
    if content_tag:
        # Remove scripts, styles, etc.
        for tag in content_tag.select("script, style, nav, footer, .ad, .advertisement"):
            tag.decompose()
        
        paragraphs = content_tag.find_all("p")
        data["text"] = " ".join(p.get_text(strip=True) for p in paragraphs)
    
    return data

def extract_location(text):
    """Extract Madison neighborhood from text."""
    text_lower = text.lower()
    
    locations = {
        "state street": ["state street", "state st"],
        "willy street": ["willy street", "williamson"],
        "downtown": ["downtown", "capitol square"],
        "east side": ["east side", "eastside", "east washington"],
        "west side": ["west side", "westside"],
        "monroe street": ["monroe street"],
        "atwood": ["atwood"],
        "hilldale": ["hilldale"],
        "middleton": ["middleton"],
        "sun prairie": ["sun prairie"],
    }
    
    for loc, keywords in locations.items():
        if any(kw in text_lower for kw in keywords):
            return loc
    
    return "general madison"

# =============================================================================
# MAIN
# =============================================================================

def run_scraper():
    # Suppress output - save to log file instead
    import sys
    log_file = open("isthmus_scrape.log", "w")
    
    # Step 1: Get article links from sections
    all_article_links = []
    
    for section in SECTIONS:
        log_file.write(f"Section: {section}\n")
        articles = scrape_section(section, max_pages=3)
        all_article_links.extend(articles)
        log_file.write(f"  Found {len(articles)} relevant articles\n")
        time.sleep(1)
    
    # Deduplicate by URL
    seen_urls = set()
    unique_articles = []
    for article in all_article_links:
        if article["url"] not in seen_urls:
            seen_urls.add(article["url"])
            unique_articles.append(article)
    
    log_file.write(f"\nTotal unique articles to scrape: {len(unique_articles)}\n")
    
    # Step 2: Scrape full article content
    full_articles = []
    
    for i, article in enumerate(unique_articles[:100]):  # Limit to 100
        log_file.write(f"[{i+1}/{min(len(unique_articles), 100)}] {article['title'][:50]}...\n")
        
        data = scrape_article(article["url"])
        if data and data["text"]:
            data["section"] = article["section"]
            data["location"] = extract_location(data["text"])
            data["source"] = "isthmus"
            full_articles.append(data)
        
        time.sleep(1)
    
    # Step 3: Save
    if full_articles:
        df = pd.DataFrame(full_articles)
        df.to_csv("isthmus_articles.csv", index=False)
        df.to_json("isthmus_articles.json", orient="records", indent=2)
        
        # Also save as JSONL for NLP
        import json
        with open("isthmus_articles.jsonl", "w") as f:
            for _, row in df.iterrows():
                f.write(json.dumps(row.to_dict()) + "\n")
        
        log_file.write(f"\nSaved {len(df)} articles\n")
        log_file.write(f"By location:\n{df['location'].value_counts().to_string()}\n")
        log_file.write(f"By section:\n{df['section'].value_counts().to_string()}\n")
        
        # Print minimal summary to terminal
        print(f"✅ Scraped {len(df)} articles → isthmus_articles.csv")
    else:
        print("❌ No articles scraped")
    
    log_file.close()

if __name__ == "__main__":
    run_scraper()
