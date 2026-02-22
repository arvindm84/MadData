# UrbanPlot: Madison Retail Desert Analysis

UrbanPlot is a civic-tech data platform that bridges urban planners and entrepreneurs by identifying retail gaps in Madison, WI. We integrate zoning data, parcel information, and hyper-local sentiment analysis to help identify underserved areas and guide new business development.

## Project Overview

The platform analyzes community demand across Madison by aggregating sentiment from multiple sources:
- **Reddit discussions** about local businesses and needs
- **Isthmus articles** covering Madison business and community news
- **Google Trends data** showing search interest by business category
- **Civic meeting transcripts** reflecting local development priorities
- **Zoning and parcel data** from City of Madison
- **Census data** for demographic context

The analysis identifies "Retail Deserts"—areas where community demand is unmet—and provides entrepreneurs with data-driven recommendations for new business locations.

## Key Features

- **Interactive Map** (`pages/map.html`): Visualizes demand scores and available parcels across Madison
- **Venture Recommender** (`pages/venture.html`): Helps entrepreneurs find optimal locations for their business ideas
- **Sentiment Analysis**: Uses transformer models to classify business types and analyze community sentiment
- **Dynamic Scoring**: Combines demand signals from multiple sources into actionable location scores

## Technology Stack

- **Frontend**: HTML5, CSS3, JavaScript with Mapbox integration
- **Backend**: Python with data pipeline
- **Data Processing**: Pandas, GeoPandas, PyTorch with HuggingFace Transformers
- **Analysis Tools**: Sentiment analysis (RoBERTa), Zero-shot classification (BART), scikit-learn

## Project Structure

### Core Files
- `index.html`, `pages/map.html`, `pages/venture.html` - Web interface
- `fix_final_scores.py` - Data quality and deduplication

### Data Pipeline (`scripts/`, `nlp/`, `scoring/`)

**Data Collection & Extraction:**
- `scripts/reddit_scraper.py` - Scrapes Reddit discussions
- `scripts/scrape_isthmus.py` - Extracts Isthmus articles
- `scripts/google_trends.py` - Analyzes Google Trends for 14 business categories
- `scripts/calc_sentiment_transcripts.py` - Processes civic meeting transcripts

**Data Processing:**
- `nlp/extract_reddit.py`, `nlp/extract_isthmus.py` - Cleans and structures extracted data
- `nlp/sentiment_analysis.py` - Performs NLP sentiment classification and business type identification
- `nlp/aggregate_sentiment.py` - Combines sentiment across sources
- `nlp/attach_coordinates.py` - Maps sentiment data to geographic coordinates
- `nlp/merge_sources.py` - Integrates multiple data sources

**Scoring & Analysis:**
- `scoring/probability_score.py` - Calculates demand probability scores
- `scoring/fix_nulls.py` - Handles missing data
- `scripts/business_score.py` - Generates final location recommendation scores
- `scripts/filter_business.py`, `scripts/filter_reddit.py` - Data filtering

### Data Directories

**Raw Data** (`data/raw/`):
- Reddit discussions, Isthmus articles, Google Trends results, transcripts

**Processed Data** (`data/processed/`):
- `final_scores.csv/json` - Final location recommendation scores (676 parcels)
- `sentiment_by_area.csv/json` - Aggregated sentiment by geographic area
- `sentiment_by_area_business.csv/json` - Sentiment by area and business type
- `business_scores.csv` - Individual business category demand scores
- `trends_demand_score.json` - Google Trends analysis results

**Geographic Data** (`data/`):
- `all_businesses_madison.geojson` - Existing business locations
- `vacant_lots_madison.geojson` - Available parcels
- `vacant_lots_scored.geojson` - Parcels with demand scores
- `census_data_2024.geojson` - Demographic data

### Frontend Assets
- `js/map.js` - Mapbox map interactions
- `js/venture.js` - Business recommender logic
- `css/` - Styling for home, map, venture pages, and responsive design
- `audio/` - Supporting media

## Dependencies

### Python Requirements

```
requests >= 2.28.0          # HTTP library for web scraping
pandas >= 2.0.0            # Data manipulation
pytrends >= 4.9.0          # Google Trends API
beautifulsoup4 >= 4.12.0   # Web scraping
lxml >= 4.9.0              # XML/HTML processing
geopandas                  # Geospatial data analysis
scikit-learn               # Machine learning utilities
torch                      # Deep learning (for transformers)
transformers               # HuggingFace NLP models
```

## Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/[your-org]/UrbanPlot.git
   cd UrbanPlot
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   source .venv/bin/activate  # macOS/Linux
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the data pipeline** (optional - processed data included)
   ```bash
   python scripts/reddit_scraper.py
   python scripts/scrape_isthmus.py
   python scripts/google_trends.py
   python nlp/sentiment_analysis.py
   python nlp/aggregate_sentiment.py
   python scoring/probability_score.py
   python scripts/business_score.py
   python fix_final_scores.py
   ```

5. **View the application**
   - Open `index.html` in a web browser
   - Navigate to the Map page to explore demand scores
   - Use the Ventures page to get recommendations

## Data Sources

- **Reddit**: General community discussions about local businesses and needs
- **Isthmus**: Madison's alternative weekly reporting on business and development
- **Google Trends**: Search volume trends across 14 business categories
- **City of Madison**: Zoning codes, parcel boundaries, development data
- **Census**: Demographic and economic data by area
- **Civic Meetings**: Community input on development priorities

## How It Works

1. **Data Collection**: Aggregate text from multiple sources (Reddit, news, civic meetings)
2. **Sentiment Analysis**: Use transformer models to classify sentiment and business types
3. **Geographic Mapping**: Attach sentiment scores to Madison coordinates and parcels
4. **Demand Scoring**: Combine multiple signals (sentiment, trends, business types) into location scores
5. **Visualization**: Display results on interactive map and venture recommender tool
