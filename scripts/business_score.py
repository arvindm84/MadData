import geopandas as gpd
import pandas as pd
import json
from shapely.geometry import Point, box
import os

# Define business categories and keywords
CATEGORIES = {
    "coffee shop": ["coffee", "cafe", "espresso", "latte", "cappuccino"],
    "restaurant": ["restaurant", "food", "eat", "dining", "lunch", "dinner", "brunch"],
    "pharmacy": ["pharmacy", "drugstore", "prescriptions", "CVS", "Walgreens", "medicine"],
    "grocery store": ["grocery", "groceries", "supermarket", "produce", "Whole Foods", "Aldi"],
    "bar": ["bar", "drinks", "beer", "cocktails", "nightlife", "brewery", "pub"],
    "gym": ["gym", "fitness", "workout", "exercise", "yoga", "crossfit"],
    "late night food": ["late night", "2am", "midnight", "after hours", "open late"],
    "bakery": ["bakery", "bread", "pastry", "donuts", "croissant", "baked goods"],
    "convenience store": ["convenience", "corner store", "bodega", "7-eleven"],
    "coworking space": ["coworking", "cowork", "workspace", "WeWork", "shared office"],
    "daycare": ["daycare", "childcare", "nursery", "preschool", "kids"],
    "hardware store": ["hardware", "tools", "Home Depot", "lumber", "plumbing"],
    "urgent care": ["urgent care", "clinic", "walk-in", "emergency", "doctor"],
    "general business": []
}

def categorize_business(row):
    """Assign a category based on keywords in name or osm tags"""
    text = str(row.get('name', '')).lower()
    tags_to_check = ['amenity', 'shop', 'leisure', 'healthcare']
    for tag in tags_to_check:
        val = str(row.get(tag, '')).lower()
        if val and val != 'nan':
            text += " " + val

    for category, keywords in CATEGORIES.items():
        if any(kw.lower() in text for kw in keywords):
            return category
    return "general business"

def calculate_recommendations():
    print("Loading datasets...")
    # Paths
    LOTS_PATH = 'data/vacant_lots_madison.geojson'
    BIZ_PATH = 'data/all_businesses_madison.geojson'
    TAX_PATH = 'data/Tax_Parcels.geojson'
    CENSUS_PATH = 'data/census_data_2024.geojson'
    
    # Load primary data
    vacant_lots = gpd.read_file(LOTS_PATH)
    all_businesses = gpd.read_file(BIZ_PATH)
    census_tracts = gpd.read_file(CENSUS_PATH)

    # Optimization: Use a bounding box mask for the huge Tax Parcels file
    print("Preparing spatial mask for tax parcels...")
    # We buffer the bounding box of the vacant lots to capture the immediate neighborhood
    # Bounding box is in degrees (EPSG:4326)
    bbox = vacant_lots.total_bounds
    mask_geom = box(bbox[0]-0.01, bbox[1]-0.01, bbox[2]+0.01, bbox[3]+0.01)
    
    print(f"Loading Tax Parcels (masked by {mask_geom.bounds})...")
    try:
        # Load only necessary columns and use the spatial mask to save memory/time
        tax_parcels = gpd.read_file(TAX_PATH, mask=mask_geom)
        print(f"Loaded {len(tax_parcels)} relevant parcels.")
    except Exception as e:
        print(f"Masked load failed or unsupported, attempting full load (Memory-heavy): {e}")
        tax_parcels = gpd.read_file(TAX_PATH)

    # Project to metric CRS for accurate distance calculations (Madison UTM 16N)
    target_crs = "EPSG:32616"
    print(f"Projecting layers to {target_crs}...")
    
    # Harmonize CRS before projection
    for name, gdf in [("Lots", vacant_lots), ("Business", all_businesses), ("Parcels", tax_parcels), ("Census", census_tracts)]:
        if gdf.crs is None:
            print(f"Warning: {name} missing CRS, assuming EPSG:4326")
            gdf.set_crs("EPSG:4326", inplace=True)

    vacant_lots = vacant_lots.to_crs(target_crs)
    all_businesses = all_businesses.to_crs(target_crs)
    census_tracts = census_tracts.rename(columns={'B19013001': 'Median_Income'}).to_crs(target_crs)
    
    # Keep only what we need from tax parcels to minimize memory usage
    if 'TotalTaxes' in tax_parcels.columns:
        tax_parcels = tax_parcels[['TotalTaxes', 'geometry']].to_crs(target_crs)
    else:
        # Fallback if column names differ
        print("Warning: 'TotalTaxes' column not found in Tax Parcels. Using 0.")
        tax_parcels = tax_parcels[['geometry']].copy()
        tax_parcels['TotalTaxes'] = 0
        tax_parcels = tax_parcels.to_crs(target_crs)

    # Averages for normalization
    avg_city_income = census_tracts['Median_Income'].mean()
    avg_city_tax = tax_parcels['TotalTaxes'].mean()
    print(f"City Averages: Tax=${avg_city_tax:,.2f}, Income=${avg_city_income:,.2f}")

    # Process vacant lots (use centroids for point-in-polygon and distance)
    vacant_lots['geometry'] = vacant_lots.centroid

    # Spatial joins to enrich lots
    print("Enriching lots with tax and census data...")
    # 1. Tax Join
    vacant_lots = gpd.sjoin(vacant_lots, tax_parcels, how='left', predicate='intersects')
    vacant_lots = vacant_lots.sort_values('TotalTaxes', ascending=False).drop_duplicates(subset=['id'])
    vacant_lots['TotalTaxes'] = vacant_lots['TotalTaxes'].fillna(avg_city_tax)
    
    # 2. Census Join
    # Ensure index doesn't conflict
    if 'index_right' in vacant_lots.columns: vacant_lots = vacant_lots.drop(columns=['index_right'])
    vacant_lots = gpd.sjoin(vacant_lots, census_tracts[['Median_Income', 'geometry']], how='left', predicate='within')
    vacant_lots['Median_Income'] = vacant_lots['Median_Income'].fillna(avg_city_income)

    # Scoring Setup
    all_businesses['category'] = all_businesses.apply(categorize_business, axis=1)
    
    results_geojson = []
    results_csv = []

    print(f"Scoring {len(vacant_lots)} lots across {len(CATEGORIES)} categories...")
    for idx, lot in vacant_lots.iterrows():
        # Distances in meters (EPSG:32616)
        comp_dist = all_businesses.distance(lot.geometry)
        
        # Competition counts (0.25 mile buffer)
        category_counts = all_businesses[comp_dist <= 402.34]['category'].value_counts()
        
        # Foot traffic proxy (0.5 mile total business density)
        total_nearby = (comp_dist <= 804.67).sum()
        
        # Scoring Factors
        pop_bonus = min(15, (total_nearby / 50) * 15)
        tax_ratio = lot['TotalTaxes'] / (avg_city_tax if avg_city_tax > 0 else 1)
        upkeep_penalty = min(20, max(0, (tax_ratio - 1) * 10))
        income_ratio = lot['Median_Income'] / (avg_city_income if avg_city_income > 0 else 1)
        demo_bonus = min(15, max(0, (income_ratio - 1) * 15)) if income_ratio > 1 else 0

        lot_category_scores = []
        for category in CATEGORIES.keys():
            count = category_counts.get(category, 0) if category != "general business" else 0
            
            # Integrated Score Formula
            prob = 85 - (count * 20) - upkeep_penalty + pop_bonus + demo_bonus
            prob = max(5, min(98, prob))
            
            # Recommendation Reason
            reason_parts = []
            if count == 0: reason_parts.append(f"High demand: no existing {category}s nearby.")
            else: reason_parts.append(f"Moderate competition: {count} {category}(s) in the immediate area.")
            
            if pop_bonus > 10: reason_parts.append("Excellent foot traffic potential.")
            if demo_bonus > 10: reason_parts.append("Perfect demographic fit for premium services.")
            if upkeep_penalty > 10: reason_parts.append("Caution: high overhead/taxes in this sector.")
            
            lot_category_scores.append({
                "category": category, 
                "score": int(prob), 
                "reason": " ".join(reason_parts)
            })
            
            # CSV Data (Coexistence with legacy pipeline)
            results_csv.append({
                "id": lot['id'], 
                "business_type": category,
                "saturation_score": round(max(0, 1-(count*0.2)), 3), 
                "traffic_score": round(min(1.0, total_nearby/50), 3),
                "demo_score": round(min(1.0, lot['Median_Income']/(avg_city_income*2 if avg_city_income > 0 else 1)), 3),
                "business_score": round(float(prob/100), 3), 
                "lat": 0.0, # Will be filled in post-repro
                "lon": 0.0
            })

        # Sort category scores for this lot
        lot_category_scores.sort(key=lambda x: x['score'], reverse=True)
        
        # Prepare GeoJSON properties
        props = lot.to_dict()
        # Clean up spatial join artifacts
        for k in ['index_right', 'index_left', 'index_right0', 'index_left0']:
            if k in props: del props[k]
        del props['geometry']
        
        props['top_recommendations_json'] = lot_category_scores[:3]
        props['all_scores_json'] = lot_category_scores
        
        results_geojson.append({
            "type": "Feature", 
            "properties": props, 
            "geometry": {
                "type": "Point", 
                "coordinates": [lot.geometry.x, lot.geometry.y]
            }
        })

    print("Finalizing outputs...")
    # 1. Save GeoJSON for Map integration
    scored_gdf = gpd.GeoDataFrame.from_features(results_geojson, crs=target_crs).to_crs(epsg=4326)
    scored_gdf.to_file('data/vacant_lots_scored.geojson', driver='GeoJSON')
    
    # 2. Save CSV for Pipeline/Analytics coexistence
    csv_df = pd.DataFrame(results_csv)
    # Get web coordinates back into CSV
    coords = scored_gdf[['id', 'geometry']].copy()
    coords['lat'] = coords.geometry.y
    coords['lon'] = coords.geometry.x
    csv_df = csv_df.drop(columns=['lat', 'lon']).merge(coords[['id', 'lat', 'lon']], on='id')
    
    # Save to standard location
    csv_df.to_csv('data/business_scores.csv', index=False)
    
    # Also update data/processed/ for legacy checks
    os.makedirs('data/processed', exist_ok=True)
    csv_df.to_csv('data/processed/business_scores.csv', index=False)
    
    print("✓ Success! Integrated scoring complete.")
    print(f"  - Map Data: datasets/vacant_lots_scored.geojson")
    print(f"  - Scorer CSV: business_scores.csv & data/processed/business_scores.csv")

if __name__ == "__main__":
    calculate_recommendations()