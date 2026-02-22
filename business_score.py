import geopandas as gpd
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

"""
This script calculates a "business score" for vacant lots in Madison, WI, to help identify the best locations for a new coffee shop.
Input: Nothing :)
Output: A DataFrame with the following columns:
- id: Unique identifier for each vacant lot
- saturation_score: A score from 0 to 1 indicating how saturated the area is with
    coffee shops (1 = no competitors, 0 = very saturated)
- traffic_score: A score from 0 to 1 indicating the foot traffic potential based on nearby businesses (1 = high traffic, 0 = low traffic)
- demo_score: A score from 0 to 1 indicating the demographic fit based on median
    income (1 = high income, 0 = low income)
- business_score: The final combined score (average of the three individual scores)
- lat: Latitude of the vacant lot (for mapping)
- lon: Longitude of the vacant lot (for mapping)
"""
def calculate_business_score():
    # Load the file
    vacant_lots = gpd.read_file('static\\datasets\\vacant_lots_madison.geojson')

    # Convert degrees to metres by projecting to a Metric CRS
    # EPSG:32616 is the UTM Zone 16N projection, which perfectly covers Madison, WI.
    vacant_lots = vacant_lots.to_crs(epsg=32616)
    # Convert back to degrees before sending to the frontend for visualization
    # vacant_lots = vacant_lots.to_crs(epsg=4326)

    # Standardize the Geometry Points
    vacant_lots['geometry'] = vacant_lots.centroid

    # Clean the data by keeping only relevant columns
    cols_to_keep = ['id', 'geometry']

    if 'name' in vacant_lots.columns:
        cols_to_keep.append('name')
    if 'addr:street' in vacant_lots.columns:
        cols_to_keep.append('addr:street')

    vacant_lots_clean = vacant_lots[cols_to_keep].copy()

    # Load the master list of all Madison businesses
    all_businesses = gpd.read_file('static\\datasets\\all_businesses_madison.geojson')

    # Project to meters (EPSG:32616 for Madison)
    all_businesses = all_businesses.to_crs(epsg=32616)
    all_businesses['geometry'] = all_businesses.centroid

    # Filter out JUST the coffee shops for your saturation score
    # OSM tags coffee shops as amenity=cafe
    existing_coffee_shops = all_businesses[all_businesses['amenity'] == 'cafe'].copy()

    # Load the Census GeoJSON you just downloaded
    census_tracts = gpd.read_file('static\\datasets\\census_data_2024.geojson')

    # Project to meters to match your other data!
    census_tracts = census_tracts.to_crs(epsg=32616)

    # Rename the messy census income column to something readable
    # (Open the GeoJSON or print the columns to see the exact column name, it usually looks like 'B19013001')
    census_tracts = census_tracts.rename(columns={'B19013001': 'Median_Income'})

    # Create the 400m buffer around each vacant lot to define the analysis zone
    analysis_zones = vacant_lots_clean.copy()
    analysis_zones['geometry'] = analysis_zones.geometry.buffer(400)

    # Join the existing coffee shop points to the 400m buffer
    competitors_in_zone = gpd.sjoin(existing_coffee_shops, analysis_zones, predicate='within')

    # Count how many points fell into each buffer's index ('index_right')
    comp_counts = competitors_in_zone.groupby('index_right').size()

    # Map the counts back to the analysis_zones dataframe
    analysis_zones['competitor_count'] = comp_counts
    analysis_zones['competitor_count'] = analysis_zones['competitor_count'].fillna(0)

    # Custom penalty logic
    def score_saturation(count):
        if count == 0: return 1.0
        elif count == 1: return 0.7
        elif count == 2: return 0.3
        else: return 0.0

    analysis_zones['saturation_score'] = analysis_zones['competitor_count'].apply(score_saturation)

    # Foot Traffic Proxy Score
    # Join all businesses to the 400m buffers to gauge activity density
    traffic_in_zone = gpd.sjoin(all_businesses, analysis_zones, predicate='within')
    traffic_counts = traffic_in_zone.groupby('index_right').size()
    analysis_zones['traffic_count'] = traffic_counts.fillna(0)

    # Normalize traffic counts strictly between 0 and 1 using MinMaxScaler
    scaler = MinMaxScaler()
    analysis_zones[['traffic_score']] = scaler.fit_transform(analysis_zones[['traffic_count']])

    # Demographic Fit Score
    # We revert to the original POINT data to see which Census Tract polygon the lot falls inside
    demo_join = gpd.sjoin(vacant_lots_clean, census_tracts, predicate='within')

    # Normalize the Median Income directly into a demographic score (0 to 1)
    demo_join[['demo_score']] = scaler.fit_transform(demo_join[['Median_Income']])

    # Merge everything back together based on your unique ID column
    final_data = analysis_zones[['id', 'saturation_score', 'traffic_score']].merge(
        demo_join[['id', 'demo_score']], on='id'
    )

    # The Master Formula
    # Calculate the final combined business score
    final_data['business_score'] = (
        final_data['saturation_score'] + 
        final_data['traffic_score'] + 
        final_data['demo_score']
    ) / 3

    # Convert the original points back to Web Coordinates (Degrees)
    # We use EPSG:4326, which is the universal standard for web maps (Lat/Lon)
    web_ready_points = vacant_lots_clean.to_crs(epsg=4326)

    # Extract the Latitude (y) and Longitude (x) into standard columns
    web_ready_points['lat'] = web_ready_points.geometry.y
    web_ready_points['lon'] = web_ready_points.geometry.x

    # Merge these new coordinate columns into your existing final_data
    # We only bring over 'id', 'lat', and 'lon' so we don't clutter the table
    final_data = final_data.merge(web_ready_points[['id', 'lat', 'lon']], on='id')

    return final_data

if __name__ == "__main__":
    print(calculate_business_score())