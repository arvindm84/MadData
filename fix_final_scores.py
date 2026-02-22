import pandas as pd
import json
import sys

def main():
    csv_file = 'data/processed/final_scores.csv'
    json_file = 'data/processed/final_scores.json'

    print(f"Loading {csv_file}...")
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        sys.exit(1)

    initial_rows = len(df)
    print(f"Initial row count: {initial_rows}")

    # Drop duplicates keeping only the first occurrence
    df_clean = df.drop_duplicates(subset=['id', 'business_type'], keep='first')
    
    clean_rows = len(df_clean)
    print(f"Clean row count: {clean_rows}")
    print(f"Removed {initial_rows - clean_rows} duplicate rows.")

    # Verifications
    print("\n--- Verifications ---")
    
    # 3. Verifies the result has exactly 676 rows
    if clean_rows == 676:
        print("✅ Row count is exactly 676.")
    else:
        print(f"❌ Warning: Row count is {clean_rows}, expected 676.")

    # 4. Verifies 52 unique lots and 13 unique business types
    unique_lots = df_clean['id'].nunique()
    unique_business_types = df_clean['business_type'].nunique()
    
    if unique_lots == 52:
        print("✅ Exactly 52 unique lots found.")
    else:
        print(f"❌ Warning: Found {unique_lots} unique lots, expected 52.")
        
    if unique_business_types == 13:
        print("✅ Exactly 13 unique business types found.")
    else:
        print(f"❌ Warning: Found {unique_business_types} unique business types, expected 13.")

    # 5. Verifies no nulls in final_probability
    null_probs = df_clean['final_probability'].isnull().sum()
    if null_probs == 0:
        print("✅ No nulls found in final_probability.")
    else:
        print(f"❌ Warning: Found {null_probs} nulls in final_probability.")

    print("---------------------\n")

    # Sort by final_probability descending to get top 10
    df_clean = df_clean.sort_values('final_probability', ascending=False)

    # 6. Saves clean file back to data/processed/final_scores.csv
    print(f"Saving clean CSV to {csv_file}...")
    df_clean.to_csv(csv_file, index=False)

    # 7. Also saves to data/processed/final_scores.json as a clean array of objects with indent=2
    print(f"Saving clean JSON to {json_file}...")
    records = df_clean.to_dict(orient='records')
    with open(json_file, 'w') as f:
        json.dump(records, f, indent=2)

    # 9. Prints top 10 rows by final_probability showing: id, business_type, final_probability, reason
    print("\nTOP 10 OPPORTUNITIES:")
    print(f"{'ID':>15} | {'BUSINESS TYPE':>20} | {'PROB':>5} | REASON")
    print("-" * 80)
    
    top_10 = df_clean.head(10)
    for _, row in top_10.iterrows():
        print(f"{row['id']:>15} | {row['business_type']:>20} | {row['final_probability']:5.1f} | {row.get('reason', '')}")

if __name__ == '__main__':
    main()
