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

    df_clean = df.drop_duplicates(subset=['id', 'business_type'], keep='first')
    
    clean_rows = len(df_clean)
    print(f"Clean row count: {clean_rows}")
    print(f"Removed {initial_rows - clean_rows} duplicate rows.")

    # Verifications
    print("\n--- Verifications ---")
    
    if clean_rows == 676:
        print("[OK] Row count is exactly 676.")
    else:
        print(f"[WARNING] Row count is {clean_rows}, expected 676.")

    unique_lots = df_clean['id'].nunique()
    unique_business_types = df_clean['business_type'].nunique()
    
    if unique_lots == 52:
        print("[OK] Exactly 52 unique lots found.")
    else:
        print(f"[WARNING] Found {unique_lots} unique lots, expected 52.")
        
    if unique_business_types == 13:
        print("[OK] Exactly 13 unique business types found.")
    else:
        print(f"[WARNING] Found {unique_business_types} unique business types, expected 13.")

    null_probs = df_clean['final_probability'].isnull().sum()
    if null_probs == 0:
        print("[OK] No nulls found in final_probability.")
    else:
        print(f"[WARNING] Found {null_probs} nulls in final_probability.")

    print("---------------------\n")

    df_clean = df_clean.sort_values('final_probability', ascending=False)

    print(f"Saving clean CSV to {csv_file}...")
    df_clean.to_csv(csv_file, index=False)

    print(f"Saving clean JSON to {json_file}...")
    records = df_clean.to_dict(orient='records')
    with open(json_file, 'w') as f:
        json.dump(records, f, indent=2)

    print("\nTOP 10 OPPORTUNITIES:")
    print(f"{'ID':>15} | {'BUSINESS TYPE':>20} | {'PROB':>5} | REASON")
    print("-" * 80)
    
    top_10 = df_clean.head(10)
    for _, row in top_10.iterrows():
        print(f"{row['id']:>15} | {row['business_type']:>20} | {row['final_probability']:5.1f} | {row.get('reason', '')}")

if __name__ == '__main__':
    main()
