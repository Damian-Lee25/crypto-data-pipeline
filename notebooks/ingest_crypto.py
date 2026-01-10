import requests
import pandas as pd
import boto3
from datetime import datetime, timezone
import io
import os
import sys

# 1. Configuration - Use Environment Variables for security!
COINGECKO_KEY = os.getenv("COINGECKO_DEMO_KEY")
S3_BUCKET = "crypto-pipeline-838693051523"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def validate_env_vars():
    """Check that all required environment variables are set."""
    missing = []
    if not COINGECKO_KEY:
        missing.append("COINGECKO_DEMO_KEY")
    if not AWS_ACCESS_KEY:
        missing.append("AWS_ACCESS_KEY_ID")
    if not AWS_SECRET_KEY:
        missing.append("AWS_SECRET_ACCESS_KEY")
    
    if missing:
        print(f"Error: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

def fetch_and_upload():
    # Validate environment variables first
    validate_env_vars()
    
    try:
        # 2. Extract: Call CoinGecko Markets Endpoint
        url = "https://api.coingecko.com/api/v3/coins/markets"
        headers = {"x-cg-demo-api-key": COINGECKO_KEY}
        params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 50}
        
        print("Fetching data from CoinGecko API...")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if not data:
            print("Warning: API returned empty data")
            return
        
        # 3. Transform (Light): Convert to DataFrame & add a timestamp
        df = pd.DataFrame(data)
        # Using a consistent UTC timestamp for the data rows
        now_utc = datetime.now(timezone.utc)
        df['ingested_at'] = now_utc
        
        # 4. Load: Write to S3 as a Parquet file
        print("Converting to Parquet format...")
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name='us-east-1'
        )
        
        # --- NEW HISTORICAL PATH LOGIC ---
        # partition_date creates a folder: raw/partition_date=2024-05-20/
        # file_timestamp creates a unique name: crypto_20240520_120000.parquet
        partition_date = now_utc.strftime('%Y-%m-%d')
        file_timestamp = now_utc.strftime('%Y%m%d_%H%M%S')
        
        file_name = f"raw/partition_date={partition_date}/crypto_{file_timestamp}.parquet"
        # ---------------------------------
        
        print(f"Uploading to s3://{S3_BUCKET}/{file_name}...")
        parquet_buffer.seek(0)
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=file_name,
            Body=parquet_buffer.getvalue()
        )
        print(f"✅ Successfully uploaded {file_name} to {S3_BUCKET}")
        print(f"   Records uploaded: {len(df)}")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data from API: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_and_upload()