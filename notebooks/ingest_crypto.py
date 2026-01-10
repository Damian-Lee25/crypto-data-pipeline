import requests
import pandas as pd
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from datetime import datetime, timezone
import io
import os
import sys

# 1. Configuration
COINGECKO_KEY = os.getenv("COINGECKO_DEMO_KEY")
S3_BUCKET = "crypto-pipeline-838693051523"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def validate_env_vars():
    """Check that all required environment variables are set."""
    missing = []
    if not COINGECKO_KEY: missing.append("COINGECKO_DEMO_KEY")
    if not AWS_ACCESS_KEY: missing.append("AWS_ACCESS_KEY_ID")
    if not AWS_SECRET_KEY: missing.append("AWS_SECRET_ACCESS_KEY")
    
    if missing:
        print(f"❌ Critical Error: Missing environment variables: {', '.join(missing)}")
        sys.exit(1)

def fetch_and_upload():
    validate_env_vars()
    now_utc = datetime.now(timezone.utc)
    
    # --- STAGE 1: API Extraction ---
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        headers = {"x-cg-demo-api-key": COINGECKO_KEY}
        params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 50}
        
        print(f"[{now_utc}] Fetching data from CoinGecko...")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        # specific check for rate limits (common with CoinGecko free tier)
        if response.status_code == 429:
            print("❌ Rate limit exceeded (HTTP 429). Pipeline should retry later.")
            sys.exit(1)
            
        response.raise_for_status()
        data = response.json()
        
        if not data or not isinstance(data, list):
            raise ValueError("API returned empty or malformed data")

    except requests.exceptions.HTTPError as errh:
        print(f"❌ HTTP Error: {errh}")
        sys.exit(1)
    except requests.exceptions.ConnectionError as errc:
        print(f"❌ Connection Error: {errc}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected API Extraction Error: {e}")
        sys.exit(1)

    # --- STAGE 2: Transformation ---
    try:
        df = pd.DataFrame(data)
        df['ingested_at'] = now_utc
        
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)
    except Exception as e:
        print(f"❌ Data Transformation Error: {e}")
        sys.exit(1)

    # --- STAGE 3: S3 Loading ---
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name='us-east-1'
        )
        
        partition_date = now_utc.strftime('%Y-%m-%d')
        file_timestamp = now_utc.strftime('%Y%m%d_%H%M%S')
        file_name = f"raw/partition_date={partition_date}/crypto_{file_timestamp}.parquet"
        
        print(f"Uploading to s3://{S3_BUCKET}/{file_name}...")
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=file_name,
            Body=parquet_buffer.getvalue()
        )
        print(f"✅ Successfully uploaded to S3. Records: {len(df)}")

    except NoCredentialsError:
        print("❌ AWS Credentials not found.")
        sys.exit(1)
    except ClientError as e:
        print(f"❌ AWS S3 Client Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected Upload Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_and_upload()