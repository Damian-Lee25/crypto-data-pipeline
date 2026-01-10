{{ config(materialized='view') }}

with source_data as (
    select 
        *,
        -- This extracts the date from the folder name (partition_date=YYYY-MM-DD)
        partition_date::DATE as snapshot_date 
    from read_parquet(
        's3://crypto-pipeline-838693051523/raw/*/*.parquet',
        hive_partitioning = true
    )
)

select
    id,
    symbol,
    name,
    current_price,
    market_cap,
    market_cap_rank,
    total_volume,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_percentage_24h,
    circulating_supply,
    total_supply,
    ath,
    atl,
    ingested_at,
    snapshot_date
from source_data