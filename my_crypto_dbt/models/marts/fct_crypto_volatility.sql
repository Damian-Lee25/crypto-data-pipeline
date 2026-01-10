{{ config(materialized='table') }}

with staging as (
    -- This pulls from your staging model
    select * from {{ ref('stg_crypto_data') }}
),

market_stats as (
    -- Calculate market-wide averages and standard deviation for the 50 coins
    select
        avg(price_change_percentage_24h) as avg_market_change,
        stddev(price_change_percentage_24h) as stddev_market_change
    from staging
)

select
    s.symbol,
    s.name,
    s.current_price,
    s.price_change_percentage_24h,
    -- The Z-Score Formula: (Value - Mean) / Standard Deviation
    (s.price_change_percentage_24h - m.avg_market_change) / nullif(m.stddev_market_change, 0) as price_z_score,
    
    -- Labeling the movements for easier analysis
    case 
        when abs((s.price_change_percentage_24h - m.avg_market_change) / nullif(m.stddev_market_change, 0)) > 2 then 'Outlier'
        when abs((s.price_change_percentage_24h - m.avg_market_change) / nullif(m.stddev_market_change, 0)) > 1 then 'Significant'
        else 'Normal'
    end as volatility_rank,
    
    s.ingested_at
from staging s, market_stats m