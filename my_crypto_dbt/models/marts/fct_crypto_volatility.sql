{{ config(materialized='table') }}

with staging as (
    select * from {{ ref('stg_crypto_data') }}
),

market_stats_per_snapshot as (
    -- We now calculate market stats for EVERY individual sync
    select
        ingested_at,
        avg(price_change_percentage_24h) as avg_market_change,
        stddev(price_change_percentage_24h) as stddev_market_change
    from staging
    group by 1
)

select
    s.symbol,
    s.name,
    s.current_price,
    s.price_change_percentage_24h,
    s.snapshot_date,
    s.ingested_at,
    
    -- Z-Score relative to that specific snapshot's market condition
    (s.price_change_percentage_24h - m.avg_market_change) / nullif(m.stddev_market_change, 0) as price_z_score,
    
    case 
        when abs((s.price_change_percentage_24h - m.avg_market_change) / nullif(m.stddev_market_change, 0)) > 2 then 'Extreme Outlier'
        when abs((s.price_change_percentage_24h - m.avg_market_change) / nullif(m.stddev_market_change, 0)) > 1 then 'High Volatility'
        else 'Stable'
    end as volatility_rank
from staging s
join market_stats_per_snapshot m 
    on s.ingested_at = m.ingested_at