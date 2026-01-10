{{ config(materialized='table') }}

with base_data as (
    select * from {{ ref('stg_crypto_data') }}
),

historical_trends as (
    select
        id,
        name,
        symbol,
        current_price,
        snapshot_date,
        ingested_at,
        -- FALLBACK LOGIC: Try 4 rows back (24h). If NULL, try 1 row back.
        coalesce(
            lag(current_price, 4) over (partition by id order by ingested_at),
            lag(current_price, 1) over (partition by id order by ingested_at),
            current_price
        ) as comparison_price,
        
        -- Calculate a 7-day moving average (last 28 snapshots)
        avg(current_price) over (
            partition by id 
            order by ingested_at 
            rows between 27 preceding and current row
        ) as moving_avg_7d
    from base_data
)

select
    *,
    (current_price - comparison_price) as absolute_change,
    ((current_price - comparison_price) / nullif(comparison_price, 0)) * 100 as percent_change,
    case 
        when current_price > moving_avg_7d then 'Bullish'
        when current_price < moving_avg_7d then 'Bearish'
        else 'Neutral'
    end as trend_signal
from historical_trends
-- We removed the WHERE clause so the table is never empty!