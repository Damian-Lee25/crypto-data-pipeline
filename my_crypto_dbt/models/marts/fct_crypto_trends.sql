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
        -- Get the price from exactly 1 day ago (4 snapshots back if running every 6 hours)
        lag(current_price, 4) over (partition by id order by ingested_at) as price_24h_ago,
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
    (current_price - price_24h_ago) as absolute_change_24h,
    ((current_price - price_24h_ago) / price_24h_ago) * 100 as percent_change_24h,
    case 
        when current_price > moving_avg_7d then 'Bullish'
        when current_price < moving_avg_7d then 'Bearish'
        else 'Neutral'
    end as trend_signal
from historical_trends
where price_24h_ago is not null -- Only show rows where we have history