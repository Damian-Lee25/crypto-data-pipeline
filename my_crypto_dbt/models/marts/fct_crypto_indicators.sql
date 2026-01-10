{{ config(materialized='table') }}

WITH price_changes AS (
    SELECT 
        symbol,
        ingested_at,
        current_price,
        current_price - LAG(current_price) OVER (PARTITION BY symbol ORDER BY ingested_at) AS price_diff
    FROM {{ ref('stg_crypto_data') }}
),

gains_losses AS (
    SELECT 
        *,
        CASE WHEN price_diff > 0 THEN price_diff ELSE 0 END AS gain,
        CASE WHEN price_diff < 0 THEN ABS(price_diff) ELSE 0 END AS loss
    FROM price_changes
),

avg_gains_losses AS (
    SELECT 
        *,
        AVG(gain) OVER (PARTITION BY symbol ORDER BY ingested_at ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY ingested_at ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM gains_losses
),

rsi_calc AS (
    SELECT 
        *,
        CASE 
            WHEN avg_loss = 0 THEN 100
            ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
        END AS rsi
    FROM avg_gains_losses
)

SELECT 
    symbol,
    ingested_at,
    current_price,
    rsi,
    CASE 
        WHEN rsi >= 70 THEN 'Overbought'
        WHEN rsi <= 30 THEN 'Oversold'
        ELSE 'Neutral'
    END AS rsi_status
FROM rsi_calc