{{ config(materialized='view') }}

-- 1. Create a temporary secret for this session
-- This is the modern, secure way to handle S3 in DuckDB
{% set init_s3 %}
    CREATE SECRET IF NOT EXISTS (
        TYPE S3,
        KEY_ID '{{ env_var("AWS_ACCESS_KEY_ID") }}',
        SECRET '{{ env_var("AWS_SECRET_ACCESS_KEY") }}',
        REGION 'us-east-1'
    );
{% endset %}

-- Execute the secret creation (only works in DuckDB/MotherDuck)
{% do run_query(init_s3) %}

with source_data as (
    select *
    from read_parquet('s3://crypto-pipeline-838693051523/data/crypto_data.parquet')
)

select *
from source_data