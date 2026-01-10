---
name: Configure MotherDuck and S3 Staging Model
overview: Configure dbt-duckdb to connect to MotherDuck using the MOTHERDUCK_TOKEN environment variable, and create a staging model that reads parquet files from S3.
todos:
  - id: update_profiles
    content: Update profiles.yml to connect to MotherDuck using MOTHERDUCK_TOKEN environment variable
    status: completed
  - id: create_staging_dir
    content: Create models/staging/ directory structure
    status: completed
  - id: create_staging_model
    content: Create stg_crypto_data.sql staging model with S3 parquet read functionality
    status: completed
  - id: create_staging_schema
    content: Create _staging.yml schema documentation file
    status: completed
  - id: update_dbt_project
    content: Add staging model configuration to dbt_project.yml
    status: completed
---

# Configure

MotherDuck Connection and Create S3 Staging Model

## Overview

This plan will:

1. Update `profiles.yml` to connect to MotherDuck using the `MOTHERDUCK_TOKEN` environment variable
2. Create a staging model directory structure
3. Create a staging model SQL file that reads parquet data from S3

## Implementation Details

### 1. Update profiles.yml

Update `C:\Users\dam91\.dbt\profiles.yml` to:

- Change the connection type from local DuckDB to MotherDuck
- Use the `MOTHERDUCK_TOKEN` environment variable for authentication
- Keep the existing profile name `my_crypto_dbt` to match the project configuration

The MotherDuck connection string format is: `md:?motherduck_token={{ env_var('MOTHERDUCK_TOKEN') }}`

### 2. Create Staging Model Structure

- Create `my_crypto_dbt/models/staging/` directory
- Create `my_crypto_dbt/models/staging/_staging.yml` for documentation (optional but recommended)
- Create `my_crypto_dbt/models/staging/stg_crypto_data.sql` as the staging model

### 3. Staging Model Implementation

The staging model will:

- Use DuckDB's `read_parquet()` function to read from S3
- Include S3 authentication configuration (placeholder values for AWS credentials)
- Be materialized as a view (standard for staging models)
- Include a config block for S3 access