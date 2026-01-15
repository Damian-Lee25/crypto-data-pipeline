# 🚀 Crypto Intelligence Terminal
**A Modern Data Stack (MDS) pipeline for real-time market analysis, automated technical indicators, and price forecasting.**

Click Link to View Dashboard [![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://crypto-data-pipeline-cszkrffulmsesvbrvumw4u.streamlit.app/)

[![dbt-core](https://img.shields.io/badge/dbt-v1.11-orange.svg)](https://www.getdbt.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-App-FF4B4B.svg)](https://streamlit.io/)
[![MotherDuck](https://img.shields.io/badge/MotherDuck-DuckDB-blue.svg)](https://motherduck.com/)

---

## 📊 Overview
This project is a production-ready data pipeline that transforms raw cryptocurrency market data into actionable trading intelligence. It automates the extraction of data for the top 50 cryptocurrencies, applies advanced analytics via dbt, and serves a high-performance interactive dashboard.

---

## 👤 Intended Users
* **Momentum Traders:** Users seeking 24h RSI and crossover signals to identify entry/exit points.
* **Market Analysts:** Users exploring volatility regimes across the top 50 assets.
* **Analytics Managers:** Professionals evaluating the implementation of a Modern Data Stack (S3 + DuckDB + dbt).

---

## 🛠️ Tech Stack
* **Ingestion:** Python + GitHub Actions (6-hour intervals)
* **Data Lake:** AWS S3 (Parquet storage)
* **Warehouse:** MotherDuck (Cloud DuckDB)
* **Transformation:** dbt (Data Build Tool)
* **Machine Learning:** Scikit-Learn (Linear Regression)
* **Visualization:** Streamlit

---

## 🧱 Data Modeling Approach
The analytics layer is built on a **Star-Schema** foundation to ensure high performance and clear metric attribution.

* **Fact Tables:**
    * `fct_crypto_trends`: Grain is **one row per asset per ingestion timestamp**. Contains price delta and moving average crossover signals.
    * `fct_crypto_indicators`: Grain is **one row per asset per timestamp**. Houses technical metrics like **14-period RSI** and price momentum.
    * `fct_crypto_volatility`: Grain is **one row per asset per timestamp**. Specifically handles **Z-Score outlier detection** and rolling standard deviation metrics.
* **Dimensions:**
    *Assets are categorized by unique CoinGecko IDs and symbols to ensure historical continuity even if ticker symbols change.

### 📏 Metric Governance
All core market indicators (RSI, Moving Averages, and Volatility Z-Scores) are calculated within the **dbt layer**. This establishes dbt as the **Single Source of Truth (SSOT)**. 
* **Consistency:** Streamlit and ML models consume these pre-calculated metrics directly, preventing "logic drift" where different tools calculate indicators differently.

---

## ⚡ Key Features

### 1. Automated Analytics Pipeline
* **Trend Analysis:** Uses 7-day Moving Averages to generate **Bullish/Bearish** signals.
* **Technical Indicators:** Custom SQL logic for **14-period RSI** calculations.
* **Volatility Scoring:** Z-Score outlier detection to identify extreme market movements.

### 2. Data Reliability & Observability
* **Source Freshness:** Automated monitoring to ensure S3 data is updated within a 12-hour SLA.
* **Schema Testing:** 13+ automated dbt tests ensuring `not_null` constraints and categorical integrity for all market signals.

### 3. Predictive Insights
* **Price Forecasting:** A lightweight ML model that projects price targets for the next 24 hours based on historical price action.

---

## 🔁 Automation & CI/CD
* **Scheduled Ingestion:** Python scripts trigger every 6 hours via GitHub Actions.
* **Proactive Testing:** Every dbt run is preceded by `dbt test` to ensure invalid market signals (e.g., negative prices or null IDs) never reach the production dashboard.
* **Fail-Fast Architecture:** If the S3 source is older than 12 hours, the `source freshness` check fails the pipeline, preventing stale data visualization.

---

## ⚠️ Operational Realism & Edge Cases
Building in a volatile market requires handling data imperfections:
* **Gap Filling:** Missing price intervals are handled via forward-filling logic to prevent "breaks" in 7-day moving average calculations.
* **API Rate Limits:** Ingestion logic includes exponential backoff to handle CoinGecko's free-tier rate limits without failing the pipeline.
* **Model Constraints:** The Linear Regression forecast is optimized for **short-term (24h) trend continuity** and is explicitly not intended for long-horizon or "black swan" event prediction.

---

## 🧠 Technical Decisions & Trade-offs

### 1. Unified Model Architecture
Instead of a complex multi-layered dbt DAG, I chose to implement **direct-to-mart modeling**. 
* **Why:** This minimizes transformation latency and maintains a clear, traceable path from the CoinGecko API to the Streamlit UI.
* **Trade-off:** While less modular, this approach reduced development overhead and ensured maximum performance for the MotherDuck OLAP engine.

### 2. Linear Regression for Forecasting
I opted for a **Scikit-Learn Linear Regression** model over more complex neural networks (like LSTM).
* **Why:** Linear Regression provides high interpretability and low computational cost, allowing the dashboard to generate projections instantly without dedicated GPU infrastructure.

### 3. "Pull" vs "Push" Data Strategy
The architecture uses **MotherDuck** to "pull" from the S3 data lake.
* **Why:** This decouples storage (S3) from compute (MotherDuck). The data lake remains a cost-effective record of raw history while MotherDuck handles the heavy analytical lifting for the frontend.

---

## 🚀 Local Setup & Usage

### 1. Prerequisites
* **Python 3.9+**
* **MotherDuck Account** (Data Warehouse)
* **AWS Account** (S3 read/write access)
* **CoinGecko API Key** (Free tier)

### 2. Installation
1. **Clone the repository:**
   git clone https://github.com/Damian-Lee25/crypto-intelligence-terminal.git

2. **Navigate into the directory:**
   cd crypto-intelligence-terminal

3. **Install dependencies:**
   pip install -r requirements.txt

### 3. Environment Configuration
Create a file named **.env** in the root directory and add your credentials. This ensures your secrets are never committed to GitHub:

* MOTHERDUCK_TOKEN=your_token_here
* AWS_ACCESS_KEY_ID=your_key_here
* AWS_SECRET_ACCESS_KEY=your_secret_here
* COINGECKO_API_KEY=your_api_key_here

### 4. Running the Pipeline
Execute these commands in order to transform your data and launch the UI:

* **Verify Data Freshness:** `dbt source freshness`
* **Run Transformations:** `dbt run`
* **Run Quality Tests:** `dbt test`
* **Launch Dashboard:** `streamlit run dashboard.py`