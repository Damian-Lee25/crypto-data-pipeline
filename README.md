# 🪙 Crypto Intelligence Terminal
**A Professional Data Engineering Pipeline for Real-Time Crypto Analytics & Price Forecasting.**

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://crypto-data-pipeline-cszkrffulmsesvbrvumw4u.streamlit.app/)

## 🎯 Project Overview
This project is a full-stack data engineering pipeline designed to ingest, transform, and visualize cryptocurrency market data. It moves beyond simple price tracking by implementing technical indicators (RSI), volatility analysis, and machine learning-based price projections.

The primary goal was to build a resilient, automated "Modern Data Stack" that handles everything from raw API ingestion to predictive analytics.

---

## 🏗️ Architecture & Data Flow


1.  **Ingestion (Python & S3):** A Python script fetches live market data from the CoinGecko API every 6 hours and stores it as versioned Parquet files in an AWS S3 Data Lake.
2.  **Orchestration (GitHub Actions):** Automates the ingestion and dbt transformation cycles without requiring a dedicated server.
3.  **Data Warehousing (MotherDuck):** Uses MotherDuck (DuckDB in the cloud) for high-performance OLAP processing.
4.  **Transformation (dbt):**
    * **fct_crypto_trends:** Calculates 7-day moving averages and trend signals.
    * **fct_crypto_indicators:** Computes the Relative Strength Index (RSI) to identify overbought/oversold assets.
    * **fct_crypto_volatility:** Identifies high-risk assets using Z-Score analysis.
5.  **Analytics Layer (Streamlit & Scikit-Learn):** A custom-themed dashboard that runs Linear Regression models to project price trends 24 hours into the future.

---

## 🛠️ Tech Stack
* **Language:** Python 3.x
* **Database:** MotherDuck / DuckDB
* **Transformation:** dbt (Data Build Tool)
* **Orchestration:** GitHub Actions
* **Storage:** AWS S3 (Data Lake)
* **Machine Learning:** Scikit-Learn (Linear Regression)
* **Frontend:** Streamlit

---

## 🧪 Key Features
* **Predictive Forecasting:** Extends historical trends into the future using mathematical modeling.
* **Technical Terminal UI:** Custom CSS-injected dashboard designed for high-readability in trading environments.
* **Fault-Tolerant Ingestion:** Implements API rate-limiting handling and automated retries.
* **Automated Pipeline:** Fully hands-off data updates every 6 hours.

---

## 🚀 Getting Started
### Prerequisites
* Python 3.10+
* MotherDuck Account & Token
* AWS S3 Bucket (for data lake)

### Local Setup
1. **Clone the repo:**
   ```bash
   git clone [https://github.com/yourusername/crypto-intelligence-terminal.git](https://github.com/yourusername/crypto-intelligence-terminal.git)
   cd crypto-intelligence-terminal