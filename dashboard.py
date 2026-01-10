import streamlit as st
import duckdb
import plotly.express as px
import os

# 1. Page Configuration
st.set_page_config(page_title="Crypto Analytics Dashboard", layout="wide", page_icon="🪙")

# Sidebar for controls
st.sidebar.title("Settings")
if st.sidebar.button('🔄 Refresh Data'):
    st.cache_data.clear()
    st.rerun()

st.title("📈 Crypto Trend & Volatility Tracker")
st.markdown(f"**Status:** Connected to MotherDuck | **Updates:** Every 6 Hours")

# 2. Connection Logic (using st.secrets for cloud-readiness)
# This will check for secrets first, then fallback to environment variables
token = st.secrets.get("MOTHERDUCK_TOKEN") or os.getenv("MOTHERDUCK_TOKEN")

if not token:
    st.error("MotherDuck Token not found! Please set it in secrets or environment.")
    st.stop()

@st.cache_data(ttl=600)
def load_data():
    con = duckdb.connect(f"md:my_db?motherduck_token={token}")
    # Load Trends and Volatility data
    trends = con.execute("SELECT * FROM main.fct_crypto_trends ORDER BY ingested_at DESC").df()
    vol = con.execute("SELECT * FROM main.fct_crypto_volatility ORDER BY ingested_at DESC").df()
    con.close()
    return trends, vol

# 3. Execution Logic
try:
    df_trends, df_vol = load_data()
    
    # --- Data Filtering ---
    # Get the latest timestamp to show current market state in metrics
    latest_ts = df_trends['ingested_at'].max()
    df_latest_trends = df_trends[df_trends['ingested_at'] == latest_ts]
    df_latest_vol = df_vol[df_vol['ingested_at'] == latest_ts]

    # --- UI Layout: Top Metrics (Current Snapshot) ---
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Coins Tracked", len(df_latest_trends))
    with col2:
        bullish = len(df_latest_trends[df_latest_trends['trend_signal'] == 'Bullish'])
        st.metric("Bullish Signals", bullish, delta=f"{bullish} coins")
    with col3:
        bearish = len(df_latest_trends[df_latest_trends['trend_signal'] == 'Bearish'])
        st.metric("Bearish Signals", bearish, delta=f"-{bearish} coins", delta_color="inverse")
    with col4:
        outliers = len(df_latest_vol[df_latest_vol['volatility_rank'].str.contains('Outlier')])
        st.metric("High Volatility", outliers)

    st.divider()

    # --- UI Layout: Price Chart (Historical) ---
    st.subheader("📊 Historical Price Movements")
    
    # Sidebar Filter for Coins
    all_coins = sorted(df_trends['name'].unique())
    selected_coins = st.sidebar.multiselect("Select Coins to View", all_coins, default=all_coins[:5])
    
    df_plot = df_trends[df_trends['name'].isin(selected_coins)]
    
    if not df_plot.empty:
        fig = px.line(
            df_plot, 
            x='ingested_at', 
            y='current_price', 
            color='name',
            labels={'ingested_at': 'Time', 'current_price': 'Price (USD)'},
            template="plotly_dark"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Please select at least one coin to view the trend chart.")

    # --- UI Layout: Analysis Table ---
    st.subheader("🔍 Detailed Market Analysis (Latest)")
    # Merge trends and volatility for a "Master Table" view
    df_master = df_latest_trends.merge(
        df_latest_vol[['symbol', 'price_z_score', 'volatility_rank']], 
        on='symbol'
    )
    
    st.dataframe(
        df_master[['name', 'symbol', 'current_price', 'trend_signal', 'volatility_rank', 'price_z_score']],
        use_container_width=True,
        hide_index=True
    )

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Ensure your dbt models have materialized as tables in MotherDuck.")