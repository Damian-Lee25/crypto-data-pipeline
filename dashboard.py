import streamlit as st
import duckdb
import plotly.express as px
import os
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

# 1. Page Configuration
st.set_page_config(page_title="Crypto Analytics Dashboard", layout="wide", page_icon="🪙")

# Sidebar for controls
st.sidebar.title("Settings")
if st.sidebar.button('🔄 Refresh Data'):
    st.cache_data.clear()
    st.rerun()

st.title("📈 Crypto Trend & Volatility Tracker")
st.markdown(f"**Status:** Connected to MotherDuck | **Updates:** Every 6 Hours")

# 2. Connection Logic
token = st.secrets.get("MOTHERDUCK_TOKEN") or os.getenv("MOTHERDUCK_TOKEN")

if not token:
    st.error("MotherDuck Token not found! Please set it in secrets or environment.")
    st.stop()

@st.cache_data(ttl=600)
def load_data():
    con = duckdb.connect(f"md:my_db?motherduck_token={token}")
    # Load Trends, Volatility, and RSI Indicators
    trends = con.execute("SELECT * FROM main.fct_crypto_trends ORDER BY ingested_at DESC").df()
    vol = con.execute("SELECT * FROM main.fct_crypto_volatility ORDER BY ingested_at DESC").df()
    indicators = con.execute("SELECT * FROM main.fct_crypto_indicators ORDER BY ingested_at DESC").df()
    con.close()
    return trends, vol, indicators

# 3. Execution Logic
try:
    df_trends, df_vol, df_indicators = load_data()
    
    # Ensure ingested_at is datetime
    df_trends['ingested_at'] = pd.to_datetime(df_trends['ingested_at'])
    
    # --- Data Filtering ---
    latest_ts = df_trends['ingested_at'].max()
    df_latest_trends = df_trends[df_trends['ingested_at'] == latest_ts]
    df_latest_vol = df_vol[df_vol['ingested_at'] == latest_ts]

    # --- UI Layout: Top Metrics ---
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

    # --- TABS: Market Overview, Technicals, and Forecast ---
    tab_price, tab_rsi, tab_pred = st.tabs(["📊 Price Movements", "🧪 Technical Indicators (RSI)", "🔮 Price Forecast"])

    all_coins = sorted(df_trends['name'].unique())
    selected_coins = st.sidebar.multiselect("Select Coins to View", all_coins, default=all_coins[:4])

    with tab_price:
        df_plot = df_trends[df_trends['name'].isin(selected_coins)]
        if not df_plot.empty:
            fig_price = px.line(
                df_plot, x='ingested_at', y='current_price', 
                facet_col='name', facet_col_wrap=2, color='name',
                labels={'ingested_at': 'Time', 'current_price': 'Price (USD)'},
                template="plotly_dark", height=500
            )
            fig_price.update_yaxes(matches=None, showticklabels=True)
            fig_price.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
            st.plotly_chart(fig_price, use_container_width=True)

    with tab_rsi:
        st.subheader("Relative Strength Index (14-Period)")
        df_rsi_plot = df_indicators.merge(df_trends[['symbol', 'name']].drop_duplicates(), on='symbol')
        df_rsi_plot = df_rsi_plot[df_rsi_plot['name'].isin(selected_coins)]

        if not df_rsi_plot.empty:
            fig_rsi = px.line(
                df_rsi_plot, x='ingested_at', y='rsi', 
                facet_col='name', facet_col_wrap=2, color='name',
                labels={'rsi': 'RSI Value', 'ingested_at': 'Time'},
                template="plotly_dark", height=500
            )
            fig_rsi.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Overbought")
            fig_rsi.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Oversold")
            fig_rsi.update_yaxes(range=[0, 100])
            fig_rsi.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
            st.plotly_chart(fig_rsi, use_container_width=True)

    with tab_pred:
        st.subheader("Linear Regression Price Projection (Next 24 Hours)")
        st.info("⚠️ This is a mathematical trend projection based on your historical data, not financial advice.")
        
        # Select one coin for prediction to keep the chart readable
        pred_coin = st.selectbox("Select Coin for Prediction", selected_coins if selected_coins else all_coins[:1])
        
        df_pred = df_trends[df_trends['name'] == pred_coin].sort_values('ingested_at')
        
        # We need at least 5 points to make a somewhat sane line
        if len(df_pred) > 5:
            # Prepare data
            df_pred['ts_numeric'] = df_pred['ingested_at'].map(pd.Timestamp.timestamp)
            X = df_pred[['ts_numeric']].values
            y = df_pred['current_price'].values
            
            # Train model
            model = LinearRegression()
            model.fit(X, y)
            
            # Predict next 24 hours (4 steps of 6 hours)
            last_ts = df_pred['ts_numeric'].max()
            future_ts = np.array([last_ts + (i * 6 * 3600) for i in range(1, 6)]).reshape(-1, 1)
            future_preds = model.predict(future_ts)
            
            # Build Forecast DataFrame
            df_forecast = pd.DataFrame({
                'ingested_at': pd.to_datetime(future_ts.flatten(), unit='s'),
                'current_price': future_preds,
                'name': f"{pred_coin} (Forecast)"
            })
            
            # Combine for plotting
            df_full_pred = pd.concat([df_pred[['ingested_at', 'current_price', 'name']], df_forecast])
            
            fig_pred = px.line(
                df_full_pred, x='ingested_at', y='current_price', color='name',
                labels={'current_price': 'Price (USD)', 'ingested_at': 'Time'},
                template="plotly_dark",
                title=f"Trend Extension for {pred_coin}"
            )
            
            # Make forecast line dashed
            fig_pred.update_traces(patch={"line": {"dash": "dash"}}, selector={"name": f"{pred_coin} (Forecast)"})
            st.plotly_chart(fig_pred, use_container_width=True)
            
            # Forecast Metric
            current_p = y[-1]
            future_p = future_preds[-1]
            diff = ((future_p - current_p) / current_p) * 100
            st.metric(f"Projected Price (24h)", f"${future_p:,.2f}", delta=f"{diff:.2f}%")
        else:
            st.warning("Collecting more data points... Need at least 6 historical records to generate a forecast.")

    # --- UI Layout: Analysis Table ---
    st.subheader("🔍 Detailed Market Analysis (Latest)")
    df_latest_rsi = df_indicators[pd.to_datetime(df_indicators['ingested_at']) == latest_ts]
    
    # Merge all stats
    df_master = df_latest_trends.merge(
        df_latest_vol[['symbol', 'price_z_score', 'volatility_rank']], on='symbol'
    ).merge(
        df_latest_rsi[['symbol', 'rsi', 'rsi_status']], on='symbol'
    )
    
    st.dataframe(
        df_master[['name', 'symbol', 'current_price', 'trend_signal', 'rsi', 'rsi_status', 'volatility_rank', 'price_z_score']],
        use_container_width=True,
        hide_index=True
    )

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Check if all dbt models (trends, volatility, indicators) have run successfully.")