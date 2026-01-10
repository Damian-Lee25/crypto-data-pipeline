import streamlit as st
import duckdb
import plotly.express as px
import os
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

# 1. Page Configuration
st.set_page_config(page_title="Crypto Analytics Dashboard", layout="wide", page_icon="🪙")

# --- UI/UX: Custom CSS Injection ---
st.markdown("""
    <style>
    /* Custom Card Styling for Metrics */
    div[data-testid="stMetric"] {
        background-color: #1f2630;
        border: 1px solid #313d4f;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
    }
    /* Metric Label styling */
    div[data-testid="stMetricLabel"] {
        font-size: 16px;
        font-weight: bold;
        color: #fafafa;
    }
    /* Clean up the header and footer */
    header {visibility: hidden;}
    footer {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

# Sidebar for controls
st.sidebar.title("🛠️ Control Panel")
if st.sidebar.button('🔄 Refresh Data Pipeline'):
    st.cache_data.clear()
    st.rerun()

st.title("📈 Crypto Intelligence Terminal")
st.markdown(f"**Network:** MotherDuck Cloud | **Pipeline Frequency:** 6-Hour Batch")

# 2. Connection Logic
token = st.secrets.get("MOTHERDUCK_TOKEN") or os.getenv("MOTHERDUCK_TOKEN")

if not token:
    st.error("MotherDuck Token not found! Please set it in secrets or environment.")
    st.stop()

@st.cache_data(ttl=600)
def load_data():
    con = duckdb.connect(f"md:my_db?motherduck_token={token}")
    trends = con.execute("SELECT * FROM main.fct_crypto_trends ORDER BY ingested_at DESC").df()
    vol = con.execute("SELECT * FROM main.fct_crypto_volatility ORDER BY ingested_at DESC").df()
    indicators = con.execute("SELECT * FROM main.fct_crypto_indicators ORDER BY ingested_at DESC").df()
    con.close()
    return trends, vol, indicators

# 3. Execution Logic
try:
    df_trends, df_vol, df_indicators = load_data()
    df_trends['ingested_at'] = pd.to_datetime(df_trends['ingested_at'])
    
    # --- Data Filtering ---
    latest_ts = df_trends['ingested_at'].max()
    df_latest_trends = df_trends[df_trends['ingested_at'] == latest_ts]
    df_latest_vol = df_vol[df_vol['ingested_at'] == latest_ts]

    # --- UI Layout: Top Metrics ---
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Assets Tracked", len(df_latest_trends), help="Total number of unique coins in the data warehouse")
    with col2:
        bullish = len(df_latest_trends[df_latest_trends['trend_signal'] == 'Bullish'])
        st.metric("Bullish Sentiment", bullish, delta=f"{bullish} Coins", help="Assets trading above their 7-day average")
    with col3:
        bearish = len(df_latest_trends[df_latest_trends['trend_signal'] == 'Bearish'])
        st.metric("Bearish Sentiment", bearish, delta=f"-{bearish} Coins", delta_color="inverse")
    with col4:
        outliers = len(df_latest_vol[df_latest_vol['volatility_rank'].str.contains('Outlier')])
        mood = "🔥 High Vol" if outliers > 3 else "💎 Stable"
        st.metric("Market Mood", mood, delta=f"{outliers} Outliers")

    st.divider()

    # --- TABS: Market Overview, Technicals, and Forecast ---
    tab_price, tab_rsi, tab_pred = st.tabs(["📊 Price Movements", "🧪 Technical Indicators (RSI)", "🔮 Predictive Forecast"])

    all_coins = sorted(df_trends['name'].unique())
    selected_coins = st.sidebar.multiselect("Select Assets for Analysis", all_coins, default=all_coins[:4])

    if not selected_coins:
        st.warning("Please select at least one asset in the sidebar to view charts.")
    else:
        with tab_price:
            df_plot = df_trends[df_trends['name'].isin(selected_coins)]
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

            fig_rsi = px.line(
                df_rsi_plot, x='ingested_at', y='rsi', 
                facet_col='name', facet_col_wrap=2, color='name',
                labels={'rsi': 'RSI Value', 'ingested_at': 'Time'},
                template="plotly_dark", height=500
            )
            fig_rsi.add_hline(y=70, line_dash="dash", line_color="#ff4b4b", annotation_text="Overbought")
            fig_rsi.add_hline(y=30, line_dash="dash", line_color="#00ffcc", annotation_text="Oversold")
            fig_rsi.update_yaxes(range=[0, 100])
            fig_rsi.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
            st.plotly_chart(fig_rsi, use_container_width=True)

        with tab_pred:
            st.subheader("Trend Projection (Next 24 Hours)")
            pred_coin = st.selectbox("Select Asset for Prediction", selected_coins)
            df_pred = df_trends[df_trends['name'] == pred_coin].sort_values('ingested_at')
            
            if len(df_pred) > 5:
                df_pred['ts_numeric'] = df_pred['ingested_at'].map(pd.Timestamp.timestamp)
                X, y = df_pred[['ts_numeric']].values, df_pred['current_price'].values
                model = LinearRegression().fit(X, y)
                
                last_ts = df_pred['ts_numeric'].max()
                future_ts = np.array([last_ts + (i * 6 * 3600) for i in range(1, 6)]).reshape(-1, 1)
                future_preds = model.predict(future_ts)
                
                df_forecast = pd.DataFrame({
                    'ingested_at': pd.to_datetime(future_ts.flatten(), unit='s'),
                    'current_price': future_preds,
                    'name': f"{pred_coin} (Forecast)"
                })
                
                df_full_pred = pd.concat([df_pred[['ingested_at', 'current_price', 'name']], df_forecast])
                fig_pred = px.line(df_full_pred, x='ingested_at', y='current_price', color='name', template="plotly_dark")
                fig_pred.update_traces(patch={"line": {"dash": "dash"}}, selector={"name": f"{pred_coin} (Forecast)"})
                st.plotly_chart(fig_pred, use_container_width=True)
                
                diff = ((future_preds[-1] - y[-1]) / y[-1]) * 100
                st.metric(f"Projected {pred_coin} Target", f"${future_preds[-1]:,.2f}", delta=f"{diff:.2f}%")
            else:
                st.warning("Additional data cycles required for predictive modeling.")

    # --- UI Layout: Analysis Table with Conditional Styling ---
    st.subheader("🔍 Real-Time Market Intelligence")
    
    df_latest_rsi = df_indicators[pd.to_datetime(df_indicators['ingested_at']) == latest_ts]
    df_master = df_latest_trends.merge(
        df_latest_vol[['symbol', 'price_z_score', 'volatility_rank']], on='symbol'
    ).merge(
        df_latest_rsi[['symbol', 'rsi', 'rsi_status']], on='symbol'
    )
    
    # Conditional Styling Function
    def color_signal(val):
        color = '#00ffcc' if val == 'Bullish' or val == 'Oversold' else '#ff4b4b' if val == 'Bearish' or val == 'Overbought' else '#fafafa'
        return f'color: {color}; font-weight: bold;'

    st.dataframe(
        df_master[['name', 'symbol', 'current_price', 'trend_signal', 'rsi', 'rsi_status', 'volatility_rank']]
        .style.applymap(color_signal, subset=['trend_signal', 'rsi_status']),
        use_container_width=True,
        hide_index=True
    )

except Exception as e:
    st.error(f"System Error: {e}")