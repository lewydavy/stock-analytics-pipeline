import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import os
from dotenv import load_dotenv

load_dotenv()

# --- 1. CONFIG ---
DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

def get_data(query):
    """Helper function to fetch data from Postgres"""
    conn = psycopg2.connect(**DB_PARAMS)
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# --- 2. PAGE LAYOUT ---
st.set_page_config(page_title="Personal Stock Analytics", layout="wide")
st.title("📈 Personal Stock Analytics: The 'King of Tech' Tracker")
st.markdown("Data Pipeline: **Python (Yahoo) → Dagster → dbt → Postgres → Streamlit**")

# --- 3. SECTION: CUMULATIVE ALPHA ---
st.header("1. Who is beating the sector? (Cumulative Alpha)")
st.write("""
This chart shows 'Excess Return' over time. 
A rising line means the stock is consistently outperforming the average of its sector peers.
""")

alpha_query = """
    select trading_date, symbol, cumulative_alpha 
    from personal_data_schema.mart_cumulative_performance
    order by trading_date
"""
df_alpha = get_data(alpha_query)

if not df_alpha.empty:
    fig_alpha = px.line(
        df_alpha, 
        x="trading_date", 
        y="cumulative_alpha", 
        color="symbol", 
        title="Cumulative Alpha (Performance vs Sector Average)",
        labels={"cumulative_alpha": "Cumulative Excess Return"},
        height=500
    )
    st.plotly_chart(fig_alpha, use_container_width=True)
else:
    st.warning("No data found in mart_cumulative_performance. Run your Dagster pipeline!")

# --- 4. SECTION: DEEP DIVE ---
st.divider()
st.header("2. Stock Deep Dive")

col1, col2 = st.columns([1, 3])

with col1:
    # Selector
    stock_list = get_data("select distinct symbol from personal_data_schema.dim_stock order by 1")['symbol'].tolist()
    selected_stock = st.selectbox("Select a Stock to Analyze:", stock_list)

    # Metrics Card
    latest_metrics = get_data(f"""
        select close_price, daily_return, volatility_30_day 
        from personal_data_schema.fct_stock_prices 
        where symbol = '{selected_stock}' 
        order by trading_date desc limit 1
    """)
    
    if not latest_metrics.empty:
        price = latest_metrics.iloc[0]['close_price']
        ret = latest_metrics.iloc[0]['daily_return']
        st.metric("Latest Price", f"${price:,.2f}", f"{ret:.2%}")

with col2:
    # Price vs MA Chart
    ma_query = f"""
        select trading_date, close_price, ma_50_day, ma_200_day
        from personal_data_schema.fct_stock_prices
        where symbol = '{selected_stock}'
        order by trading_date
    """
    df_ma = get_data(ma_query)
    
    # Melt for easier plotting with Plotly
    df_melted = df_ma.melt(id_vars=['trading_date'], value_vars=['close_price', 'ma_50_day', 'ma_200_day'], 
                           var_name='Metric', value_name='Price')
    
    fig_ma = px.line(
        df_melted, 
        x="trading_date", 
        y="Price", 
        color="Metric", 
        title=f"{selected_stock}: Price vs Moving Averages",
        color_discrete_map={"close_price": "blue", "ma_50_day": "orange", "ma_200_day": "green"}
    )
    st.plotly_chart(fig_ma, use_container_width=True)

# --- 5. SECTION: MONTHLY VOLATILITY ---
st.divider()
st.header("3. Risk Analysis")
st.write("Which stocks are the most volatile (risky) each month?")

vol_query = """
    select month_start, symbol, monthly_volatility_stddev
    from personal_data_schema.mart_monthly_summary
    order by month_start
"""
df_vol = get_data(vol_query)

if not df_vol.empty:
    fig_vol = px.bar(
        df_vol, 
        x="month_start", 
        y="monthly_volatility_stddev", 
        color="symbol", 
        barmode="group",
        title="Monthly Standard Deviation of Returns"
    )
    st.plotly_chart(fig_vol, use_container_width=True)