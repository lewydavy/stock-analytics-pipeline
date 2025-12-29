{{ config(materialized='table') }}

with daily_data as (
    select * from {{ ref('fct_stock_prices') }}
)

select
    -- Group by Month
    DATE_TRUNC('month', trading_date) as month_start,
    symbol,
    sector,
    industry,
    
    -- Price Metrics
    AVG(close_price) as avg_close_price,
    MAX(close_price) as max_close_price,
    MIN(close_price) as min_close_price,
    
    -- Return Metrics
    SUM(daily_return) as total_monthly_return,
    
    -- Risk Metric (Standard deviation of daily returns within the month)
    STDDEV(daily_return) as monthly_volatility_stddev

from daily_data
group by 1, 2, 3, 4