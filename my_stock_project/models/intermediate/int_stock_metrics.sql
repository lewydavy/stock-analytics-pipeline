{{ config(materialized='view') }}

with base_prices as (
    select * from {{ ref('stg_stock_prices') }}
),

calc_window_metrics as (
    select 
        symbol,
        trading_date,
        close_price,
        
        -- 1. Get Previous Day's Close
        LAG(close_price) over (partition by symbol order by trading_date) as previous_close,
        
        -- 2. Moving Averages (50-day and 200-day)
        AVG(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 49 preceding and current row
        ) as ma_50_day,
        
        AVG(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 199 preceding and current row
        ) as ma_200_day,

        -- 3. Volatility
        STDDEV(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 29 preceding and current row
        ) as volatility_30_day

    from base_prices
)

select 
    *,
    -- Calculate Daily Return
    case 
        when previous_close is null or previous_close = 0 then null
        else (close_price - previous_close) / previous_close 
    end as daily_return
from calc_window_metrics