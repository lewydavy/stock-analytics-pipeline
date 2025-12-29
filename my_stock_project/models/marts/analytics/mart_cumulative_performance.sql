{{ config(materialized='table') }}

with daily_alpha as (
    select 
        trading_date,
        symbol,
        excess_return_vs_sector
    from {{ ref('mart_stock_vs_sector') }}
)

select
    trading_date,
    symbol,
    -- calculates the running total of Alpha over time
    SUM(excess_return_vs_sector) OVER (
        PARTITION BY symbol 
        ORDER BY trading_date
    ) as cumulative_alpha
from daily_alpha