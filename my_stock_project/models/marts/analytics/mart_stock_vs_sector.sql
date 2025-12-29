{{ config(materialized='table') }}

with daily_data as (
    select * from {{ ref('fct_stock_prices') }}
),

-- 1. Calculate the daily average return for the entire sector
sector_daily_performance as (
    select
        trading_date,
        sector,
        AVG(daily_return) as sector_avg_return
    from daily_data
    group by trading_date, sector
)

-- 2. Compare each stock against that sector average
select
    d.trading_date,
    d.symbol,
    d.company_name,
    d.sector,
    d.industry,
    
    d.daily_return as stock_return,
    s.sector_avg_return,
    
    -- The Alpha: Positive = Outperformed sector, Negative = Underperformed
    (d.daily_return - s.sector_avg_return) as excess_return_vs_sector

from daily_data d
join sector_daily_performance s 
    on d.trading_date = s.trading_date 
    and d.sector = s.sector