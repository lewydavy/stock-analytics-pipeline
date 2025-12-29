{{ config(materialized='table') }}

with metrics as (
    select * from {{ ref('int_stock_metrics') }}
),

metadata as (
    select * from {{ ref('dim_stock') }}
)

select
    -- Primary Keys
    m.trading_date,
    m.symbol,
    
    -- Context (From Seed)
    s.company_name,
    s.sector,
    s.industry,
    
    -- Metrics
    m.close_price,
    m.daily_return,
    m.ma_50_day,
    m.ma_200_day,
    m.volatility_30_day,
    
    -- Analysis Flags 
    case 
        when m.close_price > m.ma_50_day then true 
        else false 
    end as is_trading_above_ma50,

    case
        when m.daily_return > 0 then 'Up'
        when m.daily_return < 0 then 'Down'
        else 'Flat'
    end as price_movement_direction

from metrics m
left join metadata s 
    on m.symbol = s.symbol