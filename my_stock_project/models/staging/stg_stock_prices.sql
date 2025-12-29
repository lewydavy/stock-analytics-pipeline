{{ config(materialized='view') }}

{% set tickers = ['AMZN', 'ASML', 'AVGO', 'GOOGL', 'META', 'MSFT', 'NBIS', 'NVDA', 'PLTR', 'TSLA'] %}

with source_data as (
    {% for ticker in tickers %}
    select 
        '{{ ticker }}' as symbol,
        date as trading_date,
        close as close_price,
        volume as volume,
        open as open,
        high as high,
        low as low
    from {{ source('raw_data', ticker|lower ~ '_stock_prices') }}
    
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select * from source_data