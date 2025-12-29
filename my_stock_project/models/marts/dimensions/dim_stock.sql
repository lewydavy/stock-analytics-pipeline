{{ config(materialized='table') }}

select
    symbol,
    company_name,
    sector,
    industry
from {{ ref('stock_metadata') }}