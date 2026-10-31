{{ config(materialized='table') }}

select
    order_id,
    product_id,
    quantity,
    currency_code,
    cast(unit_price as decimal(12, 2)) as unit_price,
    cast(quantity * unit_price as decimal(12, 2)) as total_price,
    event_timestamp,
    event_date,
    status
from {{ ref('stg_orders_items') }}
