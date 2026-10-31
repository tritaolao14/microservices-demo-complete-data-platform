select
    order_id,
    product_id,
    count(*) as row_count
from {{ ref('fact_orders_items') }}
group by 1, 2
having count(*) > 1
