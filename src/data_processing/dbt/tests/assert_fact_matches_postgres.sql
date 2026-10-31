with fact as (
    select
        count(*) as item_count,
        sum(quantity) as unit_count,
        sum(total_price) as revenue
    from {{ ref('fact_orders_items') }}
    where event_date between date '2026-10-11' and date '2026-10-15'
),
reference as (
    select
        count(*) as item_count,
        sum(quantity) as unit_count,
        sum(total_price) as revenue
    from postgres.analytics.order_items
    where cast(event_timestamp as date) between date '2026-10-11' and date '2026-10-15'
)
select
    fact.item_count,
    reference.item_count as reference_item_count,
    fact.unit_count,
    reference.unit_count as reference_unit_count,
    fact.revenue,
    reference.revenue as reference_revenue
from fact
cross join reference
where fact.item_count <> reference.item_count
   or fact.unit_count <> reference.unit_count
   or fact.revenue <> reference.revenue
   or fact.item_count <> 67013
