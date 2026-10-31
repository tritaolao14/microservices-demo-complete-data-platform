{{ config(materialized='table') }}

with source as (
    select * from {{ source('bronze', 'orders') }}
),
exploded as (
    select
        source.order_id,
        json_extract_scalar(item, '$.item.product_id') as product_id,
        cast(json_extract_scalar(item, '$.item.quantity') as integer) as quantity,
        json_extract_scalar(item, '$.cost.currency_code') as currency_code,
        cast(json_extract_scalar(item, '$.cost.units') as decimal(18, 0))
            + cast(json_extract_scalar(item, '$.cost.nanos') as decimal(18, 9)) / 1000000000 as unit_price,
        cast(from_iso8601_timestamp(json_extract_scalar(source.raw, '$.timestamp')) as timestamp(6) with time zone) as event_timestamp,
        source._event_date as event_date,
        'SUCCEED' as status,
        source._ingested_at,
        source._kafka_partition,
        source._kafka_offset
    from source
    cross join unnest(cast(json_extract(source.raw, '$.items') as array(json))) as t(item)
),
deduplicated as (
    select
        *,
        row_number() over (
            partition by order_id, product_id
            order by _ingested_at, _kafka_partition, _kafka_offset
        ) as _row_number
    from exploded
)
select
    order_id,
    product_id,
    quantity,
    currency_code,
    unit_price,
    event_timestamp,
    event_date,
    status
from deduplicated
where _row_number = 1
