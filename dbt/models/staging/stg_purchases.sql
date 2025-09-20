-- models/staging/stg_purchases.sql
-- Clean and standardize purchase events from bronze layer

with source as (
    select * from {{ ref('bronze_purchases') }}
),

cleaned as (
    select
        event_id,
        user_id,
        session_id,
        product_id,
        product_name,
        category,
        price,
        quantity,
        timestamp as event_timestamp,
        date(timestamp) as event_date,
        hour(timestamp) as event_hour,
        -- Calculate derived fields
        price * quantity as total_amount,
        -- Categorize purchase size
        case 
            when price * quantity >= 1000 then 'Large'
            when price * quantity >= 100 then 'Medium'
            else 'Small'
        end as purchase_size,
        -- Data quality flags
        case when price <= 0 then 1 else 0 end as invalid_price,
        case when quantity <= 0 then 1 else 0 end as invalid_quantity
    from source
    where 
        -- Basic data quality filters
        timestamp is not null
        and user_id is not null
        and product_id is not null
        and price > 0
        and quantity > 0
)

select * from cleaned