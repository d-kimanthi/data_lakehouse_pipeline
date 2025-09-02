{{
    config(
        materialized='view'
    )
}}

with source as (
    select distinct
        product_id,
        product_name,
        category as product_category
    from {{ source('retail', 'orders') }}
),

final as (
    select
        product_id,
        product_name,
        product_category,
        -- Create a category code for easier analysis
        dense_rank() over (order by product_category) as category_id
    from source
)

select * from final
