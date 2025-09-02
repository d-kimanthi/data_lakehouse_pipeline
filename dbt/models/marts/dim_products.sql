{{
    config(
        materialized='table'
    )
}}

with stg_products as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
        product_id,
        product_name,
        product_category,
        category_id,
        current_timestamp() as dbt_loaded_at
    from stg_products
)

select * from final
