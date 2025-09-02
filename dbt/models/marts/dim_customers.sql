{{
    config(
        materialized='table'
    )
}}

with stg_customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
        customer_id,
        visited_stores,
        payment_methods_used,
        customer_segment,
        current_timestamp() as dbt_loaded_at
    from stg_customers
)

select * from final
