{{
    config(
        materialized='table'
    )
}}

with stg_stores as (
    select * from {{ ref('stg_stores') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_key,
        store_id,
        store_state,
        store_city,
        total_customers,
        store_city_rank,
        current_timestamp() as dbt_loaded_at
    from stg_stores
)

select * from final
