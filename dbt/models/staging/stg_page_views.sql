-- models/staging/stg_page_views.sql
-- Clean and standardize page view events from bronze layer

with source as (
    select * from {{ ref('bronze_page_views') }}
),

cleaned as (
    select
        event_id,
        user_id,
        session_id,
        page_url,
        page_title,
        referrer_url,
        user_agent,
        ip_address,
        timestamp as event_timestamp,
        -- Standardize timestamp format
        date(timestamp) as event_date,
        hour(timestamp) as event_hour,
        -- Extract URL components
        regexp_extract(page_url, '^https?://[^/]+/([^?#]*)', 1) as page_path,
        regexp_extract(page_url, '\\?(.*)#?', 1) as query_params,
        -- Clean user agent
        case 
            when user_agent like '%Mobile%' then 'Mobile'
            when user_agent like '%Tablet%' then 'Tablet'
            else 'Desktop'
        end as device_type,
        -- Data quality flags
        case when user_id is null then 1 else 0 end as is_anonymous_user,
        case when session_id is null then 1 else 0 end as missing_session
    from source
    where 
        -- Basic data quality filters
        timestamp is not null
        and page_url is not null
        and page_url != ''
)

select * from cleaned