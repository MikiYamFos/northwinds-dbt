with merged_companies as (
    select business_name from {{ ref('stg_hubspot_companies') }} union all
    select business_name from {{ ref('stg_rds_companies') }}
)
select business_name from merged_companies group by business_name