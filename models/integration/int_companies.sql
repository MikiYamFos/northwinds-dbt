with hubspot_company as (SELECT * 
FROM {{ ref('stg_hubspot_companies') }}
),
rds_company as (
    SELECT * FROM {{ ref('stg_rds_companies') }}
),
comp_joined as (
    SELECT company_id as hubspot_id, null as rds_id, business_name from hubspot_company union all
    SELECT null as hubspot_id, company_id as rds_id, business_name from rds_company
),
deduped as (
    SELECT {{ dbt_utils.surrogate_key(['business_name']) }} as contact_pk,
    max(hubspot_id), max(rds_id), business_name
    FROM comp_joined
    GROUP BY business_name
    ORDER BY business_name
)
SELECT * from deduped