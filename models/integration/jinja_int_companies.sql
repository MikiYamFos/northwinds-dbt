{% set sources = ["stg_hubspot_companies", "stg_rds_companies"] %}


with merged_companies as (
    {% for source in sources %}
    select business_name, {{'company_id' if 'hubspot' in source else 'null'}} as hubspot_company_id,
    {{'company_id' if 'rds' in source else 'null'}} as rds_company_id
    from {{ref(source)}}
    {% if not loop.last %} union all {% endif %} {% endfor %}
),
deduped as (SELECT
    max(hubspot_company_id) as hubspot_company_id, max(rds_company_id) as rds_company_id, 
    business_name from merged_companies 
    group by business_name
    ORDER by business_name
)

SELECT {{ dbt_utils.surrogate_key(['deduped.business_name']) }} as contact_pk,
hubspot_company_id, rds_company_id, deduped.business_name,
rds.clean_phone as phone, rds.address, rds.city, rds.postal_code
FROM deduped
LEFT JOIN {{ref('stg_rds_companies')}} as rds on deduped.rds_company_id = rds.company_id