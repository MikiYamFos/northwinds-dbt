{% set sources = ["stg_hubspot_contacts", "stg_rds_customers"] %}

with merged_contacts as (
    {% for source in sources %}
    SELECT 
    {{'contact_id' if 'hubspot' in source else 'null'}} as hubspot_contact_id,
    {{'customer_id' if 'rds' in source else 'null' }} as rds_contact_id,
    first_name, last_name, clean_phone as phone,
    {{'business_name' if 'hubspot' in source else 'null'}} as hubspot_company_id,
    {{'company_name' if 'rds' in source else 'null'}} as rds_company_id

    FROM {{ref(source)}}
    {% if not loop.last %} union all {% endif %} {% endfor %}
), deduped as (
SELECT
max(hubspot_contact_id) as hubspot_contact_id, max(rds_contact_id) as rds_contact_id,
first_name, last_name, max(phone) as phone,
max(CONCAT('hubspot-', (REPLACE(REPLACE(hubspot_company_id, ' ', '-'), ',', '')))) as hubspot_company_id, 
max(CONCAT('rds-', (REPLACE(REPLACE(rds_company_id, ' ', '-'), ',', '')))) as rds_company_id
FROM merged_contacts
GROUP BY first_name, last_name
ORDER BY first_name, last_name

), contact_ids as (
    SELECT {{ dbt_utils.surrogate_key(['first_name', 'last_name', 'phone']) }} as contact_pk, *
    FROM deduped
)
SELECT * FROM contact_ids