WITH hubspot_contacts as (
    SELECT  
    contact_id,
    first_name,
    last_name,
    clean_phone as phone,
    business_name

    FROM {{ ref('stg_hubspot_contacts') }}

),
rds_customers as (
     SELECT 
     Customer_ID as contact_id,
     first_name,
     last_name, 
     clean_phone as phone,
     company_name as business_name    
     FROM {{ ref('stg_rds_customers') }}
),


merged_contacts as (
    SELECT contact_id as hubspot_id, null as rds_id, first_name, last_name, phone, business_name as hubspot_company_id, null as rds_company_id
    from hubspot_contacts union all
    SELECT null as hubspot_id, contact_id as rds_id, first_name, last_name, phone, null as hubspot_company_id, business_name as rds_company_id
    from rds_customers
),
deduped as (
    select max(hubspot_id) as hubspot_id, max(rds_id) as rds_id, first_name, last_name, max(phone) as phone, 
    max(hubspot_company_id), max(rds_company_id)
    FROM merged_contacts
    GROUP BY first_name, last_name
    ORDER BY first_name, last_name
),
contacts_ids as (
    SELECT {{ dbt_utils.surrogate_key(['first_name', 'last_name', 'phone']) }} as contact_pk, *
    FROM deduped
)

SELECT  * FROM contacts_ids