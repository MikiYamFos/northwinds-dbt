WITH source AS (
    SELECT * FROM "FIVETRAN_DATABASE"."NORTHWINDS_RDS_PUBLIC"."SUPPLIERS"
),
renamed as (
    SELECT  
    SPLIT_PART(CONTACT_NAME, ' ', 1) as contact_first_name,
    SPLIT_PART(CONTACT_NAME, ' ', -1) as contact_last_name,
    TRANSLATE(phone, '.,(, ), -', '') as translphone, *
    FROM source
), clean_nums as (
    SELECT  supplier_id, country, contact_first_name, contact_last_name,
    address, city, 
    CASE WHEN len(translphone) = 10 THEN 
    concat('(', substr(translphone, 1, 3), ') ')
    ||''||
    substr(translphone, 4, 3)
    ||'-'||
    substr(translphone, 7, 4)
    else null 
    END as clean_phone,
    company_name, region, postal_code, fax, contact_title,
    homepage, _FIVETRAN_DELETED, _FIVETRAN_SYNCED
    
    FROM renamed
)

SELECT * FROM clean_nums

