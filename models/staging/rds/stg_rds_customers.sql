WITH source as (
    SELECT * FROM {{source('rds', 'customers') }}),

renamed as (
SELECT 
    CONCAT('rds-', customer_id) as customer_id, 
    country,
    SPLIT_PART(contact_name, ' ', 1) as first_name,
    SPLIT_PART(contact_name, ' ', -1) as last_name,
    TRANSLATE(phone, '.,(, ), -', '') as translphone, 
    company_name

    FROM source
)
SELECT customer_id,
FIRST_NAME,
LAST_NAME,
CASE WHEN len(translphone) = 10 THEN 
    concat('(', substr(translphone, 1, 3), ') ')
    ||''||
    substr(translphone, 4, 3)
    ||'-'||
    substr(translphone, 7, 4)
    else null 
    END as clean_phone,
    company_name

FROM renamed