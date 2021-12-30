WITH source as (
    SELECT * FROM {{source('rds', 'customers')}}
),
companies as (
    SELECT 
    concat('rds-', replace(lower(company_name), ' ', '-')) as company_id,
    company_name,
    REPLACE(TRANSLATE(phone, '.,(, ), -', ''), ' ', '') as translphone,
    CASE WHEN len(translphone) = 10 THEN 
    concat('(', substr(translphone, 1, 3), ') ')
    ||''||
    substr(translphone, 4, 3)
    ||'-'||
    substr(translphone, 7, 4)
    else null 
    END as clean_phone,
    address,
    city, 
    postal_code
    FROM source
    
)

SELECT * FROM companies