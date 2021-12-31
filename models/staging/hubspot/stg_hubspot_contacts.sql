WITH source as (
    SELECT *
    FROM {{source('hubspot', 'contacts')}}
),
renamed as (SELECT 
CONCAT('hubspot-', HUBSPOT_ID) as CONTACT_ID,
FIRST_NAME,
LAST_NAME,
TRANSLATE(phone, '.,(, ), -', '') as translphone, 
business_name  
FROM source


)
SELECT 
CONTACT_ID,
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
business_name
FROM renamed
