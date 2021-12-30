WITH source as (
    SELECT * FROM {{source('hubspot', 'contacts')}}
),

renamed as (SELECT
CONCAT('hubspot-', (REPLACE(REPLACE(BUSINESS_NAME, ' ', '-'), ',', ''))) as COMPANY_ID,
BUSINESS_NAME
FROM source
GROUP BY BUSINESS_NAME)

SELECT DISTINCT * from renamed
ORDER BY BUSINESS_NAME ASC