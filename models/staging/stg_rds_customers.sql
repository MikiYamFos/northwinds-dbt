WITH source as (
    SELECT * FROM {{source('rds', 'customers') }},

renamed as (
SELECT 
    CONCAT('rds-', customer_id) as customer_id, 
    country,
    SPLIT_PART(contact_name, ' ', 1) as first_name,
    SPLIT_PART(contact_name, ' ', -1) as last_name
    FROM source
)
SELECT * FROM renamed