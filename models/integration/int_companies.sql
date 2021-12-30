WITH source as (
    SELECT * FROM {{ ref('stg_rds_companies') }}
),
company_join as (
     SELECT * FROM {{ ref('stg_rds_companies') }}
)
SELECT * FROM company_join