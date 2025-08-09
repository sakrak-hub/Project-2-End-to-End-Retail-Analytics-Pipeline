{{config(
    materialized='view'
)}}

SELECT
 store_id ,
 store_name ,
 address ,
 city ,
 state ,
 zip_code ,
 phone ,
 manager ,
 store_type ,
 opening_date
FROM {{source ('RETAILITICS_TRANSACTIONS', 'STORES' )}}