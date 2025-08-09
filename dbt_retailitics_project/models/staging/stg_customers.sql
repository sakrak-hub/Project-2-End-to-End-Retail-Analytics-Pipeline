{{config(
    materialized='view'
)}}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    state,
    zip_code,
    date_of_birth,
    gender,
    registration_date,
    loyalty_member,
    preferred_contact,
    customer_segment,
    total_lifetime_value
FROM {{source ('RETAILITICS_TRANSACTIONS', 'CUSTOMERS' )}}