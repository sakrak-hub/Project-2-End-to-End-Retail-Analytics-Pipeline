CREATE OR REPLACE TABLE CUSTOMERS(
    customer_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip_code VARCHAR,
    date_of_birth DATE,
    gender VARCHAR,
    registration_date DATE,
    loyalty_member VARCHAR,
    preferred_contact VARCHAR,
    customer_segment VARCHAR,
    total_lifetime_value FLOAT
);

COPY INTO CUSTOMERS
FROM (
SELECT
    $1:customer_id::VARCHAR,
    $1:first_name::VARCHAR,
    $1:last_name::VARCHAR,
    $1:email::VARCHAR,
    $1:phone::VARCHAR,
    $1:address::VARCHAR,
    $1:city::VARCHAR,
    $1:state::VARCHAR,
    $1:zip_code::VARCHAR,
    $1:date_of_birth::DATE,
    $1:gender::VARCHAR,
    $1:registration_date::DATE,
    $1:loyalty_member::VARCHAR,
    $1:preferred_contact::VARCHAR,
    $1:customer_segment::VARCHAR,
    $1:total_lifetime_value::FLOAT
FROM @MY_S3_STATIC/customers.parquet
(FILE_FORMAT => PARQUET_FORMAT)
);