CREATE OR REPLACE TABLE STORES(
 store_id VARCHAR,
 store_name VARCHAR,
 address VARCHAR,
 city VARCHAR,
 state VARCHAR,
 zip_code VARCHAR,
 phone VARCHAR,
 manager VARCHAR,
 store_type VARCHAR,
 opening_date DATE
);

COPY INTO STORES
FROM(
SELECT
 $1:store_id::VARCHAR,
 $1:store_name::VARCHAR,
 $1:address::VARCHAR,
 $1:city::VARCHAR,
 $1:state::VARCHAR,
 $1:zip_code::VARCHAR,
 $1:phone::VARCHAR,
 $1:manager::VARCHAR,
 $1:store_type::VARCHAR,
 $1:opening_date::DATE
FROM @MY_S3_STATIC/stores.parquet
(FILE_FORMAT => PARQUET_FORMAT)
)