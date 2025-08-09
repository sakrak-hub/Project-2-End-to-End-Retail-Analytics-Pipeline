CREATE OR REPLACE TABLE PRODUCTS(
 product_id VARCHAR,
 product_name VARCHAR,
 category VARCHAR,
 subcategory VARCHAR,
 brand VARCHAR,
 price FLOAT,
 cost FLOAT,
 sku VARCHAR,
 description VARCHAR,
 weight FLOAT,
 dimensions VARCHAR,
 stock_quantity INTEGER,
 supplier VARCHAR,
 launch_date DATE
);

COPY INTO PRODUCTS
FROM(
SELECT
 $1:product_id::VARCHAR,
 $1:product_name::VARCHAR,
 $1:category::VARCHAR,
 $1:subcategory::VARCHAR,
 $1:brand::VARCHAR,
 $1:price::FLOAT,
 $1:cost::FLOAT,
 $1:sku::VARCHAR,
 $1:description::VARCHAR,
 $1:weight::FLOAT,
 $1:dimensions::VARCHAR,
 $1:stock_quantity::INTEGER,
 $1:supplier::VARCHAR,
 $1:launch_date::DATE
FROM @MY_S3_STATIC/products.parquet
(FILE_FORMAT => PARQUET_FORMAT)
);