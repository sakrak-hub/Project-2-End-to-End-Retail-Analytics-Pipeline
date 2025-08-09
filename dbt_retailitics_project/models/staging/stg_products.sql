{{config(
    materialized='view'
)}}

SELECT
 product_id ,
 product_name ,
 category ,
 subcategory ,
 brand ,
 price ,
 cost ,
 sku ,
 description ,
 weight ,
 dimensions ,
 stock_quantity ,
 supplier ,
 launch_date
FROM {{source('RETAILITICS_TRANSACTIONS', 'PRODUCTS' )}}