{{config(
    materialized='view'
)}}

SELECT
    transaction_id,
    to_date(to_timestamp(date,9)) AS transaction_date,
    time AS transaction_time,
    to_timestamp(datetime,9) AS transaction_datetime,
    customer_id,
    store_id,
    store_name,
    cashier_id,
    payment_method,
    subtotal,
    tax_amount,
    total_amount,
    items_count,
    loyalty_points_earned,
    promotion_code,
    refund_reason,
    status,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    discount_percent,
    line_total
FROM {{source ('RETAILITICS_TRANSACTIONS', 'TRANSACTIONS' )}}