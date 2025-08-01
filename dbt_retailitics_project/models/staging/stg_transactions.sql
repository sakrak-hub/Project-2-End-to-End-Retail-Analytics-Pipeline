{{ config(materialized='view') }}

select
    -- Transaction identifiers
    transaction_id,
    customer_id,
    store_id,
    store_name,
    cashier_id,
    
    -- Convert nanosecond timestamps to proper dates/timestamps
    to_date(to_timestamp(date, 9)) as transaction_date,
    time as transaction_time,
    to_timestamp(datetime, 9) as transaction_datetime,
    
    -- Financial fields
    subtotal,
    tax_amount,
    total_amount,
    items_count,
    loyalty_points_earned,
    
    -- Promotional fields
    promotion_code,
    discount_percent,
    
    -- Product details
    product_id,
    product_name,
    category as product_category,
    quantity,
    unit_price,
    line_total,
    
    -- Transaction details
    payment_method,
    status as transaction_status,
    refund_reason,
    
    -- Calculated fields
    case 
        when refund_reason is not null then true 
        else false 
    end as is_refund,
    
    case 
        when promotion_code is not null then true 
        else false 
    end as has_promotion

from {{ source('raw_s3_data', 'transactions') }}