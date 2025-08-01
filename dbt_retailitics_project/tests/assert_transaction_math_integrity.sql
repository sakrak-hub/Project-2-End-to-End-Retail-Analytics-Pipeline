-- tests/assert_transaction_math_integrity.sql
-- Test that transaction math is consistent across all records
{{ config(severity = 'error') }}

select 
    transaction_id,
    subtotal,
    tax_amount,
    total_amount,
    quantity,
    unit_price,
    abs(subtotal + tax_amount - total_amount) as amount_difference,
    abs(quantity * unit_price - subtotal) as line_item_difference
from {{ source('raw_s3_data', 'transactions') }}
where 
    -- Flag transactions where amounts don't add up (allowing for small rounding)
    abs(subtotal + tax_amount - total_amount) > 0.02
    or 
    -- Flag where quantity * unit_price doesn't match subtotal (allowing 10% variance for discounts)
    abs(quantity * unit_price - subtotal) > greatest(subtotal * 0.1, 0.02)

---

-- tests/assert_no_duplicate_transactions.sql
-- More comprehensive duplicate check beyond just transaction_id
{{ config(severity = 'warn') }}

select 
    customer_id,
    store_id,
    datetime,
    total_amount,
    count(*) as duplicate_count
from {{ source('raw_s3_data', 'transactions') }}
group by customer_id, store_id, datetime, total_amount
having count(*) > 1

---

-- tests/assert_reasonable_transaction_patterns.sql
-- Check for suspicious transaction patterns
{{ config(severity = 'warn') }}

with transaction_stats as (
    select 
        customer_id,
        date,
        count(*) as daily_transaction_count,
        sum(total_amount) as daily_total_amount,
        max(total_amount) as max_transaction_amount
    from {{ source('raw_s3_data', 'transactions') }}
    where status = 'completed'
    group by customer_id, date
)
select *
from transaction_stats
where 
    -- Flag customers with unusually high transaction counts per day
    daily_transaction_count > 50
    or 
    -- Flag customers with unusually high daily spending
    daily_total_amount > 10000
    or
    -- Flag individual transactions that are exceptionally large
    max_transaction_amount > 5000

---

-- tests/assert_datetime_date_consistency.sql
-- Ensure datetime and date fields are consistent
{{ config(severity = 'error') }}

select 
    transaction_id,
    date,
    datetime,
    date(datetime) as derived_date
from {{ source('raw_s3_data', 'transactions') }}
where date != date(datetime)

---

-- tests/assert_no_future_transactions.sql
-- Ensure no transactions are dated in the future
{{ config(severity = 'error') }}

select 
    transaction_id,
    date,
    datetime
from {{ source('raw_s3_data', 'transactions') }}
where 
    date > current_date
    or 
    datetime > current_timestamp + interval '1 hour'  -- Allow small clock skew

---

-- tests/assert_business_hours_logic.sql
-- Flag transactions outside reasonable business hours (adjust as needed)
{{ config(severity = 'warn') }}

select 
    transaction_id,
    store_id,
    datetime,
    extract(hour from datetime) as transaction_hour,
    extract(dow from datetime) as day_of_week  -- 0=Sunday, 6=Saturday
from {{ source('raw_s3_data', 'transactions') }}
where 
    -- Flag transactions outside 6 AM - 11 PM
    extract(hour from datetime) < 6 
    or 
    extract(hour from datetime) > 23
    -- Uncomment below if you want to flag weekend transactions
    -- or extract(dow from datetime) in (0, 6)

---

-- tests/assert_payment_method_amount_logic.sql
-- Business logic checks for payment methods
{{ config(severity = 'warn') }}

select 
    transaction_id,
    payment_method,
    total_amount
from {{ source('raw_s3_data', 'transactions') }}
where 
    -- Cash transactions over $500 might be suspicious
    (payment_method = 'cash' and total_amount > 500)
    or
    -- Gift card transactions over $200 might need review
    (payment_method = 'gift_card' and total_amount > 200)

---

-- tests/assert_refund_logic.sql
-- Validate refund transactions have proper negative amounts
{{ config(severity = 'error') }}

select 
    transaction_id,
    status,
    total_amount,
    subtotal,
    tax_amount
from {{ source('raw_s3_data', 'transactions') }}
where 
    status = 'refunded' 
    and (
        total_amount > 0 
        or subtotal > 0 
        or tax_amount > 0
    )