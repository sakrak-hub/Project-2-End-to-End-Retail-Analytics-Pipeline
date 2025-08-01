{{ config(materialized='view') }}

select
    'raw_transactions' as table_name,
    count(*) as total_records,
    count(distinct transaction_id) as unique_transactions,
    count(case when transaction_id is null then 1 end) as null_transaction_ids,
    count(case when date is null then 1 end) as null_dates,
    count(case when total_amount <= 0 then 1 end) as negative_amounts,
    current_timestamp() as quality_check_timestamp
from {{ source('raw_s3_data', 'transactions') }}