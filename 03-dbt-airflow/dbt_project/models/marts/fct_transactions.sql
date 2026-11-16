-- Mart: fact table of clean transactions.
select
    transaction_id,
    transaction_date,
    transaction_month,
    amount,
    currency,
    fx_rate,
    amount_usd,
    merchant,
    channel,
    account_id,
    is_high_value
from {{ ref('int_clean_transactions') }}
