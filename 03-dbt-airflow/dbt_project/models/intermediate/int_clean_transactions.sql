-- Intermediate: only valid rows, deduplicated, with derived fields.
select
    transaction_id,
    transaction_date,
    amount,
    currency,
    fx_rate,
    amount_usd,
    merchant,
    channel,
    account_id,
    date_trunc('month', transaction_date) as transaction_month,
    amount_usd >= 1000 as is_high_value
from {{ ref('stg_transactions') }}
where is_valid
qualify row_number() over (partition by transaction_id order by transaction_date) = 1
