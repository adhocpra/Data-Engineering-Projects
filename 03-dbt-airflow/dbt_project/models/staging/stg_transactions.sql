-- Staging: cast types, convert to USD, flag rows that fail validity rules.
-- Bad rows are NOT dropped here; they are flagged so tests can measure them.
select
    transaction_id,
    cast(transaction_date as date) as transaction_date,
    cast(amount as double) as amount,
    currency,
    cast(fx_rate as double) as fx_rate,
    merchant,
    channel,
    account_id,
    status,
    case
        when amount is not null then amount * fx_rate
        else null
    end as amount_usd,
    case
        when amount is null then false
        when amount <= 0 then false
        when currency not in ('USD', 'EUR', 'GBP') then false
        else true
    end as is_valid,
    case
        when amount is null then 'missing_amount'
        when amount <= 0 then 'non_positive_amount'
        when currency not in ('USD', 'EUR', 'GBP') then 'bad_currency'
        else null
    end as invalid_reason
from {{ source('raw', 'raw_transactions') }}
