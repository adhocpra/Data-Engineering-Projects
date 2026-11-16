-- Mart: monthly summary for reporting.
select
    transaction_month,
    count(*) as transaction_count,
    round(sum(amount_usd), 2) as total_amount_usd,
    round(avg(amount_usd), 2) as avg_amount_usd,
    sum(case when is_high_value then 1 else 0 end) as high_value_count
from {{ ref('fct_transactions') }}
group by transaction_month
order by transaction_month
