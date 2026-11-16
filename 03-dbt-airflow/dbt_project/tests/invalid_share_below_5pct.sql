-- Singular test: invalid rows in staging must stay below 5% of all rows.
-- The generator injects ~2% bad rows, so this passes while still catching
-- a real data-quality regression if the bad-row share spikes.
select
    count(*) as invalid_rows,
    (select count(*) from {{ ref('stg_transactions') }}) as total_rows
from {{ ref('stg_transactions') }}
where not is_valid
having count(*) * 1.0 / (select count(*) from {{ ref('stg_transactions') }}) >= 0.05
