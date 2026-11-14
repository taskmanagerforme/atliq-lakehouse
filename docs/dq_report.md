# Data quality report

Source: `atliq.silver.dq_log` (one row per cleaning rule per run, written by `01_bronze_to_silver`)
and the §7.7 checkpoint queries in `02_checkpoints`. Figures below are from the seed load
(`run_date = 2026-08-31`), which is the period the §7.7 targets describe.

**Rule of the pipeline:** text is normalised through lookup maps; anything a map does not recognise
becomes NULL and a dbt `not_null` / `accepted_values` test fails the build. Every "not recognised"
rule below logged **0**, so no unknown variant slipped through.

## 1. What the source teams' issues looked like (§4.3), and what Silver did

| Source | Issue found | Rows | Handling |
|---|---|---:|---|
| customers | city casing / spaces / old names (e.g. Bangalore) | 130 | trim, proper case, map to current city |
| customers | email casing / spaces | 52 | lower-case, trim |
| customers | QA test accounts | 8 | flagged `is_test`, excluded in dbt |
| products | category spellings | 13 | mapped to the 6 categories |
| products | product names with stray spaces | 5 | trimmed, spaces collapsed |
| orders | status text variants (`canceled`, casing…) | 789 | mapped to the 5 statuses |
| orders | legacy `1900-01-01` order_date | 55 | replaced by the `created_at` date |
| orders | orders placed by QA test accounts | 39 | kept in Silver, excluded in dbt staging |
| order_items | double-submitted lines | 101 | kept in Silver, de-duplicated in dbt staging |
| payments | payment-method variants | 1,009 | mapped to the 6 methods |
| payments | gateway retries (extra payments per order) | 108 | kept in Silver, one per order in dbt staging |
| supplier_price_list | amounts as `₹` / `Rs.` / thousands separators | 32 | parsed to decimal |
| supplier_price_list | non-ISO dates | 39 | parsed from 3 formats |
| supplier_price_list | exact duplicate rows | 4 | removed |
| supplier_price_list | unknown / discontinued products | 3 | ignored |
| marketing_spend | channel variants | 466 | mapped to the 5 channels |
| marketing_spend | exact duplicate rows | 54 | removed |
| marketing_spend | blank spend | 17 | dropped |
| marketing_spend | negative correction rows | 44 | kept (they net off) |
| clickstream | unparseable JSON lines | 426 | quarantined to `web_events_quarantine` |
| clickstream | re-sent events (same `event_id`) | 2,680 | one row per event_id |
| clickstream | bot sessions (1,059 sessions) | 38,414 events | dropped |
| clickstream | events without session_id | 638 | dropped |
| clickstream | unknown / retired products | 186 | dropped |
| clickstream | epoch-millisecond timestamps | 6,816 | parsed |
| clickstream | event_type casing / spaces | 6,833 | normalised |
| clickstream | late events (event date before file date) | 1,773 | kept, dated by `event_ts` |

## 2. Checkpoints (§7.7): all 28 match

| Check | Expected | Ours |
|---|---|---|
| Customers (no QA) | 1,000 in 10 cities | 1,000 in 10 cities |
| Products | 60 in 6 categories | 60 in 6 categories |
| Orders (no test accounts) | 9,961 | 9,961 |
| Status split | 47 / 102 / 6,838 / 1,778 / 1,196 | 47 / 102 / 6,838 / 1,778 / 1,196 |
| Order items in fact_sales | 22,959 | 22,959 |
| Payments | 8,183 totalling 32,424,661.00 | 8,183 / 32,424,661.00 |
| Gross revenue (excl. Cancelled) | 32,424,661.00 | 32,424,661.00 |
| Returned / net revenue | 4,574,944.00 / 27,849,717.00 | 4,574,944.00 / 27,849,717.00 |
| Supplier current-cost rows | 60 / 56,871.53 | 60 / 56,871.53 |
| Marketing | 3,037 rows · 8,920,850.16 · 3,274,996 clicks | same |
| Clickstream | 426 quarantined · 93,436 events · 23,193 sessions | same |
| Funnel | 13,309 → 7,391 → 4,442 → 3,446 | same |

## 3. After the first ADF batch (2026-11-14)

The simulator added 20 orders and moved 5 existing orders one status step; ADF copied the batch.
Silver went from 10,000 to **10,020** orders (no duplicates), order_items 23,203, payments 8,341.
The 5 moved orders (358, 1240, 6337, 8479, 8537) now show **Returned** with `updated_at` 2026-11-14:
updated in place by the MERGE, not duplicated. New messy rows were cleaned by the same rules
(payment-method mappings 1,009 → 1,013, gateway retries 108 → 109). The §7.7 targets describe the
seed period, so totals now differ from them by exactly these new rows.

## 4. Tests

dbt: 37 data tests (not_null, unique, accepted_values, relationships, and
`assert_payments_match_orders`) all pass, locally and in the CI target (PASS=49, ERROR=0).
