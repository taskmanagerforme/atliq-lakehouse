# AtliQ batch lakehouse: architecture

## 1. Why OLTP and OLAP are separate

OLTP is for production: it is good at many small writes (orders, payments) but not meant for analysis. In this build the OLTP side is Azure SQL with five normalised tables. If we ran reports directly on it, the heavy queries would slow down checkouts, because it is the same database the shop writes to, and every question would need many joins across the normalised tables. So we copy the data once a night (D-1, batch, not live) into a separate lakehouse and reshape it into a star schema: fact_sales (one row per order item) with dim_customer, dim_product and dim_date, plus fact_web_sessions for the funnel. There the storage is columnar, analysis needs far fewer joins, and the cleaning never touches the live data.

## 2. How the control table and watermarks drive the nightly sync

The watermark is a bookmark kept for each table in etl.control_table. It records up to when that table has been copied, so the next run copies only what changed since then. customers and products are small, so they are copied in full every night. orders, order_items and payments are copied incrementally: only rows where the watermark column is newer than last_loaded_at go to bronze/<table>/ingest_date=<date>/. Orders and payments use updated_at because their rows change (Placed -> Delivered -> Returned); order_items uses created_at because a line never changes once written.

It is one generic ADF pipeline that loops over the control table, so adding a table means adding a row, not building a new pipeline. A table's watermark moves only if that table's copy succeeded, and it moves to the time the run started, not when it ended, so rows that arrive during the run are picked up the next night instead of being missed. On the first run (2026-11-14) the watermark was 1900-01-01, so all 10,020 orders were copied; after that only changes are copied.

## 3. What happens on a quiet night

A quiet night is a night with no new data in the source, so ADF writes no Bronze folder for that date. Without a guard, the notebook would try to read the missing folder, the job would fail and we would get a failure alert for nothing. So we set up a guard: it checks whether tonight's folder exists, skips reading it if not, and writes a note in dq_log that no data was available that day and it was skipped. The job still runs and succeeds, but any other error still fails it and sends the alert. Silver stays the same, so the Gold that dbt rebuilds from it is the same as the previous night, which is idempotent in practice.

## 4. How I proved idempotency

Idempotent means running the job again gives the same result: rows are only added when there is new data, and no duplicates appear. Silver uses MERGE on the business keys (order_id, order_item_id, payment_id): a new key is inserted, an existing key is updated only if the incoming row is newer, and anything else is ignored. dbt rebuilds Gold from Silver on every run.

I proved it in two ways. First, two runs of the job on 2026-11-13 gave identical results in atliq.silver.run_audit: 22,959 fact rows and 39,392,719.00 gross revenue both times. Second, the first ADF batch on 2026-11-14 contained all 10,020 orders, yet Silver went from 10,000 to exactly 10,020 orders, and the 5 orders the simulator had changed now show Returned: updated in place, not duplicated. (The 39.39M includes Cancelled orders; the checkpoint figure 32,424,661.00 excludes them.)
