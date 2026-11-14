# Databricks notebook source
# MAGIC %md
# MAGIC # Run audit: one row per nightly run
# MAGIC Last task of the nightly job. Appends the run's start time, key row counts and status to
# MAGIC `atliq.silver.run_audit`, so a run that silently changed the numbers is visible later.
# MAGIC It only runs when Silver and dbt both succeeded (a failure stops the job and emails the owner).

# COMMAND ----------

from datetime import datetime, timezone

dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("start_time", "")
run_id, run_date, start_time = (dbutils.widgets.get(k) for k in ("run_id", "run_date", "start_time"))

row = spark.sql("""
SELECT (SELECT COUNT(*) FROM atliq.silver.orders)           AS silver_orders,
       (SELECT COUNT(*) FROM atliq.silver.order_items)      AS silver_order_items,
       (SELECT COUNT(*) FROM atliq.silver.payments)         AS silver_payments,
       (SELECT COUNT(*) FROM atliq.gold.fact_sales)         AS fact_rows,
       (SELECT SUM(gross_revenue) FROM atliq.gold.fact_sales) AS total_gross_revenue,
       (SELECT COUNT(*) FROM atliq.gold.fact_web_sessions)  AS web_sessions
""").first().asDict()

audit = spark.createDataFrame(
    [(run_id, run_date, start_time, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "SUCCESS",
      row["silver_orders"], row["silver_order_items"], row["silver_payments"],
      row["fact_rows"], row["total_gross_revenue"], row["web_sessions"])],
    "run_id STRING, run_date STRING, start_time STRING, finished_at STRING, status STRING, "
    "silver_orders LONG, silver_order_items LONG, silver_payments LONG, "
    "fact_rows LONG, total_gross_revenue DECIMAL(22,2), web_sessions LONG")
audit.write.mode("append").saveAsTable("atliq.silver.run_audit")
display(spark.table("atliq.silver.run_audit").orderBy("finished_at"))
