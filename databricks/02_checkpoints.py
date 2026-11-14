# Databricks notebook source
# MAGIC %md
# MAGIC # §7.7 checkpoint totals (seed period, up to 31 Aug 2026)
# MAGIC Run after `dbt build`. Every row should show `match = true`.
# MAGIC Silver-side checks are repeated here so the whole comparison is one table (the DQ report evidence).

# COMMAND ----------

display(spark.sql("""
WITH sales AS (SELECT * FROM atliq.gold.fact_sales WHERE order_date <= '2026-08-31'),
orders AS (SELECT order_id, status FROM atliq.gold.stg_orders WHERE order_date <= '2026-08-31'),
pay AS (SELECT p.* FROM atliq.gold.stg_payments p JOIN atliq.gold.stg_orders o USING (order_id)
        WHERE o.order_date <= '2026-08-31'),
sup AS (SELECT * FROM atliq.gold.stg_supplier_price_list),
mk AS (SELECT * FROM atliq.gold.stg_marketing_spend),
ws AS (SELECT * FROM atliq.gold.fact_web_sessions),
checks AS (
  SELECT 1 n, 'Customers (excl. test)' chk, CAST(COUNT(*) AS STRING) actual, '1000' expected FROM atliq.gold.dim_customer
  UNION ALL SELECT 2, 'Customer cities', CAST(COUNT(DISTINCT city) AS STRING), '10' FROM atliq.gold.dim_customer
  UNION ALL SELECT 3, 'Products', CAST(COUNT(*) AS STRING), '60' FROM atliq.gold.dim_product
  UNION ALL SELECT 4, 'Product categories', CAST(COUNT(DISTINCT category) AS STRING), '6' FROM atliq.gold.dim_product
  UNION ALL SELECT 5, 'Orders (excl. test)', CAST(COUNT(*) AS STRING), '9961' FROM orders
  UNION ALL SELECT 6, '  Placed',    CAST(COUNT_IF(status='Placed') AS STRING),    '47'   FROM orders
  UNION ALL SELECT 7, '  Shipped',   CAST(COUNT_IF(status='Shipped') AS STRING),   '102'  FROM orders
  UNION ALL SELECT 8, '  Delivered', CAST(COUNT_IF(status='Delivered') AS STRING), '6838' FROM orders
  UNION ALL SELECT 9, '  Cancelled', CAST(COUNT_IF(status='Cancelled') AS STRING), '1778' FROM orders
  UNION ALL SELECT 10, '  Returned', CAST(COUNT_IF(status='Returned') AS STRING),  '1196' FROM orders
  UNION ALL SELECT 11, 'Order items in fact_sales', CAST(COUNT(*) AS STRING), '22959' FROM sales
  UNION ALL SELECT 12, 'Payments (one per paid order)', CAST(COUNT(*) AS STRING), '8183' FROM pay
  UNION ALL SELECT 13, 'Payments total', CAST(SUM(amount) AS STRING), '32424661.00' FROM pay
  UNION ALL SELECT 14, 'Gross revenue (excl. Cancelled)', CAST(SUM(gross_revenue) FILTER (WHERE status <> 'Cancelled') AS STRING), '32424661.00' FROM sales
  UNION ALL SELECT 15, 'Returned revenue', CAST(SUM(gross_revenue) FILTER (WHERE status = 'Returned') AS STRING), '4574944.00' FROM sales
  UNION ALL SELECT 16, 'Net revenue', CAST(SUM(gross_revenue) FILTER (WHERE status NOT IN ('Cancelled','Returned')) AS STRING), '27849717.00' FROM sales
  UNION ALL SELECT 17, 'Supplier current cost rows', CAST(COUNT(*) AS STRING), '60' FROM sup
  UNION ALL SELECT 18, 'Supplier current cost sum', CAST(SUM(supplier_cost) AS STRING), '56871.53' FROM sup
  UNION ALL SELECT 19, 'Marketing rows', CAST(COUNT(*) AS STRING), '3037' FROM mk
  UNION ALL SELECT 20, 'Marketing spend', CAST(SUM(spend_amount) AS STRING), '8920850.16' FROM mk
  UNION ALL SELECT 21, 'Marketing clicks', CAST(SUM(clicks) AS STRING), '3274996' FROM mk
  UNION ALL SELECT 22, 'Clickstream lines quarantined', CAST(COUNT(*) AS STRING), '426' FROM atliq.silver.web_events_quarantine
  UNION ALL SELECT 23, 'Clickstream clean events', CAST(COUNT(*) AS STRING), '93436' FROM atliq.silver.web_events
  UNION ALL SELECT 24, 'Sessions', CAST(COUNT(*) AS STRING), '23193' FROM ws
  UNION ALL SELECT 25, '  → product view', CAST(SUM(reached_product_view) AS STRING), '13309' FROM ws
  UNION ALL SELECT 26, '  → add to cart',  CAST(SUM(reached_add_to_cart) AS STRING),  '7391' FROM ws
  UNION ALL SELECT 27, '  → checkout',     CAST(SUM(reached_checkout) AS STRING),     '4442' FROM ws
  UNION ALL SELECT 28, '  → purchase',     CAST(SUM(reached_purchase) AS STRING),     '3446' FROM ws
)
SELECT chk AS checkpoint, actual, expected, actual = expected AS match FROM checks ORDER BY n
"""))

# COMMAND ----------

# MAGIC %md ## Data-quality log for the seed run (rows touched per §4.3 rule)

# COMMAND ----------

display(spark.sql("""
SELECT table_name, rule, rows_affected
FROM atliq.silver.dq_log
WHERE run_ts = (SELECT MIN(run_ts) FROM atliq.silver.dq_log)
ORDER BY table_name, rule
"""))
