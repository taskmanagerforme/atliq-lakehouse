# Databricks notebook source
# MAGIC %md
# MAGIC # AtliQ Commerce: Bronze → Silver (PySpark + Delta)
# MAGIC
# MAGIC Reads the Bronze landing zone (Parquet from the OLTP copy, CSVs and clickstream JSON as-is)
# MAGIC and writes clean, conformed Delta tables to `atliq.silver`.
# MAGIC
# MAGIC * **Full tables** (customers, products, both CSVs, clickstream): clean, then overwrite.
# MAGIC * **Incremental tables** (orders, order_items, payments): read only this run's
# MAGIC   `ingest_date=<run_date>` batch, keep the latest row per key, clean, then **Delta MERGE**.
# MAGIC   A night with no batch is skipped cleanly (quiet-night guard, §7.4).
# MAGIC * Every §4.3 rule records the rows it touched in `atliq.silver.dq_log`.
# MAGIC * Business duplicates (retried payments, double-submitted lines) and QA test accounts are
# MAGIC   **flagged/kept** here and removed in dbt staging, so Silver stays an auditable copy of the source.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

dbutils.widgets.text("run_date", "2026-08-31")      # set by the nightly job
run_date = dbutils.widgets.get("run_date")
RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

CATALOG = "atliq"
BRONZE = f"/Volumes/{CATALOG}/raw/landing/bronze"
S = f"{CATALOG}.silver"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
print("run_date =", run_date, "| run_ts =", RUN_TS)

# COMMAND ----------

# MAGIC %md ## Cleaning rules (one place, shared by the full-load and MERGE paths)

# COMMAND ----------

def norm(col):
    """trimmed, lower-case, single-spaced text: the key every lookup map uses"""
    return F.lower(F.regexp_replace(F.trim(col), r"\s+", " "))

def lookup(mapping):
    return F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])

CITY_MAP = {"bengaluru": "Bengaluru", "bangalore": "Bengaluru", "mumbai": "Mumbai", "bombay": "Mumbai",
            "delhi": "Delhi", "new delhi": "Delhi", "hyderabad": "Hyderabad", "chennai": "Chennai",
            "madras": "Chennai", "pune": "Pune", "kolkata": "Kolkata", "calcutta": "Kolkata",
            "ahmedabad": "Ahmedabad", "jaipur": "Jaipur", "surat": "Surat"}
CATEGORY_MAP = {"electronics": "Electronics", "home & kitchen": "Home & Kitchen",
                "home and kitchen": "Home & Kitchen", "fashion": "Fashion", "books": "Books",
                "beauty": "Beauty", "sports": "Sports"}
STATUS_MAP = {"placed": "Placed", "shipped": "Shipped", "delivered": "Delivered",
              "cancelled": "Cancelled", "canceled": "Cancelled", "returned": "Returned"}
METHOD_MAP = {"upi": "UPI", "credit card": "Credit Card", "creditcard": "Credit Card",
              "debit card": "Debit Card", "debitcard": "Debit Card", "net banking": "Net Banking",
              "netbanking": "Net Banking", "wallet": "Wallet", "cod": "COD", "cash on delivery": "COD"}
CHANNEL_MAP = {"meta ads": "Meta Ads", "facebook ads": "Meta Ads", "google ads": "Google Ads",
               "googleads": "Google Ads", "email": "Email", "e-mail": "Email", "seo": "SEO",
               "influencer": "Influencer", "influencer marketing": "Influencer"}
BOT_UA = r"(?i)(bot|crawler|spider|python-requests)"
TEST_DOMAIN = "@atliq-test.com"

_dq_rows = []
def log_dq(table_name, rule, rows_affected):
    _dq_rows.append((run_date, RUN_TS, table_name, rule, int(rows_affected)))
    print(f"  dq | {table_name:22s} | {rule:55s} | {int(rows_affected)}")

def flush_dq():
    if _dq_rows:
        (spark.createDataFrame(_dq_rows, "run_date STRING, run_ts STRING, table_name STRING, rule STRING, rows_affected LONG")
              .write.mode("append").saveAsTable(f"{S}.dq_log"))
        _dq_rows.clear()

def changed(df, raw_col, clean_col):
    """rows whose value was changed by a cleaning rule (null-safe)"""
    return df.filter(~F.col(raw_col).eqNullSafe(F.col(clean_col))).count()

def batch_exists(path: str) -> bool:
    """True when this run's Bronze folder exists and holds at least one Parquet file."""
    try:
        return any(f.name.endswith(".parquet") for f in dbutils.fs.ls(path))
    except Exception as e:
        if "FileNotFound" in str(e) or "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e):
            return False
        raise   # anything else (e.g. permissions) should still fail the run

# COMMAND ----------

# MAGIC %md ## Full tables: customers, products

# COMMAND ----------

raw = spark.read.parquet(f"{BRONZE}/customers")
c = (raw.withColumnRenamed("city", "city_raw").withColumnRenamed("email", "email_raw")
        .withColumn("customer_name", F.regexp_replace(F.trim("customer_name"), r"\s+", " "))
        .withColumn("city", lookup(CITY_MAP)[norm(F.col("city_raw"))])
        .withColumn("email", F.lower(F.trim("email_raw")))
        .withColumn("is_test", F.col("email").endswith(TEST_DOMAIN))
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"]))
c = c.cache()
log_dq("customers", "city trimmed / proper-cased / old name mapped", changed(c, "city_raw", "city"))
log_dq("customers", "city not recognised (NULL, fails dbt test)", c.filter("city IS NULL").count())
log_dq("customers", "email lower-cased / trimmed", changed(c, "email_raw", "email"))
log_dq("customers", "QA test accounts flagged is_test", c.filter("is_test").count())
(c.select("customer_id", "customer_name", "email", "city", "signup_date", "is_test", "updated_at")
  .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.customers"))

raw = spark.read.parquet(f"{BRONZE}/products")
p = (raw.withColumnRenamed("category", "category_raw").withColumnRenamed("product_name", "name_raw")
        .withColumn("product_name", F.regexp_replace(F.trim("name_raw"), r"\s+", " "))
        .withColumn("category", lookup(CATEGORY_MAP)[norm(F.col("category_raw"))])
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"]))
p = p.cache()
log_dq("products", "product name trimmed / spaces collapsed", changed(p, "name_raw", "product_name"))
log_dq("products", "category mapped to the 6 categories", changed(p, "category_raw", "category"))
log_dq("products", "category not recognised (NULL)", p.filter("category IS NULL").count())
(p.select("product_id", "product_name", "category", F.col("unit_price").cast("decimal(10,2)").alias("unit_price"), "updated_at")
  .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.products"))

# COMMAND ----------

# MAGIC %md ## Incremental tables: orders, order_items, payments (latest per key → Delta MERGE)

# COMMAND ----------

def merge_batch(table, key, order_col, clean, insert_only=False):
    path = f"{BRONZE}/{table}/ingest_date={run_date}"
    if not batch_exists(path):
        print(f"No new {table} batch for {run_date}: Silver unchanged.")
        log_dq(table, "no batch for run_date: skipped (quiet night)", 0)
        return
    batch = spark.read.parquet(path)
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    latest = batch.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    log_dq(table, "same key twice in one batch: older versions dropped", batch.count() - latest.count())
    src = clean(latest.filter(F.col(key).isNotNull()))
    target = f"{S}.{table}"
    DeltaTable.createIfNotExists(spark).tableName(target).addColumns(src.schema).execute()
    m = DeltaTable.forName(spark, target).alias("t").merge(src.alias("s"), f"t.{key} = s.{key}")
    if not insert_only:
        m = m.whenMatchedUpdateAll(condition=f"s.{order_col} > t.{order_col}")
    m.whenNotMatchedInsertAll().execute()
    stats = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").select("operationMetrics").first()[0]
    print(f"{target}: MERGE inserted={stats.get('numTargetRowsInserted')} updated={stats.get('numTargetRowsUpdated')}")

def clean_orders(df):
    out = (df.withColumnRenamed("status", "status_raw").withColumnRenamed("order_date", "order_date_raw")
             .withColumn("status", lookup(STATUS_MAP)[norm(F.col("status_raw"))])
             .withColumn("order_date", F.when(F.col("order_date_raw") == F.lit("1900-01-01").cast("date"),
                                              F.to_date("created_at")).otherwise(F.col("order_date_raw")))
             .withColumn("order_amount", F.col("order_amount").cast("decimal(12,2)"))).cache()
    log_dq("orders", "status text mapped to the 5 statuses", changed(out, "status_raw", "status"))
    log_dq("orders", "status not recognised (NULL)", out.filter("status IS NULL").count())
    log_dq("orders", "legacy 1900-01-01 order_date replaced by created_at date", changed(out, "order_date_raw", "order_date"))
    return out.select("order_id", "customer_id", "order_date", "status", "order_amount", "created_at", "updated_at")

def clean_items(df):
    dup = df.count() - df.dropDuplicates(["order_id", "product_id", "quantity", "item_price", "created_at"]).count()
    log_dq("order_items", "double-submitted lines (kept; removed in dbt staging)", dup)
    return df.withColumn("item_price", F.col("item_price").cast("decimal(10,2)"))

def clean_payments(df):
    out = (df.withColumnRenamed("method", "method_raw")
             .withColumn("method", lookup(METHOD_MAP)[norm(F.col("method_raw"))])
             .withColumn("amount", F.col("amount").cast("decimal(12,2)"))).cache()
    log_dq("payments", "payment method mapped to the 6 methods", changed(out, "method_raw", "method"))
    log_dq("payments", "payment method not recognised (NULL)", out.filter("method IS NULL").count())
    log_dq("payments", "gateway retries: extra payments per order (kept; removed in dbt staging)",
           out.count() - out.select("order_id").distinct().count())
    return out.select("payment_id", "order_id", "amount", "method", "paid_at", "updated_at")

merge_batch("orders", "order_id", "updated_at", clean_orders)
merge_batch("order_items", "order_item_id", "created_at", clean_items, insert_only=True)
merge_batch("payments", "payment_id", "updated_at", clean_payments)

test_orders = spark.sql(f"""SELECT COUNT(*) FROM {S}.orders o JOIN {S}.customers c USING (customer_id)
                            WHERE c.is_test""").first()[0]
log_dq("orders", "orders by QA test accounts (kept; excluded in dbt staging)", test_orders)

# COMMAND ----------

# MAGIC %md ## CSV sources: supplier price list, marketing spend (all columns arrive as text)

# COMMAND ----------

def read_csv(name):
    return spark.read.option("header", True).option("inferSchema", False).csv(f"{BRONZE}/{name}")

def parse_amount(col):
    """extract the number, never strip characters ('Rs. 1202.45' must stay 1202.45)"""
    a = F.regexp_replace(F.regexp_extract(col, r"-?[0-9][0-9,]*(\.[0-9]+)?", 0), ",", "")
    return F.when(a != "", a.cast("decimal(12,2)"))

sup_raw = read_csv("supplier_price_list")
n0 = sup_raw.count()
sup = sup_raw.dropDuplicates()
log_dq("supplier_price_list", "exact duplicate rows removed", n0 - sup.count())
sup = (sup.withColumn("product_id", F.col("product_id").cast("int"))
          .withColumn("supplier_cost_raw", F.col("supplier_cost"))
          .withColumn("supplier_cost", parse_amount(F.col("supplier_cost")))
          .withColumn("effective_date_raw", F.col("effective_date"))
          .withColumn("effective_date", F.coalesce(*[F.to_date(F.try_to_timestamp("effective_date", F.lit(f)))
                                                     for f in ["yyyy-MM-dd", "dd/MM/yyyy", "dd-MMM-yyyy"]]))).cache()
log_dq("supplier_price_list", "amount parsed from ₹ / Rs. / thousands separators",
       sup.filter(~F.col("supplier_cost_raw").rlike(r"^-?[0-9]+(\.[0-9]+)?$")).count())
log_dq("supplier_price_list", "date parsed from a non-ISO format",
       sup.filter(~F.col("effective_date_raw").rlike(r"^\d{4}-\d{2}-\d{2}$")).count())
log_dq("supplier_price_list", "amount or date unparseable (NULL)",
       sup.filter("supplier_cost IS NULL OR effective_date IS NULL").count())
known = spark.table(f"{S}.products").select("product_id")
sup_known = sup.join(known, "product_id", "left_semi")
log_dq("supplier_price_list", "rows for unknown / discontinued products ignored", sup.count() - sup_known.count())
(sup_known.select("product_id", F.trim("product_name").alias("product_name"), F.trim("supplier_name").alias("supplier_name"),
                  "supplier_cost", "effective_date")
          .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.supplier_price_list"))

mk = read_csv("marketing_spend")
n0 = mk.count()
mk = mk.filter(F.col("spend_amount").isNotNull() & (F.trim("spend_amount") != ""))
log_dq("marketing_spend", "rows with blank spend dropped", n0 - mk.count())
n1 = mk.count()
mk = mk.dropDuplicates()
log_dq("marketing_spend", "exact duplicate rows removed", n1 - mk.count())
mk = (mk.withColumn("channel_raw", F.col("channel"))
        .withColumn("channel", lookup(CHANNEL_MAP)[norm(F.col("channel_raw"))])
        .withColumn("clicks", F.regexp_replace("clicks", ",", "").cast("int"))
        .withColumn("spend_amount", parse_amount(F.col("spend_amount")))
        .withColumn("spend_date", F.to_date("spend_date"))).cache()
log_dq("marketing_spend", "channel mapped to the 5 channels", changed(mk, "channel_raw", "channel"))
log_dq("marketing_spend", "channel not recognised (NULL)", mk.filter("channel IS NULL").count())
log_dq("marketing_spend", "negative correction rows kept (they net off)", mk.filter("spend_amount < 0").count())
(mk.select("spend_date", "channel", F.trim("campaign").alias("campaign"), "spend_amount", "clicks")
   .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.marketing_spend"))

# COMMAND ----------

# MAGIC %md ## Clickstream: raw JSON lines → `web_events` (+ quarantine)

# COMMAND ----------

SCHEMA = ("event_id STRING, session_id STRING, customer_id BIGINT, event_type STRING, "
          "event_ts STRING, product_id BIGINT, order_id BIGINT, device STRING, "
          "user_agent STRING, utm_source STRING")
lines = spark.read.text(f"{BRONZE}/clickstream").withColumn("source_file", F.col("_metadata.file_name"))
parsed = lines.withColumn("e", F.from_json("value", SCHEMA)).cache()

quarantine = parsed.filter("e.event_id IS NULL").select("value", "source_file")
quarantine.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.web_events_quarantine")
log_dq("web_events", "unparseable lines quarantined", quarantine.count())

ts = (F.when(F.col("event_ts").rlike(r"^\d+$"), F.timestamp_millis(F.col("event_ts").cast("bigint")))
       .otherwise(F.try_to_timestamp("event_ts", F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSX"))))
ev0 = parsed.filter("e.event_id IS NOT NULL").select("e.*", "source_file")
log_dq("web_events", "event_ts given as epoch milliseconds (parsed)", ev0.filter(F.col("event_ts").rlike(r"^\d+$")).count())
ev = (ev0.withColumn("event_type_raw", F.col("event_type"))
         .withColumn("event_ts", ts)
         .withColumn("event_type", norm(F.col("event_type"))))
log_dq("web_events", "event_type casing / spaces normalised", changed(ev, "event_type_raw", "event_type"))
n0 = ev.count()
ev = ev.dropDuplicates(["event_id"])
n1 = ev.count()
log_dq("web_events", "re-sent events removed (one row per event_id)", n0 - n1)
ev = ev.filter(F.col("session_id").isNotNull() & (F.trim("session_id") != ""))
n2 = ev.count()
log_dq("web_events", "events without a session_id dropped", n1 - n2)
bots = ev.filter(F.col("user_agent").rlike(BOT_UA)).select("session_id").distinct()
ev = ev.join(bots, "session_id", "left_anti")
n3 = ev.count()
log_dq("web_events", f"bot sessions dropped ({bots.count()} sessions): events", n2 - n3)
known = spark.table(f"{S}.products").select(F.col("product_id").alias("_pid"))
ev = (ev.join(known, F.col("product_id") == F.col("_pid"), "left")
        .filter(F.col("product_id").isNull() | F.col("_pid").isNotNull()).drop("_pid"))
n4 = ev.count()
log_dq("web_events", "events for unknown / retired products dropped", n3 - n4)
log_dq("web_events", "late events (event date before file date, kept, dated by event_ts)",
       ev.filter(F.to_date("event_ts") < F.to_date(F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1))).count())
(ev.select("event_id", "session_id", "customer_id", "event_type", "event_ts", "product_id", "order_id",
           "device", "user_agent", "utm_source", "source_file")
   .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.web_events"))

flush_dq()

# COMMAND ----------

# MAGIC %md ## This run's data-quality log

# COMMAND ----------

display(spark.sql(f"SELECT table_name, rule, rows_affected FROM {S}.dq_log WHERE run_ts = '{RUN_TS}' ORDER BY table_name"))

# COMMAND ----------

# MAGIC %md ## Silver-side §7.7 checkpoints (Gold-side checks run after dbt in `02_checkpoints`)

# COMMAND ----------

display(spark.sql(f"""
SELECT 'supplier current cost rows' AS check, CAST(COUNT(*) AS STRING) AS actual, '60' AS expected FROM (
  SELECT product_id FROM {S}.supplier_price_list
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY effective_date DESC) = 1)
UNION ALL SELECT 'supplier current cost sum', CAST(SUM(supplier_cost) AS STRING), '56871.53' FROM (
  SELECT supplier_cost FROM {S}.supplier_price_list
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY effective_date DESC) = 1)
UNION ALL SELECT 'marketing rows', CAST(COUNT(*) AS STRING), '3037' FROM {S}.marketing_spend
UNION ALL SELECT 'marketing spend', CAST(SUM(spend_amount) AS STRING), '8920850.16' FROM {S}.marketing_spend
UNION ALL SELECT 'marketing clicks', CAST(SUM(clicks) AS STRING), '3274996' FROM {S}.marketing_spend
UNION ALL SELECT 'clickstream quarantined', CAST(COUNT(*) AS STRING), '426' FROM {S}.web_events_quarantine
UNION ALL SELECT 'clickstream clean events', CAST(COUNT(*) AS STRING), '93436' FROM {S}.web_events
UNION ALL SELECT 'clickstream sessions', CAST(COUNT(DISTINCT session_id) AS STRING), '23193' FROM {S}.web_events
UNION ALL SELECT 'customers (excl. test)', CAST(COUNT(*) AS STRING), '1000' FROM {S}.customers WHERE NOT is_test
UNION ALL SELECT 'distinct cities', CAST(COUNT(DISTINCT city) AS STRING), '10' FROM {S}.customers WHERE NOT is_test
UNION ALL SELECT 'products / categories', COUNT(*) || ' / ' || COUNT(DISTINCT category), '60 / 6' FROM {S}.products
"""))
