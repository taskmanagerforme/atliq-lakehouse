# Databricks notebook source
# MAGIC %md
# MAGIC # AtliQ Phase 2 (LEARNER STARTER) — Kafka → Delta with Structured Streaming
# MAGIC Complete the TODOs to build Bronze → Silver → Gold as **streams**.
# MAGIC
# MAGIC **Free Edition (serverless) rules:** checkpoints go in a Unity Catalog
# MAGIC **Volume** (no DBFS), streams write to **managed tables**, and every stream
# MAGIC needs its **own** checkpoint folder.
# MAGIC
# MAGIC **Triggers on serverless:** always-on (processing-time) triggers are not
# MAGIC available, so every stream uses `.trigger(availableNow=True)`: it processes
# MAGIC everything new since its checkpoint, then stops. Follow each `.toTable(...)`
# MAGIC with `query.awaitTermination()`. While the producer runs, re-run the notebook
# MAGIC (or schedule it as a job every few minutes); the checkpoints make each run
# MAGIC pick up exactly where the last one stopped.
# MAGIC
# MAGIC **Running on Azure Databricks instead (guide section 3.1)?** Use
# MAGIC `.trigger(processingTime="30 seconds")` and do **not** call
# MAGIC `awaitTermination()`: the streams keep running while the producer sends events.
# MAGIC When you finish, stop them with `for q in spark.streams.active: q.stop()` and
# MAGIC terminate the cluster.

# COMMAND ----------
# MAGIC %run ./00_config

# COMMAND ----------
# KAFKA_BOOTSTRAP, KAFKA_API_KEY and KAFKA_API_SECRET come from the 00_config notebook
# (%run above), so no credentials live in this notebook. See 00_config.example.py.
TOPIC = "atliq.orders.events"

CATALOG, SCHEMA = "atliq", "streaming"
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# One-time setup (given):
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# COMMAND ----------
# MAGIC %md ## TASK 1 — Bronze: raw events off Kafka, no parsing
# MAGIC Read the topic with `spark.readStream.format("kafka")` and append the raw
# MAGIC records to `atliq.streaming.bronze_order_events`.
# MAGIC
# MAGIC Hints:
# MAGIC - Options you need: `kafka.bootstrap.servers`, `subscribe`,
# MAGIC   `startingOffsets = earliest`, `kafka.security.protocol = SASL_SSL`,
# MAGIC   `kafka.sasl.mechanism = PLAIN`, and `kafka.sasl.jaas.config`
# MAGIC   (on Databricks the login module class is
# MAGIC   `kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule`).
# MAGIC - Kafka gives you binary key/value — CAST both to STRING.
# MAGIC - Keep topic, partition, offset, timestamp columns too. Bronze keeps everything.
# MAGIC - writeStream: outputMode "append", `.trigger(availableNow=True)` (Azure: see above),
# MAGIC   checkpointLocation f"{CKPT}/bronze", .toTable(...), then
# MAGIC   `query.awaitTermination()`

# COMMAND ----------
# Bronze: raw Kafka records, no parsing. Bronze keeps everything.
from pyspark.sql import functions as F

BRONZE = f"{CATALOG}.{SCHEMA}.bronze_order_events"
JAAS = ('kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{KAFKA_API_KEY}" password="{KAFKA_API_SECRET}";')

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .option("kafka.security.protocol", "SASL_SSL")
       .option("kafka.sasl.mechanism", "PLAIN")
       .option("kafka.sasl.jaas.config", JAAS)
       .load())

bronze = raw.select(
    F.col("key").cast("string").alias("key"),
    F.col("value").cast("string").alias("value"),
    "topic", "partition", "offset",
    F.col("timestamp").alias("kafka_ts"),
    F.current_timestamp().alias("ingested_at"),
)

q = (bronze.writeStream
     .outputMode("append")
     .option("checkpointLocation", f"{CKPT}/bronze")
     .trigger(availableNow=True)
     .toTable(BRONZE))
q.awaitTermination()
print("bronze rows:", spark.table(BRONZE).count())


# COMMAND ----------
# MAGIC %md ## TASK 2 — Silver: parse, de-duplicate, handle late data
# MAGIC Stream FROM the Bronze table into `atliq.streaming.silver_order_events`:
# MAGIC 1. Parse the JSON value with an explicit schema (event_id, event_type,
# MAGIC    event_ts, order_id, customer_id, city, product_id, quantity,
# MAGIC    order_amount, payment_method).
# MAGIC 2. Convert event_ts to a real timestamp.
# MAGIC 3. The producer sends a few broken records on purpose: from_json returns NULL for
# MAGIC    fields it cannot read. Drop (or write to a quarantine table) rows where event_id,
# MAGIC    event_ts or order_amount is NULL, **before** the watermark.
# MAGIC 4. Add a **10-minute watermark** on event_ts, then
# MAGIC    **dropDuplicates(["event_id", "event_ts"])** — so a replayed event can never land twice.
# MAGIC    Including the event-time column lets Spark forget old event_ids once the
# MAGIC    watermark passes them, so the de-dup state stays small.
# MAGIC    (`dropDuplicatesWithinWatermark(["event_id"])` is also accepted.)
# MAGIC
# MAGIC Hint: `spark.readStream.table(...)`, `F.from_json`, `withWatermark`,
# MAGIC `.trigger(availableNow=True)`, checkpoint f"{CKPT}/silver".

# COMMAND ----------
# Silver: explicit schema, broken records quarantined, 10-min watermark, de-dup on event_id.
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType

SILVER = f"{CATALOG}.{SCHEMA}.silver_order_events"
QUARANTINE = f"{CATALOG}.{SCHEMA}.silver_quarantine"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),      # parsed to a timestamp below
    StructField("order_id", LongType()),
    StructField("customer_id", LongType()),
    StructField("city", StringType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("order_amount", DoubleType()),  # "N/A" cannot be read -> NULL
    StructField("payment_method", StringType()),
])

parsed = (spark.readStream.table(BRONZE)
          .withColumn("e", F.from_json("value", EVENT_SCHEMA))
          .select("e.*", "value", "partition", "offset", "kafka_ts")
          .withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd'T'HH:mm:ss.SSSX")))

is_valid = (F.col("event_id").isNotNull() & F.col("event_ts").isNotNull()
            & F.col("order_amount").isNotNull())

# Broken records go to a quarantine table (own checkpoint), so we can count what was dropped.
q_bad = (parsed.filter(~is_valid)
         .select("value", "partition", "offset", "kafka_ts",
                 F.when(F.col("event_id").isNull(), "unparseable / missing event_id")
                  .when(F.col("event_ts").isNull(), "missing/bad event_ts")
                  .otherwise("missing/bad order_amount").alias("reason"))
         .writeStream.outputMode("append")
         .option("checkpointLocation", f"{CKPT}/silver_quarantine")
         .trigger(availableNow=True)
         .toTable(QUARANTINE))

# Valid records: drop NULLs BEFORE the watermark, then de-duplicate replays.
silver = (parsed.filter(is_valid).drop("value")
          .withWatermark("event_ts", "10 minutes")
          .dropDuplicates(["event_id", "event_ts"]))

q = (silver.writeStream.outputMode("append")
     .option("checkpointLocation", f"{CKPT}/silver")
     .trigger(availableNow=True)
     .toTable(SILVER))
q.awaitTermination()
q_bad.awaitTermination()
print("silver rows:", spark.table(SILVER).count(), "| quarantined:", spark.table(QUARANTINE).count())


# COMMAND ----------
# MAGIC %md ## TASK 3 — Gold: the live revenue ticker
# MAGIC From the Silver stream, keep only `payment_received` events and aggregate
# MAGIC into **5-minute tumbling windows**: orders_paid = count, revenue = sum of
# MAGIC order_amount. Append closed windows to `atliq.streaming.gold_revenue_5min`.
# MAGIC
# MAGIC Hints:
# MAGIC - A stream read from a table does **not** inherit Silver's watermark.
# MAGIC   Declare `withWatermark("event_ts", "10 minutes")` again before the window,
# MAGIC   or the append-mode aggregation will refuse to start.
# MAGIC - `F.window("event_ts", "5 minutes")` returns a struct. Flatten it:
# MAGIC   `F.col("window.start").alias("window_start")`,
# MAGIC   `F.col("window.end").alias("window_end")` (the Verify query uses `window_start`).
# MAGIC - `.trigger(availableNow=True)`, checkpoint f"{CKPT}/gold".
# MAGIC - Think about WHY a window only appears after the watermark passes its end
# MAGIC   (you will explain this in your write-up). With availableNow, the newest
# MAGIC   windows usually appear on a later run, once newer events move the watermark.

# COMMAND ----------
# Gold: 5-minute tumbling windows over payment_received (watermark re-declared).
GOLD = f"{CATALOG}.{SCHEMA}.gold_revenue_5min"

gold = (spark.readStream.table(SILVER)
        .filter(F.col("event_type") == "payment_received")
        .withWatermark("event_ts", "10 minutes")       # NOT inherited from Silver
        .groupBy(F.window("event_ts", "5 minutes"))
        .agg(F.count("*").alias("orders_paid"),
             F.round(F.sum("order_amount"), 2).alias("revenue"))
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "orders_paid", "revenue"))

q = (gold.writeStream.outputMode("append")
     .option("checkpointLocation", f"{CKPT}/gold")
     .trigger(availableNow=True)
     .toTable(GOLD))
q.awaitTermination()
print("gold windows:", spark.table(GOLD).count())


# COMMAND ----------
# MAGIC %md ## Verify (given)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT event_type, COUNT(*) AS events
# MAGIC FROM atliq.streaming.silver_order_events GROUP BY event_type;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM atliq.streaming.gold_revenue_5min ORDER BY window_start DESC LIMIT 12;

# COMMAND ----------
# MAGIC %md ## Evidence for the write-up: what did Silver drop?

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM atliq.streaming.bronze_order_events)                AS bronze_rows,
# MAGIC   (SELECT COUNT(*) FROM atliq.streaming.silver_quarantine)                  AS broken_quarantined,
# MAGIC   (SELECT COUNT(*) FROM atliq.streaming.silver_order_events)                AS silver_rows,
# MAGIC   (SELECT COUNT(*) - COUNT(DISTINCT get_json_object(value, '$.event_id'))
# MAGIC      FROM atliq.streaming.bronze_order_events)                              AS replayed_duplicates_in_bronze;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT reason, COUNT(*) AS rows FROM atliq.streaming.silver_quarantine GROUP BY reason;
