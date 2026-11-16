# Databricks notebook source
# MAGIC %md ## a6 · Silver rentals
# MAGIC Starts from the 2005-06 history, then MERGEs in every streamed rental: new rentals are inserted, and rentals that come back late (re-sent with a return date) update the row that is already there.
# MAGIC **Reads** a7_bronze.rental_raw, rental_stream · **Writes** a7_silver.rental
# COMMAND ----------
from pyspark.sql import functions as F, types as T, Window
CAT = "workspace"
BRONZE, SILVER, ENRICHED, GOLD = f"{CAT}.a7_bronze", f"{CAT}.a7_silver", f"{CAT}.a7_silver_enriched", f"{CAT}.a7_gold"
VOL = "/Volumes/workspace/a7_bronze/landing"
# control totals sent by the source systems with every drop: what the checks compare against
CTRL = {r.metric: r.value for r in spark.table(f"{BRONZE}.control_totals").collect()}
print("drops landed so far:", CTRL["drop"])

def with_changes(table, kind):
    """The Sakila table from Bronze plus every master-data change file that has landed for it (incoming/<kind>/). All strings."""
    base = spark.table(f"{BRONZE}.{table}_raw").drop("_source_file", "_ingested_at")
    changes = (spark.read.option("header", True).option("multiLine", True).option("escape", '"')
                    .schema(base.schema).csv(f"{VOL}/incoming/{kind}/"))
    return base.unionByName(changes)

def latest(df, key):
    """One row per key: the most recent version (largest last_update)."""
    w = Window.partitionBy(key).orderBy(F.desc("last_update"))
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
# COMMAND ----------
from delta.tables import DeltaTable
COLS = ["rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id"]

def parse_ts(c):
    # FIX (drop 3, schema/format change): the POS switched from yyyy-MM-dd to MM/dd/yyyy; accept both
    return F.coalesce(F.try_to_timestamp(F.col(c), F.lit("yyyy-MM-dd HH:mm:ss")),
                      F.try_to_timestamp(F.col(c), F.lit("MM/dd/yyyy HH:mm:ss")))

def typed(df):
    return df.select(F.col("rental_id").cast("int").alias("rental_id"), parse_ts("rental_date").alias("rental_date"),
                     F.col("inventory_id").cast("int").alias("inventory_id"), F.col("customer_id").cast("int").alias("customer_id"),
                     parse_ts("return_date").alias("return_date"), F.col("staff_id").cast("int").alias("staff_id"))

typed(spark.table(f"{BRONZE}.rental_raw").select(*COLS)) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.rental")      # history first

# FIX (drop 1, duplicates): keep one row per rental_id, the most recent by last_update, before the MERGE
# FIX (drop 3): sort by the PARSED last_update, not the string ("05/..." sorts below "2006-..." as text)
s = spark.table(f"{BRONZE}.rental_stream").withColumn("last_update", parse_ts("last_update"))
updates = typed(latest(s, "rental_id").select(*COLS))
(DeltaTable.forName(spark, f"{SILVER}.rental").alias("t")
   .merge(updates.alias("s"), "t.rental_id = s.rental_id")
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll()
   .execute())
# COMMAND ----------
# CHECK: this cell decides green or red
r = spark.table(f"{SILVER}.rental")
n, ids = r.count(), r.select("rental_id").distinct().count()
open_now = r.filter(F.col("return_date").isNull()).count()
assert n == ids, f"{n - ids:,} rental_ids appear more than once in Silver"
assert n == int(CTRL["rentals"]), f"expected {int(CTRL['rentals']):,} rentals, got {n:,}"
assert open_now == int(CTRL["open_rentals"]), f"expected {CTRL['open_rentals']} rentals still out, got {open_now}"
print("PASS a6_silver_rentals:", n, "rentals,", open_now, "still out")
