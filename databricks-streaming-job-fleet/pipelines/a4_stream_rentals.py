# Databricks notebook source
# MAGIC %md ## a4 · Stream rentals into Bronze
# MAGIC A Structured Streaming job. Each run reads the rental files that have landed since the last run, and appends them to Bronze.
# MAGIC **Reads** the volume folder incoming/rentals · **Writes** a7_bronze.rental_stream (append)
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
import uuid
SCHEMA = "rental_id STRING, rental_date STRING, inventory_id STRING, customer_id STRING, return_date STRING, staff_id STRING, last_update STRING"
CHECKPOINT = f"{VOL}/_checkpoints/rentals"   # FIX (drop 1 second run, duplicates): one fixed checkpoint, not a new uuid per run
q = (spark.readStream.format("csv").option("header", True).schema(SCHEMA)
       .load(f"{VOL}/incoming/rentals/")                                     # every file that has landed so far
       .withColumn("_source_file", F.col("_metadata.file_name"))
       .withColumn("_ingested_at", F.current_timestamp())
       .writeStream.format("delta")
       .option("checkpointLocation", CHECKPOINT)                         # the checkpoint remembers which files are done
       .trigger(availableNow=True)                                       # process what is there now, then stop
       .toTable(f"{BRONZE}.rental_stream"))
q.awaitTermination()
print("files processed this run:", sum((p["numInputRows"] or 0) for p in q.recentProgress), "rows")
# COMMAND ----------
# CHECK: this cell decides green or red
n = spark.table(f"{BRONZE}.rental_stream").count()
assert n == int(CTRL["rentals_landed"]), f"Bronze has {n:,} streamed rental rows, but {int(CTRL['rentals_landed']):,} have landed"
print("PASS a4_stream_rentals:", n)
