# Databricks notebook source
# MAGIC %md ## e3 · Silver Enriched: inventory copy history, SCD Type 2
# MAGIC Silver keeps only today's version. **Silver Enriched keeps every version**: every time a copy **moved to the other store**.
# MAGIC
# MAGIC One row per version: `version`, `valid_from`, `valid_to` (empty while it is still true) and `is_current`.
# MAGIC This job **rebuilds** the history from the change log each run: sort each inventory copy's rows by date, keep a row only when a tracked column changed, and let each version end the day before the next one starts.
# MAGIC **Writes** a7_silver_enriched.inventory_scd2
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
# SCD Type 2 rebuilt from the change log: every version of every inventory copie, with the dates it was true.
HISTORY_START = "2005-01-01"      # version 1 is valid from the start of the history
KEY = "inventory_id"
TRACKED = ["store_id"]             # a change in any of these columns starts a new version

log = (with_changes("inventory", "inventory")
       .select(F.col("inventory_id").cast("int").alias("inventory_id"), F.col("film_id").cast("int").alias("film_id"),
               F.col("store_id").cast("int").alias("store_id"), F.to_date(F.substring("last_update", 1, 10)).alias("effective_from")))

w = Window.partitionBy(KEY).orderBy("effective_from")
fingerprint = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in TRACKED]), 256)
history = (log
    .withColumn("fp", fingerprint)
    .withColumn("prev_fp", F.lag("fp").over(w))
    .filter(F.col("prev_fp").isNull() | (F.col("fp") != F.col("prev_fp")))      # keep a row only if something tracked changed
    .withColumn("version", F.row_number().over(w))
    .withColumn("next_from", F.lead("effective_from").over(w))
    .withColumn("valid_from", F.when(F.col("version") == 1, F.to_date(F.lit(HISTORY_START))).otherwise(F.col("effective_from")))
    .withColumn("valid_to", F.date_sub(F.col("next_from"), 1))                       # the day before the next version starts
    .withColumn("is_current", F.col("next_from").isNull())
    .drop("fp", "prev_fp", "next_from", "effective_from"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ENRICHED}")
history.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ENRICHED}.inventory_scd2")
h = spark.table(f"{ENRICHED}.inventory_scd2")
print(h.count(), "versions of", h.select(KEY).distinct().count(), "inventory copies;", h.filter("version > 1").select(KEY).distinct().count(), "with a history")
# COMMAND ----------
# CHECK: this cell decides green or red
h = spark.table(f"{ENRICHED}.inventory_scd2")
cur = h.filter("is_current")
assert cur.count() == cur.select(KEY).distinct().count() == int(CTRL["inventory"]), \
    f"expected exactly one current version for each of {CTRL['inventory']} inventory copies, got {cur.count()} current rows"
nxt = h.select(KEY, (F.col("version") - 1).alias("version"), F.col("valid_from").alias("next_valid_from"))
overlaps = h.join(nxt, [KEY, "version"]).filter(F.col("valid_to") >= F.col("next_valid_from")).count()
assert overlaps == 0, f"{overlaps} versions overlap the next version: on the boundary day a point-in-time query finds TWO versions"
assert h.count() == int(CTRL["inventory_versions"]), f"expected {int(CTRL['inventory_versions']):,} versions in total, got {h.count():,}"
print("PASS e3_enriched_inventory_scd2:", h.count(), "versions")
