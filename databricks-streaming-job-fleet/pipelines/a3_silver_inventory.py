# Databricks notebook source
# MAGIC %md ## a3 · Silver inventory
# MAGIC Every physical copy of a film, and the store that owns it **today** (copies move between stores).
# MAGIC **Reads** a7_bronze.inventory_raw + incoming/inventory · **Writes** a7_silver.inventory
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
inv = latest(with_changes("inventory", "inventory"), "inventory_id").select(F.col("inventory_id").cast("int").alias("inventory_id"),
        F.col("film_id").cast("int").alias("film_id"), F.col("store_id").cast("int").alias("store_id"))
inv.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.inventory")
# COMMAND ----------
# CHECK: this cell decides green or red
n = spark.table(f"{SILVER}.inventory").count()
assert n == int(CTRL["inventory"]), f"expected {CTRL['inventory']} copies, got {n}"
print("PASS a3_silver_inventory:", n)
