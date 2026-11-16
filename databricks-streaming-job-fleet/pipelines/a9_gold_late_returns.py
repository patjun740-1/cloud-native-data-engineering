# Databricks notebook source
# MAGIC %md ## a9 · Gold late returns
# MAGIC Rentals that came back after the film's allowed rental period, and how late they were.
# MAGIC **Reads** a7_silver.rental, inventory, films · **Writes** a7_gold.late_returns
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
rent, inv, films = spark.table(f"{SILVER}.rental"), spark.table(f"{SILVER}.inventory"), spark.table(f"{SILVER}.films")
late = (rent.filter(F.col("return_date").isNotNull())   # FIX (drop 1, silent logic bug): != None is never true
            .join(inv.select("inventory_id", "film_id"), "inventory_id")
            .join(films.select("film_id", "title", "rental_duration"), "film_id")
            .withColumn("days_out", F.datediff("return_date", "rental_date"))
            .filter(F.col("days_out") > F.col("rental_duration"))
            .withColumn("days_late", F.col("days_out") - F.col("rental_duration")))
late.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.late_returns")
# COMMAND ----------
# CHECK: this cell decides green or red
n = spark.table(f"{GOLD}.late_returns").count()
assert n == int(CTRL["late_returns"]), f"expected {int(CTRL['late_returns']):,} late returns, got {n:,}"
print("PASS a9_gold_late_returns:", n)
