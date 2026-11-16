# Databricks notebook source
# MAGIC %md ## a10 · Gold top 3 films per category, per month
# MAGIC **Reads** a7_silver.rental, inventory, films · **Writes** a7_gold.top_films
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
counts = (rent.join(inv.select("inventory_id", "film_id"), "inventory_id")
              .join(films.select("film_id", "title", "category"), "film_id")
              .withColumn("month", F.date_format("rental_date", "yyyy-MM"))
              .groupBy("category", "month", "film_id", "title").agg(F.count("*").alias("rentals")))
# FIX (drop 1, silent logic bug): rank within each category and month
w = Window.partitionBy("category", "month").orderBy(F.desc("rentals"), "film_id")
top = counts.withColumn("rn", F.row_number().over(w)).filter("rn <= 3")
top.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.top_films")
# COMMAND ----------
# CHECK: this cell decides green or red
n = spark.table(f"{GOLD}.top_films").count()
assert n == int(CTRL["top_films_rows"]), f"expected {CTRL['top_films_rows']} rows (top 3 per category per month), got {n}"
print("PASS a10_gold_top_films:", n)
