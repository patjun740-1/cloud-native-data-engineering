# Databricks notebook source
# MAGIC %md ## a2 · Silver films
# MAGIC Each film **as it is today** (latest price), with its category and rental terms.
# MAGIC **Reads** a7_bronze.film_raw + incoming/films, film_category_raw, category_raw · **Writes** a7_silver.films
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
film = latest(with_changes("film", "films"), "film_id").select(F.col("film_id").cast("int").alias("film_id"), "title", "rating",
        F.col("rental_duration").cast("int").alias("rental_duration"), F.col("rental_rate").cast("decimal(4,2)").alias("rental_rate"))
fc = spark.table(f"{BRONZE}.film_category_raw").select(F.col("film_id").cast("int").alias("film_id"), F.col("category_id").cast("int").alias("category_id"))
cat = spark.table(f"{BRONZE}.category_raw").select(F.col("category_id").cast("int").alias("category_id"), F.col("name").alias("category"))
films = film.join(fc, "film_id").join(cat, "category_id").drop("category_id")
films.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.films")
# COMMAND ----------
# CHECK: this cell decides green or red
n = spark.table(f"{SILVER}.films").count()
assert n == int(CTRL["films"]), f"expected {CTRL['films']} films, got {n}"
print("PASS a2_silver_films:", n)
