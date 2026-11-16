# Databricks notebook source
# MAGIC %md ## a12 · Gold customer journeys
# MAGIC One row per **version** of each customer: where they lived and which store they used during that version, and how many rentals and how much they paid while it was true.
# MAGIC This is a **point-in-time join**: a rental belongs to the version whose `valid_from`..`valid_to` contains the rental date.
# MAGIC **Reads** a7_silver_enriched.customers_scd2, a7_silver.rental, a7_silver.payment · **Writes** a7_gold.customer_journeys
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
hist = spark.table(f"{ENRICHED}.customers_scd2").alias("h")
rent = (spark.table(f"{SILVER}.rental").withColumn("rental_day", F.to_date("rental_date"))
        .join(spark.table(f"{SILVER}.payment").groupBy("rental_id").agg(F.sum("amount").alias("paid")), "rental_id", "left").alias("r"))
in_version = ((F.col("r.customer_id") == F.col("h.customer_id")) & (F.col("r.rental_day") >= F.col("h.valid_from"))
              & (F.col("h.valid_to").isNull() | (F.col("r.rental_day") <= F.col("h.valid_to"))))
journeys = (hist.join(rent, in_version, "left")
    .groupBy("h.customer_id", "h.version", "h.valid_from", "h.valid_to", "h.is_current", "h.store_id", "h.city", "h.country", "h.active")
    .agg(F.count("r.rental_id").alias("rentals"), F.coalesce(F.sum("r.paid"), F.lit(0)).alias("paid"),
         F.min("r.rental_day").alias("first_rental"), F.max("r.rental_day").alias("last_rental")))
journeys.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.customer_journeys")
# COMMAND ----------
# CHECK: this cell decides green or red
j = spark.table(f"{GOLD}.customer_journeys")
rentals = j.agg(F.sum("rentals")).first()[0]
assert j.count() == int(CTRL["customer_versions"]), f"expected one row per customer version ({CTRL['customer_versions']}), got {j.count()}"
assert rentals == int(CTRL["rentals"]), f"every rental should land in exactly one version: {rentals:,} placed, {int(CTRL['rentals']):,} rentals"
print("PASS a12_gold_customer_journeys:", j.count(), "versions,", rentals, "rentals placed")
