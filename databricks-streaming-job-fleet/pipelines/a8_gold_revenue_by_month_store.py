# Databricks notebook source
# MAGIC %md ## a8 · Gold revenue by month and store
# MAGIC Each payment, matched to the rental it paid for, the copy that was rented, and the store that owns the copy.
# MAGIC **Reads** a7_silver.payment, rental, inventory · **Writes** a7_gold.revenue_by_month_store
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
pay, rent, inv = spark.table(f"{SILVER}.payment"), spark.table(f"{SILVER}.rental"), spark.table(f"{SILVER}.inventory")
# FIX (drop 1, silent logic bug): join each payment to ITS rental (rental_id), not every rental of the customer
rev = (pay.join(rent.select("rental_id", "inventory_id"), "rental_id")
          .join(inv.select("inventory_id", "store_id"), "inventory_id")
          .withColumn("month", F.date_format("payment_date", "yyyy-MM"))
          .groupBy("month", "store_id")
          .agg(F.sum("amount").alias("revenue"), F.count("*").alias("payments")))
rev.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.revenue_by_month_store")
# COMMAND ----------
# CHECK: this cell decides green or red
g = spark.table(f"{GOLD}.revenue_by_month_store")
total = float(g.agg(F.sum("revenue")).first()[0])
want = float(CTRL["gold_revenue"])
assert abs(total - want) < 0.01, f"revenue {total:,.2f}, expected {want:,.2f} ({total / want:.1f}x)"
print("PASS a8_gold_revenue_by_month_store:", g.count(), "rows,", f"{total:,.2f}")
