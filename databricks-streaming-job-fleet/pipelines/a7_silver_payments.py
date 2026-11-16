# Databricks notebook source
# MAGIC %md ## a7 · Silver payments
# MAGIC History plus every streamed payment, with real types.
# MAGIC **Reads** a7_bronze.payment_raw, payment_stream · **Writes** a7_silver.payment
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
COLS = ["payment_id", "customer_id", "staff_id", "rental_id", "amount", "payment_date"]
pay = (spark.table(f"{BRONZE}.payment_raw").select(*COLS)
       .unionByName(spark.table(f"{BRONZE}.payment_stream").select(*COLS))
       .select(F.col("payment_id").cast("int").alias("payment_id"), F.col("customer_id").cast("int").alias("customer_id"),
               F.col("rental_id").cast("int").alias("rental_id"),
               F.col("amount").cast("decimal(5,2)").alias("amount"),   # FIX (drop 1, code error): money has cents
               F.to_timestamp("payment_date").alias("payment_date")))
pay.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.payment")
# COMMAND ----------
# CHECK: this cell decides green or red
p = spark.table(f"{SILVER}.payment")
n, total = p.count(), float(p.agg(F.sum("amount")).first()[0])
assert n == int(CTRL["payments"]), f"expected {int(CTRL['payments']):,} payments, got {n:,}"
assert abs(total - float(CTRL["revenue"])) < 0.01, f"payments total {total:,.2f}, expected {float(CTRL['revenue']):,.2f}"
print("PASS a7_silver_payments:", n, f"{total:,.2f}")
