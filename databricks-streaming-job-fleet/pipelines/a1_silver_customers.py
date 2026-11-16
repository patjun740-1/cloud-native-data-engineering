# Databricks notebook source
# MAGIC %md ## a1 · Silver customers
# MAGIC Each customer **as they are today** (the latest version, after the head office's changes), with their store, city and country (customer -> address -> city -> country).
# MAGIC **Reads** a7_bronze.customer_raw + incoming/customers, address_raw, city_raw, country_raw · **Writes** a7_silver.customers
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
cust = latest(with_changes("customer", "customers"), "customer_id").select(F.col("customer_id").cast("int").alias("customer_id"),
        F.col("store_id").cast("int").alias("store_id"), "first_name", "last_name", F.col("address_id").cast("int").alias("address_id"),
        F.col("active").cast("int").alias("active"))
addr = spark.table(f"{BRONZE}.address_raw").select(F.col("address_id").cast("int").alias("address_id"), F.col("city_id").cast("int").alias("city_id"))
city = spark.table(f"{BRONZE}.city_raw").select(F.col("city_id").cast("int").alias("city_id"), "city", F.col("country_id").cast("int").alias("country_id"))
country = spark.table(f"{BRONZE}.country_raw").select(F.col("country_id").cast("int").alias("country_id"), "country")

customers = (cust.join(addr, "address_id")
                 .join(city, "city_id")
                 .join(country, "country_id")   # FIX (drop 1, code error): join country on country_id
                 .select("customer_id", "store_id", "first_name", "last_name", "active", "city", "country"))
customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.customers")
# COMMAND ----------
# CHECK: this cell decides green or red
n = spark.table(f"{SILVER}.customers").count()
assert n == int(CTRL["customers"]), f"expected {CTRL['customers']} customers, got {n}"
print("PASS a1_silver_customers:", n)
