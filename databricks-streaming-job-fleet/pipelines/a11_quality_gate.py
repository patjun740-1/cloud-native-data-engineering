# Databricks notebook source
# MAGIC %md ## a11 · Quality gate
# MAGIC Runs last. Every Silver and Gold table against the control totals for the drops landed so far.
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
checks = [
    (f"{SILVER}.customers", None, "customers"), (f"{SILVER}.films", None, "films"), (f"{SILVER}.inventory", None, "inventory"),
    (f"{BRONZE}.rental_stream", None, "rentals_landed"), (f"{BRONZE}.payment_stream", None, "payments_landed"),
    (f"{SILVER}.rental", None, "rentals"), (f"{SILVER}.payment", None, "payments"),
    (f"{GOLD}.late_returns", None, "late_returns"), (f"{GOLD}.top_films", None, "top_films_rows"),
    (f"{ENRICHED}.customers_scd2", None, "customer_versions"), (f"{ENRICHED}.films_scd2", None, "film_versions"),
    (f"{ENRICHED}.inventory_scd2", None, "inventory_versions"), (f"{GOLD}.customer_journeys", None, "customer_versions")]
problems = []
for table, _, metric in checks:
    if not spark.catalog.tableExists(table):
        problems.append(f"{table}: missing"); continue
    got, want = spark.table(table).count(), int(CTRL[metric])
    print(f"{table:34s} {got:>9,}  (expect {want:,})")
    if got != want:
        problems.append(f"{table}: {got:,} rows, expected {want:,}")
for table, col, metric in [(f"{SILVER}.payment", "amount", "revenue"), (f"{GOLD}.revenue_by_month_store", "revenue", "gold_revenue")]:
    if spark.catalog.tableExists(table):
        got = float(spark.table(table).agg(F.sum(col)).first()[0] or 0)
        if abs(got - float(CTRL[metric])) > 0.01:
            problems.append(f"{table} totals {got:,.2f}, expected {float(CTRL[metric]):,.2f}")
# COMMAND ----------
# CHECK: this cell decides green or red
assert not problems, "QUALITY GATE FAILED:\n  " + "\n  ".join(problems)
print(f"PASS a11_quality_gate: ALL GREEN after {CTRL['drop']} drop(s)")
