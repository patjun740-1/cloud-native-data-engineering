# Fix log: 10 planted breaks across 9 jobs

Every red job, in the order it was found. "Kind of failure" uses seven categories: transient, cascade, code error, data error, schema or format change, duplicates or re-runs, silent logic bug. One extra platform issue (not a planted break) is listed separately at the end. The fixed notebooks are in [`pipelines/`](pipelines/).

| # | Job · drop where it broke | Error class or failed check · kind of failure | Cause, in one sentence | Fix (the line changed) |
|---|---|---|---|---|
| 1 | a1_silver_customers · 1 | `UNRESOLVED_USING_COLUMN_FOR_JOIN` (`city_id` not on the right side) · **code error** | The country table was joined on `city_id`, a column it does not have, instead of `country_id`. | `.join(country, "city_id")` → `.join(country, "country_id")` |
| 2 | a4_stream_rentals · 1 (second run) | CHECK: *Bronze has 30,122 streamed rental rows, but 15,061 have landed* · **duplicates or re-runs** | The checkpoint path contained a random `uuid`, so every run started a new checkpoint, re-read every landed file and appended it again. | `CHECKPOINT = f"{VOL}/_checkpoints/rentals_{uuid.uuid4().hex[:8]}"` → `CHECKPOINT = f"{VOL}/_checkpoints/rentals"` (then `reset_stream("rentals")` once) |
| 3 | a6_silver_rentals · 1 | CHECK: *61 rental_ids appear more than once in Silver* · **duplicates or re-runs** | The point-of-sale system's retries sent some rental rows twice (15,061 = 15,000 + 61), and the MERGE inserted both copies. | `updates = typed(spark.table(f"{BRONZE}.rental_stream").select(*COLS))` → `updates = typed(latest(spark.table(f"{BRONZE}.rental_stream"), "rental_id").select(*COLS))` |
| 4 | a6_silver_rentals · 3 | `CANNOT_PARSE_TIMESTAMP` (*Text '05/14/2006 22:33:00' could not be parsed*) · **schema or format change** | From drop 3 the point-of-sale system sends dates as `MM/dd/yyyy`, which the single-format `to_timestamp` could not parse, and `latest()` compared `last_update` as text. | `parse_ts` → `F.coalesce(F.try_to_timestamp(F.col(c), F.lit("yyyy-MM-dd HH:mm:ss")), F.try_to_timestamp(F.col(c), F.lit("MM/dd/yyyy HH:mm:ss")))`; `latest()` now orders by the parsed `last_update` |
| 5 | a7_silver_payments · 1 | `CAST_INVALID_INPUT` (*'2.99' cannot be cast to "INT"*) · **code error** | Payment amounts have cents but were cast to an integer. | `F.col("amount").cast("int")` → `F.col("amount").cast("decimal(5,2)")` |
| 6 | e1_enriched_customers_scd2 · 1 | CHECK: *expected 632 versions in total, got 626* · **silent logic bug** | `store_id` was missing from the tracked columns, so customers who only switched store got no new version. | `TRACKED = ["address_id", "active"]` → `TRACKED = ["address_id", "store_id", "active"]` |
| 7 | e2_enriched_films_scd2 · 1 | CHECK: *42 versions overlap the next version* · **silent logic bug** | `valid_to` was set to the next version's start date instead of the day before, so on the boundary day two versions matched. | `.withColumn("valid_to", F.col("next_from"))` → `.withColumn("valid_to", F.date_sub(F.col("next_from"), 1))` |
| 8 | a8_gold_revenue_by_month_store · 1 | `TABLE_OR_VIEW_NOT_FOUND a7_silver.payment` (cascade from a7), with a wrong join key underneath · **silent logic bug** | Payments were joined to rentals on `customer_id`, so each payment matched every rental that customer made, multiplying revenue. *(Fixed in the same pass as a7, so the inflated CHECK was not observed on its own.)* | `pay.join(rent.select("rental_id", "customer_id", "inventory_id"), "customer_id")` → `pay.join(rent.select("rental_id", "inventory_id"), "rental_id")` |
| 9 | a9_gold_late_returns · 1 | CHECK: *expected 13,695 late returns, got 0* · **silent logic bug** | `F.col("return_date") != None` compares with NULL, which is never true, so the filter dropped every row. | `.filter(F.col("return_date") != None)` → `.filter(F.col("return_date").isNotNull())` |
| 10 | a10_gold_top_films · 1 | CHECK: *expected 288 rows (top 3 per category per month), got 3* · **silent logic bug** | The ranking window had no `partitionBy`, so it ranked the whole table once instead of per category per month. | `Window.orderBy(F.desc("rentals"), "film_id")` → `Window.partitionBy("category", "month").orderBy(F.desc("rentals"), "film_id")` |

## Extra issue (not one of the 10 planted breaks)

| Job · drop | Error · kind | Cause | Fix |
|---|---|---|---|
| a4_stream_rentals and a5_stream_payments · 1 | `TypeError: unsupported operand type(s) for +: 'int' and 'NoneType'` · **code error (serverless runtime)** | On serverless, at least one entry in `q.recentProgress` returned `None` for `numInputRows`, so the summary `print` failed after the stream had already written its data correctly. | `sum(p["numInputRows"] for p in q.recentProgress)` → `sum((p["numInputRows"] or 0) for p in q.recentProgress)` |

## Cascades (red, no code change)

- **Drop 1:** a12_gold_customer_journeys (`a7_silver.payment` missing; depends on e1 versions) and a11_quality_gate (red until every upstream job matched the control totals).
- **Drop 3:** a8, a9, a10, a12 and a11 went red only because a6 failed. They went green after the a6 fix, with no change to their code.

## Verification

- Every drop, 1 through 8, was 15 of 15 green on two consecutive `run_fleet()` runs. Drops 1 to 3 were run by hand; drops 4 to 8 ran in a loop that landed each drop and ran the fleet twice (46 minutes, never stopped).
- Final control totals after drop 8 all matched: 136,044 rentals, 1,046 still out, 136,049 payments, revenue 606,116.51, Gold revenue 606,106.56, 796 customer versions, 1,289 film versions.
- No CHECK cell was changed.
