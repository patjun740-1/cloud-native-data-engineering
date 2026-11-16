# EC1 (extra credit): the whole fleet as ONE Databricks job. Run in the deploy notebook after Steps 1-7.
# EC1: the whole fleet as ONE job, with each task waiting only for the jobs whose tables it reads
DEPS = {
    "a1_silver_customers": [], "a2_silver_films": [], "a3_silver_inventory": [],
    "a4_stream_rentals": [], "a5_stream_payments": [],
    "a6_silver_rentals": ["a4_stream_rentals"],
    "a7_silver_payments": ["a5_stream_payments"],
    "e1_enriched_customers_scd2": [], "e2_enriched_films_scd2": [], "e3_enriched_inventory_scd2": [],
    "a8_gold_revenue_by_month_store": ["a6_silver_rentals", "a7_silver_payments", "a3_silver_inventory"],
    "a9_gold_late_returns": ["a6_silver_rentals", "a3_silver_inventory", "a2_silver_films"],
    "a10_gold_top_films": ["a6_silver_rentals", "a3_silver_inventory", "a2_silver_films"],
    "a12_gold_customer_journeys": ["e1_enriched_customers_scd2", "a6_silver_rentals", "a7_silver_payments"],
    "a11_quality_gate": ["a1_silver_customers", "e2_enriched_films_scd2", "e3_enriched_inventory_scd2",
                         "a8_gold_revenue_by_month_store", "a9_gold_late_returns",
                         "a10_gold_top_films", "a12_gold_customer_journeys"],
}
assert set(DEPS) == set(PIPELINES)

PIPE_NAME = "A7 fleet pipeline (EC1)"
for old in w.jobs.list(name=PIPE_NAME):
    w.jobs.delete(job_id=old.job_id)

tasks = [jobs.Task(task_key=n,
                   notebook_task=jobs.NotebookTask(notebook_path=f"{FOLDER}/{n}", source=jobs.Source.WORKSPACE),
                   depends_on=[jobs.TaskDependency(task_key=d) for d in DEPS[n]],
                   max_retries=0)
         for n in PIPELINES]
pipe = w.jobs.create(name=PIPE_NAME, max_concurrent_runs=1, tasks=tasks,
                     tags={"course": "adsp31011", "lab": "a7", "ec": "1"})
print("created job", pipe.job_id)

wt = w.jobs.run_now(job_id=pipe.job_id)
run_id = wt.response.run_id if hasattr(wt, "response") else wt.run_id
_wait([run_id], timeout_min=40)
run = w.jobs.get_run(run_id=run_id)
print("result:", run.state.result_state.value if run.state.result_state else run.state.life_cycle_state.value)
print("run page:", run.run_page_url)
