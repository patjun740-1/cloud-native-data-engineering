# Cloud-Native Data Engineering Portfolio

**JungYoung (Patrick) Jun** · M.S. Applied Data Science, University of Chicago · [LinkedIn](https://www.linkedin.com/in/jungyoung-patrick-jun)

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD4)
![Apache Spark](https://img.shields.io/badge/Spark%20SQL-E25A1C?logo=apachespark&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?logo=postgresql&logoColor=white)

End-to-end data engineering projects built on the Sakila rental dataset in UChicago's **ADSP 31011 Cloud-Native Data Engineering** (Autumn 2026): from raw CSVs to a medallion lakehouse, a star-schema warehouse, and a guard-railed natural-language analytics app that exposes the warehouse to AI agents through an MCP server.

Every number below was verified against the source data, not just produced by a query.

## Projects

| Project | What it shows | Stack |
| --- | --- | --- |
| [**Databricks Medallion Lakehouse**](databricks-medallion-lakehouse/) | Bronze → Silver → Silver-Enriched → Gold → Platinum pipeline, incremental MERGE of a new month of data, deduplication, Delta history and time travel | Databricks, Delta Lake, Spark SQL, PySpark, Unity Catalog |
| [**Snowflake MCP Analytics App**](snowflake-mcp-analytics-app/) | Four bounded, parameterized tools on a Snowflake managed MCP server, plus a Streamlit app that answers plain-English questions with transparent SQL and query cost | Snowflake, Python stored procedures, MCP, Streamlit in Snowflake |

## Highlights

- **Incremental loading done right.** A March 2006 batch (2,575 rental rows, including 25 re-sent duplicates and 150 late returns) merged into Silver as exactly **2,400 inserts + 150 updates**, verified in Delta history and with `VERSION AS OF` time travel.
- **Caught a silent-failure risk.** The un-deduplicated MERGE failed on duplicates that matched existing rows, but 15 duplicates of brand-new rentals would have been inserted with *no error*. Deduping before every MERGE prevents both.
- **No join fan-out.** Payments are aggregated per rental before joining, so the enriched table keeps 16,044 = 16,044 rows and revenue reconciles to the cent ($67,406.56 vs. $67,416.51 total payments, with the $9.95 gap traced to 5 payments with no rental).
- **Safe AI access to a warehouse.** Instead of letting an agent run arbitrary SQL, the MCP server exposes four allow-listed tools with bound parameters, capped result sizes, explicit row grain, and honest `no_data` / "credits unavailable" responses.
- **Adversarial testing.** The app was attacked with vague, false-premise, missing-data, and prompt-injection questions; two failures were found, fixed, and retested.

## Repository layout

```
.
├── databricks-medallion-lakehouse/
│   ├── README.md
│   └── sakila_lakehouse.ipynb        # executed notebook with outputs
└── snowflake-mcp-analytics-app/
    ├── README.md
    ├── streamlit_app.py              # Streamlit in Snowflake app
    ├── .streamlit/secrets.toml.example
    └── sql/
        ├── snowflake_mcp_setup.sql   # tools, procedures, MCP server spec
        └── verification_queries.sql  # read-only baseline checks
```

No credentials are stored in this repository. Local connection settings belong in a git-ignored `.streamlit/secrets.toml`; see the example file.

## Dataset

[Sakila](https://dev.mysql.com/doc/sakila/en/) is MySQL's sample DVD-rental database: 16,044 rentals, 16,049 payments, 599 customers, 1,000 films and 16 categories, covering May 2005 to February 2006. Raw data files are not included here.

---
*Course scaffolding and datasets were provided by ADSP 31011; each project README states which parts are my own work.*
