-- ============================================================================
-- Sakila Analytics · Snowflake managed MCP server setup
--
-- Creates four bounded, read-only tools over the SAKILA.DW star schema and
-- registers them on SAKILA.MCP_APP.SAKILA_MCP_SERVER.
--
--   1. describe_schema           SQL function     table grains, keys, limitations
--   2. run_parameterized_query   Python procedure grouped revenue / payment counts
--   3. get_aggregate             Python procedure single-metric aggregates
--   4. get_query_cost            Python procedure elapsed time, warehouse, credits
--
-- Run top to bottom in a Snowsight worksheet. Requires the SAKILA database and
-- CLASS_WH warehouse (change below if needed). Development used an admin role;
-- in production, create objects with an admin role and serve the app/MCP server
-- through a least-privilege, read-only application role.
-- Baseline checks and smoke tests live in verification_queries.sql.
-- ============================================================================

USE ROLE ACCOUNTADMIN;      -- replace with a role that can create objects
USE WAREHOUSE CLASS_WH;
USE DATABASE SAKILA;

CREATE SCHEMA IF NOT EXISTS SAKILA.MCP_APP
COMMENT = 'Bounded analytics tools and MCP server for the Sakila warehouse';

-- ============================================================================
-- Tool 1: describe_schema
-- ============================================================================

CREATE OR REPLACE FUNCTION SAKILA.MCP_APP.DESCRIBE_SCHEMA()
RETURNS OBJECT
LANGUAGE SQL
COMMENT = 'Describes the Sakila dimensional warehouse, table grains, and relationships.'
AS
$$
    OBJECT_CONSTRUCT(
        'database', 'SAKILA',
        'schema', 'DW',
        'description', 'Sakila rental business payment analytics',

        'tables', ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT(
                'name', 'FACT_PAYMENT',
                'type', 'fact',
                'grain', 'One row per payment_id',
                'primary_key', 'PAYMENT_ID',
                'measure', 'AMOUNT'
            ),
            OBJECT_CONSTRUCT(
                'name', 'DIM_CUSTOMER',
                'type', 'SCD Type 2 dimension',
                'grain', 'One row per customer version',
                'primary_key', 'CUSTOMER_SK'
            ),
            OBJECT_CONSTRUCT(
                'name', 'DIM_FILM',
                'type', 'SCD Type 2 dimension',
                'grain', 'One row per film version',
                'primary_key', 'FILM_SK'
            ),
            OBJECT_CONSTRUCT(
                'name', 'DIM_DATE',
                'type', 'date dimension',
                'grain', 'One row per calendar day',
                'primary_key', 'DATE_KEY'
            ),
            OBJECT_CONSTRUCT(
                'name', 'DIM_STAFF',
                'type', 'Type 1 dimension',
                'grain', 'One row per staff member',
                'primary_key', 'STAFF_ID'
            ),
            OBJECT_CONSTRUCT(
                'name', 'DIM_STORE',
                'type', 'Type 1 dimension',
                'grain', 'One row per store',
                'primary_key', 'STORE_ID'
            )
        ),

        'important_notes', ARRAY_CONSTRUCT(
            'FACT_PAYMENT counts payments, not necessarily rental events.',
            'Join DIM_CUSTOMER and use COUNT(DISTINCT CUSTOMER_ID) for unique paying customers.',
            'Join SCD Type 2 dimensions using surrogate keys.',
            'The warehouse does not contain rental return dates.'
        )
    )
$$;

-- ============================================================================
-- Tool 2: run_parameterized_query
-- ============================================================================

CREATE OR REPLACE PROCEDURE SAKILA.MCP_APP.RUN_PARAMETERIZED_QUERY(
    START_DATE VARCHAR,
    END_DATE VARCHAR,
    CATEGORY VARCHAR,
    STORE_ID NUMBER,
    GROUP_BY VARCHAR,
    LIMIT_ROWS NUMBER
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Runs a bounded, read-only payment analytics query with validated filters.'
EXECUTE AS CALLER
AS
$$
from datetime import date
from decimal import Decimal
import re


def run(session, start_date, end_date, category,
        store_id, group_by, limit_rows):

    # 1. Validate dates
    def parse_date(value):
        if not isinstance(value, str) or not re.fullmatch(
            r"\d{4}-\d{2}-\d{2}", value
        ):
            raise ValueError("Dates must use YYYY-MM-DD format.")

        return date.fromisoformat(value)

    start = parse_date(start_date)
    end = parse_date(end_date)

    if start > end:
        raise ValueError("START_DATE cannot be after END_DATE.")

    # 2. Allow-list for GROUP BY
    group_options = {
        "category": "F.CATEGORY",
        "store": "P.STORE_ID",
        "month": "TO_CHAR(P.PAYMENT_TS, 'YYYY-MM')",
        "none": "'ALL'"
    }

    group_by = (group_by or "").lower().strip()

    if group_by not in group_options:
        raise ValueError(
            "GROUP_BY must be category, store, month, or none."
        )

    group_expression = group_options[group_by]

    # 3. Validate store
    if store_id is not None:
        if store_id not in (1, 2):
            raise ValueError("STORE_ID must be 1 or 2.")

    # 4. Validate and cap LIMIT
    if limit_rows is None:
        limit_rows = 20

    if limit_rows != int(limit_rows) or limit_rows < 1:
        raise ValueError(
            "LIMIT_ROWS must be a positive integer."
        )

    safe_limit = min(int(limit_rows), 50)

    # 5. Normalize category
    if category is not None:
        category = category.strip()

        if not category:
            category = None

    # 6. Build parameterized SQL
    sql = f"""
        SELECT
            {group_expression} AS GROUP_LABEL,
            ROUND(SUM(P.AMOUNT), 2) AS TOTAL_REVENUE,
            COUNT(*) AS PAYMENT_COUNT,
            COUNT(DISTINCT C.CUSTOMER_ID) AS UNIQUE_PAYING_CUSTOMERS
        FROM SAKILA.DW.FACT_PAYMENT P
        LEFT JOIN SAKILA.DW.DIM_CUSTOMER C
            ON P.CUSTOMER_SK = C.CUSTOMER_SK
        LEFT JOIN SAKILA.DW.DIM_FILM F
            ON P.FILM_SK = F.FILM_SK
        WHERE P.PAYMENT_TS >= TO_DATE(?)
          AND P.PAYMENT_TS < DATEADD(DAY, 1, TO_DATE(?))
    """

    params = [
        start.isoformat(),
        end.isoformat()
    ]

    # Optional category filter
    if category is not None:
        sql += " AND F.CATEGORY = ?"
        params.append(category)

    # Optional store filter
    if store_id is not None:
        sql += " AND P.STORE_ID = ?"
        params.append(int(store_id))

    # Grouping
    sql += f"""
        GROUP BY {group_expression}
        HAVING COUNT(*) > 0
    """

    # Ordering
    if group_by == "month":
        sql += " ORDER BY GROUP_LABEL"
    else:
        sql += " ORDER BY TOTAL_REVENUE DESC"

    # Apply validated LIMIT
    sql += f" LIMIT {safe_limit}"

    # 7. Execute query safely
    result = session.sql(
        sql,
        params=params
    ).collect()

    # 8. Capture query ID
    query_id = session.sql(
        "SELECT LAST_QUERY_ID(-1)"
    ).collect()[0][0]

    # 9. Format results
    output_rows = []

    for row in result:
        output_rows.append({
            "group": str(row["GROUP_LABEL"]),
            "total_revenue": float(
                row["TOTAL_REVENUE"] or Decimal("0")
            ),
            "payment_count": int(
                row["PAYMENT_COUNT"]
            ),
            "unique_paying_customers": int(
                row["UNIQUE_PAYING_CUSTOMERS"]
            )
        })

    # 10. Describe what each row represents
    grain = {
        "category": "One row per film category",
        "store": "One row per payment store",
        "month": "One row per payment month",
        "none": "One row for the selected period"
    }

    # 11. Return structured response
    return {
        "tool": "run_parameterized_query",
        "status": (
            "success" if output_rows else "no_data"
        ),
        "message": (
            "Query completed successfully."
            if output_rows
            else "No data found for the selected filters."
        ),
        "query_id": query_id,
        "sql": sql.strip(),
        "parameters": params,
        "row_count": len(output_rows),
        "row_grain": grain[group_by],
        "results": output_rows
    }
$$;

-- ============================================================================
-- Tool 3: get_aggregate
-- ============================================================================

CREATE OR REPLACE PROCEDURE SAKILA.MCP_APP.GET_AGGREGATE(
    START_DATE VARCHAR,
    END_DATE VARCHAR,
    METRIC VARCHAR,
    CATEGORY VARCHAR,
    STORE_ID NUMBER
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Returns a validated aggregate business metric from Sakila.'
EXECUTE AS CALLER
AS
$$
from datetime import date
from decimal import Decimal
import re


def run(session, start_date, end_date, metric,
        category, store_id):

    # 1. Validate dates
    def parse_date(value):
        if not isinstance(value, str) or not re.fullmatch(
            r"\d{4}-\d{2}-\d{2}", value
        ):
            raise ValueError("Dates must use YYYY-MM-DD format.")

        return date.fromisoformat(value)

    start = parse_date(start_date)
    end = parse_date(end_date)

    if start > end:
        raise ValueError("START_DATE cannot be after END_DATE.")

    # 2. Allow-list of metrics
    metric_options = {
        "total_revenue": "ROUND(SUM(P.AMOUNT), 2)",
        "payment_count": "COUNT(*)",
        "unique_paying_customers":
            "COUNT(DISTINCT C.CUSTOMER_ID)",
        "average_payment": "ROUND(AVG(P.AMOUNT), 2)"
    }

    metric = (metric or "").lower().strip()

    if metric not in metric_options:
        raise ValueError(
            "METRIC must be total_revenue, payment_count, "
            "unique_paying_customers, or average_payment."
        )

    metric_expression = metric_options[metric]

    # 3. Validate store
    if store_id is not None and store_id not in (1, 2):
        raise ValueError("STORE_ID must be 1 or 2.")

    # 4. Normalize category
    if category is not None:
        category = category.strip()

        if not category:
            category = None

    # 5. Construct parameterized SQL
    sql = f"""
        SELECT
            COUNT(*) AS MATCHING_PAYMENTS,
            {metric_expression} AS METRIC_VALUE
        FROM SAKILA.DW.FACT_PAYMENT P
        JOIN SAKILA.DW.DIM_CUSTOMER C
            ON P.CUSTOMER_SK = C.CUSTOMER_SK
        LEFT JOIN SAKILA.DW.DIM_FILM F
            ON P.FILM_SK = F.FILM_SK
        WHERE P.PAYMENT_TS >= TO_DATE(?)
          AND P.PAYMENT_TS < DATEADD(DAY, 1, TO_DATE(?))
    """

    params = [start.isoformat(), end.isoformat()]

    if category is not None:
        sql += " AND F.CATEGORY = ?"
        params.append(category)

    if store_id is not None:
        sql += " AND P.STORE_ID = ?"
        params.append(int(store_id))

    # 6. Execute SQL
    result = session.sql(
        sql,
        params=params
    ).collect()

    # 7. Capture query ID
    query_id = session.sql(
        "SELECT LAST_QUERY_ID(-1)"
    ).collect()[0][0]

    row = result[0]

    matching_payments = int(row["MATCHING_PAYMENTS"])

    # 8. Distinguish no data from zero
    if matching_payments == 0:
        return {
            "tool": "get_aggregate",
            "status": "no_data",
            "message": "No data found for the selected filters.",
            "metric": metric,
            "value": None,
            "query_id": query_id,
            "sql": sql.strip(),
            "parameters": params,
            "row_count": len(result),
            "row_grain": "One aggregate result for the selected period",
            "matching_payments": 0
        }

    # 9. Format metric value
    value = row["METRIC_VALUE"]

    if metric in ("total_revenue", "average_payment"):
        value = float(value)
    else:
        value = int(value)

    # 10. Return structured result
    return {
        "tool": "get_aggregate",
        "status": "success",
        "message": "Aggregate calculated successfully.",
        "metric": metric,
        "value": value,
        "query_id": query_id,
        "sql": sql.strip(),
        "parameters": params,
        "row_count": len(result),
        "row_grain": "One aggregate result for the selected period",
        "matching_payments": matching_payments
    }
$$;

-- ============================================================================
-- Tool 4: get_query_cost
-- ============================================================================

CREATE OR REPLACE PROCEDURE SAKILA.MCP_APP.GET_QUERY_COST(
    QUERY_ID VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Reports query elapsed time and available compute-credit attribution.'
EXECUTE AS CALLER
AS
$$
import re
from snowflake.snowpark.exceptions import SnowparkSQLException


def run(session, query_id):

    # 1. Validate query ID
    if not isinstance(query_id, str):
        raise ValueError("QUERY_ID must be a string.")

    query_id = query_id.strip()

    pattern = (
        r"[0-9a-fA-F]{8}-"
        r"[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{12}"
    )

    if not re.fullmatch(pattern, query_id):
        raise ValueError("Invalid Snowflake QUERY_ID format.")

    # 2. Retrieve execution metadata
    history_sql = """
        SELECT
            QUERY_ID,
            TOTAL_ELAPSED_TIME,
            WAREHOUSE_NAME,
            EXECUTION_STATUS,
            USER_NAME,
            CREDITS_USED_CLOUD_SERVICES
        FROM TABLE(
            SAKILA.INFORMATION_SCHEMA.QUERY_HISTORY(
                END_TIME_RANGE_START =>
                    DATEADD('day', -1, CURRENT_TIMESTAMP()),
                RESULT_LIMIT => 10000
            )
        )
        WHERE QUERY_ID = ?
          AND USER_NAME = CURRENT_USER()
    """

    history = session.sql(
        history_sql,
        params=[query_id]
    ).collect()

    # 3. Handle missing query history
    if not history:
        return {
            "tool": "get_query_cost",
            "status": "not_found",
            "query_id": query_id,
            "message": (
                "Query not found in the recent accessible history."
            ),
            "elapsed_ms": None,
            "cloud_services_credits": None,
            "credits_attributed": None,
            "row_count": 0,
            "row_grain": "One record per query",
            "sql": history_sql.strip(),
            "parameters": [query_id]
        }

    row = history[0]

    elapsed_ms = int(row["TOTAL_ELAPSED_TIME"])
    warehouse = row["WAREHOUSE_NAME"]
    execution_status = row["EXECUTION_STATUS"]
    cloud_services_credits = (
        float(row["CREDITS_USED_CLOUD_SERVICES"])
        if row["CREDITS_USED_CLOUD_SERVICES"] is not None
        else None
    )

    # 4. Look up compute-credit attribution
    credits_sql = """
        SELECT
            CREDITS_ATTRIBUTED_COMPUTE,
            CREDITS_USED_QUERY_ACCELERATION
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
        WHERE QUERY_ID = ?
          AND USER_NAME = CURRENT_USER()
          AND START_TIME >= DATEADD(
              'day', -2, CURRENT_TIMESTAMP()
          )
    """

    credits = None
    acceleration_credits = None
    cost_status = "pending_or_not_attributed"

    try:
        cost_rows = session.sql(
            credits_sql,
            params=[query_id]
        ).collect()

        if cost_rows:
            cost_row = cost_rows[0]

            if cost_row["CREDITS_ATTRIBUTED_COMPUTE"] is not None:
                credits = float(
                    cost_row["CREDITS_ATTRIBUTED_COMPUTE"]
                )
                cost_status = "available"

            if cost_row["CREDITS_USED_QUERY_ACCELERATION"] is not None:
                acceleration_credits = float(
                    cost_row["CREDITS_USED_QUERY_ACCELERATION"]
                )

    except SnowparkSQLException:
        cost_status = "credit_lookup_failed"

    # 5. Return structured metadata
    return {
        "tool": "get_query_cost",
        "status": "success",
        "query_id": query_id,
        "elapsed_ms": elapsed_ms,
        "warehouse": warehouse,
        "execution_status": execution_status,
        "cloud_services_credits": cloud_services_credits,
        "credits_attributed": credits,
        "acceleration_credits": acceleration_credits,
        "cost_status": cost_status,
        "message": (
            "Query history retrieved successfully."
        ),
        "row_count": 1,
        "row_grain": "One record per query",
        "sql": history_sql.strip(),
        "credit_sql": credits_sql.strip(),
        "parameters": [query_id]
    }
$$;

-- ============================================================================
-- Register all four tools on the managed MCP server
-- ============================================================================

CREATE OR REPLACE MCP SERVER SAKILA.MCP_APP.SAKILA_MCP_SERVER
FROM SPECIFICATION $$
tools:

  # Tool 1: Describe Schema
  - name: "describe_schema"
    title: "Describe Sakila Warehouse Schema"
    type: "GENERIC"
    identifier: "SAKILA.MCP_APP.DESCRIBE_SCHEMA"
    description: >
      Use when a user asks about warehouse tables,
      their grain, relationships, or data limitations.
      Returns schema metadata, not business metrics.
    annotations:
      readOnlyHint: true
      destructiveHint: false
      idempotentHint: true
      openWorldHint: false
    config:
      type: "function"
      warehouse: "CLASS_WH"
      input_schema:
        type: "object"
        properties: {}
        additionalProperties: false

  # Tool 2: Parameterized Query
  - name: "run_parameterized_query"
    title: "Query Sakila Revenue by Group"
    type: "GENERIC"
    identifier: "SAKILA.MCP_APP.RUN_PARAMETERIZED_QUERY"
    description: >
      Use for grouped payment analytics by film category,
      payment store, or month. Supports date, category,
      and store filters. Returns revenue, payment counts,
      and distinct paying customer counts. Do not use for
      rental duration or unsupported business questions.
      Never accept arbitrary SQL.
    annotations:
      readOnlyHint: true
      destructiveHint: false
      idempotentHint: true
      openWorldHint: false
    config:
      type: "procedure"
      warehouse: "CLASS_WH"
      query_timeout: 120
      input_schema:
        type: "object"
        properties:
          start_date:
            type: "string"
            format: "date"
            description: "Inclusive start date, YYYY-MM-DD."
          end_date:
            type: "string"
            format: "date"
            description: "Inclusive end date, YYYY-MM-DD."
          category:
            type: ["string", "null"]
            description: "Film category or null for all."
          store_id:
            type: ["integer", "null"]
            description: "Store ID 1 or 2, or null."
          group_by:
            type: "string"
            enum: ["category", "store", "month", "none"]
            description: "Approved grouping dimension."
          limit_rows:
            type: "integer"
            minimum: 1
            maximum: 50
            description: "Maximum result rows."
        required:
          - start_date
          - end_date
          - category
          - store_id
          - group_by
          - limit_rows
        additionalProperties: false

  # Tool 3: Get Aggregate
  - name: "get_aggregate"
    title: "Get Sakila Business Aggregate"
    type: "GENERIC"
    identifier: "SAKILA.MCP_APP.GET_AGGREGATE"
    description: >
      Use for a single business metric over a specified
      period, including total revenue, payment count,
      distinct paying customers, and average payment.
      Supports category and store filters. Returns
      no_data when no matching payments exist.
    annotations:
      readOnlyHint: true
      destructiveHint: false
      idempotentHint: true
      openWorldHint: false
    config:
      type: "procedure"
      warehouse: "CLASS_WH"
      query_timeout: 120
      input_schema:
        type: "object"
        properties:
          start_date:
            type: "string"
            format: "date"
            description: "Inclusive start date, YYYY-MM-DD."
          end_date:
            type: "string"
            format: "date"
            description: "Inclusive end date, YYYY-MM-DD."
          metric:
            type: "string"
            enum:
              - "total_revenue"
              - "payment_count"
              - "unique_paying_customers"
              - "average_payment"
            description: "Aggregate metric."
          category:
            type: ["string", "null"]
            description: "Film category or null for all."
          store_id:
            type: ["integer", "null"]
            description: "Store ID 1 or 2, or null."
        required:
          - start_date
          - end_date
          - metric
          - category
          - store_id
        additionalProperties: false

  # Tool 4: Get Query Cost
  - name: "get_query_cost"
    title: "Report Snowflake Query Cost"
    type: "GENERIC"
    identifier: "SAKILA.MCP_APP.GET_QUERY_COST"
    description: >
      Use after an analytics query when the user needs
      execution time, query status, warehouse information,
      cloud-services credits from QUERY_HISTORY, or
      attributed compute credits. Accepts a Snowflake
      query ID. Returns null credits when attribution is
      pending or unavailable. Never estimate unknown costs.
    annotations:
      readOnlyHint: true
      destructiveHint: false
      idempotentHint: true
      openWorldHint: false
    config:
      type: "procedure"
      warehouse: "CLASS_WH"
      query_timeout: 120
      input_schema:
        type: "object"
        properties:
          query_id:
            type: "string"
            description: "Snowflake query ID to inspect."
        required:
          - query_id
        additionalProperties: false
$$;

DESCRIBE MCP SERVER SAKILA.MCP_APP.SAKILA_MCP_SERVER;
