-- Read-only baseline checks for the SAKILA.DW star schema.
-- Expected: 16,044 payments · $67,406.56 revenue · 599 distinct paying customers
--           payment coverage 2005-05-24 to 2006-02-14

USE DATABASE SAKILA;
USE SCHEMA DW;

-- 1. Payment grain and total revenue
SELECT
    COUNT(*)                   AS total_payments,
    COUNT(DISTINCT payment_id) AS unique_payments,
    SUM(amount)                AS total_revenue
FROM SAKILA.DW.FACT_PAYMENT;

-- 2. Date coverage
SELECT
    MIN(payment_ts) AS earliest_payment,
    MAX(payment_ts) AS latest_payment
FROM SAKILA.DW.FACT_PAYMENT;

-- 3. Distinct BUSINESS customers (DIM_CUSTOMER is SCD Type 2, so count
--    CUSTOMER_ID after the surrogate-key join, not CUSTOMER_SK versions)
SELECT COUNT(DISTINCT c.customer_id) AS unique_paying_customers
FROM SAKILA.DW.FACT_PAYMENT p
JOIN SAKILA.DW.DIM_CUSTOMER c ON p.customer_sk = c.customer_sk;

-- 4. Smoke tests through the bounded tools
CALL SAKILA.MCP_APP.GET_AGGREGATE('2005-05-24', '2006-02-14', 'total_revenue', NULL, NULL);
CALL SAKILA.MCP_APP.RUN_PARAMETERIZED_QUERY('2005-05-24', '2006-02-14', NULL, NULL, 'category', 50);

-- 5. Out-of-range period must return status = 'no_data' (not a zero)
CALL SAKILA.MCP_APP.GET_AGGREGATE('2020-01-01', '2020-12-31', 'total_revenue', NULL, NULL);

DESCRIBE MCP SERVER SAKILA.MCP_APP.SAKILA_MCP_SERVER;
