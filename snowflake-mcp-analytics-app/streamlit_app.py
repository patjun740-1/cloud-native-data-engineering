"""Sakila analytics: bounded natural-language dashboard for Snowflake Streamlit.

This app calls the same Snowflake procedures exposed by the managed MCP server.
Its in-app Snowpark calls are not themselves MCP protocol calls.
"""

import json
import re
from datetime import date

import streamlit as st


st.set_page_config(page_title="Sakila Analytics", page_icon="📊", layout="wide")
st.title("📊 Sakila Business Analytics")
st.caption("Snowflake · MCP tools · Streamlit")
st.write("Ask about payment revenue, payment counts, or unique paying customers.")

DATA_START = date(2005, 5, 24)
DATA_END = date(2006, 2, 14)
CATEGORIES = [
    "Action", "Animation", "Children", "Classics", "Comedy", "Documentary",
    "Drama", "Family", "Foreign", "Games", "Horror", "Music", "New",
    "Sci-Fi", "Sports", "Travel",
]


def decode_variant(value):
    """Snowpark generally returns VARIANT procedure results as JSON strings."""
    return json.loads(value) if isinstance(value, str) else value


with st.sidebar:
    st.header("Filters")
    start_filter = st.date_input("Start date", DATA_START, key="start_filter")
    end_filter = st.date_input("End date", DATA_END, key="end_filter")
    store_label = st.selectbox(
        "Store", ["All stores", "Store 1", "Store 2"], key="store_filter"
    )
    category_label = st.selectbox(
        "Film category", ["All categories"] + CATEGORIES, key="category_filter"
    )
    st.caption("Data available: May 24, 2005 – February 14, 2006")

store_filter = {"All stores": None, "Store 1": 1, "Store 2": 2}[store_label]
category_filter = None if category_label == "All categories" else category_label
filters_now = (
    start_filter.isoformat(), end_filter.isoformat(), store_label, category_label
)


def interpret_question(question):
    """Map supported language to validated tool arguments, never raw SQL."""
    q = question.casefold().strip()
    if not q:
        return {"error": "Please enter a business question."}

    if any(re.search(pattern, q) for pattern in (
        r"\bdrop\s+table\b", r"\bdelete\s+from\b",
        r"\btruncate\s+table\b", r"\balter\s+table\b",
        r"\bignore\s+(all|previous)\s+instructions\b",
        r"\bbypass\s+(the\s+)?(rules|restrictions)\b",
    )):
        return {"error": "Database changes and instruction overrides are not allowed."}

    # Check historical coverage before interpreting the question's premise.
    years = [int(y) for y in re.findall(r"\b(?:19|20)\d{2}\b", q)]
    if any(y not in (2005, 2006) for y in years):
        return {"error": "No payment data covers that year (only 2005–2006)."}

    if re.search(r"\b(why|cause|reason)\b", q):
        return {"error": "The warehouse shows metrics, not causes of changes."}

    # Reject questions that assume a change over time (false-premise guard).
    if re.search(
        r"\b(fall|fell|drop|dropped|decline|declined|decrease|decreased|"
        r"rise|rose|increase|increased|grow|grew|growth)\b", q
    ):
        return {"error": (
            "This question assumes revenue changed over time, which a single "
            "total can't confirm. Payments cover May–Aug 2005 and only "
            "February 2006, so there is no comparable 2006 period. "
            "Try 'Show monthly revenue' to see what the data contains."
        )}

    if "customer" in q and any(w in q for w in ("rented", "renters", "rental")):
        return {"error": (
            "This model tracks payments, not every rental. "
            "I can count unique paying customers, not all customers who rented."
        )}
        # Handle questions about late rental returns
    if "late" in q and "return" in q:
        return {
            "error": (
                "The warehouse cannot determine how many films "
                "were returned late because FACT_PAYMENT does not "
                "contain actual rental return dates. "
                "This question requires rental-level data."
            )
        }
    if any(phrase in q for phrase in (
        "return date", "late return", "rental duration", "inventory level",
        "profit margin", "customer churn", "film availability",
    )):
        return {"error": "The warehouse lacks the data needed for that question."}

    if start_filter > end_filter:
        return {"error": "Start date must be on or before end date."}
    start, end = start_filter, end_filter

    dates = re.findall(r"\b(?:19|20)\d{2}-\d{2}-\d{2}\b", q)
    if dates:
        try:
            selected = [date.fromisoformat(d) for d in dates]
        except ValueError:
            return {"error": "An explicit date is invalid; use YYYY-MM-DD."}
        start, end = min(selected), max(selected)
    elif years:
        start, end = date(min(years), 1, 1), date(max(years), 12, 31)

    if not dates and re.search(
        r"\b(january|february|march|april|may|june|july|august|"
        r"september|october|november|december)\b", q
    ):
        return {"error": "Use the date filters for a specific named month."}

    selected_store = store_filter
    store_match = re.search(r"\bstore\s*([12])\b", q)
    if store_match:
        selected_store = int(store_match.group(1))

    selected_category = category_filter
    mentioned = [
        c for c in CATEGORIES
        if c != "New" and re.search(rf"\b{re.escape(c.casefold())}\b", q)
    ]
    if len(mentioned) > 1:
        return {"error": "Select one film category in the sidebar."}
    if mentioned:
        selected_category = mentioned[0]

    if "customer" in q and any(w in q for w in ("pay", "paid", "purchase")):
        metric = "unique_paying_customers"
    elif "customer" in q:
        return {"error": "Specify unique *paying* customers to avoid confusion."}
    elif ("how many" in q and "payment" in q) or any(p in q for p in (
        "number of payments", "payment count", "transaction count"
    )):
        metric = "payment_count"
    elif "average" in q and "payment" in q:
        metric = "average_payment"
    elif any(w in q for w in ("revenue", "sales", "earnings")):
        metric = "total_revenue"
    else:
        return {"error": (
            "Please specify revenue, payment count, average payment, "
            "or unique paying customers."
        )}

    group_by = None
    if (re.search(r"\b(which|top|each|per|by)\s+(film\s+)?categor(?:y|ies)\b", q)
            or "categories" in q):
        group_by = "category"
    elif any(p in q for p in (
        "by month", "per month", "monthly", "over time", "revenue trend"
    )):
        group_by = "month"
    elif any(p in q for p in (
        "by store", "per store", "each store", "which store", "compare stores"
    )):
        group_by = "store"

    if group_by and metric not in ("total_revenue", "payment_count"):
        return {"error": (
            "Grouped averages and distinct paying-customer counts are not "
            "supported by the current grouped-query tool."
        )}

    # Flag periods that only partly overlap the available payment data.
    partial = (
        start < DATA_START or end > DATA_END
    ) and start <= DATA_END and end >= DATA_START

    return {
        "kind": "group" if group_by else "aggregate",
        "group_by": group_by, "metric": metric,
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "store_id": selected_store, "category": selected_category,
        "coverage_note": partial,
    }


def ask(question):
    intent = interpret_question(question)
    if "error" in intent:
        return {"status": "unsupported", "message": intent["error"]}

    # No Snowflake connection or query runs while the page first loads.
    session = st.connection("snowflake").session()
    if intent["kind"] == "group":
        raw = session.call(
            "SAKILA.MCP_APP.RUN_PARAMETERIZED_QUERY",
            intent["start_date"], intent["end_date"], intent["category"],
            intent["store_id"], intent["group_by"], 50,
        )
    else:
        raw = session.call(
            "SAKILA.MCP_APP.GET_AGGREGATE",
            intent["start_date"], intent["end_date"], intent["metric"],
            intent["category"], intent["store_id"],
        )

    result = decode_variant(raw)
    cost = None
    if result.get("query_id"):
        try:
            cost = decode_variant(session.call(
                "SAKILA.MCP_APP.GET_QUERY_COST", result["query_id"]
            ))
        except Exception:
            cost = {"status": "unavailable"}
    return {"status": result["status"], "intent": intent,
            "result": result, "cost": cost}


with st.form("question_form"):
    question = st.text_input(
        "Your question", placeholder="Which category generated the most revenue?",
        key="question_text",
    )
    submitted = st.form_submit_button("Ask question", type="primary")

if submitted:
    with st.spinner("Checking your question and running an approved tool..."):
        try:
            st.session_state["answer"] = ask(question)
        except Exception as exc:
            st.session_state["answer"] = {
                "status": "error", "message": "Snowflake could not complete the query.",
                "details": str(exc),
            }
        st.session_state["answer_filters"] = filters_now

answer = st.session_state.get("answer")
if answer:
    st.divider()
    st.subheader("Answer")

    if st.session_state.get("answer_filters") != filters_now:
        st.warning("Filters changed. Submit the question again for an updated result.")
        st.stop()

    if answer["status"] in ("unsupported", "error"):
        st.warning(answer["message"])
        st.caption("No analytics result was produced; query cost is not available.")
        if answer["status"] == "error":
            with st.expander("Technical details"):
                st.code(answer.get("details", ""))
        st.stop()

    result, intent = answer["result"], answer["intent"]
    if result["status"] == "no_data":
        st.info("No data for the selected question and filters—not a zero result.")
    elif intent["kind"] == "aggregate":
        labels = {
            "total_revenue": "Total revenue", "payment_count": "Payment count",
            "unique_paying_customers": "Unique paying customers",
            "average_payment": "Average payment",
        }
        value = result["value"]
        formatted = (f"${value:,.2f}" if intent["metric"] in
                     ("total_revenue", "average_payment") else f"{value:,}")
        st.metric(labels[intent["metric"]], formatted)
    else:
        import pandas as pd

        df = pd.DataFrame(result.get("results", []))
        if not df.empty:
            measure = ("payment_count" if intent["metric"] == "payment_count"
                       else "total_revenue")
            st.subheader("Payments by group" if measure == "payment_count"
                         else "Revenue by group")
            series = df.set_index("group")[measure]
            if intent["group_by"] == "month":
                st.bar_chart(series)
                st.caption(
                    "Only months with recorded payments are shown. "
                    "Missing months are not assumed to have zero revenue."
                )
            else:
                st.bar_chart(series)
            st.dataframe(df[["group", "total_revenue", "payment_count"]],
                         use_container_width=True, hide_index=True)

    if intent.get("coverage_note"):
        st.warning(
            "This period is only partly covered: payment data runs "
            "May 24, 2005 – Feb 14, 2006."
        )

    st.subheader("How this answer was produced")
    st.write(f"**Tool:** `{result['tool']}`")
    st.write(f"**Rows returned:** {result['row_count']}")
    st.write(f"**One row represents:** {result['row_grain']}")
    with st.expander("Executed SQL and bound parameters"):
        st.code(result.get("sql", ""), language="sql")
        for number, param in enumerate(result.get("parameters", []), 1):
            st.write(f"Parameter {number}: `{param}`")

    st.subheader("Query cost")
    cost = answer.get("cost")
    if cost and cost.get("status") == "success":
        c1, c2, c3 = st.columns(3)
        c1.metric("Elapsed", f"{cost['elapsed_ms']:,} ms")
        c2.metric("Warehouse", cost.get("warehouse") or "N/A")
        credits = cost.get("credits_attributed")
        c3.metric("Attributed credits", f"{credits:.6f}" if credits is not None
                  else "Unavailable")
        if credits is None:
            st.caption("No credit figure is available yet; this is not zero cost.")
    else:
        st.info("Query-cost metadata is unavailable for this result.")
    st.caption(f"Snowflake query ID: {result.get('query_id', 'N/A')}")
