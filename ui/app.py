"""Fraud analyst console (Streamlit).

    streamlit run ui/app.py

Tabs:
  Ask the analyst  chat with the Strands agent on AgentCore Runtime (or in-process with "local" mode)
  Live alerts      today's DynamoDB alerts, refreshed every 10 seconds
  Lakehouse        saved and ad-hoc Athena queries over the Iceberg tables
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import json  # noqa: E402

import boto3  # noqa: E402
import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from agent.client import ANALYST_STACK, LocalAnalyst, RemoteAnalyst, new_session_id, stack_outputs  # noqa: E402
from common.athena import Athena  # noqa: E402
from ui import data  # noqa: E402

SAMPLE_QUESTIONS = [
    "How many fraud alerts were raised today, and which payer had the most?",
    "Which 5 cities have the highest alert rate, and how much money is at risk there?",
    "Explain the most recent critical alert and what I should do next.",
    "How precise is the model compared with the rules?",
    "What is a velocity burst and how do we respond to one?",
]

st.set_page_config(page_title="UPI Fraud Analyst", page_icon=":material/shield:", layout="wide")


@st.cache_resource
def settings() -> dict:
    return stack_outputs(ANALYST_STACK)


@st.cache_resource
def session() -> boto3.Session:
    return boto3.Session()


@st.cache_resource
def analyst(mode: str):
    return LocalAnalyst() if mode == "local" else RemoteAnalyst(settings().get("AgentRuntimeArn"))


@st.cache_resource
def athena() -> Athena:
    return Athena(settings()["AthenaWorkgroup"], settings()["GlueDatabase"], session())


def init_state() -> None:
    st.session_state.setdefault("messages", [])
    st.session_state.setdefault("session_id", new_session_id())
    st.session_state.setdefault("pending", None)


def render_trace(result: dict) -> None:
    calls = result.get("tool_calls", [])
    if not calls:
        return
    with st.expander(f"How I got this: {len(calls)} tool call(s), {result.get('latency_ms', 0) / 1000:.1f}s"):
        for call in calls:
            icon = ":material/check_circle:" if call["status"] == "success" else ":material/error:"
            st.markdown(f"{icon} **{call['name']}**")
            if "sql" in call["input"]:
                st.code(call["input"]["sql"].strip(), language="sql")
            elif call["input"]:
                st.code(json.dumps(call["input"], indent=2), language="json")
        usage = result.get("usage", {})
        st.caption(f"Model {result.get('model_id', '')} · {usage.get('input', 0):,} input / {usage.get('output', 0):,} output tokens")


def sidebar() -> str:
    with st.sidebar:
        st.title(":material/shield: UPI Fraud Analyst")
        st.caption("Real-time fraud detection on AWS with an AI analyst (Strands agent on Bedrock AgentCore).")
        mode = st.radio(
            "Agent runs on", ["agentcore", "local"], horizontal=True,
            format_func=lambda m: "AgentCore Runtime" if m == "agentcore" else "This machine",
            help="AgentCore calls the deployed runtime; 'This machine' runs the same agent code in-process.",
        )
        if st.button("New conversation", icon=":material/refresh:", use_container_width=True):
            st.session_state.messages = []
            st.session_state.session_id = new_session_id()
        st.subheader("Try asking")
        for q in SAMPLE_QUESTIONS:
            if st.button(q, use_container_width=True):
                st.session_state.pending = q
        st.caption(f"Session `{st.session_state.session_id[-8:]}`")
    return mode


def chat_tab(mode: str) -> None:
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg["role"] == "assistant":
                render_trace(msg.get("result", {}))

    question = st.chat_input("Ask about alerts, payers, cities, trends, or the fraud playbook")
    question = question or st.session_state.pending
    st.session_state.pending = None
    if not question:
        return
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)
    with st.chat_message("assistant"):
        with st.spinner("Investigating..."):
            try:
                result = analyst(mode).ask(question, st.session_state.session_id)
            except Exception as e:  # noqa: BLE001
                result = {"error": f"{type(e).__name__}: {e}"}
        answer = result.get("answer") or f":red[{result.get('error', 'No answer')}]"
        st.markdown(answer)
        render_trace(result)
    st.session_state.messages.append({"role": "assistant", "content": answer, "result": result})


@st.fragment(run_every="10s")
def alerts_tab() -> None:
    day = data.today_utc()
    table = session().resource("dynamodb").Table(settings()["AlertsTableName"])
    alerts = data.fetch_alerts(table, day)
    kpis = data.alert_kpis(alerts)
    cols = st.columns(5)
    cols[0].metric("Alerts today", f"{kpis['alerts']:,}")
    cols[1].metric("Last 10 minutes", f"{kpis['last_window']:,}")
    cols[2].metric("Critical (score >= 0.95)", f"{kpis['critical']:,}")
    cols[3].metric("Payers affected", f"{kpis['payers']:,}")
    cols[4].metric("Money at risk", f"Rs {kpis['money_at_risk']:,.0f}")
    if not alerts:
        st.info(f"No alerts yet for {day} (UTC). Start the generator: `python -m generator live --url auto --rate 5 --duration 300`")
        return

    left, right = st.columns(2)
    with left:
        st.markdown("**Alerts by reason**")
        reasons = data.reason_counts(alerts)
        st.bar_chart(pd.DataFrame({"alerts": dict(reasons)}).sort_values("alerts", ascending=False), horizontal=True, height=240)
    with right:
        st.markdown("**Alerts by city**")
        cities = pd.Series([a["city"] for a in alerts]).value_counts().rename("alerts")
        st.bar_chart(cities, horizontal=True, height=240)

    st.markdown(f"**Latest alerts** (newest first, {day} UTC, refreshes every 10 s)")
    st.dataframe(
        pd.DataFrame(data.table_rows(alerts[:200])),
        hide_index=True,
        use_container_width=True,
        column_config={
            "score": st.column_config.ProgressColumn("score", min_value=0.0, max_value=1.0, format="%.3f"),
            "amount_inr": st.column_config.NumberColumn("amount (Rs)", format="%.2f"),
        },
    )


def lakehouse_tab() -> None:
    st.caption("Athena over the Iceberg tables (bronze is loaded every 5 minutes; silver/gold after the Glue job runs).")
    saved = data.saved_queries(session(), settings()["AthenaWorkgroup"])
    choice = st.selectbox("Saved query", ["(write your own)", *saved], format_func=lambda n: n if n not in saved else f"{n}: {saved[n][0]}")
    default = saved[choice][1] if choice in saved else "SELECT * FROM v_fraud_by_pattern ORDER BY flagged DESC"
    sql = st.text_area("SQL (read-only)", value=default, height=160, key=f"sql_{choice}")
    if st.button("Run query", type="primary", icon=":material/play_arrow:"):
        with st.spinner("Running on Athena..."):
            try:
                result = data.run_sql(athena(), sql)
            except Exception as e:  # noqa: BLE001
                st.error(str(e))
                return
        st.dataframe(pd.DataFrame(result.rows, columns=result.columns), hide_index=True, use_container_width=True)
        st.caption(
            f"{len(result.rows)} rows{' (truncated)' if result.truncated else ''} · "
            f"{result.bytes_scanned / 1e6:.2f} MB scanned (about ${result.bytes_scanned / 1e12 * 5:.6f}) · "
            f"{result.elapsed_ms} ms · query id `{result.query_id}`"
        )


def main() -> None:
    init_state()
    mode = sidebar()
    chat, live, lake = st.tabs([":material/chat: Ask the analyst", ":material/notifications_active: Live alerts", ":material/database: Lakehouse"])
    with chat:
        chat_tab(mode)
    with live:
        alerts_tab()
    with lake:
        lakehouse_tab()


main()
