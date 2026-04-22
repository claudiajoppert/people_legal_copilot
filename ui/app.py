"""
People & Legal Operations Copilot — a conversational analytics interface.

Run from the project root:

    streamlit run ui/app.py

Architecture:

    User question
        │
        ▼
    Snowflake Cortex COMPLETE  (stubbed locally via cortex.client)
        │  returns JSON routing object
        ▼
    QUERY_REGISTRY dispatch     (ui.analytics)
        │  returns DataFrame + narrative
        ▼
    Streamlit chat bubble + Altair chart
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Ensure the project root is on sys.path so sibling packages (cortex, data,
# ui) resolve when Streamlit executes this file directly.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import altair as alt
import pandas as pd
import streamlit as st

from cortex.client import complete, DEFAULT_MODEL, USE_LOCAL_STUB
from ui.analytics import QUERY_REGISTRY
from ui.prompts import SYSTEM_PROMPT

# ---------------------------------------------------------------------------
# Page config & styling
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="People & Legal Copilot",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Editorial / warm-neutral aesthetic. Serif display for headers, sans body.
# Intentionally NOT the default Streamlit purple gradient look.
CUSTOM_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,400;9..144,500;9..144,600&family=Inter:wght@300;400;500;600&display=swap');

    :root {
        --bg: #faf7f2;
        --bg-panel: #ffffff;
        --ink: #2a2420;
        --ink-muted: #6b635c;
        --accent: #8b5a3c;
        --accent-soft: #e8dcc8;
        --rule: #e5dfd6;
    }

    html, body, [class*="stApp"] {
        background-color: var(--bg);
        color: var(--ink);
        font-family: 'Inter', -apple-system, sans-serif;
    }

    /* Display type */
    h1, h2, h3 { font-family: 'Fraunces', Georgia, serif !important;
                 font-weight: 500 !important; letter-spacing: -0.01em; }
    h1 { font-size: 2.4rem !important; }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #f3ede3;
        border-right: 1px solid var(--rule);
    }
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 { color: var(--ink); }

    /* Chat bubbles */
    [data-testid="stChatMessage"] {
        background: var(--bg-panel);
        border: 1px solid var(--rule);
        border-radius: 4px;
        padding: 1rem 1.25rem;
        box-shadow: 0 1px 2px rgba(42, 36, 32, 0.04);
    }

    /* User bubble differentiator */
    [data-testid="stChatMessage"]:has([data-testid="stChatMessageAvatarUser"]) {
        background: var(--accent-soft);
        border-color: var(--accent-soft);
    }

    /* Chat input */
    [data-testid="stChatInput"] {
        background: var(--bg-panel);
        border: 1px solid var(--rule);
        border-radius: 4px;
    }

    /* Buttons (suggested prompts) */
    .stButton > button {
        background: var(--bg-panel);
        color: var(--ink);
        border: 1px solid var(--rule);
        border-radius: 3px;
        padding: 0.5rem 0.85rem;
        font-family: 'Inter', sans-serif;
        font-size: 0.85rem;
        font-weight: 400;
        text-align: left;
        white-space: normal;
        line-height: 1.35;
        transition: all 0.15s ease;
        width: 100%;
        min-height: 2.6rem;
    }
    .stButton > button:hover {
        border-color: var(--accent);
        color: var(--accent);
        background: var(--bg-panel);
    }

    /* Tables */
    [data-testid="stDataFrame"] {
        border: 1px solid var(--rule);
        border-radius: 3px;
    }

    /* Hide Streamlit chrome */
    #MainMenu, footer, header { visibility: hidden; }

    /* Header block */
    .app-header {
        border-bottom: 1px solid var(--rule);
        padding-bottom: 1rem;
        margin-bottom: 1.5rem;
    }
    .app-header .eyebrow {
        font-family: 'Inter', sans-serif;
        font-size: 0.72rem;
        font-weight: 500;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        color: var(--ink-muted);
        margin-bottom: 0.4rem;
    }
    .app-header .subtitle {
        color: var(--ink-muted);
        font-size: 0.95rem;
        margin-top: 0.25rem;
    }

    /* Status pill */
    .pill {
        display: inline-block;
        padding: 0.2rem 0.6rem;
        border-radius: 999px;
        font-size: 0.72rem;
        font-weight: 500;
        letter-spacing: 0.03em;
    }
    .pill-stub  { background: #fff4e3; color: #8a5a12; border: 1px solid #f0d9a8; }
    .pill-live  { background: #e6f3ea; color: #1f5d34; border: 1px solid #b9dcc4; }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

pill_class = "pill-stub" if USE_LOCAL_STUB else "pill-live"
pill_text = "Cortex · local stub" if USE_LOCAL_STUB else f"Cortex · {DEFAULT_MODEL}"

st.markdown(
    f"""
    <div class="app-header">
        <div class="eyebrow">Internal · People & Legal Operations</div>
        <h1>The Copilot</h1>
        <div class="subtitle">
            Ask natural-language questions about workforce, attrition,
            compensation, legal matters, and outside counsel spend.
            <span class="pill {pill_class}" style="margin-left: 0.5rem;">{pill_text}</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("### About")
    st.markdown(
        "This copilot routes natural-language questions through **Snowflake "
        "Cortex COMPLETE**, which returns a structured intent. A Python "
        "analytics layer then executes the matching query against the "
        "warehouse and renders the result inline."
    )

    st.markdown("### Integration")
    mode = "Local stub (demo)" if USE_LOCAL_STUB else f"Live · {DEFAULT_MODEL}"
    st.markdown(f"**Mode:** {mode}")
    st.markdown(
        "Set `CORTEX_MODE=snowflake` and provide a Snowpark session to route "
        "to a real Cortex endpoint. The rest of the app is unchanged."
    )

    st.markdown("### Data")
    st.markdown(
        "- `employees` — 1,346 rows\n"
        "- `compensation_history` — 2,880 rows\n"
        "- `legal_matters` — 180 rows\n"
        "- `legal_invoices` — 754 rows\n\n"
        "Synthetic, but patterned to match realistic HR & legal ops "
        "distributions (higher attrition in Sales/CS, legal spend "
        "concentrated in a handful of firms, etc.)."
    )

    st.markdown("### Reset")
    if st.button("Clear conversation", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

if "messages" not in st.session_state:
    st.session_state.messages: list[dict] = []


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

def render_chart(df: pd.DataFrame, kind: str | None) -> alt.Chart | None:
    """Build an Altair chart that matches the app's warm-neutral palette."""
    if kind is None or df.empty or len(df.columns) < 2:
        return None

    # Pick x (first non-numeric) and y (first numeric)
    x_col = df.columns[0]
    y_cols = [c for c in df.columns[1:] if pd.api.types.is_numeric_dtype(df[c])]
    if not y_cols:
        return None
    y_col = y_cols[0]

    palette = ["#8b5a3c", "#a67c52", "#c19875", "#d9b896"]

    base = alt.Chart(df).properties(height=320)

    if kind == "line":
        chart = base.mark_line(
            point=alt.OverlayMarkDef(color="#8b5a3c", size=60),
            color="#8b5a3c",
            strokeWidth=2.5,
        ).encode(
            x=alt.X(f"{x_col}:N", title=x_col.replace("_", " ").title(),
                    axis=alt.Axis(labelAngle=-30)),
            y=alt.Y(f"{y_col}:Q", title=y_col.replace("_", " ").title()),
            tooltip=list(df.columns),
        )
    else:
        chart = base.mark_bar(
            color="#8b5a3c",
            cornerRadiusTopLeft=2,
            cornerRadiusTopRight=2,
        ).encode(
            x=alt.X(f"{x_col}:N",
                    sort="-y",
                    title=x_col.replace("_", " ").title(),
                    axis=alt.Axis(labelAngle=-30)),
            y=alt.Y(f"{y_col}:Q", title=y_col.replace("_", " ").title()),
            tooltip=list(df.columns),
        )

    return chart.configure_axis(
        labelFont="Inter",
        titleFont="Inter",
        labelColor="#6b635c",
        titleColor="#2a2420",
        gridColor="#e5dfd6",
        domainColor="#e5dfd6",
    ).configure_view(strokeWidth=0)


def render_assistant_message(msg: dict) -> None:
    """Render an assistant turn with its narrative, chart, and table."""
    with st.chat_message("assistant", avatar="💬"):
        st.markdown(msg["narrative"])
        if msg.get("chart_df") is not None:
            df = pd.DataFrame(msg["chart_df"])
            chart = render_chart(df, msg.get("chart_kind"))
            if chart is not None:
                st.altair_chart(chart, use_container_width=True)
            with st.expander("View data", expanded=False):
                st.dataframe(df, use_container_width=True, hide_index=True)
        if msg.get("routing"):
            with st.expander("Cortex routing", expanded=False):
                st.code(
                    json.dumps(msg["routing"], indent=2),
                    language="json",
                )


def handle_question(question: str) -> None:
    """Run the full pipeline for a single user question and append to state."""
    st.session_state.messages.append({"role": "user", "content": question})

    # 1. Build the conversation for Cortex (system + user turns)
    prompt_messages: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]
    for m in st.session_state.messages:
        if m["role"] == "user":
            prompt_messages.append({"role": "user", "content": m["content"]})
        elif m["role"] == "assistant":
            prompt_messages.append({
                "role": "assistant",
                "content": m.get("narrative", ""),
            })

    # 2. Call Cortex COMPLETE (real signature — stub or live)
    raw = complete(
        model=DEFAULT_MODEL,
        prompt=prompt_messages,
        session=None,
    )

    # 3. Parse the routing JSON
    try:
        routing = json.loads(raw)
    except json.JSONDecodeError:
        # A real LLM might wrap JSON in prose; extract it defensively.
        import re
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        routing = json.loads(match.group(0)) if match else {"query": "overview", "params": {}}

    query_name = routing.get("query", "overview")
    params = routing.get("params", {}) or {}
    chart_kind = routing.get("chart")

    # 4. Dispatch to the analytics layer
    fn = QUERY_REGISTRY.get(query_name, QUERY_REGISTRY["overview"])
    try:
        df, narrative = fn(**params)
    except TypeError:
        # Param mismatch from the router — fall back gracefully
        df, narrative = fn()

    # 5. Append assistant turn
    st.session_state.messages.append({
        "role": "assistant",
        "narrative": narrative,
        "chart_df": df.to_dict(orient="records") if df is not None else None,
        "chart_kind": chart_kind,
        "routing": routing,
    })


# ---------------------------------------------------------------------------
# Suggested prompts (show only at the start of a session)
# ---------------------------------------------------------------------------

SUGGESTED_PROMPTS = [
    "What is the attrition risk in the Engineering department?",
    "Show me legal spend by outside counsel this quarter",
    "How has legal spend trended over the last 12 months?",
    "Compensation bands for Sales",
    "How many open legal matters do we have, and of what type?",
    "Hiring trend over the last year",
]

if not st.session_state.messages:
    st.markdown("#### Try asking")
    cols = st.columns(2)
    for i, prompt in enumerate(SUGGESTED_PROMPTS):
        with cols[i % 2]:
            if st.button(prompt, key=f"sugg_{i}"):
                handle_question(prompt)
                st.rerun()

# ---------------------------------------------------------------------------
# Render history
# ---------------------------------------------------------------------------

for msg in st.session_state.messages:
    if msg["role"] == "user":
        with st.chat_message("user", avatar="🧑"):
            st.markdown(msg["content"])
    else:
        render_assistant_message(msg)

# ---------------------------------------------------------------------------
# Chat input
# ---------------------------------------------------------------------------

if question := st.chat_input("Ask about workforce, attrition, comp, or legal spend…"):
    handle_question(question)
    st.rerun()
