# People & Legal Operations Copilot

A conversational analytics interface built with **Streamlit** and designed
around **Snowflake Cortex COMPLETE**. Stakeholders in Talent Acquisition and
Legal Operations can ask natural-language questions — *"What is the attrition
risk in the Engineering department?"*, *"Show me legal spend by outside
counsel this quarter"* — and get back narrative answers, charts, and the
underlying data in one chat turn.

The Cortex integration is **shaped like the real Snowflake API** but runs
locally out of the box, so the app is demoable without a Snowflake account.
Flipping one environment variable routes it to a live Cortex endpoint.

---

## Why this project

This is a reference implementation for the kind of internal tool a People &
Legal analytics team would build to reduce one-off ticket volume: instead of
routing every "how many engineers do we have in London?" question to an
analyst, stakeholders self-serve through a chat UI that's backed by governed
warehouse tables.

Design choices:

- **Cortex picks the query, not the SQL.** The LLM returns a structured
  routing object (`{query, params, chart, summary_hint}`). A Python
  analytics layer owns every SQL statement. This keeps the LLM out of the
  query plan, which is the same pattern you'd use with Cortex Analyst or a
  semantic layer in production.
- **Synthetic data with real patterns.** Attrition is higher in Sales and
  Customer Success. Legal spend concentrates in a handful of firms. Comp
  bands step up by level with location multipliers. The demo answers are
  non-trivial.
- **Editorial aesthetic over dashboard-default.** Warm neutrals, a serif
  display face, restrained type scale. An internal tool people actually want
  to open.

---

## Quick start

```bash
pip install -r requirements.txt
python -m data.generate       # builds data/warehouse.db
streamlit run ui/app.py
```

The app runs on `http://localhost:8501`. The first time you open it, try one
of the suggested prompts.

### Environment variables

| Variable       | Default              | Purpose                                     |
|----------------|----------------------|---------------------------------------------|
| `CORTEX_MODE`  | `stub`               | Set to `snowflake` to use a live Cortex endpoint. |
| `CORTEX_MODEL` | `claude-sonnet-4-5`  | Any Cortex-supported model name.            |

---

## Architecture

```
        User question
             │
             ▼
    ┌────────────────────┐
    │  Streamlit chat UI │   ui/app.py
    └────────┬───────────┘
             │  prompt + history
             ▼
    ┌────────────────────┐
    │ Cortex COMPLETE    │   cortex/client.py
    │ (stub or live)     │   signature: Complete(model, prompt, session)
    └────────┬───────────┘
             │  JSON: {query, params, chart}
             ▼
    ┌────────────────────┐
    │ Analytics dispatch │   ui/analytics.py — QUERY_REGISTRY
    └────────┬───────────┘
             │  SQL
             ▼
    ┌────────────────────┐
    │ SQLite warehouse   │   data/warehouse.db
    │  • employees       │
    │  • comp_history    │
    │  • legal_matters   │
    │  • legal_invoices  │
    └────────────────────┘
```

### The Cortex seam

`cortex/client.py` exposes a single function whose signature matches
`snowflake.cortex.Complete`:

```python
from cortex.client import complete

text = complete(
    model="claude-sonnet-4-5",
    prompt=[{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": "Attrition in Engineering?"}],
    session=None,
)
```

In stub mode, a rule-based classifier returns the same JSON shape a real
Cortex-hosted LLM would return when prompted with `ui/prompts.py`. Swap in a
Snowpark `Session` and set `CORTEX_MODE=snowflake`, and the identical call
path hits the real model. The rest of the app is unchanged.

---

## Extending it

### Add a new analytics capability

1. Write the query function in `ui/analytics.py` — it should return
   `(pandas.DataFrame, narrative_str)`.
2. Register it in `QUERY_REGISTRY`.
3. Add a bullet to the query list in `ui/prompts.py` so the real LLM knows it
   exists. (For the stub, add a routing rule in `cortex/client._classify_intent`.)

### Point at a real warehouse

Replace the SQLite connection in `ui/analytics.py._conn()` with a Snowpark
session or a Snowflake connector connection. The query SQL is standard enough
to port directly; the few date functions (`julianday`, `strftime`) have
Snowflake equivalents (`DATEDIFF`, `TO_CHAR`).

### Add guardrails

Cortex COMPLETE supports a `guardrails: true` option that filters harmful
output via Cortex Guard. To enable it, pass `options={"guardrails": True}` to
`complete()`. In live mode this is forwarded to Snowflake; in stub mode it's
a no-op.

---

## Project layout

```
people_legal_copilot/
├── cortex/
│   ├── __init__.py
│   └── client.py          # Cortex COMPLETE wrapper + local stub
├── data/
│   ├── __init__.py
│   ├── generate.py        # Synthetic HR + Legal data generator
│   └── warehouse.db       # Generated SQLite DB (gitignored in practice)
├── ui/
│   ├── __init__.py
│   ├── analytics.py       # Query functions + QUERY_REGISTRY
│   ├── app.py             # Streamlit entry point
│   └── prompts.py         # System prompt for Cortex
├── requirements.txt
└── README.md
```
