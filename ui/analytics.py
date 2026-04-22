"""
Analytics query layer.

Each function here corresponds to a ``query`` value that the Cortex router
can return. The separation means the LLM only picks a route; it never writes
SQL directly. This is the same pattern you'd use with Cortex Analyst or a
semantic layer in production — keeps the LLM out of the query plan.

All functions return ``(pandas.DataFrame, narrative_string)``.
"""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd

DB_PATH = Path(__file__).parent.parent / "data" / "warehouse.db"


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


# ---------------------------------------------------------------------------
# HR queries
# ---------------------------------------------------------------------------

def attrition_by_department(department: str | None = None) -> tuple[pd.DataFrame, str]:
    sql = """
        SELECT department,
               SUM(CASE WHEN status='Terminated' THEN 1 ELSE 0 END) AS terminations_ltm,
               COUNT(*) AS avg_headcount,
               ROUND(100.0 * SUM(CASE WHEN status='Terminated' THEN 1 ELSE 0 END)
                           / COUNT(*), 1) AS attrition_rate_pct
        FROM employees
        GROUP BY department
        ORDER BY attrition_rate_pct DESC
    """
    with _conn() as c:
        df = pd.read_sql(sql, c)

    if department:
        row = df[df["department"].str.lower() == department.lower()]
        if not row.empty:
            r = row.iloc[0]
            narrative = (
                f"**{r['department']}** has a trailing-12-month attrition rate of "
                f"**{r['attrition_rate_pct']}%** "
                f"({int(r['terminations_ltm'])} terminations against an avg headcount "
                f"of {int(r['avg_headcount'])}). "
            )
            company_avg = df["attrition_rate_pct"].mean()
            delta = r["attrition_rate_pct"] - company_avg
            direction = "above" if delta > 0 else "below"
            narrative += (
                f"That's **{abs(delta):.1f} pp {direction}** the company average "
                f"of {company_avg:.1f}%."
            )
            return df, narrative

    top = df.iloc[0]
    bottom = df.iloc[-1]
    narrative = (
        f"Trailing-12-month attrition ranges from **{bottom['attrition_rate_pct']}%** "
        f"in {bottom['department']} to **{top['attrition_rate_pct']}%** in "
        f"{top['department']}. Company-wide rate is "
        f"**{df['attrition_rate_pct'].mean():.1f}%**."
    )
    return df, narrative


def headcount_by_department(department: str | None = None) -> tuple[pd.DataFrame, str]:
    sql = """
        SELECT department,
               SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active_headcount,
               SUM(CASE WHEN status='Terminated' THEN 1 ELSE 0 END) AS terminated_ltm
        FROM employees
        GROUP BY department
        ORDER BY active_headcount DESC
    """
    with _conn() as c:
        df = pd.read_sql(sql, c)

    total = int(df["active_headcount"].sum())

    if department:
        row = df[df["department"].str.lower() == department.lower()]
        if not row.empty:
            r = row.iloc[0]
            pct = 100 * r["active_headcount"] / total
            narrative = (
                f"**{r['department']}** currently has **{int(r['active_headcount'])} "
                f"active employees** ({pct:.1f}% of the company)."
            )
            return df, narrative

    narrative = (
        f"Total active headcount is **{total:,}** across {len(df)} departments. "
        f"**{df.iloc[0]['department']}** is the largest at "
        f"{int(df.iloc[0]['active_headcount'])} people "
        f"({100*df.iloc[0]['active_headcount']/total:.0f}% of the company)."
    )
    return df, narrative


def compensation_by_level(department: str | None = None) -> tuple[pd.DataFrame, str]:
    where = ""
    params: list = []
    if department:
        where = "WHERE LOWER(department) = LOWER(?)"
        params.append(department)

    sql = f"""
        SELECT level,
               COUNT(*) AS n,
               ROUND(AVG(annual_compensation_usd), 0) AS avg_comp,
               ROUND(MIN(annual_compensation_usd), 0) AS min_comp,
               ROUND(MAX(annual_compensation_usd), 0) AS max_comp
        FROM employees
        WHERE status='Active'
        {"AND LOWER(department) = LOWER(?)" if department else ""}
        GROUP BY level
        ORDER BY avg_comp
    """
    with _conn() as c:
        df = pd.read_sql(sql, c, params=params)

    scope = f"in **{department}**" if department else "company-wide"
    narrative = (
        f"Compensation bands {scope} span **${df['avg_comp'].min():,.0f}** at the "
        f"entry IC level to **${df['avg_comp'].max():,.0f}** at the most senior "
        f"level. Median band midpoint is **${df['avg_comp'].median():,.0f}**."
    )
    return df, narrative


def hires_last_12_months(**_: object) -> tuple[pd.DataFrame, str]:
    sql = """
        SELECT strftime('%Y-%m', hire_date) AS month,
               COUNT(*) AS hires
        FROM employees
        WHERE hire_date >= date('2025-04-22')
        GROUP BY month
        ORDER BY month
    """
    with _conn() as c:
        df = pd.read_sql(sql, c)

    total = int(df["hires"].sum())
    peak = df.loc[df["hires"].idxmax()]
    narrative = (
        f"**{total} new hires** in the last 12 months. Peak month was "
        f"**{peak['month']}** with {int(peak['hires'])} hires."
    )
    return df, narrative


def tenure_by_department(**_: object) -> tuple[pd.DataFrame, str]:
    sql = """
        SELECT department,
               ROUND(AVG((julianday('2026-04-22') - julianday(hire_date)) / 365.0), 1)
                   AS avg_tenure_years
        FROM employees
        WHERE status='Active'
        GROUP BY department
        ORDER BY avg_tenure_years DESC
    """
    with _conn() as c:
        df = pd.read_sql(sql, c)

    narrative = (
        f"Longest average tenure is in **{df.iloc[0]['department']}** at "
        f"**{df.iloc[0]['avg_tenure_years']} years**. Shortest is "
        f"**{df.iloc[-1]['department']}** at {df.iloc[-1]['avg_tenure_years']} years."
    )
    return df, narrative


# ---------------------------------------------------------------------------
# Legal queries
# ---------------------------------------------------------------------------

def legal_spend_by_firm(quarter: str | None = None) -> tuple[pd.DataFrame, str]:
    date_filter, label = _quarter_to_range(quarter)
    sql = f"""
        SELECT firm,
               ROUND(SUM(amount_usd), 0) AS total_spend_usd,
               COUNT(*) AS invoice_count,
               ROUND(SUM(hours_billed), 1) AS total_hours
        FROM legal_invoices
        {date_filter}
        GROUP BY firm
        ORDER BY total_spend_usd DESC
    """
    with _conn() as c:
        df = pd.read_sql(sql, c)

    if df.empty:
        return df, f"No legal invoices found for {label}."

    total = df["total_spend_usd"].sum()
    top = df.iloc[0]
    top_share = 100 * top["total_spend_usd"] / total
    narrative = (
        f"Total legal spend {label} is **${total:,.0f}** across "
        f"**{len(df)} firms**. **{top['firm']}** leads at "
        f"${top['total_spend_usd']:,.0f} ({top_share:.0f}% of spend)."
    )
    return df, narrative


def legal_spend_trend(**_: object) -> tuple[pd.DataFrame, str]:
    sql = """
        SELECT strftime('%Y-%m', invoice_date) AS month,
               ROUND(SUM(amount_usd), 0) AS spend_usd
        FROM legal_invoices
        WHERE invoice_date >= date('2025-04-22')
        GROUP BY month
        ORDER BY month
    """
    with _conn() as c:
        df = pd.read_sql(sql, c)

    if len(df) >= 2:
        delta = df.iloc[-1]["spend_usd"] - df.iloc[0]["spend_usd"]
        direction = "up" if delta > 0 else "down"
        narrative = (
            f"Monthly legal spend is **{direction}** "
            f"${abs(delta):,.0f} over the last 12 months. "
            f"Latest month (**{df.iloc[-1]['month']}**): "
            f"${df.iloc[-1]['spend_usd']:,.0f}."
        )
    else:
        narrative = "Not enough invoice history to compute a trend."
    return df, narrative


def open_matters_by_type(**_: object) -> tuple[pd.DataFrame, str]:
    sql = """
        SELECT matter_type,
               COUNT(*) AS open_matters,
               SUM(CASE WHEN priority='high' THEN 1 ELSE 0 END) AS high_priority
        FROM legal_matters
        WHERE status='Open'
        GROUP BY matter_type
        ORDER BY open_matters DESC
    """
    with _conn() as c:
        df = pd.read_sql(sql, c)

    total = int(df["open_matters"].sum())
    high = int(df["high_priority"].sum())
    narrative = (
        f"**{total} open matters** right now, of which **{high} are high priority**. "
        f"Largest category is **{df.iloc[0]['matter_type']}** with "
        f"{int(df.iloc[0]['open_matters'])} open matters."
    )
    return df, narrative


def spend_by_matter_type(**_: object) -> tuple[pd.DataFrame, str]:
    sql = """
        SELECT m.matter_type,
               ROUND(SUM(i.amount_usd), 0) AS total_spend_usd,
               COUNT(DISTINCT m.matter_id) AS matters,
               ROUND(SUM(i.amount_usd) / COUNT(DISTINCT m.matter_id), 0)
                   AS avg_spend_per_matter
        FROM legal_matters m
        JOIN legal_invoices i ON m.matter_id = i.matter_id
        GROUP BY m.matter_type
        ORDER BY total_spend_usd DESC
    """
    with _conn() as c:
        df = pd.read_sql(sql, c)

    top = df.iloc[0]
    narrative = (
        f"**{top['matter_type']}** is the largest spend category at "
        f"**${top['total_spend_usd']:,.0f}** across {int(top['matters'])} matters "
        f"(${top['avg_spend_per_matter']:,.0f} avg per matter)."
    )
    return df, narrative


# ---------------------------------------------------------------------------
# Fallback / overview
# ---------------------------------------------------------------------------

def overview(**_: object) -> tuple[pd.DataFrame, str]:
    with _conn() as c:
        hc = pd.read_sql(
            "SELECT COUNT(*) AS n FROM employees WHERE status='Active'", c
        ).iloc[0]["n"]
        term = pd.read_sql(
            "SELECT COUNT(*) AS n FROM employees WHERE status='Terminated'", c
        ).iloc[0]["n"]
        matters_open = pd.read_sql(
            "SELECT COUNT(*) AS n FROM legal_matters WHERE status='Open'", c
        ).iloc[0]["n"]
        spend_ytd = pd.read_sql(
            "SELECT ROUND(SUM(amount_usd), 0) AS n FROM legal_invoices "
            "WHERE invoice_date >= '2026-01-01'",
            c,
        ).iloc[0]["n"] or 0

    df = pd.DataFrame({
        "metric": ["Active headcount", "Terminations (LTM)", "Open legal matters",
                   "Legal spend YTD"],
        "value": [f"{int(hc):,}", f"{int(term):,}", f"{int(matters_open):,}",
                  f"${int(spend_ytd):,}"],
    })
    narrative = (
        "Here's a snapshot of the business. Ask me about attrition, headcount, "
        "compensation, legal spend by firm, or open matters — I can slice by "
        "department, quarter, or matter type."
    )
    return df, narrative


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _quarter_to_range(quarter: str | None) -> tuple[str, str]:
    """Return (WHERE clause, human label) for a quarter string like 'Q1 2026'."""
    if not quarter:
        return "", "across all time"

    import re
    m = re.match(r"Q([1-4])\s+(\d{4})", quarter)
    if not m:
        return "", "across all time"

    q = int(m.group(1))
    y = int(m.group(2))
    start_month = 3 * (q - 1) + 1
    end_month = start_month + 3
    start = f"{y}-{start_month:02d}-01"
    end = f"{y}-{end_month:02d}-01" if end_month <= 12 else f"{y+1}-01-01"
    clause = f"WHERE invoice_date >= '{start}' AND invoice_date < '{end}'"
    return clause, f"in {quarter}"


# Registry the app uses to dispatch routed intents to query functions.
QUERY_REGISTRY = {
    "attrition_by_department": attrition_by_department,
    "headcount_by_department": headcount_by_department,
    "compensation_by_level": compensation_by_level,
    "hires_last_12_months": hires_last_12_months,
    "tenure_by_department": tenure_by_department,
    "legal_spend_by_firm": legal_spend_by_firm,
    "legal_spend_trend": legal_spend_trend,
    "open_matters_by_type": open_matters_by_type,
    "spend_by_matter_type": spend_by_matter_type,
    "overview": overview,
}
