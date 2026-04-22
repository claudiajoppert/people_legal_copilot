"""
Synthetic data generation for the People & Legal Copilot.

Generates HR and Legal Ops data with realistic patterns so the demo produces
meaningful answers to stakeholder-style questions (e.g. higher attrition in
Engineering, legal spend concentrated in a handful of outside counsel firms,
compensation banding by level, etc.).

Run directly to regenerate the SQLite database:
    python -m data.generate
"""

from __future__ import annotations

import random
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
DB_PATH = Path(__file__).parent / "warehouse.db"

DEPARTMENTS = {
    # name -> (headcount_target, base_attrition_rate_annual, avg_tenure_years)
    "Engineering":   (340, 0.18, 2.4),
    "Product":       (85,  0.14, 2.9),
    "Design":        (45,  0.12, 3.1),
    "Sales":         (180, 0.22, 1.8),
    "Marketing":     (70,  0.15, 2.6),
    "Customer Success": (120, 0.19, 2.1),
    "Finance":       (55,  0.09, 4.2),
    "People":        (35,  0.11, 3.4),
    "Legal":         (25,  0.07, 4.6),
    "Operations":    (45,  0.10, 3.7),
}

LEVELS = ["IC1", "IC2", "IC3", "IC4", "IC5", "M1", "M2", "M3", "M4"]
LEVEL_COMP = {
    "IC1": 95_000,  "IC2": 125_000, "IC3": 160_000, "IC4": 205_000, "IC5": 260_000,
    "M1":  190_000, "M2":  240_000, "M3":  310_000, "M4":  410_000,
}
LOCATIONS = ["San Francisco", "New York", "Seattle", "Austin", "Remote-US", "London", "Dublin"]
LOCATION_MULT = {
    "San Francisco": 1.00, "New York": 0.98, "Seattle": 0.95,
    "Austin": 0.88, "Remote-US": 0.90, "London": 0.85, "Dublin": 0.78,
}

OUTSIDE_COUNSEL = [
    ("Wilson Sonsini Goodrich & Rosati", "Corporate & Securities", 1150),
    ("Latham & Watkins",                 "M&A",                    1325),
    ("Cooley LLP",                       "Emerging Companies",     1080),
    ("Fenwick & West",                   "IP Litigation",          1045),
    ("Morrison & Foerster",              "Privacy & Cyber",        1195),
    ("Gunderson Dettmer",                "Corporate",               995),
    ("Orrick, Herrington & Sutcliffe",   "Employment",              935),
    ("Perkins Coie",                     "Regulatory",              890),
    ("Baker McKenzie",                   "International Tax",      1070),
    ("Littler Mendelson",                "Employment",              815),
]

MATTER_TYPES = [
    ("Commercial Contract",      "low"),
    ("Employment",               "medium"),
    ("IP Litigation",            "high"),
    ("Regulatory Inquiry",       "high"),
    ("M&A Diligence",            "high"),
    ("Privacy & Data Protection","medium"),
    ("Corporate Governance",     "low"),
    ("Real Estate",              "low"),
    ("Tax Advisory",             "medium"),
    ("Employment Litigation",    "high"),
]

FIRST_NAMES = [
    "Ava","Liam","Noah","Emma","Olivia","Ethan","Sophia","Mason","Isabella","Logan",
    "Mia","Lucas","Harper","Elijah","Amelia","James","Evelyn","Benjamin","Abigail","Henry",
    "Priya","Arjun","Mei","Jin","Fatima","Omar","Aisha","Yusuf","Chen","Hiroshi",
    "Sofía","Diego","Camila","Mateo","Valentina","Santiago","Lucía","Daniel","Elena","Gabriel",
]
LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez",
    "Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin",
    "Patel","Nguyen","Kim","Chen","Wang","Singh","Khan","Ali","Sato","Tanaka",
]


def _set_seeds() -> None:
    random.seed(SEED)
    np.random.seed(SEED)


# ---------------------------------------------------------------------------
# HR data
# ---------------------------------------------------------------------------

def _build_employees() -> pd.DataFrame:
    rows = []
    emp_id = 10000
    today = date(2026, 4, 22)

    for dept, (target, attr_rate, avg_tenure) in DEPARTMENTS.items():
        # Generate slightly more than the current headcount target so there is
        # a pool of terminated employees to create an attrition signal.
        n_total = int(target * 1.35)
        for _ in range(n_total):
            emp_id += 1
            first = random.choice(FIRST_NAMES)
            last = random.choice(LAST_NAMES)

            # Level distribution weighted toward mid-levels
            level = np.random.choice(
                LEVELS,
                p=[0.08, 0.18, 0.22, 0.18, 0.08, 0.10, 0.08, 0.05, 0.03],
            )
            location = np.random.choice(
                LOCATIONS,
                p=[0.22, 0.18, 0.10, 0.08, 0.20, 0.12, 0.10],
            )

            # Compensation = base * location multiplier * personal jitter
            base = LEVEL_COMP[level]
            comp = int(base * LOCATION_MULT[location] * np.random.normal(1.0, 0.07))

            # Hire date: sample tenure years around dept average
            tenure_years = max(0.1, np.random.gamma(2.0, avg_tenure / 2.0))
            hire_date = today - timedelta(days=int(tenure_years * 365))

            # Decide if terminated. Base attrition * multiplier for short tenure.
            tenure_mult = 1.6 if tenure_years < 1.0 else (1.2 if tenure_years < 2.0 else 1.0)
            annual_prob = min(0.95, attr_rate * tenure_mult)
            # Probability that an employee with this tenure profile already left
            # in the last 12 months.
            left_last_12m = np.random.random() < annual_prob * 0.55

            if left_last_12m and tenure_years > 0.3:
                term_offset = random.randint(1, 365)
                term_date = today - timedelta(days=term_offset)
                reason = np.random.choice(
                    ["Voluntary - New Role", "Voluntary - Personal",
                     "Voluntary - Compensation", "Involuntary - Performance",
                     "Involuntary - Reduction in Force"],
                    p=[0.42, 0.18, 0.15, 0.15, 0.10],
                )
                status = "Terminated"
            else:
                term_date = None
                reason = None
                status = "Active"

            # Performance rating (only for currently active)
            perf = np.random.choice(
                ["Exceeds", "Strong", "Meets", "Below"],
                p=[0.15, 0.40, 0.35, 0.10],
            ) if status == "Active" else None

            rows.append({
                "employee_id": emp_id,
                "first_name": first,
                "last_name": last,
                "department": dept,
                "level": level,
                "location": location,
                "hire_date": hire_date.isoformat(),
                "termination_date": term_date.isoformat() if term_date else None,
                "termination_reason": reason,
                "status": status,
                "annual_compensation_usd": comp,
                "performance_rating": perf,
            })

    return pd.DataFrame(rows)


def _build_comp_history(employees: pd.DataFrame) -> pd.DataFrame:
    """One-row-per-employee-per-year compensation history (last 3 years)."""
    rows = []
    for _, emp in employees.iterrows():
        current = emp["annual_compensation_usd"]
        hire = date.fromisoformat(emp["hire_date"])
        for years_ago in [0, 1, 2]:
            effective = date(2026, 1, 1) - timedelta(days=365 * years_ago)
            if effective < hire:
                continue
            # Prior year comp = current / (1 + annual raise)
            raise_pct = np.random.normal(0.055, 0.025)
            comp = int(current / ((1 + raise_pct) ** years_ago))
            rows.append({
                "employee_id": emp["employee_id"],
                "effective_date": effective.isoformat(),
                "annual_compensation_usd": comp,
                "raise_pct": round(raise_pct, 4) if years_ago == 0 else None,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Legal data
# ---------------------------------------------------------------------------

def _build_matters() -> pd.DataFrame:
    rows = []
    matter_id = 50000
    today = date(2026, 4, 22)

    # ~180 matters across the last 24 months, with distribution tilted toward
    # commercial contracts + employment.
    n_matters = 180
    type_weights = np.array([0.28, 0.18, 0.08, 0.06, 0.09, 0.09, 0.07, 0.05, 0.06, 0.04])
    type_weights = type_weights / type_weights.sum()

    for _ in range(n_matters):
        matter_id += 1
        m_idx = np.random.choice(len(MATTER_TYPES), p=type_weights)
        m_type, priority = MATTER_TYPES[m_idx]

        opened_days_ago = random.randint(1, 730)
        opened = today - timedelta(days=opened_days_ago)

        # Higher-priority matters stay open longer
        lifespan = {
            "low":    random.randint(7, 90),
            "medium": random.randint(30, 210),
            "high":   random.randint(90, 540),
        }[priority]

        closed = opened + timedelta(days=lifespan)
        status = "Open" if closed > today else "Closed"
        closed_str = closed.isoformat() if status == "Closed" else None

        # Assign firm based on matter type (soft mapping)
        preferred = {
            "Commercial Contract":      ["Wilson Sonsini Goodrich & Rosati", "Gunderson Dettmer", "Cooley LLP"],
            "Employment":               ["Littler Mendelson", "Orrick, Herrington & Sutcliffe"],
            "Employment Litigation":    ["Littler Mendelson", "Orrick, Herrington & Sutcliffe"],
            "IP Litigation":            ["Fenwick & West", "Latham & Watkins"],
            "Regulatory Inquiry":       ["Perkins Coie", "Morrison & Foerster"],
            "M&A Diligence":            ["Latham & Watkins", "Wilson Sonsini Goodrich & Rosati"],
            "Privacy & Data Protection":["Morrison & Foerster", "Perkins Coie"],
            "Corporate Governance":     ["Wilson Sonsini Goodrich & Rosati", "Cooley LLP"],
            "Real Estate":              ["Baker McKenzie", "Perkins Coie"],
            "Tax Advisory":             ["Baker McKenzie", "Latham & Watkins"],
        }.get(m_type, [f[0] for f in OUTSIDE_COUNSEL])
        firm = random.choice(preferred)

        rows.append({
            "matter_id": matter_id,
            "matter_name": f"{m_type} - {matter_id}",
            "matter_type": m_type,
            "priority": priority,
            "status": status,
            "opened_date": opened.isoformat(),
            "closed_date": closed_str,
            "lead_firm": firm,
            "business_unit": random.choice(list(DEPARTMENTS.keys())),
        })

    return pd.DataFrame(rows)


def _build_invoices(matters: pd.DataFrame) -> pd.DataFrame:
    """Invoice-level legal spend. Each matter gets 1-12 invoices."""
    rows = []
    inv_id = 900000

    firm_rates = {name: rate for name, _, rate in OUTSIDE_COUNSEL}

    for _, m in matters.iterrows():
        priority = m["priority"]
        n_invoices = {
            "low":    random.randint(1, 3),
            "medium": random.randint(2, 6),
            "high":   random.randint(4, 12),
        }[priority]

        opened = date.fromisoformat(m["opened_date"])
        closed_val = m["closed_date"]
        closed = (
            date.fromisoformat(closed_val)
            if isinstance(closed_val, str)
            else date(2026, 4, 22)
        )

        rate = firm_rates.get(m["lead_firm"], 1000)

        for _ in range(n_invoices):
            inv_id += 1
            # Invoice date somewhere between opened and closed
            span = max(1, (closed - opened).days)
            inv_date = opened + timedelta(days=random.randint(0, span))

            # Hours worked: priority-weighted
            hours_mean = {"low": 8, "medium": 24, "high": 65}[priority]
            hours = max(1.0, np.random.gamma(2.5, hours_mean / 2.5))

            # Partner / associate mix
            partner_hours = hours * np.random.uniform(0.15, 0.40)
            associate_hours = hours - partner_hours

            # Effective blended rate (partner premium)
            amount = (partner_hours * rate * 1.6) + (associate_hours * rate * 0.7)
            amount = round(amount, 2)

            rows.append({
                "invoice_id": inv_id,
                "matter_id": m["matter_id"],
                "firm": m["lead_firm"],
                "invoice_date": inv_date.isoformat(),
                "hours_billed": round(hours, 1),
                "partner_hours": round(partner_hours, 1),
                "associate_hours": round(associate_hours, 1),
                "amount_usd": amount,
                "status": random.choice(["Paid", "Paid", "Paid", "Approved", "Pending Review"]),
            })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Write to SQLite
# ---------------------------------------------------------------------------

def build_warehouse(db_path: Path = DB_PATH) -> dict[str, int]:
    """Generate all tables and write them to SQLite. Returns row counts."""
    _set_seeds()

    employees = _build_employees()
    comp_history = _build_comp_history(employees)
    matters = _build_matters()
    invoices = _build_invoices(matters)

    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    try:
        employees.to_sql("employees", conn, index=False)
        comp_history.to_sql("compensation_history", conn, index=False)
        matters.to_sql("legal_matters", conn, index=False)
        invoices.to_sql("legal_invoices", conn, index=False)

        # Helpful indexes
        cur = conn.cursor()
        cur.execute("CREATE INDEX idx_emp_dept ON employees(department)")
        cur.execute("CREATE INDEX idx_emp_status ON employees(status)")
        cur.execute("CREATE INDEX idx_inv_matter ON legal_invoices(matter_id)")
        cur.execute("CREATE INDEX idx_inv_firm ON legal_invoices(firm)")
        cur.execute("CREATE INDEX idx_inv_date ON legal_invoices(invoice_date)")
        conn.commit()
    finally:
        conn.close()

    return {
        "employees": len(employees),
        "compensation_history": len(comp_history),
        "legal_matters": len(matters),
        "legal_invoices": len(invoices),
    }


if __name__ == "__main__":
    counts = build_warehouse()
    print("Warehouse built:")
    for table, n in counts.items():
        print(f"  {table:28s} {n:>6} rows")
    print(f"\nSQLite file: {DB_PATH}")
