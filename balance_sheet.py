import streamlit as st
import pandas as pd
import plotly.express as px
import sqlalchemy
import pyodbc

from contextlib import contextmanager
from typing import Iterable, Any, Optional, Tuple

# ------------------------------------------------------------------
# DB connection via Streamlit's sql connection
# ------------------------------------------------------------------

def get_sql_connection():
    # Uses [connections.mssql] from secrets.toml
    return st.connection("mssql", type="sql")


def fetch_df(sql: str, params=None) -> pd.DataFrame:
    conn = get_sql_connection()
    return conn.query(sql, params=params)


@contextmanager
def get_connection():
    # For non-query operations where you need a raw DBAPI connection
    conn = get_sql_connection().raw_connection()
    try:
        yield conn
    finally:
        conn.close()


def execute_non_query(sql: str, params: Optional[Iterable[Any]] = None) -> int:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        rows = cur.rowcount
        conn.commit()
    return rows



# ======================================================================================
# ACCOUNTING SQL QUERIES (YOUR LOGIC)
# ======================================================================================

SQL_SEED_DATA = """
-- Clear data (respect FK from journal_entries to accounts)
TRUNCATE TABLE journal_entries;
DELETE FROM journal_entries;
DELETE FROM accounts;

INSERT INTO accounts (account_id, account_name, account_type) VALUES
-- Assets
(1, 'Cash',                 'Asset'),
(2, 'Accounts Receivable',  'Asset'),
(3, 'Inventory',            'Asset'),
(4, 'Equipment',            'Asset'),
-- Liabilities
(5, 'Accounts Payable',     'Liability'),
(6, 'Notes Payable',        'Liability'),
-- Equity
(7, 'Owner Capital',        'Equity'),
(8, 'Owner Drawings',       'Equity'),
-- Revenues
(9,  'Service Revenue',     'Revenue'),
(10, 'Sales Revenue',       'Revenue'),
-- Expenses
(11, 'Rent Expense',        'Expense'),
(12, 'Wages Expense',       'Expense'),
(13, 'Supplies Expense',    'Expense');

INSERT INTO journal_entries (entry_id, account_id, entry_date, debit, credit) VALUES
-- Owner invests cash 50,000
(1,  1, '2025-01-01', 50000.00, 0.00),
(2,  7, '2025-01-01',     0.00, 50000.00),

-- Purchase inventory on account 12,000
(3,  3, '2025-01-05', 12000.00, 0.00),
(4,  5, '2025-01-05',     0.00, 12000.00),

-- Pay cash on account 5,000
(5,  5, '2025-01-10',  5000.00, 0.00),
(6,  1, '2025-01-10',     0.00,  5000.00),

-- Provide services for cash 18,000
(7,  1,  '2025-01-15', 18000.00, 0.00),
(8,  9,  '2025-01-15',     0.00, 18000.00),

-- Sell inventory on account 25,000 (ignore COGS for simplicity)
(9,  2,  '2025-01-20', 25000.00, 0.00),
(10, 10, '2025-01-20',     0.00, 25000.00),

-- Pay rent 3,000
(11, 11, '2025-01-25',  3000.00, 0.00),
(12, 1,  '2025-01-25',     0.00,  3000.00),

-- Pay wages 4,000
(13, 12, '2025-01-28',  4000.00, 0.00),
(14, 1,  '2025-01-28',     0.00,  4000.00),

-- Buy equipment for cash 10,000
(15, 4,  '2025-01-30', 10000.00, 0.00),
(16, 1,  '2025-01-30',     0.00, 10000.00),

-- Owner draws 2,000
(17, 8, '2025-01-31',  2000.00, 0.00),
(18, 1, '2025-01-31',     0.00,  2000.00);
"""

SQL_ACCOUNT_BALANCES = """
WITH account_balances AS (
    SELECT
        a.account_id,
        a.account_name,
        a.account_type,
        SUM(j.debit) AS total_debit,
        SUM(j.credit) AS total_credit,
        SUM(j.debit - j.credit) AS balance
    FROM journal_entries j
    JOIN accounts a
        ON j.account_id = a.account_id
    GROUP BY
        a.account_id,
        a.account_name,
        a.account_type
)
SELECT *
FROM account_balances
ORDER BY account_type, account_name;
"""

SQL_TRIAL_BALANCE_LONG = SQL_TRIAL_BALANCE_LONG = """
WITH tb AS (
    SELECT
        a.account_name,
        a.account_type,
        SUM(j.debit - j.credit) AS balance
    FROM journal_entries j
    JOIN accounts a ON j.account_id = a.account_id
    GROUP BY a.account_name, a.account_type
)
SELECT
    CASE account_type
        WHEN 'Asset'     THEN 'Assets'
        WHEN 'Liability' THEN 'Liabilities'
        WHEN 'Equity'    THEN 'Equity'
        WHEN 'Revenue'   THEN 'Revenues'
        WHEN 'Expense'   THEN 'Expenses'
        ELSE account_type
    END AS section,
    account_name,
    CASE WHEN balance >= 0 THEN balance ELSE 0 END AS debit,
    CASE WHEN balance <  0 THEN -balance ELSE 0 END AS credit
FROM tb
ORDER BY
    CASE
        WHEN account_type = 'Asset'     THEN 1
        WHEN account_type = 'Liability' THEN 2
        WHEN account_type = 'Equity'    THEN 3
        WHEN account_type = 'Revenue'   THEN 4
        WHEN account_type = 'Expense'   THEN 5
        ELSE 6
    END,
    account_name;
"""

SQL_TRIAL_BALANCE_SHORT = """
WITH tb AS (
    SELECT
        a.account_id,
        a.account_name,
        a.account_type,
        SUM(j.debit - j.credit) AS balance
    FROM journal_entries j
    JOIN accounts a ON j.account_id = a.account_id
    GROUP BY a.account_id, a.account_name, a.account_type
)
SELECT
    account_id,
    account_name,
    account_type,
    CASE WHEN balance >= 0 THEN balance ELSE 0 END AS debit,
    CASE WHEN balance <  0 THEN -balance ELSE 0 END AS credit
FROM tb
ORDER BY account_name;
"""

SQL_BS_DETAIL = """
WITH bs_accounts AS (
    SELECT
        a.account_name,
        a.account_type,
        SUM(j.debit - j.credit) AS balance
    FROM journal_entries j
    JOIN accounts a ON j.account_id = a.account_id
    WHERE a.account_type IN ('Asset','Liability','Equity')
    GROUP BY a.account_name, a.account_type
)
SELECT
    CASE account_type
        WHEN 'Asset'     THEN 'Assets'
        WHEN 'Liability' THEN 'Liabilities'
        WHEN 'Equity'    THEN 'Equity'
    END AS section,
    account_name,
    balance
FROM bs_accounts
ORDER BY
    CASE account_type
        WHEN 'Asset'     THEN 1
        WHEN 'Liability' THEN 2
        WHEN 'Equity'    THEN 3
    END,
    account_name;
"""

SQL_BS_TOTALS = """
WITH bs_accounts AS (
    SELECT
        a.account_name,
        a.account_type,
        SUM(j.debit - j.credit) AS balance
    FROM journal_entries j
    JOIN accounts a ON j.account_id = a.account_id
    WHERE a.account_type IN ('Asset','Liability','Equity')
    GROUP BY a.account_name, a.account_type
),
section_totals AS (
    SELECT
        SUM(CASE WHEN account_type = 'Asset'     THEN balance ELSE 0 END) AS total_assets,
        SUM(CASE WHEN account_type = 'Liability' THEN balance ELSE 0 END) AS total_liabilities,
        SUM(CASE WHEN account_type = 'Equity'    THEN balance ELSE 0 END) AS total_equity
    FROM bs_accounts
)
SELECT
    total_assets,
    total_liabilities,
    total_equity,
    total_assets - (total_liabilities + total_equity) AS balance_difference
FROM section_totals;
"""

SQL_INCOME_STATEMENT = """
WITH ie AS (
    SELECT
        a.account_type,
        a.account_name,
        SUM(j.debit - j.credit) AS balance
    FROM journal_entries j
    JOIN accounts a ON j.account_id = a.account_id
    WHERE a.account_type IN ('Revenue', 'Expense')
    GROUP BY a.account_type, a.account_name
),
totals AS (
    SELECT
        SUM(CASE WHEN account_type = 'Revenue' THEN -balance ELSE 0 END) AS total_revenue,
        SUM(CASE WHEN account_type = 'Expense' THEN  balance ELSE 0 END) AS total_expense
    FROM ie
)
SELECT
    total_revenue,
    total_expense,
    total_revenue - total_expense AS net_income
FROM totals;
"""

# Helpers wrapping SQL into Python functions

def seed_data():
    with get_connection() as conn:
        cur = conn.cursor()
        # split on semicolon is enough here because no GO batches
        for stmt in [s for s in SQL_SEED_DATA.split(";") if s.strip()]:
            cur.execute(stmt)
        conn.commit()

def clear_data_only():
    """Clear journal_entries and accounts without inserting seed rows."""
    with get_connection() as conn:
        cur = conn.cursor()
        # respect FK: clear journal_entries first
        cur.execute("TRUNCATE TABLE journal_entries;")
        cur.execute("DELETE FROM journal_entries;")
        cur.execute("DELETE FROM accounts;")
        conn.commit()

def get_trial_balance_long() -> pd.DataFrame:
    return fetch_df(SQL_TRIAL_BALANCE_LONG)


def get_trial_balance_short() -> pd.DataFrame:
    return fetch_df(SQL_TRIAL_BALANCE_SHORT)


def get_account_balances() -> pd.DataFrame:
    return fetch_df(SQL_ACCOUNT_BALANCES)


def get_balance_sheet_detail_totals() -> Tuple[pd.DataFrame, pd.DataFrame]:
    detail = fetch_df(SQL_BS_DETAIL)
    totals = fetch_df(SQL_BS_TOTALS)
    return detail, totals


def get_income_statement_summary() -> pd.DataFrame:
    return fetch_df(SQL_INCOME_STATEMENT)


# ======================================================================================
# STREAMLIT UI
# ======================================================================================

st.set_page_config(
    page_title="Accounting Statements Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Accounting Statements Dashboard")
st.caption("SQL Server + Python + Streamlit – Trial Balance, Balance Sheet, Income Statement")

# Sidebar controls
with st.sidebar:
    st.header("Controls")

    data_mode = st.radio(
        "Data source",
        ["Seed sample data", "Manual input"],
        index=0,
        help="Choose whether to use built-in seed data or your own journal entries.",
    )

    if data_mode == "Seed sample data":
        if st.button("Re-seed sample data"):
            seed_data()
            st.success("Sample data re-seeded.")
    else:
        st.info("Manual input mode: use the 'Manual Input' tab to enter entries, then click 'Apply entries'.")

    show_debug = st.checkbox("Show raw account_balances debug table", value=False)

# Tabs for different statements
tab_trial, tab_bs, tab_is, tab_analysis, tab_manual = st.tabs(
    ["Trial Balance", "Balance Sheet", "Income Statement", "Analysis / Debug", "Manual Input"]
)

# ---- Trial Balance tab ----
with tab_trial:
    st.subheader("Trial Balance (grouped by section)")
    tb_long = get_trial_balance_long()
    st.dataframe(tb_long, use_container_width=True)

    totals = tb_long[["debit", "credit"]].sum()
    c1, c2 = st.columns(2)
    c1.metric("Total Debits", f"{totals['debit']:,.2f}")
    c2.metric("Total Credits", f"{totals['credit']:,.2f}")

    fig = px.bar(
        tb_long,
        x="account_name",
        y="debit",
        color="section",
        title="Debit balances by account",
    )
    st.plotly_chart(fig, use_container_width=True)

# ---- Balance Sheet tab ----
with tab_bs:
    st.subheader("Balance Sheet")
    bs_detail, bs_totals = get_balance_sheet_detail_totals()

    st.markdown("#### Detail by account")
    st.dataframe(bs_detail, use_container_width=True)

    st.markdown("#### Section totals")
    st.table(bs_totals.style.format("{:,.2f}"))

    row = bs_totals.iloc[0]
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Assets", f"{row['total_assets']:,.2f}")
    c2.metric(
        "Total Liabilities + Equity",
        f"{row['total_liabilities'] + row['total_equity']:,.2f}",
    )
    c3.metric("Assets - (L + E)", f"{row['balance_difference']:,.2f}")

    fig_bs = px.bar(
        bs_detail,
        x="account_name",
        y="balance",
        color="section",
        title="Balance sheet accounts",
    )
    st.plotly_chart(fig_bs, use_container_width=True)

# ---- Income Statement tab ----
with tab_is:
    st.subheader("Income Statement (summary)")
    is_df = get_income_statement_summary()

    if is_df.empty:
        st.info("No revenue/expense data yet. Enter journal entries in Manual Input and click 'Apply entries'.")
    else:
        numeric_cols = ["total_revenue", "total_expense", "net_income"]

        # Ensure numeric and replace None/NaN with 0
        for col in numeric_cols:
            is_df[col] = pd.to_numeric(is_df[col], errors="coerce").fillna(0)

        # Option A: no Styler (simplest, no formatting error possible)
        st.dataframe(is_df, use_container_width=True)

        # If you still want formatted table, uncomment this instead:
        # st.table(is_df.style.format({col: "{:,.2f}" for col in numeric_cols}))

        row = is_df.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Revenue", f"{row['total_revenue']:,.2f}")
        c2.metric("Total Expense", f"{row['total_expense']:,.2f}")
        c3.metric("Net Income", f"{row['net_income']:,.2f}")

# ---- Analysis / Debug tab ----
with tab_analysis:
    st.subheader("Analysis & Debug")
    if show_debug:
        st.markdown("#### Raw account_balances CTE output")
        ab = get_account_balances()
        st.dataframe(ab, use_container_width=True)

    st.markdown("#### Trial balance per account")
    tb_short = get_trial_balance_short()
    st.dataframe(tb_short, use_container_width=True)

    st.markdown(
        "Use this tab to verify that the SQL calculations match the accounting logic."
    )

# ---- Manual Input tab ----
with tab_manual:
    st.subheader("Manual journal entry input")

    if data_mode != "Manual input":
        st.warning("Switch 'Data source' to 'Manual input' in the sidebar to use this tab.")
    else:
        st.markdown(
            "1. Optionally clear existing data.\n"
            "2. Enter or edit journal entries in the table.\n"
            "3. Click **Apply entries** to write them to SQL and update all statements."
        )

        # Step 1: optional clear
        if st.button("Clear all data (accounts & journal_entries)", type="secondary"):
            clear_data_only()
            st.success("All data cleared. You can now define your own accounts and entries.")

        st.markdown("#### Accounts setup")

        # Provide a simple editable accounts table
        default_accounts = pd.DataFrame(
            [
                {"account_id": 1, "account_name": "Cash", "account_type": "Asset"},
                {"account_id": 2, "account_name": "Accounts Receivable", "account_type": "Asset"},
                {"account_id": 3, "account_name": "Accounts Payable", "account_type": "Liability"},
                {"account_id": 4, "account_name": "Owner Capital", "account_type": "Equity"},
                {"account_id": 5, "account_name": "Service Revenue", "account_type": "Revenue"},
                {"account_id": 6, "account_name": "Rent Expense", "account_type": "Expense"},
            ]
        )

        accounts_df = st.data_editor(
            default_accounts,
            num_rows="dynamic",
            use_container_width=True,
            key="accounts_editor",
        )

        st.markdown("#### Journal entries")

        default_entries = pd.DataFrame(
            [
                {"entry_id": 1, "account_id": 1, "entry_date": "2025-01-01", "debit": 0.0, "credit": 0.0},
                {"entry_id": 2, "account_id": 2, "entry_date": "2025-01-01", "debit": 0.0, "credit": 0.0},
            ]
        )

        journal_df = st.data_editor(
            default_entries,
            num_rows="dynamic",
            use_container_width=True,
            key="journal_editor",
        )

        if st.button("Apply entries to database", type="primary"):
            # Write accounts and journal entries to SQL
            with get_connection() as conn:
                cur = conn.cursor()

                # Clear existing
                cur.execute("TRUNCATE TABLE journal_entries;")
                cur.execute("DELETE FROM journal_entries;")
                cur.execute("DELETE FROM accounts;")

                # Insert accounts
                for _, row in accounts_df.iterrows():
                    if pd.isna(row["account_id"]) or pd.isna(row["account_name"]) or pd.isna(row["account_type"]):
                        continue
                    cur.execute(
                        "INSERT INTO accounts (account_id, account_name, account_type) VALUES (?, ?, ?);",
                        int(row["account_id"]),
                        str(row["account_name"]),
                        str(row["account_type"]),
                    )

                # Insert journal entries
                for _, row in journal_df.iterrows():
                    if pd.isna(row["entry_id"]) or pd.isna(row["account_id"]):
                        continue
                    debit = float(row["debit"]) if not pd.isna(row["debit"]) else 0.0
                    credit = float(row["credit"]) if not pd.isna(row["credit"]) else 0.0
                    date_val = row["entry_date"] if not pd.isna(row["entry_date"]) else "2025-01-01"

                    cur.execute(
                        """
                        INSERT INTO journal_entries (entry_id, account_id, entry_date, debit, credit)
                        VALUES (?, ?, ?, ?, ?);
                        """,
                        int(row["entry_id"]),
                        int(row["account_id"]),
                        str(date_val),
                        debit,
                        credit,
                    )

                conn.commit()


            st.success("Entries applied. Switch to the other tabs to see updated Trial Balance, Balance Sheet, and Income Statement.")

