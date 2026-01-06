import pandas as pd
import sqlalchemy
from sqlalchemy import text
from contextlib import contextmanager
from typing import Iterable, Any, Optional, Tuple

# SQLite engine used by Gradio / Hugging Face
ENGINE = sqlalchemy.create_engine("sqlite:///accounting.db", future=True)

# DB connection via SQLite

def fetch_df(sql: str, params=None) -> pd.DataFrame:
    with ENGINE.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)

@contextmanager
def get_connection():
    # Transactional connection for non-query operations
    with ENGINE.begin() as conn:
        yield conn

def execute_non_query(sql: str, params: Optional[Iterable[Any]] = None) -> int:
    with get_connection() as conn:
        result = conn.execute(text(sql), params or {})
        return result.rowcount

# ACCOUNTING SQL QUERIES

SQL_SEED_DATA = """
-- Clear data (respect FK from journal_entries to accounts)
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

def seed_data():
    with get_connection() as conn:
        for stmt in [s for s in SQL_SEED_DATA.split(";") if s.strip()]:
            conn.execute(text(stmt))

def clear_data_only():
    """Clear journal_entries and accounts without inserting seed rows."""
    with get_connection() as conn:
        conn.execute(text("DELETE FROM journal_entries;"))
        conn.execute(text("DELETE FROM accounts;"))

def apply_manual_entries(accounts_df: pd.DataFrame, journal_df: pd.DataFrame) -> None:
    """
    Replace all rows in accounts and journal_entries
    with the ones provided from the manual input UI.
    """
    with get_connection() as conn:
        # Clear existing data
        conn.execute(text("DELETE FROM journal_entries;"))
        conn.execute(text("DELETE FROM accounts;"))

        # Insert accounts
        for _, row in accounts_df.iterrows():
            if pd.isna(row.get("account_id")) or pd.isna(row.get("account_name")) or pd.isna(row.get("account_type")):
                continue
            conn.execute(
                text(
                    """
                    INSERT INTO accounts (account_id, account_name, account_type)
                    VALUES (:account_id, :account_name, :account_type)
                    """
                ),
                {
                    "account_id": int(row["account_id"]),
                    "account_name": str(row["account_name"]),
                    "account_type": str(row["account_type"]),
                },
            )

        # Insert journal entries
        for _, row in journal_df.iterrows():
            if pd.isna(row.get("entry_id")) or pd.isna(row.get("account_id")):
                continue

            debit = float(row["debit"]) if not pd.isna(row.get("debit")) else 0.0
            credit = float(row["credit"]) if not pd.isna(row.get("credit")) else 0.0
            date_val = row["entry_date"] if not pd.isna(row.get("entry_date")) else "2025-01-01"

            conn.execute(
                text(
                    """
                    INSERT INTO journal_entries (entry_id, account_id, entry_date, debit, credit)
                    VALUES (:entry_id, :account_id, :entry_date, :debit, :credit)
                    """
                ),
                {
                    "entry_id": int(row["entry_id"]),
                    "account_id": int(row["account_id"]),
                    "entry_date": str(date_val),
                    "debit": debit,
                    "credit": credit,
                },
            )


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

def init_db():
    """Create tables in SQLite (if needed) and seed sample data."""
    with get_connection() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS accounts (
                account_id INTEGER PRIMARY KEY,
                account_name TEXT NOT NULL,
                account_type TEXT NOT NULL
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS journal_entries (
                entry_id INTEGER PRIMARY KEY,
                account_id INTEGER NOT NULL,
                entry_date TEXT NOT NULL,
                debit REAL NOT NULL DEFAULT 0,
                credit REAL NOT NULL DEFAULT 0
            );
        """))
    seed_data()
