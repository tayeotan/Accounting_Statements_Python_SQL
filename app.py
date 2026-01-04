import gradio as gr
import plotly.express as px
from core import (
    init_db,
    get_trial_balance_long,
    get_balance_sheet_detail_totals,
    get_income_statement_summary,
    apply_manual_entries,   # <- add this
)


# ---------- Helper functions ----------

def view_trial_balance():
    df = get_trial_balance_long()
    # totals like Streamlit metrics
    totals = df[["debit", "credit"]].sum()
    total_debit = float(totals["debit"])
    total_credit = float(totals["credit"])

    # bar chart: debit balances by account
    fig = px.bar(
        df,
        x="account_name",
        y="debit",
        color="section",
        title="Debit balances by account",
    )

    # Gradio Dataframe expects a plain pandas DataFrame
    return df, total_debit, total_credit, fig


def view_balance_sheet():
    detail, totals = get_balance_sheet_detail_totals()
    # first row has the totals
    row = totals.iloc[0]
    total_assets = float(row["total_assets"])
    total_liab_plus_eq = float(row["total_liabilities"] + row["total_equity"])
    balance_diff = float(row["balance_difference"])

    fig = px.bar(
        detail,
        x="account_name",
        y="balance",
        color="section",
        title="Balance sheet accounts",
    )

    return detail, totals, total_assets, total_liab_plus_eq, balance_diff, fig


def view_income_statement():
    df = get_income_statement_summary()
    if df.empty:
        # Keep shapes consistent; Gradio still needs outputs
        return df, 0.0, 0.0, 0.0

    row = df.iloc[0]
    total_rev = float(row["total_revenue"])
    total_exp = float(row["total_expense"])
    net_income = float(row["net_income"])
    return df, total_rev, total_exp, net_income


# ---------- Gradio layout ----------

with gr.Blocks(title="Accounting Statements Dashboard") as demo:
    gr.Markdown(
        """
        # Accounting Statements Dashboard (Demo)
        Trial Balance, Balance Sheet, and Income Statement computed from SQLite.
        """
    )

    # Top row: reseed button like Streamlit
    with gr.Row():
        init_btn = gr.Button("Re-seed sample data", variant="primary")
        init_msg = gr.Markdown()

        def reinit():
            init_db()
            return "Sample data re-seeded."

        init_btn.click(fn=reinit, outputs=init_msg)

    # Tabs
    with gr.Tab("Trial Balance"):
        tb_btn = gr.Button("Load Trial Balance", variant="secondary")
        tb_df = gr.Dataframe(interactive=False, wrap=True)

        with gr.Row():
            tb_total_debit = gr.Number(label="Total Debits", interactive=False)
            tb_total_credit = gr.Number(label="Total Credits", interactive=False)

        tb_plot = gr.Plot(label="Debit balances by account")

        tb_btn.click(
            fn=view_trial_balance,
            outputs=[tb_df, tb_total_debit, tb_total_credit, tb_plot],
        )

    with gr.Tab("Balance Sheet"):
        bs_btn = gr.Button("Load Balance Sheet", variant="secondary")

        gr.Markdown("#### Detail by account")
        bs_detail_df = gr.Dataframe(interactive=False, wrap=True)

        gr.Markdown("#### Section totals")
        bs_totals_df = gr.Dataframe(interactive=False, wrap=True)

        with gr.Row():
            bs_total_assets = gr.Number(label="Total Assets", interactive=False)
            bs_total_lpe = gr.Number(
                label="Total Liabilities + Equity", interactive=False
            )
            bs_diff = gr.Number(label="Assets - (L + E)", interactive=False)

        bs_plot = gr.Plot(label="Balance sheet accounts")

        bs_btn.click(
            fn=view_balance_sheet,
            outputs=[
                bs_detail_df,
                bs_totals_df,
                bs_total_assets,
                bs_total_lpe,
                bs_diff,
                bs_plot,
            ],
        )

    with gr.Tab("Income Statement"):
        is_btn = gr.Button("Load Income Statement", variant="secondary")
        is_df = gr.Dataframe(interactive=False, wrap=True)

        with gr.Row():
            is_total_rev = gr.Number(label="Total Revenue", interactive=False)
            is_total_exp = gr.Number(label="Total Expense", interactive=False)
            is_net_income = gr.Number(label="Net Income", interactive=False)

        is_btn.click(
            fn=view_income_statement,
            outputs=[is_df, is_total_rev, is_total_exp, is_net_income],
        )

    with gr.Tab("Manual Input"):
        gr.Markdown(
            """
            ### Manual journal entry input

            1. Optionally clear existing data.
            2. Enter or edit accounts and journal entries in the tables below.
            3. Click **Apply entries** to write them to the database and update all statements.
            """
        )

        # Default accounts: rows as lists so Gradio shows them correctly
        default_accounts = [
            [1, "Cash", "Asset"],
            [2, "Accounts Receivable", "Asset"],
            [3, "Accounts Payable", "Liability"],
            [4, "Owner Capital", "Equity"],
            [5, "Service Revenue", "Revenue"],
            [6, "Rent Expense", "Expense"],
        ]

        # Default journal entries
        default_journal = [
            [1, 1, "2025-01-01", 50000.0, 0.0],   # Owner invests cash
            [2, 7, "2025-01-01", 0.0, 50000.0],
            [3, 3, "2025-01-05", 12000.0, 0.0],   # Purchase inventory
            [4, 5, "2025-01-05", 0.0, 12000.0],
        ]


        gr.Markdown("#### Accounts setup")
        accounts_df_comp = gr.Dataframe(
            headers=["account_id", "account_name", "account_type"],
            value=default_accounts,
            datatype=["number", "str", "str"],
            interactive=True,
            wrap=True,
            row_count=(len(default_accounts), "dynamic"),
        )

        gr.Markdown("#### Journal entries")
        journal_df_comp = gr.Dataframe(
            headers=["entry_id", "account_id", "entry_date", "debit", "credit"],
            value=default_journal,
            datatype=["number", "number", "str", "number", "number"],
            interactive=True,
            wrap=True,
            row_count=(len(default_journal), "dynamic"),
        )

        apply_btn = gr.Button("Apply entries to database", variant="primary")
        apply_msg = gr.Markdown()

        def apply_entries(accounts, journal):
            import pandas as pd
            
            accounts_df = pd.DataFrame(
                accounts,
                columns=["account_id", "account_name", "account_type"],
            )
            journal_df = pd.DataFrame(
                journal,
                columns=["entry_id", "account_id", "entry_date", "debit", "credit"],
            )

            apply_manual_entries(accounts_df, journal_df)
            return "Entries applied. Switch to the other tabs to see the updated Trial Balance, Balance Sheet, and Income Statement."


        apply_btn.click(
            fn=apply_entries,
            inputs=[accounts_df_comp, journal_df_comp],
            outputs=apply_msg,
        )



if __name__ == "__main__":
    init_db()
    demo.launch()