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
(1,  1, '2025-01-01', 50000.00, 0.00),   -- Cash
(2,  7, '2025-01-01',     0.00, 50000.00), -- Owner Capital

-- Purchase inventory on account 12,000
(3,  3, '2025-01-05', 12000.00, 0.00),   -- Inventory
(4,  5, '2025-01-05',     0.00, 12000.00), -- A/P

-- Pay cash on account 5,000
(5,  5, '2025-01-10',  5000.00, 0.00),   -- A/P
(6,  1, '2025-01-10',     0.00,  5000.00), -- Cash

-- Provide services for cash 18,000
(7,  1,  '2025-01-15', 18000.00, 0.00),    -- Cash
(8,  9,  '2025-01-15',     0.00, 18000.00),-- Service Revenue

-- Sell inventory on account 25,000 (ignore COGS for simplicity)
(9,  2,  '2025-01-20', 25000.00, 0.00),    -- A/R
(10, 10, '2025-01-20',     0.00, 25000.00),-- Sales Revenue

-- Pay rent 3,000
(11, 11, '2025-01-25',  3000.00, 0.00),    -- Rent Expense
(12, 1,  '2025-01-25',     0.00,  3000.00),-- Cash

-- Pay wages 4,000
(13, 12, '2025-01-28',  4000.00, 0.00),    -- Wages Expense
(14, 1,  '2025-01-28',     0.00,  4000.00),-- Cash

-- Buy equipment for cash 10,000
(15, 4,  '2025-01-30', 10000.00, 0.00),    -- Equipment
(16, 1,  '2025-01-30',     0.00, 10000.00),-- Cash

-- Owner draws 2,000
(17, 8, '2025-01-31',  2000.00, 0.00),     -- Owner Drawings
(18, 1, '2025-01-31',     0.00,  2000.00); -- Cash


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


WITH balances AS (
    SELECT
        a.account_type,
        SUM(j.debit - j.credit) AS balance
    FROM journal_entries j
    JOIN accounts a
        ON j.account_id = a.account_id
    GROUP BY a.account_type
)
SELECT
    account_type,
    balance
FROM balances
WHERE account_type IN ('Asset', 'Liability', 'Equity');


WITH balances AS (
    SELECT
        a.account_type,
        SUM(j.debit - j.credit) AS balance
    FROM journal_entries j
    JOIN accounts a
        ON j.account_id = a.account_id
    GROUP BY a.account_type
),
pivoted AS (
    SELECT
        SUM(CASE WHEN account_type = 'Asset' THEN balance ELSE 0 END) AS assets,
        SUM(CASE WHEN account_type = 'Liability' THEN balance ELSE 0 END) AS liabilities,
        SUM(CASE WHEN account_type = 'Equity' THEN balance ELSE 0 END) AS equity
    FROM balances
)
SELECT
    assets,
    liabilities,
    equity,
    (assets - (liabilities + equity)) AS balance_difference
FROM pivoted;


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


-- 1) Detail by account
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
    *
FROM bs_accounts
ORDER BY
    CASE account_type
        WHEN 'Asset' THEN 1
        WHEN 'Liability' THEN 2
        WHEN 'Equity' THEN 3
    END,
    account_name;

-- 2) Equation check (run as a separate query)
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