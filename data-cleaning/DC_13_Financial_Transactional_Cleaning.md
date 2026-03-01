# 🧹 SQL Data Cleaning — Part 13: Financial & Transactional Data Cleaning
> *Fix double charges, reconcile mismatched totals, validate transaction IDs, detect fraud signals, and ensure ledger integrity*

---

## Table of Contents
- [1. Transaction ID Validation & Deduplication](#1-transaction-id-validation--deduplication)
- [2. Amount Reconciliation](#2-amount-reconciliation)
- [3. Double-Charge & Duplicate Payment Detection](#3-double-charge--duplicate-payment-detection)
- [4. Currency & FX Cleaning](#4-currency--fx-cleaning)
- [5. Ledger Balance Validation](#5-ledger-balance-validation)
- [6. Timestamp & Sequence Integrity](#6-timestamp--sequence-integrity)
- [7. Refund & Reversal Matching](#7-refund--reversal-matching)
- [8. Fraud Signal Detection via Regex & Patterns](#8-fraud-signal-detection-via-regex--patterns)
- [9. Tax & Fee Calculation Validation](#9-tax--fee-calculation-validation)
- [10. Final Financial Data Audit](#10-final-financial-data-audit)

---

## 1. Transaction ID Validation & Deduplication

```sql
-- ─────────────────────────────────────────
-- Validate transaction ID format
-- Expected: TXN-YYYYMMDD-XXXXXXXX (alphanumeric 8 chars)
-- ─────────────────────────────────────────
SELECT
  transaction_id,
  CASE
    WHEN transaction_id IS NULL
      THEN '❌ NULL'
    WHEN transaction_id ~ '^TXN-\d{8}-[A-Z0-9]{8}$'
      THEN '✅ Valid'
    WHEN transaction_id ~ '^\d+$'
      THEN '⚠️  Legacy numeric ID'
    WHEN transaction_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN '⚠️  UUID format (unexpected)'
    ELSE '❌ Non-standard format'
  END AS id_status,
  COUNT(*) AS count
FROM transactions
GROUP BY transaction_id, id_status
ORDER BY count DESC;

-- Detect duplicate transaction IDs (critical error)
SELECT
  transaction_id,
  COUNT(*) AS occurrences,
  ARRAY_AGG(amount ORDER BY created_at) AS amounts,
  ARRAY_AGG(created_at ORDER BY created_at) AS timestamps,
  ARRAY_AGG(status ORDER BY created_at) AS statuses
FROM transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

-- Classify duplicates: exact vs near-duplicate
WITH dup_txns AS (
  SELECT transaction_id
  FROM transactions
  GROUP BY transaction_id HAVING COUNT(*) > 1
),
dup_details AS (
  SELECT t.*,
    COUNT(*) OVER(PARTITION BY transaction_id) AS dup_count,
    ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY created_at ASC) AS rn
  FROM transactions t
  INNER JOIN dup_txns d USING(transaction_id)
)
SELECT
  transaction_id,
  dup_count,
  CASE
    WHEN COUNT(DISTINCT amount)   = 1
     AND COUNT(DISTINCT status)   = 1
     AND COUNT(DISTINCT customer_id) = 1
    THEN '🔴 True duplicate — same in every field'
    WHEN COUNT(DISTINCT amount) > 1
    THEN '🟡 Partial dup — different amounts'
    WHEN COUNT(DISTINCT status) > 1
    THEN '🟡 Partial dup — different statuses (possible retry)'
    ELSE '⚠️  Investigate'
  END AS duplicate_type
FROM dup_details
GROUP BY transaction_id, dup_count
ORDER BY dup_count DESC;

-- Remove true duplicates — keep earliest
DELETE FROM transactions
WHERE id IN (
  SELECT id FROM (
    SELECT id,
      ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY created_at ASC) AS rn
    FROM transactions
  ) ranked
  WHERE rn > 1
);
```

---

## 2. Amount Reconciliation

```sql
-- ─────────────────────────────────────────
-- Validate: order total = sum of line items
-- ─────────────────────────────────────────
SELECT
  o.order_id,
  o.total_amount         AS header_total,
  SUM(oi.line_total)     AS line_items_sum,
  ROUND(o.total_amount - SUM(oi.line_total), 2) AS discrepancy
FROM orders AS o
INNER JOIN order_items AS oi ON o.order_id = oi.order_id
GROUP BY o.order_id, o.total_amount
HAVING ABS(o.total_amount - SUM(oi.line_total)) > 0.01
ORDER BY ABS(discrepancy) DESC;

-- Fix: recalculate order total from line items
UPDATE orders AS o
SET total_amount = line_sums.correct_total
FROM (
  SELECT order_id, ROUND(SUM(line_total), 2) AS correct_total
  FROM order_items
  GROUP BY order_id
) AS line_sums
WHERE o.order_id = line_sums.order_id
  AND ABS(o.total_amount - line_sums.correct_total) > 0.01;

-- ─────────────────────────────────────────
-- Validate: line_total = qty × unit_price × (1 - discount)
-- ─────────────────────────────────────────
SELECT
  order_item_id,
  quantity,
  unit_price,
  discount_rate,
  line_total,
  ROUND(quantity * unit_price * (1 - COALESCE(discount_rate, 0)), 2)
    AS expected_line_total,
  ABS(line_total - ROUND(quantity * unit_price * (1 - COALESCE(discount_rate, 0)), 2))
    AS error_amount
FROM order_items
WHERE ABS(line_total -
  ROUND(quantity * unit_price * (1 - COALESCE(discount_rate, 0)), 2)) > 0.01
ORDER BY error_amount DESC;

-- Fix line totals
UPDATE order_items
SET line_total = ROUND(quantity * unit_price * (1 - COALESCE(discount_rate, 0)), 2)
WHERE ABS(line_total -
  ROUND(quantity * unit_price * (1 - COALESCE(discount_rate, 0)), 2)) > 0.01;

-- ─────────────────────────────────────────
-- Reconcile payments against orders
-- ─────────────────────────────────────────
SELECT
  o.order_id,
  o.total_amount AS order_total,
  COALESCE(SUM(p.amount), 0) AS total_paid,
  o.total_amount - COALESCE(SUM(p.amount), 0) AS balance_due,
  CASE
    WHEN COALESCE(SUM(p.amount), 0) = 0          THEN 'Unpaid'
    WHEN COALESCE(SUM(p.amount), 0) < o.total_amount - 0.01 THEN 'Partially paid'
    WHEN ABS(COALESCE(SUM(p.amount), 0) - o.total_amount) <= 0.01 THEN 'Paid in full'
    WHEN COALESCE(SUM(p.amount), 0) > o.total_amount + 0.01 THEN 'Overpaid'
    ELSE 'Check'
  END AS payment_status
FROM orders AS o
LEFT JOIN payments AS p ON o.order_id = p.order_id
  AND p.status = 'completed'
GROUP BY o.order_id, o.total_amount
ORDER BY balance_due DESC;
```

---

## 3. Double-Charge & Duplicate Payment Detection

```sql
-- ─────────────────────────────────────────
-- Same card, same amount, within 5 minutes
-- ─────────────────────────────────────────
SELECT
  a.payment_id   AS payment_a,
  b.payment_id   AS payment_b,
  a.customer_id,
  a.card_last4,
  a.amount,
  a.created_at   AS time_a,
  b.created_at   AS time_b,
  EXTRACT(EPOCH FROM (b.created_at - a.created_at)) AS seconds_between,
  CASE
    WHEN EXTRACT(EPOCH FROM (b.created_at - a.created_at)) < 60   THEN '🔴 <1 min apart'
    WHEN EXTRACT(EPOCH FROM (b.created_at - a.created_at)) < 300  THEN '🟠 <5 min apart'
    ELSE '🟡 Same day'
  END AS time_risk
FROM payments a
INNER JOIN payments b
  ON a.customer_id = b.customer_id
  AND a.amount     = b.amount
  AND a.card_last4 = b.card_last4
  AND a.payment_id < b.payment_id       -- avoid self-join / pairs twice
  AND a.status = 'completed'
  AND b.status = 'completed'
  AND b.created_at - a.created_at BETWEEN INTERVAL '0' AND INTERVAL '1 day'
ORDER BY seconds_between ASC;

-- ─────────────────────────────────────────
-- Same order paid twice
-- ─────────────────────────────────────────
SELECT
  order_id,
  COUNT(*) AS payment_count,
  SUM(amount) AS total_charged,
  ARRAY_AGG(payment_id ORDER BY created_at) AS payment_ids,
  ARRAY_AGG(amount ORDER BY created_at) AS amounts
FROM payments
WHERE status = 'completed'
GROUP BY order_id
HAVING COUNT(*) > 1
   AND SUM(amount) > MIN(amount)   -- not partial payments summing to total
ORDER BY payment_count DESC;

-- Mark second payment as potential duplicate
WITH ranked_payments AS (
  SELECT
    payment_id,
    order_id,
    ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY created_at ASC) AS rn
  FROM payments
  WHERE status = 'completed'
)
UPDATE payments
SET needs_review = TRUE,
    review_reason = 'possible_double_charge'
WHERE payment_id IN (
  SELECT payment_id FROM ranked_payments
  WHERE rn > 1
  AND order_id IN (
    SELECT order_id FROM payments WHERE status = 'completed'
    GROUP BY order_id HAVING COUNT(*) > 1
  )
);
```

---

## 4. Currency & FX Cleaning

```sql
-- ─────────────────────────────────────────
-- Detect mixed currencies in a single column
-- ─────────────────────────────────────────
SELECT
  currency_code,
  COUNT(*) AS row_count,
  ROUND(MIN(amount), 2) AS min_amount,
  ROUND(MAX(amount), 2) AS max_amount,
  ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
GROUP BY currency_code
ORDER BY row_count DESC;

-- Flag transactions where the amount looks wrong for the currency
-- (e.g. JPY transactions should never have decimals)
SELECT transaction_id, currency_code, amount
FROM transactions
WHERE currency_code IN ('JPY', 'KRW', 'IDR', 'VND')
  AND amount != FLOOR(amount);  -- these currencies don't use sub-units

-- Fix: round zero-decimal currencies
UPDATE transactions
SET amount = ROUND(amount)
WHERE currency_code IN ('JPY', 'KRW', 'IDR', 'VND')
  AND amount != FLOOR(amount);

-- ─────────────────────────────────────────
-- Detect amounts that are implausibly large/small for currency
-- ─────────────────────────────────────────
SELECT transaction_id, currency_code, amount,
  CASE
    -- $1,000,000 USD order is probably wrong
    WHEN currency_code = 'USD' AND amount > 1000000    THEN '⚠️  Suspiciously large USD'
    -- ¥1 JPY order is effectively zero
    WHEN currency_code = 'JPY' AND amount < 10         THEN '⚠️  Suspiciously small JPY'
    -- ₱1 PHP is essentially nothing
    WHEN currency_code = 'PHP' AND amount < 1          THEN '⚠️  Sub-centavo PHP'
    -- EUR > 100,000 is unusual for a single transaction
    WHEN currency_code = 'EUR' AND amount > 100000     THEN '⚠️  Very large EUR'
    ELSE '✅ Plausible range'
  END AS amount_sanity
FROM transactions
WHERE currency_code IN ('USD', 'JPY', 'PHP', 'EUR');

-- ─────────────────────────────────────────
-- Normalize to USD using spot rates
-- ─────────────────────────────────────────
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS amount_usd NUMERIC(18, 4);
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS fx_rate_used NUMERIC(12, 6);
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS fx_rate_date DATE;

UPDATE transactions AS t
SET
  amount_usd    = ROUND(t.amount / r.rate_to_usd, 4),
  fx_rate_used  = r.rate_to_usd,
  fx_rate_date  = r.rate_date
FROM fx_rates AS r
WHERE t.currency_code = r.currency_code
  AND r.rate_date = DATE_TRUNC('day', t.created_at)::date
  AND t.amount_usd IS NULL;

-- Flag transactions with no FX rate available
SELECT transaction_id, currency_code, amount, created_at
FROM transactions
WHERE amount_usd IS NULL
  AND currency_code != 'USD';
```

---

## 5. Ledger Balance Validation

```sql
-- ─────────────────────────────────────────
-- Running balance check: debits vs credits
-- ─────────────────────────────────────────
SELECT
  account_id,
  SUM(CASE WHEN entry_type = 'credit' THEN amount ELSE 0 END) AS total_credits,
  SUM(CASE WHEN entry_type = 'debit'  THEN amount ELSE 0 END) AS total_debits,
  SUM(CASE WHEN entry_type = 'credit' THEN amount ELSE -amount END) AS net_balance,
  CASE
    WHEN SUM(CASE WHEN entry_type = 'credit' THEN amount ELSE -amount END) < 0
    THEN '🔴 Negative balance — investigate'
    ELSE '✅ Positive balance'
  END AS balance_status
FROM ledger_entries
GROUP BY account_id
ORDER BY net_balance ASC;

-- Detect entries with invalid entry_type
SELECT entry_type, COUNT(*) AS count
FROM ledger_entries
GROUP BY entry_type;
-- Should only be 'credit' and 'debit'

-- Fix non-standard entry types
UPDATE ledger_entries
SET entry_type = CASE LOWER(TRIM(entry_type))
  WHEN 'cr'       THEN 'credit'
  WHEN 'c'        THEN 'credit'
  WHEN '+ve'      THEN 'credit'
  WHEN 'positive' THEN 'credit'
  WHEN 'dr'       THEN 'debit'
  WHEN 'd'        THEN 'debit'
  WHEN '-ve'      THEN 'debit'
  WHEN 'negative' THEN 'debit'
  ELSE entry_type
END
WHERE entry_type NOT IN ('credit', 'debit');

-- ─────────────────────────────────────────
-- Verify running balance sequence (no skips, no gaps)
-- ─────────────────────────────────────────
WITH running AS (
  SELECT
    entry_id,
    account_id,
    entry_date,
    amount,
    entry_type,
    SUM(CASE WHEN entry_type = 'credit' THEN amount ELSE -amount END)
      OVER(PARTITION BY account_id ORDER BY entry_date, entry_id
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      AS running_balance,
    LAG(
      SUM(CASE WHEN entry_type = 'credit' THEN amount ELSE -amount END)
      OVER(PARTITION BY account_id ORDER BY entry_date, entry_id
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) OVER(PARTITION BY account_id ORDER BY entry_date, entry_id)
      AS prev_balance
  FROM ledger_entries
)
SELECT *
FROM running
WHERE running_balance < 0   -- balance went negative at this entry
ORDER BY account_id, entry_date;
```

---

## 6. Timestamp & Sequence Integrity

```sql
-- ─────────────────────────────────────────
-- Transactions must be in chronological order
-- ─────────────────────────────────────────
SELECT
  a.transaction_id,
  a.order_id,
  a.created_at AS txn_time,
  o.order_date AS order_time,
  CASE
    WHEN a.created_at::date < o.order_date THEN '🔴 Payment before order'
    WHEN a.created_at::date > o.order_date + 365 THEN '🟡 Payment 1yr+ after order'
    ELSE '✅ OK'
  END AS sequence_check
FROM payments AS a
INNER JOIN orders AS o ON a.order_id = o.order_id
WHERE a.created_at::date < o.order_date
   OR a.created_at::date > o.order_date + 365;

-- ─────────────────────────────────────────
-- Detect transactions at impossible times
-- ─────────────────────────────────────────
SELECT
  transaction_id,
  created_at,
  -- Transaction at 3am could be fraud
  CASE WHEN EXTRACT(HOUR FROM created_at AT TIME ZONE 'UTC') BETWEEN 2 AND 4
    THEN '⚠️  Odd hour (2–4 AM UTC)' ELSE '✅ Normal hour' END AS hour_flag,
  -- Weekend transactions (for B2B)
  CASE WHEN EXTRACT(DOW FROM created_at) IN (0, 6)
    THEN '⚠️  Weekend transaction' ELSE '✅ Weekday' END AS weekday_flag
FROM transactions
WHERE EXTRACT(HOUR FROM created_at AT TIME ZONE 'UTC') BETWEEN 2 AND 4
   OR EXTRACT(DOW FROM created_at) IN (0, 6);

-- ─────────────────────────────────────────
-- Find transactions with same external reference (idempotency check)
-- ─────────────────────────────────────────
SELECT
  external_reference_id,
  COUNT(*) AS count,
  ARRAY_AGG(DISTINCT transaction_id) AS txn_ids,
  ARRAY_AGG(DISTINCT status) AS statuses,
  ARRAY_AGG(DISTINCT amount) AS amounts
FROM transactions
WHERE external_reference_id IS NOT NULL
GROUP BY external_reference_id
HAVING COUNT(*) > 1
ORDER BY count DESC;
```

---

## 7. Refund & Reversal Matching

```sql
-- ─────────────────────────────────────────
-- Match refunds to original transactions
-- ─────────────────────────────────────────
SELECT
  r.transaction_id AS refund_id,
  r.original_transaction_id,
  r.amount AS refund_amount,
  o.amount AS original_amount,
  CASE
    WHEN o.transaction_id IS NULL
      THEN '🔴 No matching original transaction'
    WHEN r.amount > o.amount
      THEN '🔴 Refund exceeds original amount'
    WHEN r.amount = o.amount
      THEN '✅ Full refund'
    ELSE '🟡 Partial refund'
  END AS refund_status
FROM transactions AS r
LEFT JOIN transactions AS o
  ON r.original_transaction_id = o.transaction_id
WHERE r.transaction_type = 'refund'
ORDER BY refund_status;

-- ─────────────────────────────────────────
-- Find orders refunded more than they were charged
-- ─────────────────────────────────────────
SELECT
  order_id,
  SUM(CASE WHEN transaction_type = 'charge'  THEN amount ELSE 0 END) AS total_charged,
  SUM(CASE WHEN transaction_type = 'refund'  THEN amount ELSE 0 END) AS total_refunded,
  SUM(CASE WHEN transaction_type = 'charge'  THEN amount ELSE 0 END) -
  SUM(CASE WHEN transaction_type = 'refund'  THEN amount ELSE 0 END) AS net
FROM transactions
WHERE transaction_type IN ('charge', 'refund')
GROUP BY order_id
HAVING SUM(CASE WHEN transaction_type = 'refund' THEN amount ELSE 0 END) >
       SUM(CASE WHEN transaction_type = 'charge' THEN amount ELSE 0 END)
ORDER BY net ASC;

-- Flag over-refunded orders for manual review
UPDATE orders
SET review_flag = TRUE, review_reason = 'over_refunded'
WHERE order_id IN (
  SELECT order_id FROM transactions
  WHERE transaction_type IN ('charge', 'refund')
  GROUP BY order_id
  HAVING SUM(CASE WHEN transaction_type = 'refund' THEN amount ELSE 0 END) >
         SUM(CASE WHEN transaction_type = 'charge' THEN amount ELSE 0 END)
);
```

---

## 8. Fraud Signal Detection via Regex & Patterns

```sql
-- ─────────────────────────────────────────
-- Suspicious card patterns
-- ─────────────────────────────────────────

-- All-same-digit card last4 (test cards: 4242, 1111, 0000)
SELECT customer_id, card_last4, COUNT(*) AS transactions
FROM payments
WHERE card_last4 ~ '^(.)\1{3}$'   -- same digit repeated 4 times
GROUP BY customer_id, card_last4
ORDER BY transactions DESC;

-- ─────────────────────────────────────────
-- Velocity checks: abnormal transaction patterns
-- ─────────────────────────────────────────

-- Many small transactions (structuring / micro-fraud)
SELECT
  customer_id,
  COUNT(*) AS txn_count,
  SUM(amount) AS total,
  AVG(amount) AS avg_amount,
  MIN(amount) AS min_amount,
  MAX(amount) AS max_amount
FROM transactions
WHERE created_at >= NOW() - INTERVAL '24 hours'
  AND status = 'completed'
GROUP BY customer_id
HAVING COUNT(*) > 20 AND AVG(amount) < 10
ORDER BY txn_count DESC;

-- Many transactions just below reporting thresholds
SELECT
  customer_id,
  COUNT(*) AS suspicious_txns
FROM transactions
WHERE amount BETWEEN 9000 AND 9999    -- just under $10k reporting threshold
  AND created_at >= NOW() - INTERVAL '30 days'
GROUP BY customer_id
HAVING COUNT(*) > 3
ORDER BY suspicious_txns DESC;

-- ─────────────────────────────────────────
-- Regex checks on billing address fields
-- ─────────────────────────────────────────

-- Billing address has no street number (incomplete)
SELECT transaction_id, billing_address
FROM transactions
WHERE billing_address IS NOT NULL
  AND billing_address !~ '^\d+'       -- doesn't start with a number
  AND billing_address !~ 'PO Box'     -- not a PO box either
  AND billing_address != '';

-- Billing ZIP doesn't match issuing country
SELECT
  t.transaction_id,
  t.billing_country,
  t.billing_zip,
  CASE
    WHEN t.billing_country = 'US' AND t.billing_zip !~ '^\d{5}$'   THEN '⚠️  US record, non-US ZIP'
    WHEN t.billing_country = 'CA' AND t.billing_zip !~ '^[A-Z]\d[A-Z] ?\d[A-Z]\d$' THEN '⚠️  CA record, non-CA postal'
    ELSE '✅ OK'
  END AS zip_country_mismatch
FROM transactions t
WHERE billing_country IN ('US', 'CA')
  AND zip_country_mismatch != '✅ OK';
```

---

## 9. Tax & Fee Calculation Validation

```sql
-- ─────────────────────────────────────────
-- Validate that tax = subtotal × tax_rate
-- ─────────────────────────────────────────
SELECT
  order_id,
  subtotal,
  tax_rate,
  tax_amount,
  ROUND(subtotal * tax_rate, 2) AS expected_tax,
  ABS(tax_amount - ROUND(subtotal * tax_rate, 2)) AS tax_error
FROM orders
WHERE tax_rate IS NOT NULL
  AND ABS(tax_amount - ROUND(subtotal * tax_rate, 2)) > 0.02
ORDER BY tax_error DESC;

-- Fix incorrect tax amounts
UPDATE orders
SET tax_amount = ROUND(subtotal * tax_rate, 2)
WHERE tax_rate IS NOT NULL
  AND ABS(tax_amount - ROUND(subtotal * tax_rate, 2)) > 0.02;

-- ─────────────────────────────────────────
-- Validate: grand_total = subtotal + tax + shipping - discount
-- ─────────────────────────────────────────
SELECT
  order_id,
  subtotal,
  tax_amount,
  shipping_fee,
  discount_amount,
  grand_total,
  ROUND(subtotal + tax_amount + shipping_fee - discount_amount, 2) AS expected_total,
  ABS(grand_total -
    ROUND(subtotal + tax_amount + shipping_fee - discount_amount, 2)) AS total_error
FROM orders
WHERE ABS(grand_total -
  ROUND(subtotal + COALESCE(tax_amount,0) + COALESCE(shipping_fee,0)
        - COALESCE(discount_amount,0), 2)) > 0.01
ORDER BY total_error DESC;

-- Fix grand totals
UPDATE orders
SET grand_total = ROUND(
  subtotal
  + COALESCE(tax_amount,    0)
  + COALESCE(shipping_fee,  0)
  - COALESCE(discount_amount, 0),
  2
)
WHERE ABS(grand_total -
  ROUND(subtotal + COALESCE(tax_amount,0) + COALESCE(shipping_fee,0)
        - COALESCE(discount_amount,0), 2)) > 0.01;

-- ─────────────────────────────────────────
-- Detect negative tax (should never happen)
-- ─────────────────────────────────────────
SELECT order_id, tax_amount, subtotal
FROM orders
WHERE tax_amount < 0;

UPDATE orders
SET tax_amount = ABS(tax_amount)
WHERE tax_amount < 0;

-- Detect tax rate outside realistic range (0% – 30%)
SELECT order_id, tax_rate, subtotal, tax_amount
FROM orders
WHERE tax_rate NOT BETWEEN 0 AND 0.30
  AND tax_rate IS NOT NULL;
```

---

## 10. Final Financial Data Audit

```sql
-- ════════════════════════════════════════════════
--  FINANCIAL DATA HEALTH REPORT
-- ════════════════════════════════════════════════
SELECT
  'transactions' AS table_name,

  -- ID integrity
  COUNT(*) FILTER (WHERE transaction_id IS NULL)       AS null_txn_id,
  (SELECT COUNT(*) FROM (
    SELECT transaction_id FROM transactions
    GROUP BY transaction_id HAVING COUNT(*) > 1
  ) d)                                                  AS duplicate_txn_ids,

  -- Amount integrity
  COUNT(*) FILTER (WHERE amount IS NULL)               AS null_amount,
  COUNT(*) FILTER (WHERE amount <= 0)                  AS zero_or_negative,
  COUNT(*) FILTER (WHERE amount != ROUND(amount, 2))   AS excess_precision,
  COUNT(*) FILTER (WHERE amount > 100000)              AS suspiciously_large,

  -- Currency integrity
  COUNT(*) FILTER (WHERE currency_code IS NULL)        AS null_currency,
  COUNT(*) FILTER (WHERE currency_code NOT IN (
    'USD','EUR','GBP','JPY','PHP','CAD','AUD','SGD','HKD','CNY'
  ))                                                    AS unknown_currency,

  -- Time integrity
  COUNT(*) FILTER (WHERE created_at IS NULL)           AS null_timestamp,
  COUNT(*) FILTER (WHERE created_at > NOW())           AS future_transactions,

  -- Status integrity
  COUNT(*) FILTER (WHERE status NOT IN (
    'completed','pending','failed','refunded','reversed','disputed'
  ))                                                    AS invalid_status,

  -- Summary
  COUNT(*) AS total_rows,
  ROUND(SUM(CASE WHEN currency_code = 'USD' THEN amount ELSE 0 END), 2) AS usd_total,
  ROUND(AVG(amount), 2)                                AS avg_amount

FROM transactions;
```

---

## 📌 Financial Cleaning Quick Reference

| Problem | Fix |
|---------|-----|
| Duplicate transaction ID | `ROW_NUMBER() OVER(PARTITION BY txn_id ORDER BY created_at)` → keep rn=1 |
| Order total ≠ line sum | `UPDATE orders SET total = (SELECT SUM(line_total)...)` |
| Line total wrong | `SET line_total = ROUND(qty * price * (1 - discount), 2)` |
| Double charge | Self-join on `customer_id + amount + card + time_window` |
| JPY has decimals | `ROUND(amount)` for zero-decimal currencies |
| Over-refunded order | Sum charges vs refunds per order → flag negatives |
| Tax amount wrong | `SET tax_amount = ROUND(subtotal * tax_rate, 2)` |
| Grand total wrong | `subtotal + tax + shipping - discount` recalc |
| Payment before order | `JOIN orders ON order_id WHERE payment_date < order_date` |
| Fraud velocity | `GROUP BY customer_id HAVING COUNT(*) > 20 AND AVG < 10` |

---

*End of DC_13. Full series: Parts 1–7 (core cleaning) → 8–11 (regex & forensics) → 12–13 (domain-specific)*