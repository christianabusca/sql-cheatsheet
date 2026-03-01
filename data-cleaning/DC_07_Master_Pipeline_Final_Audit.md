# 🧹 SQL Data Cleaning — Part 7: Master Cleaning Pipeline & Final Audit
> *The end-to-end cleaning playbook — a step-by-step pipeline that takes raw data to analysis-ready*

---

## Table of Contents
- [1. Pipeline Philosophy & Order of Operations](#1-pipeline-philosophy--order-of-operations)
- [2. Pre-Cleaning Snapshot (Baseline Metrics)](#2-pre-cleaning-snapshot-baseline-metrics)
- [3. Stage 1 — Load to Staging & Preserve Raw](#3-stage-1--load-to-staging--preserve-raw)
- [4. Stage 2 — Text & String Cleaning](#4-stage-2--text--string-cleaning)
- [5. Stage 3 — Date & Numeric Cleaning](#5-stage-3--date--numeric-cleaning)
- [6. Stage 4 — NULL Handling & Imputation](#6-stage-4--null-handling--imputation)
- [7. Stage 5 — Deduplication](#7-stage-5--deduplication)
- [8. Stage 6 — Referential Integrity & Schema Fix](#8-stage-6--referential-integrity--schema-fix)
- [9. Stage 7 — Business Rule Validation](#9-stage-7--business-rule-validation)
- [10. Post-Cleaning Final Audit Report](#10-post-cleaning-final-audit-report)
- [11. Ongoing Data Quality Monitoring](#11-ongoing-data-quality-monitoring)

---

## 1. Pipeline Philosophy & Order of Operations

Always clean in this order — each stage depends on the previous one being clean.

```
RAW DATA
   │
   ▼
[1] PRESERVE RAW → Copy to staging, never touch the original
   │
   ▼
[2] TEXT CLEANING → TRIM, LOWER, UPPER, REGEXP_REPLACE
   │
   ▼
[3] DATE & NUMERIC → Cast types, fix ranges, strip symbols
   │
   ▼
[4] NULL HANDLING → Classify, impute or flag missing values
   │
   ▼
[5] DEDUPLICATION → Remove or merge duplicate records
   │
   ▼
[6] REFERENTIAL INTEGRITY → Fix or quarantine orphan records
   │
   ▼
[7] BUSINESS RULE VALIDATION → Logical checks, derived fields
   │
   ▼
CLEAN DATA → Load to production / analysis-ready table
```

> 🚨 **Golden rules:**
> 1. Never modify the raw source table — always work on a copy
> 2. Log every transformation with before/after counts
> 3. Add `_imputed` flags for any filled-in values
> 4. Run the full audit BEFORE and AFTER to measure improvement

---

## 2. Pre-Cleaning Snapshot (Baseline Metrics)

Capture this before touching anything — you need it to prove improvement.

```sql
-- Create a cleaning log table to track all changes
CREATE TABLE IF NOT EXISTS cleaning_log (
  log_id          SERIAL PRIMARY KEY,
  run_id          UUID DEFAULT gen_random_uuid(),
  stage           TEXT,
  table_name      TEXT,
  metric          TEXT,
  before_value    NUMERIC,
  after_value     NUMERIC,
  rows_affected   BIGINT,
  notes           TEXT,
  logged_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Full pre-clean snapshot
WITH baseline AS (
  SELECT
    COUNT(*)                                               AS total_rows,
    COUNT(DISTINCT customer_id)                           AS unique_customers,
    COUNT(*) - COUNT(email)                               AS null_emails,
    COUNT(*) - COUNT(phone)                               AS null_phones,
    COUNT(*) - COUNT(order_date)                          AS null_dates,
    COUNT(*) - COUNT(total_amount)                        AS null_amounts,
    COUNT(*) FILTER (WHERE email != TRIM(email))          AS emails_with_spaces,
    COUNT(*) FILTER (WHERE total_amount < 0)              AS negative_amounts,
    COUNT(*) FILTER (WHERE order_date > CURRENT_DATE)     AS future_dates,
    COUNT(DISTINCT email) AS unique_emails
  FROM orders_raw
)
INSERT INTO cleaning_log (stage, table_name, metric, before_value)
SELECT 'pre_clean', 'orders', key, value
FROM baseline,
LATERAL (VALUES
  ('total_rows',          total_rows),
  ('null_emails',         null_emails),
  ('null_phones',         null_phones),
  ('null_dates',          null_dates),
  ('negative_amounts',    negative_amounts),
  ('future_dates',        future_dates),
  ('emails_with_spaces',  emails_with_spaces)
) AS kv(key, value);

SELECT * FROM cleaning_log ORDER BY logged_at DESC;
```

---

## 3. Stage 1 — Load to Staging & Preserve Raw

```sql
-- NEVER touch raw tables. Create a working copy.
CREATE TABLE customers_staging AS SELECT * FROM customers_raw;
CREATE TABLE orders_staging    AS SELECT * FROM orders_raw;
CREATE TABLE products_staging  AS SELECT * FROM products_raw;

-- Add cleaning tracking columns
ALTER TABLE customers_staging
  ADD COLUMN _cleaning_notes TEXT,
  ADD COLUMN _is_duplicate   BOOLEAN DEFAULT FALSE,
  ADD COLUMN _is_orphan      BOOLEAN DEFAULT FALSE,
  ADD COLUMN _was_imputed    BOOLEAN DEFAULT FALSE;

ALTER TABLE orders_staging
  ADD COLUMN _cleaning_notes TEXT,
  ADD COLUMN _is_duplicate   BOOLEAN DEFAULT FALSE;

-- Record baseline counts
SELECT
  'customers_raw' AS source, COUNT(*) AS rows FROM customers_raw
UNION ALL SELECT 'orders_raw',  COUNT(*) FROM orders_raw
UNION ALL SELECT 'products_raw', COUNT(*) FROM products_raw;
```

---

## 4. Stage 2 — Text & String Cleaning

```sql
-- ═══════════════════════════════════════════
-- CUSTOMERS: Text Cleaning
-- ═══════════════════════════════════════════

-- 2.1 Trim whitespace from all text columns
UPDATE customers_staging SET
  first_name = TRIM(first_name),
  last_name  = TRIM(last_name),
  email      = TRIM(email),
  phone      = TRIM(phone),
  address    = TRIM(address),
  city       = TRIM(city),
  country    = TRIM(country);

-- 2.2 Fix casing
UPDATE customers_staging SET
  first_name = INITCAP(LOWER(first_name)),
  last_name  = INITCAP(LOWER(last_name)),
  email      = LOWER(email),
  city       = INITCAP(LOWER(city)),
  country    = UPPER(TRIM(country));  -- ISO country codes = uppercase

-- 2.3 Remove non-printable characters
UPDATE customers_staging SET
  first_name = REGEXP_REPLACE(first_name, '[^\x20-\x7E]', '', 'g'),
  last_name  = REGEXP_REPLACE(last_name,  '[^\x20-\x7E]', '', 'g');

-- 2.4 Standardize phone numbers (digits only)
UPDATE customers_staging
SET phone = REGEXP_REPLACE(phone, '[^0-9]', '', 'g')
WHERE phone IS NOT NULL;

UPDATE customers_staging
SET phone = NULL
WHERE LENGTH(phone) NOT IN (10, 11) AND phone IS NOT NULL;

-- 2.5 Standardize country codes
UPDATE customers_staging
SET country = CASE LOWER(TRIM(country))
  WHEN 'us'            THEN 'US'
  WHEN 'usa'           THEN 'US'
  WHEN 'united states' THEN 'US'
  WHEN 'america'       THEN 'US'
  WHEN 'uk'            THEN 'GB'
  WHEN 'united kingdom' THEN 'GB'
  WHEN 'england'       THEN 'GB'
  WHEN 'ph'            THEN 'PH'
  WHEN 'philippines'   THEN 'PH'
  WHEN 'pilipinas'     THEN 'PH'
  ELSE UPPER(TRIM(country))
END
WHERE country IS NOT NULL;

-- Log text cleaning results
INSERT INTO cleaning_log (stage, table_name, metric, after_value)
SELECT '2_text_clean', 'customers_staging', 'emails_with_spaces',
  COUNT(*) FILTER (WHERE email != LOWER(email))
FROM customers_staging;
```

---

## 5. Stage 3 — Date & Numeric Cleaning

```sql
-- ═══════════════════════════════════════════
-- ORDERS: Date Cleaning
-- ═══════════════════════════════════════════

-- 3.1 Parse dates stored as text
ALTER TABLE orders_staging ADD COLUMN order_date_clean DATE;

UPDATE orders_staging
SET order_date_clean = CASE
  WHEN order_date_raw ~ '^\d{4}-\d{2}-\d{2}$'  THEN order_date_raw::date
  WHEN order_date_raw ~ '^\d{2}/\d{2}/\d{4}$'  THEN TO_DATE(order_date_raw, 'MM/DD/YYYY')
  WHEN order_date_raw ~ '^\d{2}-\d{2}-\d{4}$'  THEN TO_DATE(order_date_raw, 'DD-MM-YYYY')
  WHEN order_date_raw ~ '^\d{8}$'               THEN TO_DATE(order_date_raw, 'YYYYMMDD')
  ELSE NULL
END;

-- 3.2 Null out invalid dates
UPDATE orders_staging
SET order_date_clean = NULL,
    _cleaning_notes = COALESCE(_cleaning_notes || '; ', '') || 'invalid_date'
WHERE order_date_clean > CURRENT_DATE
   OR order_date_clean < '2000-01-01';

-- ═══════════════════════════════════════════
-- ORDERS: Numeric Cleaning
-- ═══════════════════════════════════════════

-- 3.3 Strip currency symbols, cast to NUMERIC
ALTER TABLE orders_staging ADD COLUMN total_amount_clean NUMERIC(12,2);

UPDATE orders_staging
SET total_amount_clean =
  REGEXP_REPLACE(total_amount_raw, '[$£€¥,\s]', '', 'g')::NUMERIC
WHERE total_amount_raw ~ '[0-9]';

-- 3.4 Cap outliers (flag, don't delete)
UPDATE orders_staging
SET _cleaning_notes = COALESCE(_cleaning_notes || '; ', '') || 'amount_outlier'
WHERE total_amount_clean > 50000 OR total_amount_clean < 0;

-- 3.5 Round to 2 decimal places
UPDATE orders_staging
SET total_amount_clean = ROUND(total_amount_clean, 2);

-- Log numeric cleaning
INSERT INTO cleaning_log (stage, table_name, metric, after_value)
SELECT '3_numeric', 'orders_staging', 'null_amounts',
  COUNT(*) FILTER (WHERE total_amount_clean IS NULL) FROM orders_staging;
```

---

## 6. Stage 4 — NULL Handling & Imputation

```sql
-- ═══════════════════════════════════════════
-- 4.1 Convert fake NULLs to real NULLs
-- ═══════════════════════════════════════════
UPDATE customers_staging SET
  first_name = NULLIF(NULLIF(TRIM(first_name), ''), 'N/A'),
  last_name  = NULLIF(NULLIF(TRIM(last_name),  ''), 'N/A'),
  phone      = NULLIF(NULLIF(TRIM(phone),      ''), '-'),
  country    = NULLIF(NULLIF(TRIM(country),    ''), 'Unknown');

-- ═══════════════════════════════════════════
-- 4.2 Impute with group-based values
-- ═══════════════════════════════════════════

-- Fill missing country from customer's most frequent order ship-to country
UPDATE customers_staging AS c
SET country = (
  SELECT country
  FROM orders_staging AS o
  INNER JOIN order_addresses AS oa ON o.order_id = oa.order_id
  WHERE o.customer_id = c.customer_id AND oa.country IS NOT NULL
  GROUP BY oa.country ORDER BY COUNT(*) DESC LIMIT 1
),
_was_imputed = TRUE,
_cleaning_notes = COALESCE(_cleaning_notes || '; ', '') || 'country_imputed'
WHERE c.country IS NULL;

-- Fill missing price with category median
UPDATE products_staging AS p
SET price = cat_median.median_price,
    _was_imputed = TRUE
FROM (
  SELECT category,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price
  FROM products_staging WHERE price IS NOT NULL
  GROUP BY category
) AS cat_median
WHERE p.price IS NULL AND p.category = cat_median.category;

-- ═══════════════════════════════════════════
-- 4.3 Fill remaining NULLs with safe defaults
-- ═══════════════════════════════════════════
UPDATE orders_staging SET discount_rate = 0       WHERE discount_rate IS NULL;
UPDATE products_staging SET is_active   = TRUE    WHERE is_active IS NULL;

-- Log imputation counts
INSERT INTO cleaning_log (stage, table_name, metric, after_value)
SELECT '4_nulls', 'customers_staging', 'was_imputed_count',
  COUNT(*) FILTER (WHERE _was_imputed = TRUE) FROM customers_staging;
```

---

## 7. Stage 5 — Deduplication

```sql
-- ═══════════════════════════════════════════
-- 5.1 Find duplicates
-- ═══════════════════════════════════════════
WITH dups AS (
  SELECT customer_id,
    ROW_NUMBER() OVER(
      PARTITION BY LOWER(email)
      ORDER BY
        (CASE WHEN phone IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN address IS NOT NULL THEN 1 ELSE 0 END) DESC,
        created_at ASC
    ) AS rn
  FROM customers_staging
  WHERE email IS NOT NULL
)
UPDATE customers_staging AS c
SET _is_duplicate = TRUE,
    _cleaning_notes = COALESCE(_cleaning_notes || '; ', '') || 'duplicate'
FROM dups AS d
WHERE c.customer_id = d.customer_id AND d.rn > 1;

-- 5.2 Preview what will be deduplicated
SELECT
  COUNT(*) FILTER (WHERE _is_duplicate = TRUE)  AS to_be_removed,
  COUNT(*) FILTER (WHERE _is_duplicate = FALSE) AS to_be_kept,
  COUNT(*)                                      AS total
FROM customers_staging;

-- 5.3 Move duplicates to archive
CREATE TABLE IF NOT EXISTS customers_duplicates AS
SELECT * FROM customers_staging WHERE FALSE;

INSERT INTO customers_duplicates
SELECT * FROM customers_staging WHERE _is_duplicate = TRUE;

DELETE FROM customers_staging WHERE _is_duplicate = TRUE;

-- Log deduplication
INSERT INTO cleaning_log (stage, table_name, metric, after_value, rows_affected)
SELECT '5_dedup', 'customers_staging', 'duplicates_removed',
  COUNT(*), COUNT(*)
FROM customers_duplicates;
```

---

## 8. Stage 6 — Referential Integrity & Schema Fix

```sql
-- ═══════════════════════════════════════════
-- 6.1 Mark orphan orders
-- ═══════════════════════════════════════════
UPDATE orders_staging AS o
SET _is_orphan = TRUE,
    _cleaning_notes = COALESCE(_cleaning_notes || '; ', '') || 'orphan_no_customer'
WHERE NOT EXISTS (
  SELECT 1 FROM customers_staging c WHERE c.customer_id = o.customer_id
);

-- 6.2 Move orphans to quarantine
CREATE TABLE IF NOT EXISTS orders_quarantine AS SELECT * FROM orders_staging WHERE FALSE;

INSERT INTO orders_quarantine
SELECT * FROM orders_staging WHERE _is_orphan = TRUE;

DELETE FROM orders_staging WHERE _is_orphan = TRUE;

-- 6.3 Final type enforcement
ALTER TABLE orders_staging ALTER COLUMN order_date_clean   SET NOT NULL;
ALTER TABLE orders_staging ALTER COLUMN total_amount_clean SET DATA TYPE NUMERIC(12,2);
ALTER TABLE customers_staging ALTER COLUMN email SET NOT NULL;

-- Log integrity fixes
INSERT INTO cleaning_log (stage, table_name, metric, after_value)
SELECT '6_integrity', 'orders_staging', 'orphans_quarantined',
  COUNT(*) FROM orders_quarantine;
```

---

## 9. Stage 7 — Business Rule Validation

```sql
-- ═══════════════════════════════════════════
-- 7.1 Check domain-specific logic
-- ═══════════════════════════════════════════

-- Ship date must be >= order date
SELECT COUNT(*) AS violations
FROM orders_staging WHERE shipped_date < order_date_clean;

UPDATE orders_staging
SET shipped_date = NULL,
    _cleaning_notes = COALESCE(_cleaning_notes || '; ', '') || 'ship_before_order'
WHERE shipped_date < order_date_clean;

-- Discount must be 0–100% (as decimal 0–1)
UPDATE orders_staging
SET discount_rate = GREATEST(0, LEAST(1, discount_rate))
WHERE discount_rate NOT BETWEEN 0 AND 1;

-- Quantity must be a positive integer
UPDATE order_items_staging
SET quantity = ABS(ROUND(quantity)::int)
WHERE quantity <= 0 OR quantity != FLOOR(quantity);

-- Line total = qty × unit_price
UPDATE order_items_staging
SET line_total = ROUND(quantity * unit_price, 2)
WHERE ABS(line_total - quantity * unit_price) > 0.01;

-- Log business rule fixes
INSERT INTO cleaning_log (stage, table_name, metric, after_value)
SELECT '7_business_rules', 'orders_staging', 'ship_before_order_fixed',
  COUNT(*) FILTER (WHERE _cleaning_notes LIKE '%ship_before_order%')
FROM orders_staging;
```

---

## 10. Post-Cleaning Final Audit Report

```sql
-- ═══════════════════════════════════════════
-- THE FULL FINAL AUDIT — run this at the END
-- ═══════════════════════════════════════════

WITH customers_audit AS (
  SELECT
    'customers'                                                        AS table_name,
    COUNT(*)                                                           AS total_rows,
    COUNT(*) - COUNT(email)                                            AS null_emails,
    COUNT(*) - COUNT(phone)                                            AS null_phones,
    COUNT(*) - COUNT(country)                                          AS null_country,
    COUNT(*) FILTER (WHERE email != TRIM(email))                       AS emails_untrimmed,
    COUNT(*) FILTER (WHERE email != LOWER(email))                      AS emails_not_lower,
    COUNT(*) FILTER (WHERE first_name ~ '[^a-zA-Z \-'']')              AS names_with_bad_chars,
    (SELECT COUNT(*) FROM (
      SELECT LOWER(email) FROM customers_staging GROUP BY 1 HAVING COUNT(*) > 1
    ) d)                                                               AS duplicate_emails
  FROM customers_staging
),
orders_audit AS (
  SELECT
    'orders'                                                           AS table_name,
    COUNT(*)                                                           AS total_rows,
    COUNT(*) - COUNT(order_date_clean)                                 AS null_dates,
    COUNT(*) - COUNT(total_amount_clean)                               AS null_amounts,
    COUNT(*) FILTER (WHERE total_amount_clean < 0)                     AS negative_amounts,
    COUNT(*) FILTER (WHERE order_date_clean > CURRENT_DATE)            AS future_dates,
    COUNT(*) FILTER (WHERE shipped_date < order_date_clean)            AS ship_before_order,
    0 AS duplicate_emails
  FROM orders_staging
)
SELECT
  table_name,
  total_rows,
  null_emails,
  null_phones,
  null_dates,
  null_amounts,
  negative_amounts,
  future_dates,
  duplicate_emails,
  -- Overall quality score (100 = perfect)
  ROUND(
    100.0 * (
      1.0
      - (null_emails + null_dates + null_amounts +
         negative_amounts + future_dates + duplicate_emails
        )::numeric / NULLIF(total_rows * 6.0, 0)
    ), 1
  ) AS data_quality_score
FROM customers_audit
UNION ALL
SELECT table_name, total_rows,
  0, 0, null_dates, null_amounts,
  negative_amounts, future_dates, 0,
  ROUND(100.0 * (1 - (null_dates + null_amounts + negative_amounts + future_dates)::numeric
                     / NULLIF(total_rows * 4.0, 0)), 1) AS data_quality_score
FROM orders_audit;

-- ═══════════════════════════════════════════
-- Compare BEFORE vs AFTER from cleaning_log
-- ═══════════════════════════════════════════
SELECT
  metric,
  MAX(CASE WHEN stage = 'pre_clean' THEN before_value END) AS before,
  MAX(after_value)                                          AS after,
  MAX(CASE WHEN stage = 'pre_clean' THEN before_value END)
    - MAX(after_value)                                      AS improvement,
  ROUND(
    100.0 * (
      MAX(CASE WHEN stage = 'pre_clean' THEN before_value END)
      - MAX(after_value)
    ) / NULLIF(MAX(CASE WHEN stage = 'pre_clean' THEN before_value END), 0),
    1
  ) AS pct_improvement
FROM cleaning_log
GROUP BY metric
ORDER BY improvement DESC;
```

---

## 11. Ongoing Data Quality Monitoring

Keep your data clean after the initial fix — run these checks on a schedule.

```sql
-- Daily data quality check (schedule via cron or orchestration tool)
CREATE OR REPLACE VIEW daily_dq_report AS
SELECT
  CURRENT_DATE AS check_date,
  'orders' AS table_name,
  COUNT(*) AS total_rows_today,
  COUNT(*) FILTER (WHERE order_date IS NULL)             AS null_dates,
  COUNT(*) FILTER (WHERE customer_id NOT IN (SELECT customer_id FROM customers)) AS orphan_orders,
  COUNT(*) FILTER (WHERE total_amount < 0)               AS negative_amounts,
  COUNT(*) FILTER (WHERE status NOT IN (
    'pending','confirmed','shipped','delivered','cancelled','refunded'
  )) AS invalid_status,
  COUNT(*) FILTER (WHERE order_date > CURRENT_DATE)      AS future_dates,
  -- DQ score for today's batch
  ROUND(
    100.0 * COUNT(*) FILTER (
      WHERE order_date IS NOT NULL
        AND total_amount >= 0
        AND status IN ('pending','confirmed','shipped','delivered','cancelled','refunded')
        AND order_date <= CURRENT_DATE
    ) / NULLIF(COUNT(*), 0), 2
  ) AS batch_quality_score
FROM orders
WHERE order_date >= CURRENT_DATE - 1;  -- today's new records

-- Alert query: if quality drops below threshold
SELECT
  check_date,
  table_name,
  batch_quality_score,
  CASE
    WHEN batch_quality_score >= 99 THEN '✅ Excellent'
    WHEN batch_quality_score >= 95 THEN '🟡 Acceptable'
    WHEN batch_quality_score >= 90 THEN '🟠 Needs Review'
    ELSE '🔴 ALERT — Quality Below Threshold'
  END AS quality_status
FROM daily_dq_report
WHERE batch_quality_score < 99;
```

---

## 📌 Full Pipeline Checklist

```
□ Stage 0: Snapshot row counts & quality metrics (baseline)
□ Stage 1: Copy raw to staging table — NEVER modify raw
□ Stage 2: TRIM all text, fix casing, strip bad characters
□ Stage 3: Parse & cast dates, strip currency, fix numerics
□ Stage 4: Identify fake NULLs, impute or flag missing values
□ Stage 5: Deduplicate — archive, not delete
□ Stage 6: Fix orphan FKs — quarantine broken references
□ Stage 7: Validate business rules — dates, amounts, logic
□ Stage 8: Run final audit report — compare before/after
□ Stage 9: Add constraints to prevent future violations
□ Stage 10: Load clean table to production schema
□ Stage 11: Schedule ongoing DQ monitoring queries
```

---

## 📌 Cleaning Priority Matrix

| Issue | Impact | Effort | Fix First? |
|-------|--------|--------|-----------|
| Duplicate primary keys | 🔴 Critical | Low | ✅ Yes |
| Orphan foreign keys | 🔴 Critical | Low | ✅ Yes |
| NULL on join key | 🔴 Critical | Low | ✅ Yes |
| Wrong data type | 🔴 Critical | Medium | ✅ Yes |
| Invalid dates | 🟠 High | Low | ✅ Yes |
| Duplicate rows | 🟠 High | Medium | ✅ Yes |
| Inconsistent casing | 🟡 Medium | Low | ✅ Yes |
| Whitespace | 🟡 Medium | Low | ✅ Yes |
| Missing optional fields | 🟢 Low | High | ⏳ Later |
| Outliers | 🟡 Medium | Medium | ⏳ Context |
| Unit mismatches | 🟠 High | Medium | ✅ Yes |
| Category variants | 🟡 Medium | Medium | ✅ Yes |

---

*End of the SQL Data Cleaning Series.*
*This is Part 7 of 7 — refer to Parts 1–6 for detailed implementation of each stage.*