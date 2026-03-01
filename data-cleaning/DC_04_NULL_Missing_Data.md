# 🧹 SQL Data Cleaning — Part 4: NULL & Missing Data Handling
> *Detect, classify, impute, and document every missing value in your dataset*

---

## Table of Contents
- [1. Auditing NULLs Across All Columns](#1-auditing-nulls-across-all-columns)
- [2. Classifying Missing Data Types](#2-classifying-missing-data-types)
- [3. Dropping vs Keeping NULL Rows](#3-dropping-vs-keeping-null-rows)
- [4. Simple Imputation Strategies](#4-simple-imputation-strategies)
- [5. Statistical Imputation](#5-statistical-imputation)
- [6. Imputation Using Related Rows](#6-imputation-using-related-rows)
- [7. Forward Fill & Backward Fill](#7-forward-fill--backward-fill)
- [8. Handling Fake NULLs (Disguised Missing Values)](#8-handling-fake-nulls-disguised-missing-values)
- [9. Adding Missing Data Flags](#9-adding-missing-data-flags)
- [10. Final NULL Audit Checklist](#10-final-null-audit-checklist)

---

## 1. Auditing NULLs Across All Columns

Start every cleaning effort by profiling exactly where NULLs live.

```sql
-- NULL count per column (works for any table — swap table name)
SELECT
  COUNT(*)                               AS total_rows,
  COUNT(*) - COUNT(customer_id)          AS null_customer_id,
  COUNT(*) - COUNT(first_name)           AS null_first_name,
  COUNT(*) - COUNT(last_name)            AS null_last_name,
  COUNT(*) - COUNT(email)                AS null_email,
  COUNT(*) - COUNT(phone)                AS null_phone,
  COUNT(*) - COUNT(birth_date)           AS null_birth_date,
  COUNT(*) - COUNT(address)              AS null_address,
  COUNT(*) - COUNT(city)                 AS null_city,
  COUNT(*) - COUNT(country)              AS null_country,
  COUNT(*) - COUNT(registration_date)    AS null_registration_date
FROM customers;

-- NULL percentage per column (more useful at scale)
SELECT
  ROUND(100.0 * (COUNT(*) - COUNT(first_name))   / COUNT(*), 2) AS pct_null_first_name,
  ROUND(100.0 * (COUNT(*) - COUNT(email))         / COUNT(*), 2) AS pct_null_email,
  ROUND(100.0 * (COUNT(*) - COUNT(phone))         / COUNT(*), 2) AS pct_null_phone,
  ROUND(100.0 * (COUNT(*) - COUNT(birth_date))    / COUNT(*), 2) AS pct_null_birth_date,
  ROUND(100.0 * (COUNT(*) - COUNT(country))       / COUNT(*), 2) AS pct_null_country
FROM customers;

-- Rows with ANY null across key columns
SELECT *
FROM customers
WHERE first_name IS NULL
   OR email IS NULL
   OR registration_date IS NULL;

-- Count how many columns are NULL per row (completeness score)
SELECT
  customer_id,
  (CASE WHEN first_name       IS NULL THEN 1 ELSE 0 END +
   CASE WHEN last_name        IS NULL THEN 1 ELSE 0 END +
   CASE WHEN email            IS NULL THEN 1 ELSE 0 END +
   CASE WHEN phone            IS NULL THEN 1 ELSE 0 END +
   CASE WHEN birth_date       IS NULL THEN 1 ELSE 0 END +
   CASE WHEN country          IS NULL THEN 1 ELSE 0 END)  AS null_count,
  7 AS total_fields,
  ROUND(100.0 * (7 -
   (CASE WHEN first_name  IS NULL THEN 1 ELSE 0 END +
    CASE WHEN last_name   IS NULL THEN 1 ELSE 0 END +
    CASE WHEN email       IS NULL THEN 1 ELSE 0 END +
    CASE WHEN phone       IS NULL THEN 1 ELSE 0 END +
    CASE WHEN birth_date  IS NULL THEN 1 ELSE 0 END +
    CASE WHEN country     IS NULL THEN 1 ELSE 0 END)) / 7.0, 1) AS completeness_pct
FROM customers
ORDER BY null_count DESC;
```

---

## 2. Classifying Missing Data Types

Understanding WHY data is missing determines how to handle it.

```sql
-- MCAR (Missing Completely At Random): NULLs spread randomly
-- Check: are NULLs correlated with any other column?
SELECT
  CASE WHEN phone IS NULL THEN 'No Phone' ELSE 'Has Phone' END AS phone_status,
  COUNT(*) AS count,
  ROUND(AVG(total_orders), 2) AS avg_orders,
  ROUND(AVG(lifetime_value), 2) AS avg_ltv
FROM customers AS c
LEFT JOIN customer_metrics AS m USING(customer_id)
GROUP BY phone_status;
-- If avg_orders/avg_ltv differ significantly → NOT MCAR, the NULL is meaningful

-- MAR (Missing At Random): NULLs correlated with another observed variable
-- Check: does NULL rate vary by segment?
SELECT
  country,
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE phone IS NULL) AS null_phone,
  ROUND(100.0 * COUNT(*) FILTER (WHERE phone IS NULL) / COUNT(*), 1) AS pct_null
FROM customers
GROUP BY country
ORDER BY pct_null DESC;

-- MNAR (Missing Not At Random): NULLs related to the value itself
-- Example: high-income customers less likely to share salary
-- Hard to detect purely in SQL — document as assumption

-- Tag rows with a missingness pattern label
ALTER TABLE customers ADD COLUMN IF NOT EXISTS data_quality_tier TEXT;

UPDATE customers
SET data_quality_tier = CASE
  WHEN email IS NOT NULL AND phone IS NOT NULL AND address IS NOT NULL THEN 'Complete'
  WHEN email IS NOT NULL AND (phone IS NULL OR address IS NULL)        THEN 'Partial'
  WHEN email IS NULL AND first_name IS NOT NULL                        THEN 'Minimal'
  ELSE 'Sparse'
END;

SELECT data_quality_tier, COUNT(*) AS count
FROM customers
GROUP BY data_quality_tier
ORDER BY count DESC;
```

---

## 3. Dropping vs Keeping NULL Rows

```sql
-- Rule of thumb:
-- Drop if: >50% of key fields are NULL, or if the row is un-joinable
-- Keep if: the NULL field is optional, or the row has other valuable data

-- Option 1: Drop rows missing the primary identifier
DELETE FROM customers
WHERE customer_id IS NULL;

-- Option 2: Drop rows where critical fields are ALL null
DELETE FROM orders
WHERE order_date IS NULL
  AND total_amount IS NULL
  AND customer_id IS NULL;

-- Option 3: Move bad rows to a quarantine table instead of deleting
CREATE TABLE IF NOT EXISTS customers_quarantine AS
SELECT *, CURRENT_TIMESTAMP AS quarantined_at, 'Missing email + name' AS reason
FROM customers
WHERE email IS NULL AND first_name IS NULL
LIMIT 0;  -- create structure only

INSERT INTO customers_quarantine (SELECT *, CURRENT_TIMESTAMP, 'Missing email + name' FROM customers WHERE email IS NULL AND first_name IS NULL);

DELETE FROM customers WHERE email IS NULL AND first_name IS NULL;

-- Option 4: Flag instead of delete (for auditing)
ALTER TABLE customers ADD COLUMN IF NOT EXISTS is_incomplete BOOLEAN DEFAULT FALSE;
UPDATE customers
SET is_incomplete = TRUE
WHERE email IS NULL OR customer_id IS NULL;

-- How many rows would be removed at different thresholds?
SELECT
  COUNT(*) FILTER (WHERE email IS NULL)                              AS drop_if_no_email,
  COUNT(*) FILTER (WHERE email IS NULL AND phone IS NULL)            AS drop_if_no_contact,
  COUNT(*) FILTER (WHERE email IS NULL AND phone IS NULL
                     AND address IS NULL)                            AS drop_if_no_anything,
  COUNT(*) AS total
FROM customers;
```

---

## 4. Simple Imputation Strategies

```sql
-- STRATEGY 1: Replace with a constant/default
UPDATE customers
SET country = 'Unknown'
WHERE country IS NULL;

UPDATE orders
SET notes = ''
WHERE notes IS NULL;

UPDATE products
SET is_active = TRUE
WHERE is_active IS NULL;

-- STRATEGY 2: Replace with global mean/median/mode
-- Mean imputation
UPDATE products
SET weight_kg = (SELECT ROUND(AVG(weight_kg), 3) FROM products WHERE weight_kg IS NOT NULL)
WHERE weight_kg IS NULL;

-- Median imputation (more robust to outliers)
UPDATE products
SET price = (
  SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
  FROM products
  WHERE price IS NOT NULL
)
WHERE price IS NULL;

-- Mode imputation (most common value — good for categorical)
UPDATE customers
SET country = (
  SELECT country
  FROM customers
  WHERE country IS NOT NULL
  GROUP BY country
  ORDER BY COUNT(*) DESC
  LIMIT 1
)
WHERE country IS NULL;

-- STRATEGY 3: Replace with a business-logic default
-- If no discount recorded, assume 0%
UPDATE orders
SET discount_rate = 0
WHERE discount_rate IS NULL;

-- If no end date, assume subscription is still active (open-ended)
UPDATE subscriptions
SET end_date = '9999-12-31'
WHERE end_date IS NULL AND status = 'active';
```

---

## 5. Statistical Imputation

```sql
-- GROUP-BASED mean imputation (smarter than global mean)
-- Impute price with the average price of the same category
UPDATE products AS p
SET price = cat_avg.avg_price
FROM (
  SELECT category, ROUND(AVG(price), 2) AS avg_price
  FROM products
  WHERE price IS NOT NULL
  GROUP BY category
) AS cat_avg
WHERE p.price IS NULL
  AND p.category = cat_avg.category;

-- GROUP-BASED median imputation
WITH category_medians AS (
  SELECT
    category,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price
  FROM products
  WHERE price IS NOT NULL
  GROUP BY category
)
UPDATE products AS p
SET price = cm.median_price
FROM category_medians AS cm
WHERE p.price IS NULL
  AND p.category = cm.category;

-- Regression-based imputation (weight ~ length × width × height)
-- Predict missing weight from available dimensions
WITH regression AS (
  SELECT
    REGR_SLOPE(weight_kg, length_cm * width_cm * height_cm) AS slope,
    REGR_INTERCEPT(weight_kg, length_cm * width_cm * height_cm) AS intercept
  FROM products
  WHERE weight_kg IS NOT NULL
    AND length_cm IS NOT NULL
    AND width_cm IS NOT NULL
    AND height_cm IS NOT NULL
)
UPDATE products AS p
SET weight_kg = ROUND(
  (p.length_cm * p.width_cm * p.height_cm) * r.slope + r.intercept,
  3
)
FROM regression AS r
WHERE p.weight_kg IS NULL
  AND p.length_cm IS NOT NULL
  AND p.width_cm IS NOT NULL
  AND p.height_cm IS NOT NULL;
```

---

## 6. Imputation Using Related Rows

```sql
-- Fill NULL using another row from the SAME entity
-- Example: fill missing country using customer's other orders
UPDATE customers AS c
SET country = (
  SELECT country
  FROM orders AS o
  INNER JOIN order_addresses AS oa ON o.order_id = oa.order_id
  WHERE o.customer_id = c.customer_id
    AND oa.country IS NOT NULL
  GROUP BY oa.country
  ORDER BY COUNT(*) DESC
  LIMIT 1
)
WHERE c.country IS NULL;

-- Fill NULL from a lookup/reference table
UPDATE customers AS c
SET country = r.default_country
FROM region_defaults AS r
WHERE c.region = r.region
  AND c.country IS NULL;

-- Fill missing category from product hierarchy
UPDATE products AS p
SET category = parent.category
FROM products AS parent
WHERE p.parent_product_id = parent.product_id
  AND p.category IS NULL
  AND parent.category IS NOT NULL;

-- Fill missing manager_id for employees in same department
UPDATE employees AS e
SET manager_id = (
  SELECT manager_id
  FROM employees AS e2
  WHERE e2.department_id = e.department_id
    AND e2.manager_id IS NOT NULL
  LIMIT 1
)
WHERE e.manager_id IS NULL;
```

---

## 7. Forward Fill & Backward Fill

```sql
-- FORWARD FILL (Last Observation Carried Forward — LOCF)
-- Fills NULL using the most recent non-NULL value per entity
WITH grouped AS (
  SELECT
    customer_id,
    event_date,
    segment,
    -- Create a group that increments only when segment is NOT NULL
    COUNT(segment) OVER(
      PARTITION BY customer_id
      ORDER BY event_date
    ) AS grp
  FROM customer_events
)
SELECT
  customer_id,
  event_date,
  segment,
  FIRST_VALUE(segment) OVER(
    PARTITION BY customer_id, grp
    ORDER BY event_date
  ) AS segment_filled_forward
FROM grouped;

-- BACKWARD FILL (Next Observation Carried Backward — NOCB)
WITH grouped_back AS (
  SELECT
    customer_id,
    event_date,
    segment,
    COUNT(segment) OVER(
      PARTITION BY customer_id
      ORDER BY event_date DESC  -- reverse order for backward fill
    ) AS grp_back
  FROM customer_events
)
SELECT
  customer_id,
  event_date,
  segment,
  FIRST_VALUE(segment) OVER(
    PARTITION BY customer_id, grp_back
    ORDER BY event_date DESC
  ) AS segment_filled_backward
FROM grouped_back;

-- Apply forward fill as an UPDATE
WITH ffill AS (
  SELECT
    id,
    FIRST_VALUE(price) OVER(
      PARTITION BY product_id,
        COUNT(price) OVER(PARTITION BY product_id ORDER BY date)
      ORDER BY date
    ) AS filled_price
  FROM daily_prices
)
UPDATE daily_prices AS dp
SET price = f.filled_price
FROM ffill AS f
WHERE dp.id = f.id AND dp.price IS NULL;
```

---

## 8. Handling Fake NULLs (Disguised Missing Values)

```sql
-- Find common null-like strings masquerading as real values
SELECT col, COUNT(*) AS count
FROM your_table
WHERE LOWER(TRIM(col)) IN (
  'null', 'none', 'n/a', 'na', 'not available', 'not applicable',
  '-', '--', '---', '.', '?', 'unknown', 'unk', 'missing',
  'undefined', 'unspecified', 'blank', 'empty', '0000-00-00',
  'tbd', 'to be determined', '#n/a', '#null!', '(blank)', ''
)
GROUP BY col
ORDER BY count DESC;

-- Convert all fake NULLs to real NULLs in one pass
UPDATE customers
SET
  first_name = NULLIF(NULLIF(NULLIF(TRIM(first_name), ''), 'N/A'), 'Unknown'),
  last_name  = NULLIF(NULLIF(NULLIF(TRIM(last_name),  ''), 'N/A'), 'Unknown'),
  phone      = NULLIF(NULLIF(NULLIF(TRIM(phone),      ''), '-'),   'N/A'),
  country    = NULLIF(NULLIF(NULLIF(TRIM(country),    ''), 'Unknown'), 'N/A');

-- More scalable: create a helper function
CREATE OR REPLACE FUNCTION clean_null(val TEXT)
RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$
  SELECT CASE
    WHEN val IS NULL THEN NULL
    WHEN LOWER(TRIM(val)) IN (
      '', 'null', 'none', 'n/a', 'na', '-', '--', '?',
      'unknown', 'missing', 'undefined', 'unspecified', 'tbd'
    ) THEN NULL
    ELSE TRIM(val)
  END;
$$;

-- Apply to all text columns
UPDATE customers SET
  first_name = clean_null(first_name),
  last_name  = clean_null(last_name),
  email      = clean_null(email),
  phone      = clean_null(phone),
  country    = clean_null(country),
  address    = clean_null(address);
```

---

## 9. Adding Missing Data Flags

Always track what was imputed vs originally populated — this is critical for analysis trust.

```sql
-- Add flag columns for imputed fields
ALTER TABLE products
  ADD COLUMN IF NOT EXISTS price_imputed     BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS weight_imputed    BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS category_imputed  BOOLEAN DEFAULT FALSE;

-- Set flags BEFORE imputing
UPDATE products SET price_imputed    = TRUE WHERE price IS NULL;
UPDATE products SET weight_imputed   = TRUE WHERE weight_kg IS NULL;
UPDATE products SET category_imputed = TRUE WHERE category IS NULL;

-- Then do the imputation (flags stay TRUE for imputed rows)

-- Add a data completeness score column
ALTER TABLE customers ADD COLUMN IF NOT EXISTS completeness_score NUMERIC(5,2);

UPDATE customers
SET completeness_score = ROUND(
  100.0 * (
    CASE WHEN first_name IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN last_name  IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN email      IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN phone      IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN address    IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN city       IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN country    IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN birth_date IS NOT NULL THEN 1 ELSE 0 END
  ) / 8.0, 2
);

-- Segment by completeness
SELECT
  CASE
    WHEN completeness_score = 100  THEN 'Complete (100%)'
    WHEN completeness_score >= 75  THEN 'Mostly Complete (75–99%)'
    WHEN completeness_score >= 50  THEN 'Partial (50–74%)'
    ELSE 'Sparse (< 50%)'
  END AS completeness_tier,
  COUNT(*) AS customers,
  ROUND(AVG(lifetime_value), 2) AS avg_ltv
FROM customers
LEFT JOIN customer_metrics USING(customer_id)
GROUP BY completeness_tier
ORDER BY AVG(completeness_score) DESC;
```

---

## 10. Final NULL Audit Checklist

```sql
-- Run BEFORE and AFTER cleaning to measure improvement
WITH null_audit AS (
  SELECT
    'Before' AS stage,
    ROUND(100.0 * COUNT(*) FILTER (WHERE first_name IS NULL)  / COUNT(*), 2) AS pct_null_name,
    ROUND(100.0 * COUNT(*) FILTER (WHERE email IS NULL)        / COUNT(*), 2) AS pct_null_email,
    ROUND(100.0 * COUNT(*) FILTER (WHERE phone IS NULL)        / COUNT(*), 2) AS pct_null_phone,
    ROUND(100.0 * COUNT(*) FILTER (WHERE country IS NULL)      / COUNT(*), 2) AS pct_null_country,
    ROUND(AVG(completeness_score), 2)                                          AS avg_completeness,
    COUNT(*) AS total_rows
  FROM customers
)
SELECT * FROM null_audit;
```

---

## 📌 NULL Handling Quick Reference

| Situation | Strategy |
|-----------|---------|
| NULL in text (optional field) | `COALESCE(col, 'Unknown')` |
| NULL in number (should be 0) | `COALESCE(col, 0)` |
| NULL = meaningful absence | Keep as NULL, add `_is_null` flag column |
| Fake NULL strings | `clean_null()` function → NULLIF chain |
| Time series gap | Forward fill with `FIRST_VALUE()` + group trick |
| Impute with group average | `UPDATE ... SET col = (SELECT AVG FROM same_group)` |
| Impute with regression | `REGR_SLOPE` + `REGR_INTERCEPT` |
| Drop missing rows | Only if row is un-joinable or >50% empty |
| Track imputations | `_imputed BOOLEAN` flag column |
| Completeness scoring | Sum of `CASE WHEN field IS NOT NULL THEN 1`s |

---

*Part of the SQL Data Cleaning series → Next: Part 5 — Duplicate Detection & Deduplication*