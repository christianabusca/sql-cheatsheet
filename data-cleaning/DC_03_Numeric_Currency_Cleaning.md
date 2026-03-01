# 🧹 SQL Data Cleaning — Part 3: Numeric & Currency Cleaning
> *Fix non-numeric strings, wrong data types, currency symbols, outliers, rounding errors, and unit mismatches*

---

## Table of Contents
- [1. Detecting Numeric Problems](#1-detecting-numeric-problems)
- [2. Casting Strings to Numbers Safely](#2-casting-strings-to-numbers-safely)
- [3. Currency & Symbol Stripping](#3-currency--symbol-stripping)
- [4. Handling Negative Numbers](#4-handling-negative-numbers)
- [5. Rounding & Precision Cleanup](#5-rounding--precision-cleanup)
- [6. Outlier Detection & Treatment](#6-outlier-detection--treatment)
- [7. Unit Standardization](#7-unit-standardization)
- [8. Derived Numeric Columns & Ratios](#8-derived-numeric-columns--ratios)
- [9. Percentage & Rate Validation](#9-percentage--rate-validation)
- [10. Final Numeric Audit Checklist](#10-final-numeric-audit-checklist)

---

## 1. Detecting Numeric Problems

```sql
-- Full numeric health check on a column
SELECT
  COUNT(*)                                             AS total_rows,
  COUNT(price)                                         AS non_null_values,
  COUNT(*) - COUNT(price)                              AS null_count,
  COUNT(*) FILTER (WHERE price < 0)                    AS negatives,
  COUNT(*) FILTER (WHERE price = 0)                    AS zeros,
  COUNT(*) FILTER (WHERE price > 100000)               AS suspiciously_high,
  MIN(price)                                           AS min_val,
  MAX(price)                                           AS max_val,
  ROUND(AVG(price), 2)                                 AS mean,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)   AS median,
  STDDEV(price)                                        AS std_dev
FROM products;

-- Find non-numeric values stored as text (before casting)
SELECT raw_price, COUNT(*) AS count
FROM staging_products
WHERE raw_price !~ '^-?[0-9]+(\.[0-9]+)?$'
  AND raw_price IS NOT NULL
GROUP BY raw_price
ORDER BY count DESC;
-- Common culprits: 'N/A', '-', '', '$10.00', '10,000', '10.5%'

-- Find columns that SHOULD be numeric but are TEXT type
SELECT column_name, data_type, table_name
FROM information_schema.columns
WHERE table_name = 'orders'
  AND data_type IN ('character varying', 'text')
  AND column_name IN ('price', 'amount', 'quantity', 'weight', 'age', 'score');
```

---

## 2. Casting Strings to Numbers Safely

```sql
-- Direct cast (crashes on bad data)
SELECT '123.45'::numeric;  -- works
SELECT 'N/A'::numeric;     -- ERROR: crashes query

-- Safe numeric cast function (returns NULL instead of crashing)
CREATE OR REPLACE FUNCTION safe_to_numeric(txt TEXT)
RETURNS NUMERIC LANGUAGE plpgsql AS $$
BEGIN
  RETURN txt::NUMERIC;
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END;
$$;

-- Safe integer cast
CREATE OR REPLACE FUNCTION safe_to_int(txt TEXT)
RETURNS INTEGER LANGUAGE plpgsql AS $$
BEGIN
  RETURN txt::INTEGER;
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END;
$$;

-- Use in bulk updates
UPDATE staging_products
SET price_clean = safe_to_numeric(raw_price);

-- Manual pre-cleaning before cast (handles common formats)
SELECT
  raw_price,
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(raw_price, '[$£€¥,]', '', 'g'),  -- strip currency & commas
      '%$', '', 'g'),                                   -- strip trailing %
    '\s', '', 'g')                                      -- strip spaces
  ::numeric AS price_clean
FROM staging_products
WHERE raw_price ~ '^[$£€¥]?[0-9,]+(\.[0-9]+)?%?$';

-- Handle European number format (comma as decimal separator)
-- '1.234,56' (European) → '1234.56' (standard)
SELECT
  REPLACE(REPLACE(raw_amount, '.', ''), ',', '.')::numeric AS amount_clean
FROM staging_eu_orders
WHERE raw_amount ~ '^\d{1,3}(\.\d{3})*(,\d{2})?$';  -- European format pattern
```

---

## 3. Currency & Symbol Stripping

```sql
-- Strip common currency symbols and formatting characters
UPDATE products
SET price_clean = REGEXP_REPLACE(price_raw, '[$£€¥₹₩,\s]', '', 'g')::numeric
WHERE price_raw ~ '[0-9]';

-- Detect which currencies are present
SELECT
  CASE
    WHEN price_raw LIKE '$%'  THEN 'USD'
    WHEN price_raw LIKE '£%'  THEN 'GBP'
    WHEN price_raw LIKE '€%'  THEN 'EUR'
    WHEN price_raw LIKE '¥%'  THEN 'JPY/CNY'
    WHEN price_raw LIKE '₹%'  THEN 'INR'
    ELSE 'Unknown/None'
  END AS detected_currency,
  COUNT(*) AS count
FROM products
GROUP BY detected_currency;

-- Convert to a base currency (USD) using stored exchange rates
UPDATE products
SET price_usd = CASE currency_code
  WHEN 'USD' THEN price
  WHEN 'EUR' THEN ROUND(price * 1.08, 4)   -- approximate; use a rates table in production
  WHEN 'GBP' THEN ROUND(price * 1.27, 4)
  WHEN 'JPY' THEN ROUND(price * 0.0067, 4)
  WHEN 'PHP' THEN ROUND(price * 0.0175, 4)
  ELSE NULL   -- unknown currency, flag for review
END;

-- Flag rows where currency conversion result is NULL (unknown currency)
SELECT product_id, price, currency_code
FROM products
WHERE price_usd IS NULL AND price IS NOT NULL;

-- Using a rates lookup table (production approach)
UPDATE products AS p
SET price_usd = ROUND(p.price / r.rate_to_usd, 4)
FROM exchange_rates AS r
WHERE p.currency_code = r.currency_code
  AND r.rate_date = CURRENT_DATE;
```

---

## 4. Handling Negative Numbers

```sql
-- Find unexpected negatives
SELECT product_id, price, quantity, revenue
FROM products
WHERE price < 0 OR quantity < 0;

-- Decide: are these real credits/returns or data errors?
-- Strategy 1: take absolute value (errors in sign)
UPDATE orders
SET total_amount = ABS(total_amount)
WHERE total_amount < 0
  AND order_type NOT IN ('refund', 'credit', 'return');

-- Strategy 2: set to NULL (genuinely invalid)
UPDATE products
SET price = NULL
WHERE price < 0;

-- Strategy 3: flag them for review, don't alter
ALTER TABLE orders ADD COLUMN IF NOT EXISTS negative_flag BOOLEAN DEFAULT FALSE;
UPDATE orders
SET negative_flag = TRUE
WHERE total_amount < 0;

-- Report on negative patterns
SELECT
  order_type,
  COUNT(*) FILTER (WHERE total_amount < 0)   AS negative_count,
  COUNT(*) FILTER (WHERE total_amount >= 0)  AS positive_count,
  ROUND(AVG(total_amount), 2)                AS avg_amount
FROM orders
GROUP BY order_type
ORDER BY negative_count DESC;
```

---

## 5. Rounding & Precision Cleanup

```sql
-- Detect excessive decimal places (data import artifact)
SELECT price, COUNT(*)
FROM products
WHERE price != ROUND(price, 2)  -- more than 2 decimal places
GROUP BY price
ORDER BY price;

-- Standardize to 2 decimal places (monetary values)
UPDATE products
SET price = ROUND(price, 2)
WHERE price != ROUND(price, 2);

-- Standardize to 4 decimal places (exchange rates, percentages)
UPDATE exchange_rates
SET rate = ROUND(rate, 4);

-- Detect floating point noise (e.g. 9.999999999 should be 10)
SELECT price
FROM products
WHERE ABS(price - ROUND(price, 0)) < 0.0001  -- very close to an integer
  AND price != ROUND(price, 0);              -- but not exactly integer

-- Fix: round to nearest reasonable precision
UPDATE products
SET price = ROUND(price, 2)
WHERE price IS NOT NULL;

-- Remove tiny trailing values from weight/measurement columns
UPDATE shipments
SET weight_kg = ROUND(weight_kg, 3)
WHERE weight_kg IS NOT NULL;
```

---

## 6. Outlier Detection & Treatment

```sql
-- Method 1: IQR (Interquartile Range) — robust to skew
WITH stats AS (
  SELECT
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY order_value) AS q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY order_value) AS q3
  FROM orders
),
bounds AS (
  SELECT
    q1 - 1.5 * (q3 - q1) AS lower_fence,
    q3 + 1.5 * (q3 - q1) AS upper_fence,
    q1 - 3.0 * (q3 - q1) AS extreme_lower,
    q3 + 3.0 * (q3 - q1) AS extreme_upper
  FROM stats
)
SELECT
  o.order_id,
  o.order_value,
  b.lower_fence,
  b.upper_fence,
  CASE
    WHEN o.order_value < b.extreme_lower OR o.order_value > b.extreme_upper THEN 'Extreme Outlier'
    WHEN o.order_value < b.lower_fence  OR o.order_value > b.upper_fence   THEN 'Mild Outlier'
    ELSE 'Normal'
  END AS outlier_status
FROM orders AS o
CROSS JOIN bounds AS b;

-- Method 2: Z-score (assumes normal distribution)
WITH stats AS (
  SELECT AVG(revenue) AS mean, STDDEV(revenue) AS std
  FROM daily_sales
)
SELECT
  sale_date,
  revenue,
  ROUND(ABS(revenue - stats.mean) / NULLIF(stats.std, 0), 2) AS z_score,
  CASE
    WHEN ABS(revenue - stats.mean) / NULLIF(stats.std, 0) > 3 THEN 'Outlier'
    ELSE 'Normal'
  END AS status
FROM daily_sales CROSS JOIN stats;

-- Treatment 1: cap (winsorize) at fence values
UPDATE orders
SET order_value = bounds.upper_fence
FROM (
  SELECT
    q3 + 1.5 * (q3 - q1) AS upper_fence
  FROM (
    SELECT
      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY order_value) AS q1,
      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY order_value) AS q3
    FROM orders
  ) q
) bounds
WHERE order_value > bounds.upper_fence;

-- Treatment 2: NULL out extreme outliers for exclusion in analysis
UPDATE products
SET price = NULL
WHERE price > (
  SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY price) FROM products
);

-- Treatment 3: flag instead of altering (safest option)
ALTER TABLE orders ADD COLUMN IF NOT EXISTS is_outlier BOOLEAN DEFAULT FALSE;
UPDATE orders SET is_outlier = TRUE
WHERE order_value > (SELECT AVG(order_value) + 3 * STDDEV(order_value) FROM orders);
```

---

## 7. Unit Standardization

```sql
-- Find mixed units in a measurement column
SELECT unit, COUNT(*) AS count
FROM product_measurements
GROUP BY unit
ORDER BY count DESC;
-- Might find: 'kg', 'KG', 'Kg', 'kilogram', 'lbs', 'lb', 'pounds', 'g', 'gram'

-- Standardize all weight to kilograms
UPDATE product_measurements
SET weight_kg = CASE LOWER(TRIM(unit))
  WHEN 'kg'        THEN weight_value
  WHEN 'kilogram'  THEN weight_value
  WHEN 'kilograms' THEN weight_value
  WHEN 'g'         THEN weight_value / 1000.0
  WHEN 'gram'      THEN weight_value / 1000.0
  WHEN 'grams'     THEN weight_value / 1000.0
  WHEN 'lb'        THEN weight_value * 0.453592
  WHEN 'lbs'       THEN weight_value * 0.453592
  WHEN 'pound'     THEN weight_value * 0.453592
  WHEN 'pounds'    THEN weight_value * 0.453592
  WHEN 'oz'        THEN weight_value * 0.0283495
  WHEN 'ounce'     THEN weight_value * 0.0283495
  ELSE NULL  -- unknown unit, flag for review
END;

-- Standardize length to centimeters
UPDATE product_dimensions
SET length_cm = CASE LOWER(TRIM(unit))
  WHEN 'cm'          THEN length_value
  WHEN 'centimeter'  THEN length_value
  WHEN 'mm'          THEN length_value / 10.0
  WHEN 'millimeter'  THEN length_value / 10.0
  WHEN 'm'           THEN length_value * 100.0
  WHEN 'meter'       THEN length_value * 100.0
  WHEN 'in'          THEN length_value * 2.54
  WHEN 'inch'        THEN length_value * 2.54
  WHEN 'inches'      THEN length_value * 2.54
  WHEN 'ft'          THEN length_value * 30.48
  WHEN 'feet'        THEN length_value * 30.48
  ELSE NULL
END;

-- Flag rows with unrecognized units
SELECT DISTINCT unit, COUNT(*) AS count
FROM product_measurements
WHERE weight_kg IS NULL AND weight_value IS NOT NULL
GROUP BY unit
ORDER BY count DESC;
```

---

## 8. Derived Numeric Columns & Ratios

```sql
-- Recalculate totals from components (fix inconsistent totals)
-- line_total SHOULD equal quantity × unit_price
SELECT
  order_item_id,
  quantity,
  unit_price,
  line_total,
  quantity * unit_price AS expected_total,
  ABS(line_total - quantity * unit_price) AS discrepancy
FROM order_items
WHERE ABS(line_total - quantity * unit_price) > 0.01;  -- tolerance for rounding

-- Fix: recalculate from source components
UPDATE order_items
SET line_total = ROUND(quantity * unit_price, 2)
WHERE ABS(line_total - quantity * unit_price) > 0.01;

-- Recalculate order totals from line items
UPDATE orders AS o
SET total_amount = (
  SELECT ROUND(SUM(line_total), 2)
  FROM order_items AS oi
  WHERE oi.order_id = o.order_id
)
WHERE EXISTS (
  SELECT 1 FROM order_items WHERE order_id = o.order_id
);

-- Validate tax calculations (tax should = subtotal × tax_rate)
SELECT
  order_id,
  subtotal,
  tax_amount,
  tax_rate,
  ROUND(subtotal * tax_rate, 2) AS expected_tax,
  ABS(tax_amount - ROUND(subtotal * tax_rate, 2)) AS tax_error
FROM orders
WHERE ABS(tax_amount - ROUND(subtotal * tax_rate, 2)) > 0.01
ORDER BY tax_error DESC;
```

---

## 9. Percentage & Rate Validation

```sql
-- Find percentages stored as 0-100 vs 0-1 (common mix-up)
SELECT
  COUNT(*) FILTER (WHERE discount_rate BETWEEN 0 AND 1)     AS appears_0_to_1_scale,
  COUNT(*) FILTER (WHERE discount_rate BETWEEN 1 AND 100)   AS appears_0_to_100_scale,
  COUNT(*) FILTER (WHERE discount_rate > 100)               AS over_100_invalid,
  COUNT(*) FILTER (WHERE discount_rate < 0)                 AS negative_invalid,
  MAX(discount_rate) AS max_rate,
  MIN(discount_rate) AS min_rate
FROM products;

-- Normalize: convert 0-100 scale to 0-1 scale if mixed
UPDATE products
SET discount_rate = discount_rate / 100.0
WHERE discount_rate > 1 AND discount_rate <= 100;

-- Cap invalid percentages
UPDATE products
SET discount_rate = CASE
  WHEN discount_rate > 1   THEN 1.0   -- cap at 100%
  WHEN discount_rate < 0   THEN 0.0   -- floor at 0%
  ELSE discount_rate
END
WHERE discount_rate NOT BETWEEN 0 AND 1;

-- Validate scores that must be in range
UPDATE surveys
SET satisfaction_score = NULL
WHERE satisfaction_score NOT BETWEEN 1 AND 10;

UPDATE grades
SET score = NULL
WHERE score NOT BETWEEN 0 AND 100;
```

---

## 10. Final Numeric Audit Checklist

```sql
SELECT
  'products' AS table_name,

  -- Type issues
  COUNT(*) FILTER (WHERE price IS NULL)              AS null_prices,
  COUNT(*) FILTER (WHERE price <= 0)                 AS zero_or_negative_prices,

  -- Range issues
  COUNT(*) FILTER (WHERE price > 100000)             AS suspiciously_high,
  COUNT(*) FILTER (WHERE price < 0.01 AND price > 0) AS suspiciously_low,

  -- Precision issues
  COUNT(*) FILTER (WHERE price != ROUND(price, 2))   AS excess_precision,

  -- Outliers (> 3 stdev from mean)
  COUNT(*) FILTER (
    WHERE ABS(price - AVG(price) OVER()) > 3 * STDDEV(price) OVER()
  ) AS statistical_outliers,

  -- Summary stats
  ROUND(MIN(price), 2)    AS min_price,
  ROUND(MAX(price), 2)    AS max_price,
  ROUND(AVG(price), 2)    AS avg_price,
  ROUND(STDDEV(price), 2) AS std_dev,
  COUNT(*)                AS total_rows

FROM products;
```

---

## 📌 Numeric Cleaning Quick Reference

| Problem | Fix |
|---------|-----|
| Text → number | `safe_to_numeric()` function or `TRY_CAST` |
| Currency symbols | `REGEXP_REPLACE(col, '[$£€¥,]', '', 'g')` |
| European format | `REPLACE(REPLACE(col, '.',''), ',','.')` |
| Negative values | `ABS(col)` or `NULL` depending on context |
| Excessive precision | `ROUND(col, 2)` |
| Outliers (IQR) | `q3 + 1.5 × (q3 - q1)` fence |
| Outliers (Z-score) | `ABS(val - mean) / stddev > 3` |
| Unit mismatch | `CASE WHEN unit = 'lb' THEN val * 0.453592` |
| % scale mismatch | `WHERE val > 1 → val / 100.0` |
| Wrong totals | `UPDATE SET total = qty × price` recalc |

---

*Part of the SQL Data Cleaning series → Next: Part 4 — NULL & Missing Data Handling*