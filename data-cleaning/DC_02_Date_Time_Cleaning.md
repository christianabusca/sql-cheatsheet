# 🧹 SQL Data Cleaning — Part 2: Date & Time Cleaning
> *Fix inconsistent date formats, invalid dates, timezone chaos, and time-series gaps*

---

## Table of Contents
- [1. Detecting Date Problems](#1-detecting-date-problems)
- [2. Parsing & Converting Date Strings](#2-parsing--converting-date-strings)
- [3. Fixing Invalid Date Values](#3-fixing-invalid-date-values)
- [4. Standardizing Date Formats](#4-standardizing-date-formats)
- [5. Timezone Normalization](#5-timezone-normalization)
- [6. Fixing Impossible Timestamps](#6-fixing-impossible-timestamps)
- [7. Filling Date Gaps](#7-filling-date-gaps)
- [8. Date Component Extraction & Validation](#8-date-component-extraction--validation)
- [9. Handling Mixed Date Formats in One Column](#9-handling-mixed-date-formats-in-one-column)
- [10. Final Date Audit Checklist](#10-final-date-audit-checklist)

---

## 1. Detecting Date Problems

Before fixing, understand what you're dealing with.

```sql
-- Audit all date columns for NULLs, range issues, and future dates
SELECT
  COUNT(*)                                              AS total_rows,
  COUNT(order_date)                                     AS non_null_dates,
  COUNT(*) - COUNT(order_date)                          AS null_dates,
  MIN(order_date)                                       AS earliest_date,
  MAX(order_date)                                       AS latest_date,
  COUNT(*) FILTER (WHERE order_date > CURRENT_DATE)     AS future_dates,
  COUNT(*) FILTER (WHERE order_date < '2000-01-01')     AS suspiciously_old,
  COUNT(*) FILTER (WHERE EXTRACT(YEAR FROM order_date)
                         NOT BETWEEN 2010 AND 2030)     AS out_of_range_year
FROM orders;

-- Detect date strings stored as TEXT that won't cast cleanly
SELECT raw_date_col, COUNT(*) AS count
FROM staging_orders
WHERE raw_date_col !~ '^\d{4}-\d{2}-\d{2}$'   -- not ISO 8601
  AND raw_date_col !~ '^\d{2}/\d{2}/\d{4}$'   -- not MM/DD/YYYY
  AND raw_date_col !~ '^\d{2}-\d{2}-\d{4}$'   -- not DD-MM-YYYY
GROUP BY raw_date_col
ORDER BY count DESC;

-- Check for placeholder "null-like" date values
SELECT raw_date_col, COUNT(*)
FROM staging_orders
WHERE raw_date_col IN (
  '0000-00-00', '1900-01-01', '1970-01-01',
  '9999-12-31', '01/01/1900', 'N/A', 'NULL',
  'null', '', '-', 'TBD', 'Unknown'
)
GROUP BY raw_date_col;
```

---

## 2. Parsing & Converting Date Strings

Text columns with dates need careful casting based on the format they're in.

```sql
-- ISO 8601: '2024-03-15' → direct cast
SELECT '2024-03-15'::date;
SELECT CAST('2024-03-15' AS DATE);

-- MM/DD/YYYY: '03/15/2024'
SELECT TO_DATE('03/15/2024', 'MM/DD/YYYY');

-- DD/MM/YYYY: '15/03/2024' (European format)
SELECT TO_DATE('15/03/2024', 'DD/MM/YYYY');

-- DD-Mon-YYYY: '15-Mar-2024'
SELECT TO_DATE('15-Mar-2024', 'DD-Mon-YYYY');

-- Month name formats: 'March 15, 2024'
SELECT TO_DATE('March 15, 2024', 'Month DD, YYYY');

-- YYYYMMDD: '20240315'
SELECT TO_DATE('20240315', 'YYYYMMDD');

-- Unix timestamp (seconds since epoch)
SELECT TO_TIMESTAMP(1710460800) AS from_unix;

-- Unix timestamp in milliseconds
SELECT TO_TIMESTAMP(1710460800000 / 1000.0) AS from_unix_ms;

-- With full timestamp: '2024-03-15 14:30:00'
SELECT TO_TIMESTAMP('2024-03-15 14:30:00', 'YYYY-MM-DD HH24:MI:SS');

-- Safe parsing — try cast, return NULL on failure
CREATE OR REPLACE FUNCTION safe_to_date(txt TEXT, fmt TEXT)
RETURNS DATE LANGUAGE plpgsql AS $$
BEGIN
  RETURN TO_DATE(txt, fmt);
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END;
$$;

-- Use the safe function in bulk conversion
SELECT
  raw_date_col,
  safe_to_date(raw_date_col, 'MM/DD/YYYY') AS parsed_date
FROM staging_orders;
```

---

## 3. Fixing Invalid Date Values

```sql
-- Replace known placeholder "null" dates with actual NULL
UPDATE orders
SET order_date = NULL
WHERE order_date IN ('0000-00-00'::date, '1900-01-01'::date, '1970-01-01'::date)
   OR order_date > '2099-12-31'::date;

-- Fix future order dates that are clearly typos (e.g. 2204 instead of 2024)
UPDATE orders
SET order_date = (order_date - INTERVAL '180 years')::date
WHERE EXTRACT(YEAR FROM order_date) BETWEEN 2180 AND 2250;

-- Fix dates where day and month were swapped (if detectable)
-- e.g. month=15 is impossible → swap
UPDATE orders
SET order_date = TO_DATE(
  EXTRACT(YEAR  FROM order_date)::text || '-' ||
  EXTRACT(DAY   FROM order_date)::text || '-' ||  -- swap: day → month position
  EXTRACT(MONTH FROM order_date)::text,            -- swap: month → day position
  'YYYY-MM-DD'
)
WHERE EXTRACT(MONTH FROM order_date) > 12;
-- Note: only works when month > 12 makes the swap unambiguous

-- Clamp dates to a valid business range
UPDATE orders
SET order_date = CASE
  WHEN order_date < '2015-01-01' THEN '2015-01-01'
  WHEN order_date > CURRENT_DATE THEN CURRENT_DATE
  ELSE order_date
END
WHERE order_date < '2015-01-01' OR order_date > CURRENT_DATE;

-- Set to NULL if date is outside any reasonable range
UPDATE orders
SET order_date = NULL
WHERE order_date NOT BETWEEN '2010-01-01' AND '2030-12-31';
```

---

## 4. Standardizing Date Formats

All dates should live as DATE or TIMESTAMP types — not strings.

```sql
-- Add a clean date column alongside the raw text column
ALTER TABLE staging_orders ADD COLUMN order_date_clean DATE;

-- Populate using mixed-format detection
UPDATE staging_orders
SET order_date_clean = CASE
  WHEN raw_date ~ '^\d{4}-\d{2}-\d{2}$'       THEN TO_DATE(raw_date, 'YYYY-MM-DD')
  WHEN raw_date ~ '^\d{2}/\d{2}/\d{4}$'        THEN TO_DATE(raw_date, 'MM/DD/YYYY')
  WHEN raw_date ~ '^\d{2}-\d{2}-\d{4}$'        THEN TO_DATE(raw_date, 'DD-MM-YYYY')
  WHEN raw_date ~ '^\d{2}\.\d{2}\.\d{4}$'      THEN TO_DATE(raw_date, 'DD.MM.YYYY')
  WHEN raw_date ~ '^\d{8}$'                     THEN TO_DATE(raw_date, 'YYYYMMDD')
  WHEN raw_date ~ '^\d{2}-[A-Za-z]{3}-\d{4}$'  THEN TO_DATE(raw_date, 'DD-Mon-YYYY')
  ELSE NULL
END;

-- Verify how many were successfully parsed
SELECT
  COUNT(*) FILTER (WHERE order_date_clean IS NOT NULL) AS parsed_ok,
  COUNT(*) FILTER (WHERE order_date_clean IS NULL
                   AND raw_date IS NOT NULL)            AS failed_to_parse,
  COUNT(*) FILTER (WHERE raw_date IS NULL)              AS source_null
FROM staging_orders;

-- Check the failures
SELECT DISTINCT raw_date, COUNT(*) AS occurrences
FROM staging_orders
WHERE order_date_clean IS NULL
  AND raw_date IS NOT NULL
GROUP BY raw_date
ORDER BY occurrences DESC;
```

---

## 5. Timezone Normalization

Mixed timezones produce wrong calculations, incorrect aggregations, and duplicate events.

```sql
-- Detect what timezones are present
SELECT
  DISTINCT timezone,
  COUNT(*) AS row_count
FROM user_events
GROUP BY timezone
ORDER BY row_count DESC;

-- Convert all timestamps to UTC for storage
UPDATE user_events
SET event_ts_utc = event_ts AT TIME ZONE timezone AT TIME ZONE 'UTC'
WHERE timezone IS NOT NULL;

-- Convert specific named timezones to UTC
UPDATE orders
SET created_at_utc = created_at AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC'
WHERE source_region = 'US-EAST';

-- Store as TIMESTAMPTZ (timestamp with timezone) — PostgreSQL handles conversion
ALTER TABLE orders ALTER COLUMN created_at TYPE TIMESTAMPTZ
  USING created_at AT TIME ZONE 'UTC';

-- Display in a specific user timezone for reporting
SELECT
  order_id,
  created_at_utc AT TIME ZONE 'Asia/Manila'      AS manila_time,
  created_at_utc AT TIME ZONE 'America/New_York'  AS ny_time,
  created_at_utc AT TIME ZONE 'Europe/London'     AS london_time
FROM orders;

-- Find events where timezone info is missing
SELECT COUNT(*) AS missing_tz
FROM user_events
WHERE timezone IS NULL AND event_ts IS NOT NULL;

-- Default missing timezone to UTC (with a flag)
UPDATE user_events
SET
  timezone = 'UTC',
  tz_assumed = TRUE
WHERE timezone IS NULL;
```

---

## 6. Fixing Impossible Timestamps

```sql
-- Find logical contradictions: end before start
SELECT order_id, order_date, shipped_date, delivered_date
FROM orders
WHERE shipped_date < order_date
   OR delivered_date < shipped_date;

-- Fix: swap if values are clearly reversed
UPDATE orders
SET
  order_date   = shipped_date,
  shipped_date = order_date
WHERE shipped_date < order_date
  AND (shipped_date - order_date) BETWEEN -30 AND 0; -- only if within 30 days

-- Set impossible end date to NULL for manual review
UPDATE orders
SET delivered_date = NULL
WHERE delivered_date < shipped_date;

-- Find zero-duration events (start = end, clearly a data entry error)
SELECT session_id, start_ts, end_ts
FROM sessions
WHERE end_ts = start_ts;

UPDATE sessions
SET end_ts = NULL
WHERE end_ts = start_ts;

-- Find negative durations
SELECT session_id, start_ts, end_ts,
       end_ts - start_ts AS duration
FROM sessions
WHERE end_ts < start_ts;

-- Find timestamps with absurd durations (e.g. 10+ year sessions)
SELECT session_id, start_ts, end_ts,
       end_ts - start_ts AS duration
FROM sessions
WHERE end_ts - start_ts > INTERVAL '365 days';
```

---

## 7. Filling Date Gaps

```sql
-- Identify gaps in a daily time series
WITH date_spine AS (
  SELECT generate_series(
    (SELECT MIN(sale_date) FROM daily_sales),
    (SELECT MAX(sale_date) FROM daily_sales),
    INTERVAL '1 day'
  )::date AS expected_date
)
SELECT expected_date AS missing_date
FROM date_spine
LEFT JOIN daily_sales ON expected_date = sale_date
WHERE sale_date IS NULL
ORDER BY missing_date;

-- Insert zero-revenue rows for missing dates
INSERT INTO daily_sales (sale_date, revenue, orders)
SELECT expected_date, 0, 0
FROM (
  SELECT generate_series(
    '2024-01-01'::date,
    '2024-12-31'::date,
    INTERVAL '1 day'
  )::date AS expected_date
) AS spine
WHERE expected_date NOT IN (SELECT sale_date FROM daily_sales);

-- Forward-fill missing values using LOCF (Last Observation Carried Forward)
WITH filled AS (
  SELECT
    date,
    metric_value,
    COUNT(metric_value) OVER(ORDER BY date) AS grp  -- increments only on non-NULL
  FROM (
    SELECT ds.date, ds2.metric_value
    FROM (SELECT generate_series('2024-01-01'::date, '2024-12-31'::date, '1 day')::date AS date) ds
    LEFT JOIN daily_metrics ds2 ON ds.date = ds2.metric_date
  ) AS joined
)
SELECT
  date,
  metric_value,
  FIRST_VALUE(metric_value) OVER(
    PARTITION BY grp ORDER BY date
  ) AS value_filled_forward
FROM filled;
```

---

## 8. Date Component Extraction & Validation

```sql
-- Extract components for validation
SELECT
  order_date,
  EXTRACT(YEAR  FROM order_date) AS yr,
  EXTRACT(MONTH FROM order_date) AS mo,
  EXTRACT(DAY   FROM order_date) AS dy,
  EXTRACT(DOW   FROM order_date) AS day_of_week,   -- 0=Sunday
  EXTRACT(DOY   FROM order_date) AS day_of_year,
  EXTRACT(WEEK  FROM order_date) AS iso_week
FROM orders;

-- Validate month-end dates (e.g. Feb 30 should not exist)
SELECT raw_date
FROM staging
WHERE CASE
  WHEN EXTRACT(MONTH FROM raw_date::date) IN (4,6,9,11)
    THEN EXTRACT(DAY FROM raw_date::date) > 30
  WHEN EXTRACT(MONTH FROM raw_date::date) = 2
       AND EXTRACT(YEAR FROM raw_date::date) % 4 = 0
    THEN EXTRACT(DAY FROM raw_date::date) > 29   -- leap year Feb
  WHEN EXTRACT(MONTH FROM raw_date::date) = 2
    THEN EXTRACT(DAY FROM raw_date::date) > 28   -- non-leap Feb
  ELSE FALSE
END;

-- Add derived date columns for analytics
ALTER TABLE orders ADD COLUMN IF NOT EXISTS order_year   SMALLINT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS order_month  SMALLINT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS order_quarter SMALLINT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS order_week   SMALLINT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS is_weekend   BOOLEAN;

UPDATE orders
SET
  order_year    = EXTRACT(YEAR    FROM order_date),
  order_month   = EXTRACT(MONTH   FROM order_date),
  order_quarter = EXTRACT(QUARTER FROM order_date),
  order_week    = EXTRACT(WEEK    FROM order_date),
  is_weekend    = EXTRACT(DOW FROM order_date) IN (0, 6);
```

---

## 9. Handling Mixed Date Formats in One Column

This is the hardest case — a single text column containing multiple formats at once.

```sql
-- Identify each row's format using regex classification
SELECT
  raw_date,
  CASE
    WHEN raw_date ~ '^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?$' THEN 'ISO_8601'
    WHEN raw_date ~ '^\d{2}/\d{2}/\d{4}$'                        THEN 'MM/DD/YYYY'
    WHEN raw_date ~ '^\d{2}-\d{2}-\d{4}$'                        THEN 'DD-MM-YYYY'
    WHEN raw_date ~ '^\d{2}\.\d{2}\.\d{4}$'                      THEN 'DD.MM.YYYY'
    WHEN raw_date ~ '^\d{8}$'                                     THEN 'YYYYMMDD'
    WHEN raw_date ~ '^\d{1,2} [A-Za-z]+ \d{4}$'                  THEN 'D Month YYYY'
    WHEN raw_date ~ '^\d{10}$'                                    THEN 'UNIX_SECONDS'
    WHEN raw_date ~ '^\d{13}$'                                    THEN 'UNIX_MILLISECONDS'
    WHEN raw_date IN ('', 'N/A', 'NULL', 'null', '-', 'TBD')      THEN 'NULL_PLACEHOLDER'
    ELSE 'UNKNOWN'
  END AS detected_format,
  COUNT(*) AS count
FROM staging_data
GROUP BY raw_date, detected_format
ORDER BY detected_format, count DESC;

-- Parse each format into a clean DATE
SELECT
  raw_date,
  CASE
    WHEN raw_date ~ '^\d{4}-\d{2}-\d{2}$'       THEN raw_date::date
    WHEN raw_date ~ '^\d{2}/\d{2}/\d{4}$'        THEN TO_DATE(raw_date, 'MM/DD/YYYY')
    WHEN raw_date ~ '^\d{2}-\d{2}-\d{4}$'        THEN TO_DATE(raw_date, 'DD-MM-YYYY')
    WHEN raw_date ~ '^\d{2}\.\d{2}\.\d{4}$'      THEN TO_DATE(raw_date, 'DD.MM.YYYY')
    WHEN raw_date ~ '^\d{8}$'                     THEN TO_DATE(raw_date, 'YYYYMMDD')
    WHEN raw_date ~ '^\d{10}$'                    THEN TO_TIMESTAMP(raw_date::bigint)::date
    WHEN raw_date ~ '^\d{13}$'                    THEN TO_TIMESTAMP(raw_date::bigint / 1000.0)::date
    WHEN raw_date IN ('', 'N/A', 'NULL', 'null')  THEN NULL
    ELSE NULL
  END AS clean_date
FROM staging_data;
```

---

## 10. Final Date Audit Checklist

```sql
-- Run this after all date cleaning steps
SELECT
  'orders' AS table_name,

  -- Null check
  COUNT(*) FILTER (WHERE order_date IS NULL)           AS null_dates,

  -- Range check
  COUNT(*) FILTER (WHERE order_date > CURRENT_DATE)    AS future_dates,
  COUNT(*) FILTER (WHERE order_date < '2010-01-01')    AS ancient_dates,

  -- Logical order check
  COUNT(*) FILTER (WHERE shipped_date < order_date)    AS ship_before_order,
  COUNT(*) FILTER (WHERE delivered_date < shipped_date) AS delivered_before_shipped,

  -- Weekend check (if business-days-only expected)
  COUNT(*) FILTER (WHERE EXTRACT(DOW FROM order_date) IN (0,6)) AS weekend_orders,

  -- Year sanity
  COUNT(DISTINCT EXTRACT(YEAR FROM order_date)) AS distinct_years,
  MIN(order_date) AS min_date,
  MAX(order_date) AS max_date,
  COUNT(*) AS total_rows

FROM orders;
```

---

## 📌 Date Cleaning Quick Reference

| Problem | Fix |
|---------|-----|
| Text dates → DATE type | `TO_DATE(col, 'format')` |
| Mixed formats | `CASE WHEN col ~ 'pattern' THEN TO_DATE(...)` |
| Unix timestamps | `TO_TIMESTAMP(col::bigint)` |
| Future dates | `CASE WHEN col > CURRENT_DATE THEN NULL ELSE col END` |
| Placeholder NULLs ('N/A') | `NULLIF(col, 'N/A')` → then cast |
| Timezone normalization | `ts AT TIME ZONE 'src' AT TIME ZONE 'UTC'` |
| End before start | Flag and swap or NULL out |
| Missing date rows | `generate_series()` + `LEFT JOIN` + `INSERT` |
| Day/month swapped | `TO_DATE(YYYY-DAY-MONTH, 'YYYY-MM-DD')` |
| Leap year validation | `EXTRACT(DAY) <= 29 WHERE MONTH = 2` check |

---

*Part of the SQL Data Cleaning series → Next: Part 3 — Numeric & Currency Cleaning*