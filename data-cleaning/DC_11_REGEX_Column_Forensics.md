# 🧹 SQL Data Cleaning — Part 11: REGEX Column Forensics
> *Search any column for hidden patterns, scan entire tables for misplaced data, extract structured info from unstructured fields, and build a regex-powered data detective toolkit*

---

## Table of Contents
- [1. Scanning Any Column for a Known Pattern](#1-scanning-any-column-for-a-known-pattern)
- [2. Finding Emails Hidden Anywhere in the Table](#2-finding-emails-hidden-anywhere-in-the-table)
- [3. Finding Phones Hidden in Non-Phone Columns](#3-finding-phones-hidden-in-non-phone-columns)
- [4. Finding Dates Hidden in Text Columns](#4-finding-dates-hidden-in-text-columns)
- [5. Finding Numbers & Amounts in Text Fields](#5-finding-numbers--amounts-in-text-fields)
- [6. Data-in-Wrong-Column Detection](#6-data-in-wrong-column-detection)
- [7. Pattern Frequency Analysis (What Shapes Are in My Column?)](#7-pattern-frequency-analysis-what-shapes-are-in-my-column)
- [8. Multi-Column Pattern Cross-Check](#8-multi-column-pattern-cross-check)
- [9. Regex-Based Anomaly Flagging](#9-regex-based-anomaly-flagging)
- [10. Building a Full-Table Pattern Scanner](#10-building-a-full-table-pattern-scanner)
- [11. Extracting Structured Fields from a Notes/Comments Column](#11-extracting-structured-fields-from-a-notescomments-column)
- [12. Regex Pattern Library — Copy-Paste Ready](#12-regex-pattern-library--copy-paste-ready)

---

## 1. Scanning Any Column for a Known Pattern

The foundation: how to search a specific column for any regex pattern.

```sql
-- ┌─────────────────────────────────────────────────────────┐
-- │  TEMPLATE: Search column <col> of table <tbl> for a    │
-- │  pattern, and show what was found                       │
-- └─────────────────────────────────────────────────────────┘

-- Does the column CONTAIN the pattern at all?
SELECT COUNT(*) AS matching_rows
FROM customers
WHERE notes ~ 'your_pattern_here';

-- Show the rows that match
SELECT customer_id, notes
FROM customers
WHERE notes ~* 'your_pattern_here'    -- ~* = case-insensitive
ORDER BY customer_id;

-- Extract the MATCHED portion
SELECT
  customer_id,
  notes,
  SUBSTRING(notes FROM 'your_capture_pattern') AS extracted_value
FROM customers
WHERE notes ~ 'your_pattern_here';

-- Extract ALL matches (one row per match per source row)
SELECT
  customer_id,
  UNNEST(REGEXP_MATCHES(notes, 'your_pattern_here', 'g')) AS each_match
FROM customers
WHERE notes ~ 'your_pattern_here';

-- ─────────────────────────────────────────
-- Practical: find ALL rows in `customers`
-- where the `notes` column contains an email address
-- ─────────────────────────────────────────
SELECT
  customer_id,
  first_name,
  last_name,
  notes,
  SUBSTRING(notes FROM '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
    AS email_in_notes
FROM customers
WHERE notes ~* '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
ORDER BY customer_id;

-- Count how many emails are embedded vs missing from the email column
SELECT
  COUNT(*) FILTER (WHERE notes ~* '[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}')
    AS has_email_in_notes,
  COUNT(*) FILTER (WHERE email IS NULL
    AND notes ~* '[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}')
    AS recoverable_emails   -- notes has email but email column is NULL
FROM customers;
```

---

## 2. Finding Emails Hidden Anywhere in the Table

```sql
-- ─────────────────────────────────────────
-- Scan EVERY text column in a table for email patterns
-- Replace 'customers' with your table name
-- ─────────────────────────────────────────

-- Column by column search
SELECT 'first_name'   AS col, customer_id, first_name   AS value FROM customers WHERE first_name ~* '@' UNION ALL
SELECT 'last_name',            customer_id, last_name              FROM customers WHERE last_name  ~* '@' UNION ALL
SELECT 'phone',                customer_id, phone                  FROM customers WHERE phone      ~* '@' UNION ALL
SELECT 'address',              customer_id, address                FROM customers WHERE address    ~* '@' UNION ALL
SELECT 'city',                 customer_id, city                   FROM customers WHERE city       ~* '@' UNION ALL
SELECT 'notes',                customer_id, notes                  FROM customers WHERE notes      ~* '@';

-- Better: extract the actual email from whichever column it's in
SELECT
  customer_id,
  'first_name'  AS source_col,
  SUBSTRING(first_name FROM '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}') AS found_email
FROM customers WHERE first_name ~* '[a-z0-9]+@[a-z0-9]'

UNION ALL SELECT customer_id, 'phone',
  SUBSTRING(phone FROM '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
FROM customers WHERE phone ~* '@'

UNION ALL SELECT customer_id, 'address',
  SUBSTRING(address FROM '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
FROM customers WHERE address ~* '@'

UNION ALL SELECT customer_id, 'notes',
  SUBSTRING(notes FROM '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
FROM customers WHERE notes ~* '@';

-- ─────────────────────────────────────────
-- RESCUE: Move emails found in wrong columns → email column
-- ─────────────────────────────────────────
UPDATE customers
SET
  email = LOWER(TRIM(
    SUBSTRING(notes FROM '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
  )),
  notes = REGEXP_REPLACE(
    notes,
    '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    '[email moved]',
    'gi'
  )
WHERE email IS NULL
  AND notes ~* '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}';

-- Recover email from phone field
UPDATE customers
SET
  email = LOWER(TRIM(
    SUBSTRING(phone FROM '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
  )),
  phone = NULL
WHERE phone ~* '@'
  AND phone !~ '^\+?\d'   -- is not actually a phone number
  AND email IS NULL;

-- ─────────────────────────────────────────
-- Validate ALL emails after rescue
-- ─────────────────────────────────────────
SELECT
  customer_id,
  email,
  CASE
    WHEN email IS NULL
      THEN '❌ Still missing'
    WHEN email ~ '^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
      THEN '✅ Valid'
    ELSE '⚠️  Rescued but invalid format'
  END AS email_status
FROM customers
ORDER BY email_status;
```

---

## 3. Finding Phones Hidden in Non-Phone Columns

```sql
-- ─────────────────────────────────────────
-- Regex: any sequence that looks like a phone number
-- Covers US formats and international with country code
-- ─────────────────────────────────────────
-- Pattern: \+?1?[\s\-.]?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}

-- Scan notes, address, email columns for phone-like patterns
SELECT
  customer_id,
  'notes'   AS source_col,
  SUBSTRING(notes FROM '\+?1?[\s\-.]?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}')
    AS phone_found
FROM customers
WHERE notes ~ '\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}'

UNION ALL SELECT customer_id, 'address',
  SUBSTRING(address FROM '\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}')
FROM customers WHERE address ~ '\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}'

UNION ALL SELECT customer_id, 'email_field',
  SUBSTRING(email FROM '\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}')
FROM customers WHERE email ~ '\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}'
  AND email NOT LIKE '%@%';  -- not actually an email

-- ─────────────────────────────────────────
-- Detect if the entire email column is actually a phone column
-- (bulk data entry mistake from CSV import)
-- ─────────────────────────────────────────
SELECT
  COUNT(*) FILTER (WHERE email ~ '@')                       AS has_at_sign,
  COUNT(*) FILTER (WHERE email ~ '^\+?[\d\s\-\(\)]+$')     AS looks_like_phone,
  COUNT(*) FILTER (WHERE email ~ '^\d+$')                   AS pure_digits,
  COUNT(*) AS total
FROM customers;
-- If looks_like_phone >> has_at_sign → columns were swapped during import

-- Fix: swap email and phone columns if they're backwards
UPDATE customers
SET
  email = phone,
  phone = email
WHERE email ~ '^\+?[\d\s\-\(\)]{7,}$'   -- email looks like a phone
  AND phone ~* '@';                         -- phone looks like an email

-- ─────────────────────────────────────────
-- Extract phone from a multi-use "contact" column
-- e.g. "Call 555-867-5309 or email joe@x.com"
-- ─────────────────────────────────────────
SELECT
  customer_id,
  contact_info,
  -- Phone extraction
  REGEXP_REPLACE(
    SUBSTRING(contact_info FROM '\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}'),
    '[^0-9]', '', 'g'
  ) AS phone_extracted,
  -- Email extraction
  LOWER(SUBSTRING(contact_info FROM
    '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'))
    AS email_extracted
FROM customers
WHERE contact_info IS NOT NULL;
```

---

## 4. Finding Dates Hidden in Text Columns

```sql
-- ─────────────────────────────────────────
-- Scan for ISO dates: 2024-03-15
-- ─────────────────────────────────────────
SELECT
  customer_id,
  notes,
  SUBSTRING(notes FROM '\d{4}-\d{2}-\d{2}') AS iso_date_found,
  SUBSTRING(notes FROM '\d{4}-\d{2}-\d{2}')::date AS parsed_date
FROM customers
WHERE notes ~ '\d{4}-\d{2}-\d{2}';

-- ─────────────────────────────────────────
-- Scan for US dates: 03/15/2024 or 3/15/24
-- ─────────────────────────────────────────
SELECT
  id,
  description,
  SUBSTRING(description FROM '\d{1,2}/\d{1,2}/\d{2,4}') AS us_date_found
FROM products
WHERE description ~ '\d{1,2}/\d{1,2}/\d{2,4}';

-- ─────────────────────────────────────────
-- Scan for written-out dates: "March 15, 2024" or "15 March 2024"
-- ─────────────────────────────────────────
SELECT
  id,
  notes,
  SUBSTRING(notes FROM
    '(?:January|February|March|April|May|June|July|August|'
    || 'September|October|November|December)\s+\d{1,2},?\s+\d{4}')
    AS written_date_found
FROM orders
WHERE notes ~* '(?:january|february|march|april|may|june|july|august|'
             || 'september|october|november|december)\s+\d{1,2}';

-- ─────────────────────────────────────────
-- Extract multiple date types from one column
-- ─────────────────────────────────────────
SELECT
  order_id,
  notes,
  COALESCE(
    CASE WHEN notes ~ '\d{4}-\d{2}-\d{2}'
         THEN SUBSTRING(notes FROM '\d{4}-\d{2}-\d{2}')::date END,
    CASE WHEN notes ~ '\d{1,2}/\d{1,2}/\d{4}'
         THEN TO_DATE(SUBSTRING(notes FROM '\d{1,2}/\d{1,2}/\d{4}'), 'MM/DD/YYYY') END
  ) AS date_extracted
FROM orders
WHERE notes ~ '\d{4}-\d{2}-\d{2}'
   OR notes ~ '\d{1,2}/\d{1,2}/\d{4}';

-- ─────────────────────────────────────────
-- Recover order date from notes when order_date IS NULL
-- ─────────────────────────────────────────
UPDATE orders
SET order_date = SUBSTRING(notes FROM '\d{4}-\d{2}-\d{2}')::date,
    order_date_source = 'recovered_from_notes'
WHERE order_date IS NULL
  AND notes ~ '\d{4}-\d{2}-\d{2}'
  AND SUBSTRING(notes FROM '\d{4}-\d{2}-\d{2}')::date
      BETWEEN '2010-01-01' AND CURRENT_DATE;
```

---

## 5. Finding Numbers & Amounts in Text Fields

```sql
-- ─────────────────────────────────────────
-- Find currency amounts in a notes column
-- e.g. "Refund of $250.00 processed", "charged €99.99"
-- ─────────────────────────────────────────
SELECT
  order_id,
  notes,
  SUBSTRING(notes FROM '[$£€¥₱]([0-9,]+\.?[0-9]*)') AS raw_match,
  REGEXP_REPLACE(
    SUBSTRING(notes FROM '[$£€¥₱][0-9,]+\.?[0-9]*'),
    '[^0-9.]', '', 'g'
  )::numeric AS amount_extracted
FROM orders
WHERE notes ~ '[$£€¥₱][0-9]';

-- ─────────────────────────────────────────
-- Find discount percentages in product descriptions
-- e.g. "Save 25% today", "20% off", "up to 50% discount"
-- ─────────────────────────────────────────
SELECT
  product_id,
  description,
  SUBSTRING(description FROM '(\d+(?:\.\d+)?)\s*%') AS pct_string,
  SUBSTRING(description FROM '(\d+(?:\.\d+)?)\s*%')::numeric AS pct_value
FROM products
WHERE description ~ '\d+\s*%';

-- ─────────────────────────────────────────
-- Find quantities and measurements
-- e.g. "Ships in 3-5 days", "500ml bottle", "pack of 12"
-- ─────────────────────────────────────────
SELECT
  product_id,
  description,
  -- Ship time
  SUBSTRING(description FROM '(\d+)[\s\-]+\d*\s*(?:days?|weeks?|hours?)')
    AS ship_time_number,
  -- Volume
  SUBSTRING(description FROM '(\d+(?:\.\d+)?)\s*(?:ml|liter|litre|l\b|oz)')
    AS volume_raw,
  -- Count
  SUBSTRING(description FROM '(?:pack|set|lot)\s+of\s+(\d+)')
    AS unit_count
FROM products
WHERE description ~* '\d+\s*(?:days?|ml|pack|oz|liter)';

-- ─────────────────────────────────────────
-- Extract price ranges from text
-- e.g. "$10 - $50", "between 100 and 500 USD"
-- ─────────────────────────────────────────
SELECT
  id,
  price_text,
  SUBSTRING(price_text FROM '[$]?(\d+(?:\.\d+)?)\s*[-–]\s*[$]?(\d+(?:\.\d+)?)')
    AS range_raw,
  REGEXP_REPLACE(
    SUBSTRING(price_text FROM '(\d+(?:\.\d+)?)\s*[-–]'), '[^0-9.]', '', 'g'
  )::numeric AS price_low,
  REGEXP_REPLACE(
    SUBSTRING(price_text FROM '[-–]\s*[$]?(\d+(?:\.\d+)?)'), '[^0-9.]', '', 'g'
  )::numeric AS price_high
FROM products
WHERE price_text ~ '\d+\s*[-–]\s*\d+';
```

---

## 6. Data-in-Wrong-Column Detection

```sql
-- ─────────────────────────────────────────
-- Full cross-column misplacement audit
-- ─────────────────────────────────────────
SELECT
  customer_id,

  -- email column checks
  CASE WHEN email ~ '^\d{10}$'           THEN '⚠️  Phone in email'
       WHEN email !~* '@'                 THEN '⚠️  Not an email'
       WHEN email ~* '[a-z]+@[a-z]+\.'   THEN '✅ Looks like email'
       ELSE '❓ Unknown'
  END AS email_col_status,

  -- phone column checks
  CASE WHEN phone ~* '@'                  THEN '⚠️  Email in phone'
       WHEN phone ~ '^https?://'          THEN '⚠️  URL in phone'
       WHEN phone ~ '^\+?\d[\d\s\-]{7,}' THEN '✅ Looks like phone'
       WHEN phone IS NULL                 THEN 'NULL'
       ELSE '⚠️  Unrecognized'
  END AS phone_col_status,

  -- first_name column checks
  CASE WHEN first_name ~* '@'             THEN '⚠️  Email in first_name'
       WHEN first_name ~ '^\d{10}$'       THEN '⚠️  Phone in first_name'
       WHEN first_name ~ '^\d{4}-\d{2}'  THEN '⚠️  Date in first_name'
       WHEN first_name ~ '^https?://'     THEN '⚠️  URL in first_name'
       WHEN first_name ~ '\d'             THEN '⚠️  Has digits'
       WHEN first_name ~ '^[A-Za-z\s\-]+$' THEN '✅ Looks like name'
       ELSE '❓ Unknown'
  END AS name_col_status

FROM customers
WHERE
  email ~ '^\d{10}$'         OR    -- phone in email
  phone ~* '@'               OR    -- email in phone
  first_name ~* '@'          OR    -- email in name
  first_name ~ '^\d{10}$';         -- phone in name

-- ─────────────────────────────────────────
-- Detect columns where data types are mixed
-- ─────────────────────────────────────────
SELECT
  'email' AS column_name,
  COUNT(*) FILTER (WHERE email ~* '[a-z0-9]+@[a-z0-9]+\.[a-z]{2,}$') AS emails,
  COUNT(*) FILTER (WHERE email ~ '^\+?[\d\s\-\(\)\.]{7,15}$')         AS phones,
  COUNT(*) FILTER (WHERE email ~ '^\d{4}-\d{2}-\d{2}$')               AS dates,
  COUNT(*) FILTER (WHERE email ~* '^https?://')                         AS urls,
  COUNT(*) FILTER (WHERE email ~ '^[A-Za-z ]+$')                       AS names,
  COUNT(*) FILTER (WHERE email IS NULL OR email = '')                   AS nulls,
  COUNT(*) AS total
FROM customers;
```

---

## 7. Pattern Frequency Analysis (What Shapes Are in My Column?)

```sql
-- ─────────────────────────────────────────
-- "Shape fingerprint" — replace each character
-- class with a symbol to reveal structure patterns
-- ─────────────────────────────────────────
SELECT
  pattern_shape,
  COUNT(*) AS occurrences,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct,
  MIN(example) AS example_value
FROM (
  SELECT
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(email,
      '[A-Z]', 'A', 'g'),   -- uppercase → A
      '[a-z]', 'a', 'g'),   -- lowercase → a
      '[0-9]', '9', 'g'),   -- digit → 9
      '\s', '_', 'g'),      -- space → _
      '[^Aa9_@.\-+]', '?', 'g')  -- other → ?
    AS pattern_shape,
    email AS example
  FROM customers
  WHERE email IS NOT NULL
) t
GROUP BY pattern_shape
ORDER BY occurrences DESC
LIMIT 30;

/*  Example output:
    pattern_shape             | occurrences |  pct  |  example
    --------------------------+-------------+-------+--------------------
    aaa@aaaa.aaa              |      42310  | 71.2% | joe@gmail.com
    aaa.aaa@aaaa.aaa          |      10221  | 17.2% | john.doe@gmail.com
    aaa99@aaaa.aaa            |       4102  |  6.9% | bob12@yahoo.com
    aaa@aaaa.aa.aa            |       1089  |  1.8% | jane@mail.co.uk
    ?                         |        204  |  0.3% | (garbage values)
*/

-- ─────────────────────────────────────────
-- Length distribution of a column
-- ─────────────────────────────────────────
SELECT
  LENGTH(email) AS char_length,
  COUNT(*) AS count,
  REPEAT('█', (COUNT(*) / 10)::int) AS bar_chart
FROM customers
WHERE email IS NOT NULL
GROUP BY LENGTH(email)
ORDER BY char_length;

-- ─────────────────────────────────────────
-- First character distribution (detect prefixes)
-- ─────────────────────────────────────────
SELECT
  LEFT(email, 1) AS first_char,
  COUNT(*) AS count
FROM customers
WHERE email IS NOT NULL
GROUP BY first_char
ORDER BY count DESC;
-- Lots of rows starting with non-letters → import problem
```

---

## 8. Multi-Column Pattern Cross-Check

```sql
-- ─────────────────────────────────────────
-- Validate that related columns are consistent
-- ─────────────────────────────────────────

-- Email domain should match company domain (B2B data)
SELECT
  customer_id,
  email,
  company_domain,
  SUBSTRING(email FROM '@(.+)$') AS email_domain,
  CASE
    WHEN company_domain IS NULL THEN '⚠️  No company domain'
    WHEN LOWER(SUBSTRING(email FROM '@(.+)$')) = LOWER(company_domain) THEN '✅ Match'
    WHEN email ~* '@(gmail|yahoo|hotmail|outlook|icloud)\.' THEN '⚠️  Personal email on B2B record'
    ELSE '❓ Domain mismatch'
  END AS domain_check
FROM b2b_customers;

-- ─────────────────────────────────────────
-- Phone country code should match country column
-- ─────────────────────────────────────────
SELECT
  customer_id,
  country,
  phone,
  CASE
    WHEN country = 'US' AND phone !~ '^(\+1|1)?\d{10}$' THEN '⚠️  US record, non-US phone format'
    WHEN country = 'PH' AND phone !~ '^(\+63|0)\d{10}$'  THEN '⚠️  PH record, non-PH phone'
    WHEN country = 'GB' AND phone !~ '^(\+44|0)\d{10,}$' THEN '⚠️  UK record, non-UK phone'
    ELSE '✅ OK'
  END AS phone_country_check
FROM customers
WHERE country IS NOT NULL AND phone IS NOT NULL;

-- ─────────────────────────────────────────
-- ZIP code should be consistent with state (US)
-- ─────────────────────────────────────────
SELECT
  customer_id,
  state,
  zip_code,
  CASE
    -- California: 900xx–961xx
    WHEN state = 'CA'
     AND zip_code !~ '^9[0-6][0-9]{3}$'
     THEN '⚠️  CA state but non-CA ZIP'
    -- New York: 100xx–149xx
    WHEN state = 'NY'
     AND zip_code !~ '^1[0-4][0-9]{3}$'
     THEN '⚠️  NY state but non-NY ZIP'
    -- Texas: 750xx–799xx, 885xx
    WHEN state = 'TX'
     AND zip_code !~ '^7[5-9][0-9]{3}$'
     AND zip_code !~ '^885[0-9]{2}$'
     THEN '⚠️  TX state but non-TX ZIP'
    ELSE '✅ Plausible'
  END AS zip_state_check
FROM customers
WHERE state IN ('CA', 'NY', 'TX')
  AND zip_code IS NOT NULL;

-- ─────────────────────────────────────────
-- Username should be consistent with email local part
-- ─────────────────────────────────────────
SELECT
  user_id,
  username,
  email,
  SUBSTRING(email FROM '^([^@]+)@') AS email_local_part,
  CASE
    WHEN LOWER(username) = LOWER(SUBSTRING(email FROM '^([^@]+)@')) THEN '✅ Match'
    WHEN SIMILARITY(LOWER(username),
         LOWER(SUBSTRING(email FROM '^([^@]+)@'))) > 0.7            THEN '⚠️  Close (possible variant)'
    ELSE '❓ Different'
  END AS username_email_match
FROM users
WHERE email IS NOT NULL AND username IS NOT NULL;
```

---

## 9. Regex-Based Anomaly Flagging

```sql
-- ─────────────────────────────────────────
-- Create a comprehensive anomaly flag table
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_anomalies (
  anomaly_id   SERIAL PRIMARY KEY,
  table_name   TEXT,
  row_id       BIGINT,
  column_name  TEXT,
  anomaly_type TEXT,
  bad_value    TEXT,
  pattern_used TEXT,
  detected_at  TIMESTAMPTZ DEFAULT NOW(),
  resolved     BOOLEAN DEFAULT FALSE
);

-- ─────────────────────────────────────────
-- INSERT anomalies detected by regex scan
-- ─────────────────────────────────────────

-- Anomaly 1: email column doesn't look like an email
INSERT INTO data_anomalies (table_name, row_id, column_name, anomaly_type, bad_value, pattern_used)
SELECT 'customers', customer_id, 'email', 'NOT_AN_EMAIL', email,
  '^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
FROM customers
WHERE email IS NOT NULL
  AND email !~ '^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$';

-- Anomaly 2: phone column contains letters (not a phone)
INSERT INTO data_anomalies (table_name, row_id, column_name, anomaly_type, bad_value, pattern_used)
SELECT 'customers', customer_id, 'phone', 'PHONE_HAS_LETTERS', phone,
  '[a-zA-Z]'
FROM customers
WHERE phone ~ '[a-zA-Z]'
  AND phone !~* 'ext|x\d';  -- allow "ext 123" or "x123"

-- Anomaly 3: name has digits
INSERT INTO data_anomalies (table_name, row_id, column_name, anomaly_type, bad_value, pattern_used)
SELECT 'customers', customer_id, 'first_name', 'NAME_HAS_DIGITS', first_name,
  '[0-9]'
FROM customers
WHERE first_name ~ '[0-9]';

-- Anomaly 4: ZIP code wrong length
INSERT INTO data_anomalies (table_name, row_id, column_name, anomaly_type, bad_value, pattern_used)
SELECT 'customers', customer_id, 'zip_code', 'INVALID_ZIP_FORMAT', zip_code,
  '^\d{5}(-\d{4})?$'
FROM customers
WHERE zip_code IS NOT NULL
  AND zip_code !~ '^\d{5}(-\d{4})?$';

-- Anomaly 5: SKU contains spaces or lowercase
INSERT INTO data_anomalies (table_name, row_id, column_name, anomaly_type, bad_value, pattern_used)
SELECT 'products', product_id, 'sku', 'INVALID_SKU_FORMAT', sku,
  '^[A-Z0-9\-]+$'
FROM products
WHERE sku IS NOT NULL
  AND sku !~ '^[A-Z0-9\-]+$';

-- View all unresolved anomalies
SELECT
  anomaly_type,
  column_name,
  COUNT(*) AS count,
  array_agg(DISTINCT bad_value ORDER BY bad_value) FILTER (WHERE bad_value IS NOT NULL)
    AS examples
FROM data_anomalies
WHERE resolved = FALSE
GROUP BY anomaly_type, column_name
ORDER BY count DESC;
```

---

## 10. Building a Full-Table Pattern Scanner

```sql
-- ─────────────────────────────────────────
-- Stored procedure: scan all text columns
-- of a given table for a given regex pattern
-- ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION scan_table_for_pattern(
  p_table   TEXT,
  p_pattern TEXT,
  p_label   TEXT DEFAULT 'custom_pattern'
)
RETURNS TABLE(
  table_name   TEXT,
  column_name  TEXT,
  row_count    BIGINT,
  sample_value TEXT
) LANGUAGE plpgsql AS $$
DECLARE
  col_rec RECORD;
  sql     TEXT;
  cnt     BIGINT;
  sample  TEXT;
BEGIN
  FOR col_rec IN
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = p_table
      AND table_schema = 'public'
      AND data_type IN ('text', 'character varying', 'character')
    ORDER BY ordinal_position
  LOOP
    sql := format(
      'SELECT COUNT(*), MIN(%I::text) FROM %I WHERE %I::text ~* %L',
      col_rec.column_name, p_table, col_rec.column_name, p_pattern
    );
    EXECUTE sql INTO cnt, sample;

    IF cnt > 0 THEN
      RETURN QUERY SELECT
        p_table,
        col_rec.column_name,
        cnt,
        sample;
    END IF;
  END LOOP;
END;
$$;

-- Usage examples:
SELECT * FROM scan_table_for_pattern('customers', '[a-z0-9]+@[a-z0-9]+\.[a-z]{2,}', 'emails');
SELECT * FROM scan_table_for_pattern('customers', '\d{3}[\s\-\.]\d{3}[\s\-\.]\d{4}', 'phones');
SELECT * FROM scan_table_for_pattern('orders',    'https?://',                        'urls');
SELECT * FROM scan_table_for_pattern('products',  '\d{4}-\d{2}-\d{2}',               'dates');
SELECT * FROM scan_table_for_pattern('customers', '\b\d{3}-\d{2}-\d{4}\b',           'ssn_pattern');

/*  Sample output:
    table_name | column_name | row_count | sample_value
    -----------+-------------+-----------+----------------------------
    customers  | notes       |       834 | "Call me at john@gmail.com"
    customers  | address     |        12 | "info@company.com apt 3"
*/
```

---

## 11. Extracting Structured Fields from a Notes/Comments Column

```sql
-- ─────────────────────────────────────────
-- A free-text "notes" column often hides
-- structured data entered manually by staff
-- Pattern: "Key: Value" or "Key = Value"
-- ─────────────────────────────────────────
SELECT
  customer_id,
  notes,
  -- Extract specific labeled fields
  SUBSTRING(notes FROM '(?i)(?:email|e-mail|mail)[\s:=]+([^\s,;]+@[^\s,;]+)') AS email_in_notes,
  SUBSTRING(notes FROM '(?i)(?:phone|tel|mobile|cell|ph)[\s:=#]+(\+?[\d\s\-\(\)\.]{7,15})') AS phone_in_notes,
  SUBSTRING(notes FROM '(?i)(?:dob|birth|birthday|born)[\s:=]+(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})') AS dob_in_notes,
  SUBSTRING(notes FROM '(?i)(?:account|acct|acc)[\s:#=]+([A-Z0-9\-]{5,20})') AS account_in_notes,
  SUBSTRING(notes FROM '(?i)(?:order|ord)[\s:#=]+([A-Z0-9\-]{5,20})') AS order_ref_in_notes,
  SUBSTRING(notes FROM '(?i)(?:zip|postal)[\s:=]+(\d{4,10})') AS zip_in_notes,
  SUBSTRING(notes FROM '(?i)(?:city|town)[\s:=]+([A-Za-z ]{3,30})') AS city_in_notes
FROM customers
WHERE notes IS NOT NULL AND LENGTH(notes) > 10;

-- ─────────────────────────────────────────
-- Parse a structured comment block like:
-- "Name: John Smith | DOB: 1985-03-15 | Ref: ORD-12345 | Prio: High"
-- ─────────────────────────────────────────
SELECT
  ticket_id,
  comment_text,
  TRIM(SUBSTRING(comment_text FROM '(?i)Name:\s*([^|]+)'))         AS name,
  TRIM(SUBSTRING(comment_text FROM '(?i)DOB:\s*([^|]+)'))          AS dob,
  TRIM(SUBSTRING(comment_text FROM '(?i)Ref:\s*([^|]+)'))          AS ref_number,
  TRIM(SUBSTRING(comment_text FROM '(?i)Prio(?:rity)?:\s*([^|]+)')) AS priority,
  TRIM(SUBSTRING(comment_text FROM '(?i)Status:\s*([^|]+)'))        AS status
FROM support_tickets
WHERE comment_text LIKE '%|%';

-- ─────────────────────────────────────────
-- Migrate recovered data to proper columns
-- ─────────────────────────────────────────
UPDATE customers AS c
SET
  email = LOWER(TRIM(
    SUBSTRING(c.notes FROM
      '(?i)(?:email|e-mail|mail)[\s:=]+([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})'
    )
  ))
WHERE c.email IS NULL
  AND c.notes ~* '(?:email|e-mail|mail)[\s:=]+[a-z0-9._%+\-]+@';

UPDATE customers AS c
SET
  phone = REGEXP_REPLACE(
    SUBSTRING(c.notes FROM
      '(?i)(?:phone|tel|mobile|cell)[\s:=#]+(\+?[\d\s\-\(\)\.]{7,15})'
    ),
    '[^0-9]', '', 'g'
  )
WHERE c.phone IS NULL
  AND c.notes ~* '(?:phone|tel|mobile|cell)[\s:=#]+\+?[\d]';
```

---

## 12. Regex Pattern Library — Copy-Paste Ready

A consolidated library of every pattern from this file, organized by use case.

```sql
-- ══════════════════════════════════════════════════════════
--  REGEX PATTERN LIBRARY — Part 11 Supplement
--  Usage: replace 'your_column' and 'your_table'
-- ══════════════════════════════════════════════════════════

-- ── EMAILS ────────────────────────────────────────────────
-- Search:     col ~* '[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}'
-- Validate:   col ~  '^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
-- Extract:    SUBSTRING(col FROM '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
-- Local part: SUBSTRING(col FROM '^(.+)@')
-- Domain:     SUBSTRING(col FROM '@(.+)$')
-- TLD:        SUBSTRING(col FROM '\.([^.]+)$')
-- All emails: REGEXP_MATCHES(col, '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', 'g')

-- ── PHONES ────────────────────────────────────────────────
-- Search:     col ~ '\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}'
-- Digits:     REGEXP_REPLACE(col, '[^0-9]', '', 'g')
-- Intl:       col ~ '\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{1,9}'
-- Extract:    SUBSTRING(col FROM '\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}')
-- PH mobile:  col ~ '^(\+63|0)9\d{9}$'
-- US valid:   LENGTH(REGEXP_REPLACE(col,'[^0-9]','','g')) = 10

-- ── DATES ─────────────────────────────────────────────────
-- ISO:        col ~ '\d{4}-\d{2}-\d{2}'
-- US:         col ~ '\d{1,2}/\d{1,2}/\d{2,4}'
-- EU dot:     col ~ '\d{1,2}\.\d{1,2}\.\d{4}'
-- EU dash:    col ~ '\d{2}-\d{2}-\d{4}'
-- Written:    col ~* '(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+\d{1,2}'
-- Compact:    col ~ '^\d{8}$'
-- Unix sec:   col ~ '^\d{10}$'
-- Unix ms:    col ~ '^\d{13}$'
-- Extract ISO:SUBSTRING(col FROM '\d{4}-\d{2}-\d{2}')

-- ── NUMBERS ───────────────────────────────────────────────
-- Integer:    col ~ '^\d+$'
-- Decimal:    col ~ '^\d+\.\d+$'
-- Currency:   col ~ '[$£€¥₱]\d'
-- Amount:     REGEXP_REPLACE(col, '[^0-9.]', '', 'g')::numeric
-- Percentage: col ~ '\d+\s*%'
-- Negative:   col ~ '^-\d'
-- European:   col ~ '^\d{1,3}(\.\d{3})*(,\d{2})?$'

-- ── URLS ──────────────────────────────────────────────────
-- Search:     col ~* 'https?://[^\s]+'
-- Validate:   col ~* '^https?://([a-z0-9\-]+\.)+[a-z]{2,}(/.*)?$'
-- Extract:    SUBSTRING(col FROM 'https?://[^\s"''<>]+')
-- Domain:     SUBSTRING(col FROM '^https?://([^/]+)')
-- Path:       SUBSTRING(col FROM '^https?://[^/]+(/.*)$')
-- Strip:      REGEXP_REPLACE(col, 'https?://[^\s"''<>]+', '', 'g')

-- ── ADDRESSES & CODES ─────────────────────────────────────
-- US ZIP:     col ~ '^\d{5}(-\d{4})?$'
-- PH ZIP:     col ~ '^\d{4}$'
-- UK Post:    col ~* '^[A-Z]{1,2}[0-9][0-9A-Z]?\s?[0-9][A-Z]{2}$'
-- State code: col ~ '^[A-Z]{2}$'
-- IPv4:       col ~ '^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'
-- UUID:       col ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

-- ── PII DETECTION (for redaction) ─────────────────────────
-- SSN:        col ~ '\b\d{3}-\d{2}-\d{4}\b'
-- CC number:  col ~ '\b\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}\b'
-- Passport:   col ~* '[A-Z]{1,2}\d{7,9}'
-- DL number:  col ~* '[A-Z]\d{7}|[A-Z]\d{3}-\d{3}-\d{2}-\d{3}'

-- ── NAMES ─────────────────────────────────────────────────
-- Has digits: col ~ '\d'
-- Has @:      col ~* '@'
-- Title pfx:  col ~* '^(mr|mrs|ms|dr|prof)\.?\s'
-- Suffix:     col ~* '\s(jr|sr|ii|iii|iv|esq)\.?$'
-- Bad chars:  col ~ '[^a-zA-Z \-''\.]'
-- Test data:  col ~* '^(test|dummy|fake|sample|admin|null|xxx|asdf)'

-- ── CONTENT DETECTION ─────────────────────────────────────
-- Has HTML:   col ~ '<[a-zA-Z/][^>]*>'
-- Has JSON:   col ~ '^\s*[{\[]'
-- Key=Val:    col ~ '\w+=\w+'
-- CSV list:   col ~ '[^,]+,[^,]+'
-- Has emoji:  col ~ '[^\x00-\x7F]'
-- Whitespace: col != TRIM(col)

-- ══════════════════════════════════════════════════════════
--  QUICK SCAN TEMPLATES
-- ══════════════════════════════════════════════════════════

-- Template A: Does this column have a problem?
SELECT COUNT(*) AS problem_rows
FROM your_table
WHERE your_column ~ 'problem_pattern';

-- Template B: Show me the problem rows
SELECT *, SUBSTRING(your_column FROM 'extract_pattern') AS extracted
FROM your_table
WHERE your_column ~ 'problem_pattern'
LIMIT 100;

-- Template C: How many match vs don't match?
SELECT
  COUNT(*) FILTER (WHERE your_column ~  'expected_pattern') AS matches,
  COUNT(*) FILTER (WHERE your_column !~ 'expected_pattern'
                   AND your_column IS NOT NULL)              AS fails,
  COUNT(*) FILTER (WHERE your_column IS NULL)                AS nulls,
  COUNT(*) AS total
FROM your_table;

-- Template D: Fix by replacing what was found
UPDATE your_table
SET your_column = REGEXP_REPLACE(your_column, 'bad_pattern', 'replacement', 'gi')
WHERE your_column ~ 'bad_pattern';
```

---

## 📌 Column Forensics Quick Reference

| Goal | Query Pattern |
|------|--------------|
| Does column contain email? | `WHERE col ~* '[a-z0-9]+@[a-z0-9]+\.[a-z]+'` |
| Extract email from text | `SUBSTRING(col FROM '[a-zA-Z0-9._%+\-]+@...')` |
| All emails in a text blob | `UNNEST(REGEXP_MATCHES(col, 'email_pattern', 'g'))` |
| Find phone in any column | `WHERE col ~ '\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}'` |
| Detect data in wrong column | `WHERE phone ~* '@'` / `WHERE email ~ '^\d{10}$'` |
| Pattern fingerprint (shape) | `REGEXP_REPLACE(col, '[A-Z]','A','g')` chain |
| Scan all columns of a table | `scan_table_for_pattern('table', 'pattern')` |
| Log anomalies to table | `INSERT INTO data_anomalies ...` |
| Recover data from notes | `UPDATE SET email = SUBSTRING(notes FROM 'email_regex')` |
| Cross-column validation | `WHERE SUBSTRING(email FROM '@(.+)') != company_domain` |

---

*Part of the SQL Data Cleaning series → Next: DC_12_Social_UserContent_Cleaning.md*