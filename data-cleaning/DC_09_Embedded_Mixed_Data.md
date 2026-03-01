# 🧹 SQL Data Cleaning — Part 9: Embedded & Mixed Data in Columns
> *Untangle columns stuffed with multiple data types, JSON blobs, delimited lists, key=value pairs, and HTML*

---

## Table of Contents
- [1. Detecting Mixed Content in a Column](#1-detecting-mixed-content-in-a-column)
- [2. Splitting Delimited Lists into Rows](#2-splitting-delimited-lists-into-rows)
- [3. Parsing Key=Value & Attribute Strings](#3-parsing-keyvalue--attribute-strings)
- [4. Extracting Data from JSON Columns](#4-extracting-data-from-json-columns)
- [5. Cleaning HTML & Markdown from Text](#5-cleaning-html--markdown-from-text)
- [6. Multiple Emails/Phones in One Field](#6-multiple-emailsphones-in-one-field)
- [7. Address Parsing from a Single Field](#7-address-parsing-from-a-single-field)
- [8. Log File Parsing](#8-log-file-parsing)
- [9. Splitting One Column into Many](#9-splitting-one-column-into-many)
- [10. Normalizing Wide Tables to Long (Unpivot)](#10-normalizing-wide-tables-to-long-unpivot)
- [11. Cleaning Concatenated IDs](#11-cleaning-concatenated-ids)

---

## 1. Detecting Mixed Content in a Column

The first step: understand what types of values are actually hiding in a single column.

```sql
-- Profile a "notes" or "comments" free-text column
SELECT
  CASE
    WHEN col ~* '[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}'   THEN 'Contains email'
    WHEN col ~ '\d{3}[\s.\-]?\d{3}[\s.\-]?\d{4}'              THEN 'Contains phone'
    WHEN col ~* 'https?://'                                     THEN 'Contains URL'
    WHEN col ~ '\d{4}-\d{2}-\d{2}'                             THEN 'Contains ISO date'
    WHEN col ~ '\d{1,2}/\d{1,2}/\d{4}'                        THEN 'Contains US date'
    WHEN col ~ '\$[\d,]+\.\d{2}'                               THEN 'Contains currency'
    WHEN col ~ '\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'          THEN 'Contains IP'
    WHEN col ~ '^[{[]'                                          THEN 'Starts with JSON'
    WHEN col ~ '^<'                                             THEN 'Starts with HTML/XML'
    WHEN col ~ '\w+=\w+'                                        THEN 'Contains key=value'
    WHEN col ~ '[;,|]'                                          THEN 'Delimited list'
    WHEN col ~ '^\d+$'                                          THEN 'Pure digits'
    WHEN col IS NULL OR col = ''                                THEN 'NULL/Empty'
    ELSE 'Plain text'
  END AS content_type,
  COUNT(*) AS row_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct
FROM (SELECT your_column AS col FROM your_table) t
GROUP BY content_type
ORDER BY row_count DESC;

-- Find columns where data belongs in a different column
-- e.g. emails stored in the "phone" column
SELECT
  COUNT(*) FILTER (WHERE phone ~* '@')            AS emails_in_phone_col,
  COUNT(*) FILTER (WHERE email ~ '^\d+$')         AS numbers_in_email_col,
  COUNT(*) FILTER (WHERE address ~ '@')           AS emails_in_address_col,
  COUNT(*) FILTER (WHERE first_name ~ '\d{7,}')   AS phone_in_name_col
FROM customers;

-- Detect rows where a single column holds multiple values
SELECT notes,
  ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(notes, ',\s*'), 1) AS comma_count,
  ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(notes, ';\s*'), 1) AS semicolon_count
FROM customers
WHERE notes LIKE '%,%' OR notes LIKE '%;%'
ORDER BY comma_count DESC;
```

---

## 2. Splitting Delimited Lists into Rows

```sql
-- ─────────────────────────────────────────
-- Comma-separated tags: "electronics,sale,featured"
-- ─────────────────────────────────────────

-- Preview: unnest to rows
SELECT
  product_id,
  product_name,
  TRIM(tag) AS tag
FROM products,
  UNNEST(STRING_TO_ARRAY(tags, ',')) AS tag
WHERE tags IS NOT NULL AND tags != '';

-- Write to a proper junction table
INSERT INTO product_tags (product_id, tag)
SELECT
  product_id,
  LOWER(TRIM(tag)) AS tag
FROM products,
  UNNEST(STRING_TO_ARRAY(tags, ',')) AS tag
WHERE tags IS NOT NULL AND TRIM(tag) != ''
ON CONFLICT (product_id, tag) DO NOTHING;

-- ─────────────────────────────────────────
-- Pipe-separated categories: "Food|Beverage|Snacks"
-- ─────────────────────────────────────────
SELECT
  product_id,
  TRIM(UNNEST(STRING_TO_ARRAY(category_path, '|'))) AS category_level
FROM products
WHERE category_path LIKE '%|%';

-- ─────────────────────────────────────────
-- Semicolon-separated emails: "a@x.com; b@y.com; c@z.com"
-- ─────────────────────────────────────────
SELECT
  customer_id,
  LOWER(TRIM(UNNEST(STRING_TO_ARRAY(email_list, ';')))) AS email
FROM customers
WHERE email_list LIKE '%;%';

-- ─────────────────────────────────────────
-- Handle irregular delimiters: comma OR semicolon OR pipe
-- ─────────────────────────────────────────
SELECT
  id,
  TRIM(UNNEST(REGEXP_SPLIT_TO_ARRAY(raw_list, '[,;|]'))) AS item
FROM raw_data
WHERE raw_list IS NOT NULL
  AND raw_list != '';

-- ─────────────────────────────────────────
-- Numbered list: "1. Item A 2. Item B 3. Item C"
-- ─────────────────────────────────────────
SELECT
  doc_id,
  TRIM(item) AS list_item
FROM documents,
  UNNEST(REGEXP_SPLIT_TO_ARRAY(content, '\d+\.\s+')) AS item
WHERE content ~ '\d+\.'
  AND TRIM(item) != '';

-- ─────────────────────────────────────────
-- Fixed-width position splitting
-- e.g. "JOHNDOE 19850315 M" (name=8, date=8, gender=1)
-- ─────────────────────────────────────────
SELECT
  raw_record,
  TRIM(SUBSTRING(raw_record, 1, 8))  AS name,
  SUBSTRING(raw_record, 9, 8)        AS birth_date_raw,
  SUBSTRING(raw_record, 17, 1)       AS gender
FROM fixed_width_import;
```

---

## 3. Parsing Key=Value & Attribute Strings

```sql
-- ─────────────────────────────────────────
-- Format: "color=red;size=large;weight=2.5kg"
-- ─────────────────────────────────────────

-- Extract a specific attribute
SELECT
  product_id,
  attributes,
  SUBSTRING(attributes FROM 'color=([^;]+)')    AS color,
  SUBSTRING(attributes FROM 'size=([^;]+)')     AS size,
  SUBSTRING(attributes FROM 'weight=([^;]+)')   AS weight,
  SUBSTRING(attributes FROM 'brand=([^;]+)')    AS brand
FROM products
WHERE attributes IS NOT NULL;

-- ─────────────────────────────────────────
-- Unnest ALL key=value pairs into rows
-- ─────────────────────────────────────────
WITH pairs AS (
  SELECT
    product_id,
    UNNEST(STRING_TO_ARRAY(attributes, ';')) AS pair
  FROM products
  WHERE attributes IS NOT NULL
)
SELECT
  product_id,
  TRIM(SPLIT_PART(pair, '=', 1)) AS attr_key,
  TRIM(SPLIT_PART(pair, '=', 2)) AS attr_value
FROM pairs
WHERE pair LIKE '%=%';

-- ─────────────────────────────────────────
-- Format: "k1: v1 | k2: v2 | k3: v3"
-- ─────────────────────────────────────────
WITH pairs AS (
  SELECT
    id,
    UNNEST(STRING_TO_ARRAY(metadata, ' | ')) AS pair
  FROM raw_data
)
SELECT
  id,
  TRIM(SPLIT_PART(pair, ': ', 1)) AS key,
  TRIM(SPLIT_PART(pair, ': ', 2)) AS value
FROM pairs
WHERE pair LIKE '%: %';

-- ─────────────────────────────────────────
-- URL query string parsing: "?utm_source=email&utm_medium=cpc&campaign=spring"
-- ─────────────────────────────────────────
SELECT
  url,
  SUBSTRING(url FROM '[?&]utm_source=([^&]+)')   AS utm_source,
  SUBSTRING(url FROM '[?&]utm_medium=([^&]+)')   AS utm_medium,
  SUBSTRING(url FROM '[?&]campaign=([^&]+)')     AS campaign,
  SUBSTRING(url FROM '[?&]utm_content=([^&]+)')  AS utm_content
FROM marketing_clicks
WHERE url ~ '[?&]utm_';
```

---

## 4. Extracting Data from JSON Columns

```sql
-- ─────────────────────────────────────────
-- Basic JSON field extraction
-- ─────────────────────────────────────────
SELECT
  id,
  metadata->>'name'                   AS name,
  metadata->>'email'                  AS email,
  (metadata->>'age')::int             AS age,
  metadata->'address'->>'city'        AS city,
  metadata->'address'->>'country'     AS country,
  (metadata->>'is_active')::boolean   AS is_active
FROM customers
WHERE metadata IS NOT NULL;

-- ─────────────────────────────────────────
-- Validate that JSON is parseable
-- ─────────────────────────────────────────
SELECT id, raw_json
FROM customers
WHERE raw_json IS NOT NULL
  AND raw_json != ''
  AND raw_json !~ '^\{.*\}$'   -- not enclosed in {}
  AND raw_json !~ '^\[.*\]$';  -- not enclosed in []

-- Safe JSON extraction (returns NULL instead of crashing)
CREATE OR REPLACE FUNCTION safe_json_text(data JSONB, key TEXT)
RETURNS TEXT LANGUAGE plpgsql AS $$
BEGIN
  RETURN data->>key;
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END;
$$;

-- ─────────────────────────────────────────
-- Flatten a JSON array column into rows
-- ─────────────────────────────────────────

-- JSON array of strings: ["tag1", "tag2", "tag3"]
SELECT
  product_id,
  JSONB_ARRAY_ELEMENTS_TEXT(tags_json) AS tag
FROM products
WHERE JSONB_TYPEOF(tags_json) = 'array';

-- JSON array of objects: [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
SELECT
  order_id,
  item->>'product_id'             AS product_id,
  item->>'name'                   AS product_name,
  (item->>'quantity')::int        AS quantity,
  (item->>'unit_price')::numeric  AS unit_price
FROM orders,
  JSONB_ARRAY_ELEMENTS(line_items_json) AS item
WHERE JSONB_TYPEOF(line_items_json) = 'array';

-- ─────────────────────────────────────────
-- Clean malformed JSON
-- ─────────────────────────────────────────

-- Single quotes → double quotes (Python dict → JSON)
UPDATE products
SET metadata = REPLACE(REPLACE(metadata::text, '''', '"'), '""', '"')::jsonb
WHERE metadata::text LIKE '%''%';

-- Fix missing quotes around keys: {name: "John"} → {"name": "John"}
UPDATE customers
SET raw_json = REGEXP_REPLACE(
  raw_json,
  '(\{|,)\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:',
  '\1"\2":',
  'g'
)
WHERE raw_json ~ '\{[^"]*[a-zA-Z_][a-zA-Z0-9_]*\s*:';

-- ─────────────────────────────────────────
-- Extract and normalize nested JSON to flat table
-- ─────────────────────────────────────────
INSERT INTO customers_flat (customer_id, name, email, city, country, signup_date)
SELECT
  (metadata->>'id')::int,
  metadata->>'name',
  LOWER(TRIM(metadata->>'email')),
  metadata->'address'->>'city',
  UPPER(TRIM(metadata->'address'->>'country')),
  (metadata->>'created_at')::date
FROM customers_json_import
WHERE metadata IS NOT NULL
  AND metadata->>'id' IS NOT NULL;
```

---

## 5. Cleaning HTML & Markdown from Text

```sql
-- ─────────────────────────────────────────
-- Strip HTML tags
-- ─────────────────────────────────────────

-- Remove all HTML tags
SELECT REGEXP_REPLACE(description, '<[^>]+>', '', 'g') AS clean_text
FROM products;

-- Remove tags but preserve their text content (replace <br> with newline)
UPDATE products
SET description =
  REGEXP_REPLACE(
  REGEXP_REPLACE(
  REGEXP_REPLACE(
  REGEXP_REPLACE(description,
    '<br\s*/?>', E'\n', 'gi'),
    '<p\s*/?>', E'\n', 'gi'),
    '</p>', E'\n', 'gi'),
    '<[^>]+>', '', 'g');

-- Decode HTML entities
UPDATE products
SET description =
  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
    REPLACE(description,
    '&amp;',  '&'),
    '&lt;',   '<'),
    '&gt;',   '>'),
    '&quot;', '"'),
    '&#39;',  ''''),
    '&nbsp;', ' '),
    '&copy;', '©'),
    '&reg;',  '®'),
    '&trade;','™');

-- ─────────────────────────────────────────
-- Strip Markdown formatting
-- ─────────────────────────────────────────

UPDATE articles
SET content =
  REGEXP_REPLACE(
  REGEXP_REPLACE(
  REGEXP_REPLACE(
  REGEXP_REPLACE(
  REGEXP_REPLACE(
  REGEXP_REPLACE(
  REGEXP_REPLACE(content,
    '\*\*(.+?)\*\*', '\1', 'g'),   -- **bold** → bold
    '\*(.+?)\*',     '\1', 'g'),   -- *italic* → italic
    '__(.+?)__',     '\1', 'g'),   -- __bold__ → bold
    '_(.+?)_',       '\1', 'g'),   -- _italic_ → italic
    '#{1,6}\s+',     '',   'g'),   -- ## Heading → (removed)
    '\[(.+?)\]\(.+?\)', '\1', 'g'), -- [text](url) → text
    '`{1,3}[^`]*`{1,3}', '', 'g'); -- `code` → (removed)

-- ─────────────────────────────────────────
-- Detect and clean script/style injection
-- ─────────────────────────────────────────
SELECT id, content
FROM user_submissions
WHERE content ~* '<script|<style|javascript:|onclick=|onerror=';

UPDATE user_submissions
SET content = REGEXP_REPLACE(content, '<script[^>]*>.*?</script>', '', 'gi')
WHERE content ~* '<script';

UPDATE user_submissions
SET content = REGEXP_REPLACE(content, ' on\w+\s*=\s*"[^"]*"', '', 'gi')
WHERE content ~* ' on\w+\s*=';
```

---

## 6. Multiple Emails/Phones in One Field

```sql
-- ─────────────────────────────────────────
-- Multiple emails separated by comma/semicolon
-- "alice@x.com, bob@y.com, carol@z.com"
-- ─────────────────────────────────────────

-- Step 1: identify multi-email rows
SELECT customer_id, email_field
FROM customers
WHERE email_field LIKE '%@%@%'           -- two @ signs = multiple emails
   OR email_field LIKE '%,%'             -- comma-separated
   OR email_field LIKE '%;%';            -- semicolon-separated

-- Step 2: extract ALL emails from the field
SELECT
  customer_id,
  LOWER(TRIM(
    UNNEST(REGEXP_MATCHES(email_field,
      '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
      'g'))
  )) AS email
FROM customers
WHERE email_field ~ '@.*@';  -- has more than one @

-- Step 3: write to a proper 1-to-many table
CREATE TABLE IF NOT EXISTS customer_emails (
  customer_email_id SERIAL PRIMARY KEY,
  customer_id       BIGINT NOT NULL REFERENCES customers(customer_id),
  email             TEXT NOT NULL,
  is_primary        BOOLEAN DEFAULT FALSE,
  source            TEXT DEFAULT 'migrated'
);

INSERT INTO customer_emails (customer_id, email, is_primary)
SELECT
  customer_id,
  LOWER(TRIM(email)) AS email,
  ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY 1) = 1 AS is_primary
FROM (
  SELECT
    customer_id,
    UNNEST(REGEXP_MATCHES(email_field,
      '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
      'g')) AS email
  FROM customers
  WHERE email_field IS NOT NULL
) parsed
ON CONFLICT DO NOTHING;

-- ─────────────────────────────────────────
-- Multiple phones in one field
-- "555-1234, 555-5678 (mobile)"
-- ─────────────────────────────────────────
SELECT
  customer_id,
  phone_field,
  UNNEST(REGEXP_MATCHES(phone_field,
    '\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}', 'g')) AS phone_number
FROM customers
WHERE phone_field ~ '\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d.*\d';
-- Any field with 10+ digits likely has a phone number

-- Extract phone type annotations
SELECT
  customer_id,
  phone_field,
  CASE
    WHEN phone_field ~* '\(mobile\)|\bm:|\bmob\b' THEN 'mobile'
    WHEN phone_field ~* '\(home\)|\bh:|\bhome\b'  THEN 'home'
    WHEN phone_field ~* '\(work\)|\bw:|\boffice\b' THEN 'work'
    ELSE 'unknown'
  END AS phone_type,
  REGEXP_REPLACE(
    SUBSTRING(phone_field FROM '\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}'),
    '[^0-9]', '', 'g'
  ) AS digits_only
FROM customers
WHERE phone_field ~ '\d{10}';
```

---

## 7. Address Parsing from a Single Field

```sql
-- ─────────────────────────────────────────
-- Single-line address: "123 Main St, Springfield, IL 62701"
-- ─────────────────────────────────────────
SELECT
  full_address,
  -- Street: everything before first comma
  TRIM(SPLIT_PART(full_address, ',', 1))           AS street,
  -- City: second segment
  TRIM(SPLIT_PART(full_address, ',', 2))           AS city,
  -- State code: 2 uppercase letters before ZIP
  SUBSTRING(SPLIT_PART(full_address, ',', 3)
    FROM '\b([A-Z]{2})\b')                         AS state,
  -- ZIP: 5 digits (+ optional -4)
  SUBSTRING(SPLIT_PART(full_address, ',', 3)
    FROM '\b(\d{5}(?:-\d{4})?)\b')                AS zip
FROM customers
WHERE full_address ~ ',.*,';

-- ─────────────────────────────────────────
-- Multi-line address stored as single field with \n
-- "123 Main St\nApt 4B\nNew York, NY 10001"
-- ─────────────────────────────────────────
SELECT
  full_address,
  TRIM(SPLIT_PART(full_address, E'\n', 1)) AS address_line_1,
  CASE
    WHEN ARRAY_LENGTH(STRING_TO_ARRAY(full_address, E'\n'), 1) = 3
    THEN TRIM(SPLIT_PART(full_address, E'\n', 2))
    ELSE NULL
  END AS address_line_2,
  TRIM(SPLIT_PART(full_address, E'\n',
    ARRAY_LENGTH(STRING_TO_ARRAY(full_address, E'\n'), 1)
  )) AS city_state_zip
FROM customers
WHERE full_address LIKE '%' || E'\n' || '%';

-- ─────────────────────────────────────────
-- International address normalization
-- ─────────────────────────────────────────

-- Detect country from address text
SELECT
  full_address,
  CASE
    WHEN full_address ~* '\bUSA?\b|\bUnited States\b'    THEN 'US'
    WHEN full_address ~* '\bUK\b|\bUnited Kingdom\b|\bEngland\b' THEN 'GB'
    WHEN full_address ~* '\bPhilippines?\b|\bPilipinas\b|\bPH\b' THEN 'PH'
    WHEN full_address ~* '\bCanada\b|\bON\b|\bBC\b|\bAB\b'      THEN 'CA'
    WHEN full_address ~* '\bAustralia\b|\bNSW\b|\bVIC\b'         THEN 'AU'
    ELSE 'UNKNOWN'
  END AS detected_country
FROM customers
WHERE full_address IS NOT NULL;
```

---

## 8. Log File Parsing

```sql
-- ─────────────────────────────────────────
-- Apache / Nginx access log format:
-- 192.168.1.1 - - [15/Mar/2024:14:32:01 +0000] "GET /page HTTP/1.1" 200 1234
-- ─────────────────────────────────────────
SELECT
  log_line,
  SUBSTRING(log_line FROM '^(\S+)')                               AS ip_address,
  SUBSTRING(log_line FROM '\[(.+?)\]')                            AS timestamp_raw,
  SUBSTRING(log_line FROM '"(GET|POST|PUT|DELETE|PATCH|HEAD) ')   AS http_method,
  SUBSTRING(log_line FROM '"(?:GET|POST|PUT|DELETE) ([^\s"]+)')   AS request_path,
  SUBSTRING(log_line FROM '" (\d{3}) ')                           AS status_code,
  SUBSTRING(log_line FROM '" \d{3} (\d+)')                        AS response_bytes
FROM access_logs;

-- Parse the timestamp
SELECT
  log_line,
  TO_TIMESTAMP(
    SUBSTRING(log_line FROM '\[(.+?)\]'),
    'DD/Mon/YYYY:HH24:MI:SS TZHTZM'
  ) AS parsed_timestamp
FROM access_logs
WHERE log_line ~ '\[\d+/\w+/\d+';

-- ─────────────────────────────────────────
-- Application log: "2024-03-15 14:32:01 ERROR user_id=123 msg=Login failed"
-- ─────────────────────────────────────────
SELECT
  log_line,
  SUBSTRING(log_line FROM '^\d{4}-\d{2}-\d{2}')::date         AS log_date,
  SUBSTRING(log_line FROM '\d{2}:\d{2}:\d{2}')                 AS log_time,
  SUBSTRING(log_line FROM '\b(DEBUG|INFO|WARN|ERROR|FATAL)\b') AS log_level,
  SUBSTRING(log_line FROM 'user_id=(\d+)')                     AS user_id,
  SUBSTRING(log_line FROM 'msg=([^\s]+(?:\s[^\n]+)?)')         AS message
FROM app_logs;

-- Aggregate errors by type and hour
SELECT
  DATE_TRUNC('hour', log_ts) AS hour,
  SUBSTRING(log_line FROM '\b(DEBUG|INFO|WARN|ERROR|FATAL)\b') AS level,
  COUNT(*) AS count
FROM app_logs
WHERE log_ts >= NOW() - INTERVAL '24 hours'
GROUP BY hour, level
ORDER BY hour, level;
```

---

## 9. Splitting One Column into Many

```sql
-- ─────────────────────────────────────────
-- "First Last" → first_name, last_name columns
-- ─────────────────────────────────────────
ALTER TABLE customers ADD COLUMN IF NOT EXISTS first_name TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS last_name  TEXT;

UPDATE customers SET
  first_name = INITCAP(TRIM(SPLIT_PART(full_name, ' ', 1))),
  last_name  = INITCAP(TRIM(SUBSTRING(full_name
                 FROM POSITION(' ' IN full_name) + 1)))
WHERE full_name LIKE '% %';

-- ─────────────────────────────────────────
-- "City, State, ZIP" → 3 columns
-- ─────────────────────────────────────────
ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS ship_city  TEXT,
  ADD COLUMN IF NOT EXISTS ship_state TEXT,
  ADD COLUMN IF NOT EXISTS ship_zip   TEXT;

UPDATE orders SET
  ship_city  = TRIM(SPLIT_PART(ship_address, ',', 1)),
  ship_state = TRIM(SPLIT_PART(ship_address, ',', 2)),
  ship_zip   = TRIM(SPLIT_PART(ship_address, ',', 3))
WHERE ship_address LIKE '%,%,%';

-- ─────────────────────────────────────────
-- "2024-03" → year_col, month_col
-- ─────────────────────────────────────────
ALTER TABLE sales ADD COLUMN IF NOT EXISTS yr  SMALLINT;
ALTER TABLE sales ADD COLUMN IF NOT EXISTS mo  SMALLINT;

UPDATE sales SET
  yr = SPLIT_PART(period, '-', 1)::smallint,
  mo = SPLIT_PART(period, '-', 2)::smallint
WHERE period ~ '^\d{4}-\d{2}$';

-- ─────────────────────────────────────────
-- Product code "ELEC-RED-LG-2024" → parts
-- ─────────────────────────────────────────
ALTER TABLE products
  ADD COLUMN IF NOT EXISTS cat_code   TEXT,
  ADD COLUMN IF NOT EXISTS color_code TEXT,
  ADD COLUMN IF NOT EXISTS size_code  TEXT,
  ADD COLUMN IF NOT EXISTS year_code  SMALLINT;

UPDATE products SET
  cat_code   = SPLIT_PART(sku, '-', 1),
  color_code = SPLIT_PART(sku, '-', 2),
  size_code  = SPLIT_PART(sku, '-', 3),
  year_code  = SPLIT_PART(sku, '-', 4)::smallint
WHERE sku ~ '^[A-Z]+-[A-Z]+-[A-Z]+-\d{4}$';
```

---

## 10. Normalizing Wide Tables to Long (Unpivot)

```sql
-- ─────────────────────────────────────────
-- Wide: one column per month → Long: one row per month
-- month_jan | month_feb | month_mar  →  month | revenue
-- ─────────────────────────────────────────
SELECT
  product_id,
  month_name,
  revenue
FROM products
CROSS JOIN LATERAL (VALUES
  ('2024-01', jan_revenue),
  ('2024-02', feb_revenue),
  ('2024-03', mar_revenue),
  ('2024-04', apr_revenue),
  ('2024-05', may_revenue),
  ('2024-06', jun_revenue),
  ('2024-07', jul_revenue),
  ('2024-08', aug_revenue),
  ('2024-09', sep_revenue),
  ('2024-10', oct_revenue),
  ('2024-11', nov_revenue),
  ('2024-12', dec_revenue)
) AS months(month_name, revenue)
WHERE revenue IS NOT NULL;

-- ─────────────────────────────────────────
-- Wide survey: q1_score, q2_score, ... q10_score → one row per question
-- ─────────────────────────────────────────
SELECT
  respondent_id,
  survey_date,
  question_num,
  score
FROM surveys
CROSS JOIN LATERAL (VALUES
  (1,  q1_score),
  (2,  q2_score),
  (3,  q3_score),
  (4,  q4_score),
  (5,  q5_score),
  (6,  q6_score),
  (7,  q7_score),
  (8,  q8_score),
  (9,  q9_score),
  (10, q10_score)
) AS q(question_num, score)
WHERE score IS NOT NULL;

-- ─────────────────────────────────────────
-- Product specs: material, width, height, depth → attribute + value
-- ─────────────────────────────────────────
SELECT
  product_id,
  attr_name,
  attr_value
FROM products
CROSS JOIN LATERAL (VALUES
  ('material', material),
  ('width_cm', width_cm::text),
  ('height_cm', height_cm::text),
  ('depth_cm', depth_cm::text),
  ('weight_kg', weight_kg::text),
  ('color', color)
) AS attrs(attr_name, attr_value)
WHERE attr_value IS NOT NULL AND attr_value != '';
```

---

## 11. Cleaning Concatenated IDs

```sql
-- ─────────────────────────────────────────
-- IDs smashed together: "12345|67890|11111"
-- ─────────────────────────────────────────
SELECT
  order_id,
  TRIM(UNNEST(STRING_TO_ARRAY(product_ids, '|')))::bigint AS product_id
FROM orders
WHERE product_ids LIKE '%|%';

-- ─────────────────────────────────────────
-- Composite key stored as one string: "customer:123:order:456"
-- ─────────────────────────────────────────
SELECT
  composite_key,
  SPLIT_PART(composite_key, ':', 2)::bigint AS customer_id,
  SPLIT_PART(composite_key, ':', 4)::bigint AS order_id
FROM raw_events
WHERE composite_key ~ '^customer:\d+:order:\d+$';

-- ─────────────────────────────────────────
-- Legacy system: IDs encoded with prefix letters
-- "C001234" → customer 1234, "O009876" → order 9876
-- ─────────────────────────────────────────
SELECT
  legacy_id,
  LEFT(legacy_id, 1)                            AS entity_type_code,
  SUBSTRING(legacy_id FROM 2)::bigint           AS numeric_id,
  CASE LEFT(legacy_id, 1)
    WHEN 'C' THEN 'customer'
    WHEN 'O' THEN 'order'
    WHEN 'P' THEN 'product'
    WHEN 'U' THEN 'user'
    ELSE 'unknown'
  END AS entity_type
FROM legacy_records
WHERE legacy_id ~ '^[A-Z]\d+$';

-- ─────────────────────────────────────────
-- Cross-reference clean IDs back to original messy ones
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS id_crosswalk (
  clean_id     BIGINT NOT NULL,
  legacy_id    TEXT NOT NULL,
  entity_type  TEXT NOT NULL,
  migrated_at  TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO id_crosswalk (clean_id, legacy_id, entity_type)
SELECT
  ROW_NUMBER() OVER(ORDER BY legacy_id) AS clean_id,
  legacy_id,
  CASE LEFT(legacy_id, 1)
    WHEN 'C' THEN 'customer'
    WHEN 'O' THEN 'order'
    WHEN 'P' THEN 'product'
    ELSE 'unknown'
  END
FROM legacy_records;
```

---

## 📌 Embedded & Mixed Data Quick Reference

| Problem | Solution |
|---------|---------|
| Comma-separated list in column | `UNNEST(STRING_TO_ARRAY(col, ','))` |
| Pipe/semicolon delimited | `REGEXP_SPLIT_TO_TABLE(col, '[;|]')` |
| Key=value attribute string | `SUBSTRING(col FROM 'key=([^;]+)')` |
| JSON column | `col->>'field'` or `JSONB_ARRAY_ELEMENTS` |
| HTML tags in text | `REGEXP_REPLACE(col, '<[^>]+>', '', 'g')` |
| HTML entities | `REPLACE(col, '&amp;', '&')` chain |
| Multiple emails in one field | `REGEXP_MATCHES(col, email_pattern, 'g')` |
| Address in single string | `SPLIT_PART(col, ',', N)` + ZIP regex |
| Wide → long (unpivot) | `CROSS JOIN LATERAL (VALUES ...)` |
| Log line parsing | `SUBSTRING(col FROM 'pattern')` per field |
| Composite ID splitting | `SPLIT_PART(col, ':', N)` or regex |
| URL query string | `SUBSTRING(url FROM '[?&]key=([^&]+)')` |

---

*Part of the SQL Data Cleaning series → Next: DC_10_Advanced_Standardization.md*