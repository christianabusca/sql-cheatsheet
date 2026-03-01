# 🧹 SQL Data Cleaning — Part 10: Advanced Standardization & Production Hardening
> *Lookup table standardization, encoding normalization, reusable cleaning functions, data contracts, and automated quality gates*

---

## Table of Contents
- [1. Building a Cleaning Function Library](#1-building-a-cleaning-function-library)
- [2. Lookup Table Standardization](#2-lookup-table-standardization)
- [3. ISO & Standard Code Normalization](#3-iso--standard-code-normalization)
- [4. Character Encoding Normalization](#4-character-encoding-normalization)
- [5. Handling Locale-Specific Data](#5-handling-locale-specific-data)
- [6. Data Contract Enforcement (Checks Before Ingest)](#6-data-contract-enforcement-checks-before-ingest)
- [7. Automated Cleaning Triggers](#7-automated-cleaning-triggers)
- [8. Regex-Powered Data Dictionary & Column Profiler](#8-regex-powered-data-dictionary--column-profiler)
- [9. Cleaning Views for Safe Analysis](#9-cleaning-views-for-safe-analysis)
- [10. Complete Pre-Analysis Data Health Report](#10-complete-pre-analysis-data-health-report)

---

## 1. Building a Cleaning Function Library

Create reusable SQL functions so you write each cleaning rule once and apply it everywhere.

```sql
-- ─────────────────────────────────────────
-- Text normalization: trim + collapse spaces + normalize unicode
-- ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION clean_text(input TEXT)
RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$
  SELECT NULLIF(
    TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE(input, '[\t\n\r]+', ' ', 'g'),  -- tabs/newlines → space
      '\s{2,}', ' ', 'g'                             -- multiple spaces → one
    )),
    ''  -- empty string → NULL
  );
$$;

-- ─────────────────────────────────────────
-- Name cleaning: trim, title-case, strip titles/suffixes
-- ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION clean_name(input TEXT)
RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$
  SELECT INITCAP(LOWER(
    TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(input,
          '^(Mr\.?|Mrs\.?|Ms\.?|Miss\.?|Dr\.?|Prof\.?|Rev\.?)\s+', '', 'i'),
        '\s+(Jr\.?|Sr\.?|II|III|IV|Esq\.?)$', '', 'i'),
      '[^\x20-\x7E]', '', 'g'))
  ));
$$;

-- ─────────────────────────────────────────
-- Email cleaning: lowercase + trim + remove mailto:
-- ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION clean_email(input TEXT)
RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$
  SELECT NULLIF(
    LOWER(TRIM(REGEXP_REPLACE(input, '^mailto:', '', 'i'))),
    ''
  );
$$;

-- Email validator: returns TRUE if valid
CREATE OR REPLACE FUNCTION is_valid_email(input TEXT)
RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE AS $$
  SELECT input ~* '^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$';
$$;

-- ─────────────────────────────────────────
-- Phone cleaning: digits only, validated length
-- ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION clean_phone(input TEXT)
RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$
DECLARE
  digits TEXT;
BEGIN
  digits := REGEXP_REPLACE(COALESCE(input, ''), '[^0-9]', '', 'g');
  IF LENGTH(digits) = 11 AND LEFT(digits, 1) = '1' THEN
    digits := SUBSTRING(digits FROM 2);  -- strip US country code
  END IF;
  IF LENGTH(digits) != 10 THEN RETURN NULL; END IF;
  RETURN digits;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Format phone to (XXX) XXX-XXXX
CREATE OR REPLACE FUNCTION format_phone_us(input TEXT)
RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$
  SELECT CASE WHEN LENGTH(clean_phone(input)) = 10
    THEN '(' || SUBSTRING(clean_phone(input), 1, 3) || ') '
            || SUBSTRING(clean_phone(input), 4, 3) || '-'
            || SUBSTRING(clean_phone(input), 7, 4)
    ELSE NULL
  END;
$$;

-- ─────────────────────────────────────────
-- Safe type casting (no crash on bad data)
-- ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION safe_int(val TEXT)
RETURNS INTEGER LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN RETURN val::INTEGER;
EXCEPTION WHEN OTHERS THEN RETURN NULL; END; $$;

CREATE OR REPLACE FUNCTION safe_numeric(val TEXT)
RETURNS NUMERIC LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN RETURN val::NUMERIC;
EXCEPTION WHEN OTHERS THEN RETURN NULL; END; $$;

CREATE OR REPLACE FUNCTION safe_date(val TEXT, fmt TEXT DEFAULT 'YYYY-MM-DD')
RETURNS DATE LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN RETURN TO_DATE(val, fmt);
EXCEPTION WHEN OTHERS THEN RETURN NULL; END; $$;

CREATE OR REPLACE FUNCTION safe_jsonb(val TEXT)
RETURNS JSONB LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN RETURN val::JSONB;
EXCEPTION WHEN OTHERS THEN RETURN NULL; END; $$;

-- ─────────────────────────────────────────
-- NULL normalizer: converts common null-like strings to real NULL
-- ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION normalize_null(val TEXT)
RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$
  SELECT CASE
    WHEN val IS NULL THEN NULL
    WHEN LOWER(TRIM(val)) IN (
      '', 'null', 'none', 'n/a', 'na', 'nil', '-', '--', '---',
      '?', 'unknown', 'unk', 'missing', 'undefined', 'unspecified',
      'not available', 'not applicable', 'tbd', 'to be determined',
      'blank', 'empty', '#n/a', '#null!', '(blank)', '(none)',
      '(unknown)', '(empty)', '(null)', 'no data', '0000-00-00'
    ) THEN NULL
    ELSE TRIM(val)
  END;
$$;

-- ─────────────────────────────────────────
-- Apply all functions in one pass
-- ─────────────────────────────────────────
UPDATE customers SET
  first_name = clean_name(normalize_null(first_name)),
  last_name  = clean_name(normalize_null(last_name)),
  email      = clean_email(normalize_null(email)),
  phone      = clean_phone(normalize_null(phone)),
  notes      = clean_text(normalize_null(notes)),
  city       = INITCAP(LOWER(TRIM(normalize_null(city)))),
  country    = UPPER(TRIM(normalize_null(country)));
```

---

## 2. Lookup Table Standardization

```sql
-- ─────────────────────────────────────────
-- Create a master synonym/alias table for categorical columns
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS category_synonyms (
  raw_value   TEXT PRIMARY KEY,
  canonical   TEXT NOT NULL,
  entity_type TEXT NOT NULL,  -- 'product_category', 'status', 'country', etc.
  added_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Populate with known variants
INSERT INTO category_synonyms (raw_value, canonical, entity_type) VALUES
  -- Product categories
  ('electronics',   'Electronics',       'product_category'),
  ('Electronic',    'Electronics',       'product_category'),
  ('ELECTRONICS',   'Electronics',       'product_category'),
  ('elec',          'Electronics',       'product_category'),
  ('clothing',      'Apparel',           'product_category'),
  ('Clothes',       'Apparel',           'product_category'),
  ('apparel',       'Apparel',           'product_category'),
  -- Order statuses
  ('completed',     'completed',         'order_status'),
  ('Completed',     'completed',         'order_status'),
  ('COMPLETED',     'completed',         'order_status'),
  ('done',          'completed',         'order_status'),
  ('cancelled',     'cancelled',         'order_status'),
  ('canceled',      'cancelled',         'order_status'),
  -- Country names
  ('united states', 'US',                'country'),
  ('usa',           'US',                'country'),
  ('u.s.a.',        'US',                'country'),
  ('philippines',   'PH',                'country'),
  ('pilipinas',     'PH',                'country'),
  ('phil',          'PH',                'country'),
  ('united kingdom','GB',                'country'),
  ('uk',            'GB',                'country'),
  ('england',       'GB',                'country')
ON CONFLICT (raw_value) DO UPDATE SET canonical = EXCLUDED.canonical;

-- Apply standardization using the lookup table
UPDATE products AS p
SET category = cs.canonical
FROM category_synonyms AS cs
WHERE LOWER(TRIM(p.category)) = LOWER(cs.raw_value)
  AND cs.entity_type = 'product_category';

UPDATE orders AS o
SET status = cs.canonical
FROM category_synonyms AS cs
WHERE LOWER(TRIM(o.status)) = LOWER(cs.raw_value)
  AND cs.entity_type = 'order_status';

-- Find values NOT in the synonym table (need to be mapped)
SELECT DISTINCT LOWER(TRIM(category)) AS unmapped_value, COUNT(*) AS count
FROM products
WHERE LOWER(TRIM(category)) NOT IN (
  SELECT LOWER(raw_value) FROM category_synonyms
  WHERE entity_type = 'product_category'
)
GROUP BY unmapped_value
ORDER BY count DESC;
```

---

## 3. ISO & Standard Code Normalization

```sql
-- ─────────────────────────────────────────
-- ISO 3166-1 alpha-2 country codes
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iso_countries (
  iso2        CHAR(2) PRIMARY KEY,
  iso3        CHAR(3),
  name        TEXT,
  name_alt    TEXT   -- alternate English name
);

INSERT INTO iso_countries VALUES
  ('US', 'USA', 'United States',  'America'),
  ('GB', 'GBR', 'United Kingdom', 'Great Britain'),
  ('PH', 'PHL', 'Philippines',    'Pilipinas'),
  ('DE', 'DEU', 'Germany',        'Deutschland'),
  ('FR', 'FRA', 'France',         NULL),
  ('JP', 'JPN', 'Japan',          'Nippon'),
  ('CN', 'CHN', 'China',          NULL),
  ('AU', 'AUS', 'Australia',      NULL),
  ('CA', 'CAN', 'Canada',         NULL),
  ('IN', 'IND', 'India',          NULL)
ON CONFLICT DO NOTHING;

-- Normalize country column to ISO2 code
UPDATE customers AS c
SET country = ic.iso2
FROM iso_countries AS ic
WHERE LOWER(TRIM(c.country)) = LOWER(ic.name)
   OR LOWER(TRIM(c.country)) = LOWER(ic.name_alt)
   OR UPPER(TRIM(c.country)) = ic.iso3
   OR UPPER(TRIM(c.country)) = ic.iso2;

-- ─────────────────────────────────────────
-- Currency codes (ISO 4217)
-- ─────────────────────────────────────────
UPDATE transactions
SET currency_code = UPPER(TRIM(currency_code));

-- Map currency symbols to codes
UPDATE transactions
SET currency_code = CASE currency_symbol
  WHEN '$'  THEN 'USD'
  WHEN '€'  THEN 'EUR'
  WHEN '£'  THEN 'GBP'
  WHEN '¥'  THEN 'JPY'
  WHEN '₹'  THEN 'INR'
  WHEN '₱'  THEN 'PHP'
  WHEN '₩'  THEN 'KRW'
  WHEN 'A$' THEN 'AUD'
  WHEN 'C$' THEN 'CAD'
  ELSE currency_code
END
WHERE currency_symbol IS NOT NULL AND currency_code IS NULL;

-- ─────────────────────────────────────────
-- Language codes (ISO 639-1)
-- ─────────────────────────────────────────
UPDATE users
SET language_code = LOWER(TRIM(language_code));

UPDATE users
SET language_code = CASE LOWER(TRIM(language_code))
  WHEN 'english'    THEN 'en'
  WHEN 'spanish'    THEN 'es'
  WHEN 'french'     THEN 'fr'
  WHEN 'german'     THEN 'de'
  WHEN 'japanese'   THEN 'ja'
  WHEN 'chinese'    THEN 'zh'
  WHEN 'filipino'   THEN 'fil'
  WHEN 'tagalog'    THEN 'tl'
  ELSE language_code
END;
```

---

## 4. Character Encoding Normalization

```sql
-- ─────────────────────────────────────────
-- Detect non-ASCII characters
-- ─────────────────────────────────────────
SELECT id, name,
  REGEXP_REPLACE(name, '[\x20-\x7E]', '', 'g') AS non_ascii_chars,
  LENGTH(name) - LENGTH(REGEXP_REPLACE(name, '[^\x20-\x7E]', '', 'g')) AS non_ascii_count
FROM customers
WHERE name ~ '[^\x20-\x7E]';

-- ─────────────────────────────────────────
-- Common character substitutions (accented → ASCII)
-- ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION ascii_normalize(input TEXT)
RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$
  SELECT
    TRANSLATE(input,
      'ÀÁÂÃÄÅàáâãäåÆæÇçÈÉÊËèéêëÌÍÎÏìíîïÐðÑñÒÓÔÕÖØòóôõöøÙÚÛÜùúûüÝýÿŸþÞßŒœŠšŽžŸ',
      'AAAAAAaaaaaaAACcEEEEeeeeIIIIiiiiDdNnOOOOOOooooooUUUUuuuuYyyYpPsOoSsZzY'
    );
$$;

SELECT ascii_normalize('Ångström café naïve résumé Müller');
-- Returns: Angstrom cafe naive resume Muller

-- Apply to name columns
UPDATE customers
SET
  first_name = ascii_normalize(first_name),
  last_name  = ascii_normalize(last_name)
WHERE first_name ~ '[^\x20-\x7E]'
   OR last_name  ~ '[^\x20-\x7E]';

-- ─────────────────────────────────────────
-- Fix common encoding artifacts (mojibake)
-- These appear when UTF-8 is read as Latin-1
-- ─────────────────────────────────────────
UPDATE products
SET description = REPLACE(description, 'â€™', '''')   -- RIGHT APOSTROPHE
WHERE description LIKE '%â€™%';

UPDATE products
SET description = REPLACE(description, 'â€œ', '"')    -- LEFT QUOTE
WHERE description LIKE '%â€œ%';

UPDATE products
SET description = REPLACE(description, 'â€',  '"')    -- RIGHT QUOTE
WHERE description LIKE '%â€%';

UPDATE products
SET description = REPLACE(description, 'Ã©',  'é')    -- é
WHERE description LIKE '%Ã©%';

UPDATE products
SET description = REPLACE(description, 'Ã¨',  'è')    -- è
WHERE description LIKE '%Ã¨%';

-- Remove zero-width & invisible characters
UPDATE customers
SET name = REPLACE(REPLACE(REPLACE(REPLACE(name,
  E'\u200B', ''),   -- zero-width space
  E'\u200C', ''),   -- zero-width non-joiner
  E'\u200D', ''),   -- zero-width joiner
  E'\uFEFF',  '');  -- BOM
```

---

## 5. Handling Locale-Specific Data

```sql
-- ─────────────────────────────────────────
-- European number formats → standard decimal
-- ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION parse_european_number(val TEXT)
RETURNS NUMERIC LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  -- "1.234,56" → "1234.56"
  RETURN REPLACE(REPLACE(TRIM(val), '.', ''), ',', '.')::NUMERIC;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END; $$;

SELECT parse_european_number('1.234,56');  -- returns 1234.56
SELECT parse_european_number('€ 1.299,99'); -- returns NULL (has symbol)

-- Strip symbol first, then parse
SELECT
  raw_price,
  parse_european_number(REGEXP_REPLACE(raw_price, '[^0-9.,]', '', 'g')) AS price_clean
FROM eu_products;

-- ─────────────────────────────────────────
-- Detect and handle locale-specific date formats
-- ─────────────────────────────────────────

-- German/EU: "15.03.2024"
SELECT TO_DATE('15.03.2024', 'DD.MM.YYYY');

-- French written: "15 mars 2024" → requires manual month map
WITH french_months(fr_name, month_num) AS (
  VALUES ('janvier',1),('février',2),('mars',3),('avril',4),
         ('mai',5),('juin',6),('juillet',7),('août',8),
         ('septembre',9),('octobre',10),('novembre',11),('décembre',12)
)
SELECT
  raw_date,
  TO_DATE(
    REGEXP_REPLACE(raw_date,
      fm.fr_name,
      LPAD(fm.month_num::text, 2, '0'),
      'i'),
    'DD MM YYYY'
  ) AS parsed_date
FROM staging_fr_data
CROSS JOIN french_months AS fm
WHERE LOWER(raw_date) LIKE '%' || fm.fr_name || '%';

-- ─────────────────────────────────────────
-- Name order differences (East Asian: Last First)
-- ─────────────────────────────────────────
UPDATE customers
SET
  first_name_western = last_name,    -- swap for East Asian names
  last_name_western  = first_name
WHERE country IN ('JP', 'CN', 'KR', 'VN')
  AND name_order = 'eastern';
```

---

## 6. Data Contract Enforcement (Checks Before Ingest)

```sql
-- ─────────────────────────────────────────
-- Pre-ingest validation function
-- Returns a table of all contract violations
-- ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION validate_customer_batch()
RETURNS TABLE(
  row_id     BIGINT,
  field      TEXT,
  rule       TEXT,
  bad_value  TEXT,
  severity   TEXT
) LANGUAGE SQL AS $$

  -- Rule 1: email must not be null
  SELECT customer_id::bigint, 'email', 'NOT_NULL', email, 'ERROR'
  FROM customers_staging WHERE email IS NULL

  UNION ALL
  -- Rule 2: email must be valid format
  SELECT customer_id::bigint, 'email', 'FORMAT', email, 'ERROR'
  FROM customers_staging
  WHERE email IS NOT NULL
    AND NOT is_valid_email(email)

  UNION ALL
  -- Rule 3: email must be unique
  SELECT customer_id::bigint, 'email', 'UNIQUE', email, 'ERROR'
  FROM customers_staging
  WHERE email IN (
    SELECT email FROM customers_staging GROUP BY email HAVING COUNT(*) > 1
  )

  UNION ALL
  -- Rule 4: first_name should not be null
  SELECT customer_id::bigint, 'first_name', 'NOT_NULL', first_name, 'WARNING'
  FROM customers_staging WHERE first_name IS NULL

  UNION ALL
  -- Rule 5: phone length should be 10 digits
  SELECT customer_id::bigint, 'phone', 'LENGTH',
    phone, 'WARNING'
  FROM customers_staging
  WHERE phone IS NOT NULL
    AND LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) != 10

  UNION ALL
  -- Rule 6: no future registration dates
  SELECT customer_id::bigint, 'registration_date', 'RANGE',
    registration_date::text, 'ERROR'
  FROM customers_staging
  WHERE registration_date > CURRENT_DATE
$$;

-- Run the validator and see all violations
SELECT * FROM validate_customer_batch()
ORDER BY severity DESC, field, row_id;

-- Summary: count by rule and severity
SELECT rule, severity, COUNT(*) AS violations
FROM validate_customer_batch()
GROUP BY rule, severity
ORDER BY severity, violations DESC;

-- Block ingest if ERROR count > 0
DO $$
DECLARE error_count INT;
BEGIN
  SELECT COUNT(*) INTO error_count
  FROM validate_customer_batch()
  WHERE severity = 'ERROR';

  IF error_count > 0 THEN
    RAISE EXCEPTION 'Ingest blocked: % data contract violations found.', error_count;
  END IF;
END;
$$;
```

---

## 7. Automated Cleaning Triggers

Set-and-forget cleaning that fires automatically on INSERT or UPDATE.

```sql
-- ─────────────────────────────────────────
-- Auto-clean customer data on insert/update
-- ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION auto_clean_customer()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  -- Normalize text
  NEW.first_name = clean_name(normalize_null(NEW.first_name));
  NEW.last_name  = clean_name(normalize_null(NEW.last_name));
  NEW.email      = clean_email(normalize_null(NEW.email));
  NEW.phone      = clean_phone(normalize_null(NEW.phone));
  NEW.city       = INITCAP(LOWER(TRIM(normalize_null(NEW.city))));
  NEW.country    = UPPER(TRIM(normalize_null(NEW.country)));

  -- Validate email — reject if invalid
  IF NEW.email IS NOT NULL AND NOT is_valid_email(NEW.email) THEN
    RAISE EXCEPTION 'Invalid email format: %', NEW.email;
  END IF;

  -- Default values
  NEW.is_active  = COALESCE(NEW.is_active, TRUE);
  NEW.created_at = COALESCE(NEW.created_at, NOW());
  NEW.updated_at = NOW();

  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_auto_clean_customer
BEFORE INSERT OR UPDATE ON customers
FOR EACH ROW EXECUTE FUNCTION auto_clean_customer();

-- ─────────────────────────────────────────
-- Auto-clean orders on insert
-- ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION auto_clean_order()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  -- Normalize status to lowercase
  NEW.status = LOWER(TRIM(NEW.status));

  -- Validate status is in allowed set
  IF NEW.status NOT IN ('pending','confirmed','shipped','delivered','cancelled','refunded') THEN
    NEW.status = 'pending';  -- default unknown to pending
  END IF;

  -- Ensure discount is between 0 and 1
  IF NEW.discount_rate IS NOT NULL THEN
    NEW.discount_rate = GREATEST(0, LEAST(1, NEW.discount_rate));
  END IF;

  -- Round amounts
  NEW.total_amount = ROUND(NEW.total_amount, 2);

  -- Reject future order dates
  IF NEW.order_date > CURRENT_DATE THEN
    RAISE EXCEPTION 'Order date cannot be in the future: %', NEW.order_date;
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_auto_clean_order
BEFORE INSERT OR UPDATE ON orders
FOR EACH ROW EXECUTE FUNCTION auto_clean_order();
```

---

## 8. Regex-Powered Data Dictionary & Column Profiler

```sql
-- ─────────────────────────────────────────
-- Automatic column type inference from data
-- ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION profile_column(
  p_table TEXT,
  p_column TEXT
)
RETURNS TABLE(
  metric  TEXT,
  value   TEXT
) LANGUAGE plpgsql AS $$
DECLARE
  sql TEXT;
BEGIN
  sql := format($q$
    WITH raw AS (SELECT %I AS val FROM %I),
    stats AS (
      SELECT
        COUNT(*)                                             AS total,
        COUNT(val)                                           AS non_null,
        COUNT(*) - COUNT(val)                               AS null_count,
        COUNT(DISTINCT val)                                  AS distinct_count,
        MIN(LENGTH(val::text))                               AS min_len,
        MAX(LENGTH(val::text))                               AS max_len,
        ROUND(AVG(LENGTH(val::text)), 1)                     AS avg_len,
        COUNT(*) FILTER (WHERE val::text ~ '^[0-9]+$')              AS pure_numeric,
        COUNT(*) FILTER (WHERE val::text ~ '^[0-9]+\.[0-9]+$')      AS pure_decimal,
        COUNT(*) FILTER (WHERE val::text ~ '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}') AS looks_email,
        COUNT(*) FILTER (WHERE val::text ~ '^\d{4}-\d{2}-\d{2}$')  AS looks_date,
        COUNT(*) FILTER (WHERE val::text ~ '^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$') AS looks_ip,
        COUNT(*) FILTER (WHERE val::text ~* '^https?://')           AS looks_url
      FROM raw
    )
    SELECT
      'total_rows'::text,     total::text FROM stats UNION ALL
      SELECT 'non_null',       non_null::text FROM stats UNION ALL
      SELECT 'null_count',     null_count::text FROM stats UNION ALL
      SELECT 'null_pct',       ROUND(100.0 * null_count / NULLIF(total,0), 2)::text || '%' FROM stats UNION ALL
      SELECT 'distinct_count', distinct_count::text FROM stats UNION ALL
      SELECT 'min_length',     min_len::text FROM stats UNION ALL
      SELECT 'max_length',     max_len::text FROM stats UNION ALL
      SELECT 'avg_length',     avg_len::text FROM stats UNION ALL
      SELECT 'pure_numeric',   pure_numeric::text FROM stats UNION ALL
      SELECT 'pure_decimal',   pure_decimal::text FROM stats UNION ALL
      SELECT 'looks_like_email', looks_email::text FROM stats UNION ALL
      SELECT 'looks_like_date',  looks_date::text FROM stats UNION ALL
      SELECT 'looks_like_ip',    looks_ip::text FROM stats UNION ALL
      SELECT 'looks_like_url',   looks_url::text FROM stats
  $q$, p_column, p_table);

  RETURN QUERY EXECUTE sql;
END;
$$;

-- Usage
SELECT * FROM profile_column('customers', 'email');
SELECT * FROM profile_column('orders',    'status');
SELECT * FROM profile_column('products',  'price');
```

---

## 9. Cleaning Views for Safe Analysis

Create views that apply cleaning on-the-fly, so analysts always see clean data.

```sql
-- ─────────────────────────────────────────
-- Clean customer view
-- ─────────────────────────────────────────
CREATE OR REPLACE VIEW customers_clean AS
SELECT
  customer_id,
  clean_name(first_name)                      AS first_name,
  clean_name(last_name)                       AS last_name,
  clean_name(first_name) || ' ' ||
  clean_name(last_name)                       AS full_name,
  clean_email(email)                          AS email,
  CASE WHEN is_valid_email(clean_email(email)) THEN clean_email(email) ELSE NULL END
                                              AS email_validated,
  format_phone_us(phone)                      AS phone_formatted,
  clean_phone(phone)                          AS phone_digits,
  INITCAP(LOWER(TRIM(city)))                  AS city,
  UPPER(TRIM(country))                        AS country_code,
  COALESCE(is_active, TRUE)                   AS is_active,
  COALESCE(created_at, registration_date)     AS customer_since
FROM customers
WHERE customer_id IS NOT NULL;

-- ─────────────────────────────────────────
-- Clean orders view
-- ─────────────────────────────────────────
CREATE OR REPLACE VIEW orders_clean AS
SELECT
  o.order_id,
  o.customer_id,
  COALESCE(o.order_date_clean, o.order_date::date)                    AS order_date,
  LOWER(TRIM(o.status))                                                AS status,
  ROUND(COALESCE(o.total_amount_clean, o.total_amount), 2)            AS total_amount,
  GREATEST(0, LEAST(1, COALESCE(o.discount_rate, 0)))                  AS discount_rate,
  ROUND(COALESCE(o.total_amount_clean, o.total_amount)
        * (1 - COALESCE(o.discount_rate, 0)), 2)                      AS net_amount
FROM orders AS o
WHERE o.order_date_clean IS NOT NULL
  OR o.order_date IS NOT NULL
  AND o.order_id IS NOT NULL
  AND o.customer_id IS NOT NULL;

-- ─────────────────────────────────────────
-- Materialized view for performance (refresh on schedule)
-- ─────────────────────────────────────────
CREATE MATERIALIZED VIEW customers_clean_mat AS
SELECT * FROM customers_clean;

CREATE INDEX idx_customers_clean_mat_email ON customers_clean_mat(email_validated);
CREATE INDEX idx_customers_clean_mat_country ON customers_clean_mat(country_code);

-- Refresh (run daily via cron/orchestration)
REFRESH MATERIALIZED VIEW CONCURRENTLY customers_clean_mat;
```

---

## 10. Complete Pre-Analysis Data Health Report

Run this report before starting any analysis to confirm your data is clean.

```sql
-- ═══════════════════════════════════════════════════════
--  MASTER DATA HEALTH REPORT
--  Run before any analysis. Green = ready. Red = clean first.
-- ═══════════════════════════════════════════════════════

WITH
-- ── CUSTOMERS ──
c_checks AS (
  SELECT
    'customers'                                               AS tbl,
    COUNT(*)                                                  AS total_rows,
    COUNT(*) FILTER (WHERE customer_id IS NULL)               AS null_pk,
    COUNT(*) FILTER (WHERE email IS NULL)                     AS null_email,
    COUNT(*) FILTER (WHERE NOT is_valid_email(email))         AS invalid_email,
    (SELECT COUNT(*) FROM (SELECT email FROM customers GROUP BY email HAVING COUNT(*) > 1) d) AS dup_emails,
    COUNT(*) FILTER (WHERE first_name IS NULL)                AS null_first_name,
    COUNT(*) FILTER (WHERE first_name != TRIM(first_name))    AS untrimmed_name,
    0 AS null_date, 0 AS future_date, 0 AS negative_amount
  FROM customers
),
-- ── ORDERS ──
o_checks AS (
  SELECT
    'orders'                                                  AS tbl,
    COUNT(*)                                                  AS total_rows,
    COUNT(*) FILTER (WHERE order_id IS NULL)                  AS null_pk,
    COUNT(*) FILTER (WHERE customer_id NOT IN (SELECT customer_id FROM customers)) AS null_email,
    0 AS invalid_email,
    0 AS dup_emails,
    0 AS null_first_name,
    0 AS untrimmed_name,
    COUNT(*) FILTER (WHERE order_date IS NULL)                AS null_date,
    COUNT(*) FILTER (WHERE order_date > CURRENT_DATE)         AS future_date,
    COUNT(*) FILTER (WHERE total_amount < 0)                  AS negative_amount
  FROM orders
),
-- ── PRODUCTS ──
p_checks AS (
  SELECT
    'products'                                               AS tbl,
    COUNT(*)                                                 AS total_rows,
    COUNT(*) FILTER (WHERE product_id IS NULL)               AS null_pk,
    0,0,0,0,0,0,
    COUNT(*) FILTER (WHERE price IS NULL)                    AS null_date,
    COUNT(*) FILTER (WHERE price <= 0)                       AS future_date,
    COUNT(*) FILTER (WHERE price > 100000)                   AS negative_amount
  FROM products
),
all_checks AS (
  SELECT * FROM c_checks
  UNION ALL SELECT * FROM o_checks
  UNION ALL SELECT * FROM p_checks
)
SELECT
  tbl AS "Table",
  total_rows AS "Rows",
  CASE WHEN null_pk = 0 THEN '✅' ELSE '🔴 ' || null_pk END AS "Null PK",
  CASE WHEN null_email = 0 THEN '✅' ELSE '🔴 ' || null_email END AS "Orphans/Null Email",
  CASE WHEN invalid_email = 0 THEN '✅' ELSE '🟡 ' || invalid_email END AS "Invalid Emails",
  CASE WHEN dup_emails = 0 THEN '✅' ELSE '🔴 ' || dup_emails END AS "Duplicates",
  CASE WHEN null_date = 0 THEN '✅' ELSE '🟡 ' || null_date END AS "Null Dates/Price",
  CASE WHEN future_date = 0 THEN '✅' ELSE '🔴 ' || future_date END AS "Future/Zero",
  CASE WHEN negative_amount = 0 THEN '✅' ELSE '🔴 ' || negative_amount END AS "Negatives/High",
  -- Overall score
  CASE
    WHEN (null_pk + dup_emails + future_date + negative_amount) = 0
     AND (null_email + invalid_email + null_date) = 0
    THEN '🟢 CLEAN — Ready for analysis'
    WHEN (null_pk + dup_emails) = 0 AND (null_email + invalid_email) <= 5
    THEN '🟡 MOSTLY CLEAN — Minor issues remain'
    ELSE '🔴 NEEDS CLEANING — Do not analyze yet'
  END AS "Overall Status"
FROM all_checks;
```

---

## 📌 Advanced Standardization Quick Reference

| Tool | Use For |
|------|---------|
| `clean_text()` function | Trim + collapse spaces + normalize everywhere |
| `normalize_null()` function | Convert 'N/A', 'null', '-' → real NULL |
| `clean_email()` + `is_valid_email()` | Clean and gate email fields |
| `clean_phone()` + `format_phone_us()` | Normalize all phone numbers |
| `safe_int/numeric/date/jsonb()` | Never crash on bad casts |
| `ascii_normalize()` | Strip accents, fix encoding artifacts |
| `category_synonyms` table | Centralize all lookup value mappings |
| `validate_customer_batch()` | Pre-ingest data contract checking |
| `auto_clean_customer` trigger | Clean on every INSERT/UPDATE automatically |
| `profile_column()` function | Instant column profiling with type hints |
| `customers_clean` view | Always-clean read layer for analysts |
| Health Report query | Go/no-go gate before any analysis begins |

---

*End of the SQL Data Cleaning Extension Series (Parts 8–10)*
*Full series: Parts 1–7 cover foundations → Part 7 pipeline. Parts 8–10 cover regex, embedded data, and production hardening.*