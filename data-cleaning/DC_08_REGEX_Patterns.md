# 🧹 SQL Data Cleaning — Part 8: REGEX Complete Reference
> *Search, validate, extract, replace, and clean any pattern — emails, phones, URLs, IDs, addresses, and more*

---

## Table of Contents
- [1. REGEX Foundations in PostgreSQL](#1-regex-foundations-in-postgresql)
- [2. Email Patterns](#2-email-patterns)
- [3. Phone Number Patterns](#3-phone-number-patterns)
- [4. URL & Domain Patterns](#4-url--domain-patterns)
- [5. Postal Code & Address Patterns](#5-postal-code--address-patterns)
- [6. ID & Code Patterns](#6-id--code-patterns)
- [7. Name Patterns](#7-name-patterns)
- [8. Numeric & Currency Patterns](#8-numeric--currency-patterns)
- [9. Date String Patterns](#9-date-string-patterns)
- [10. IP Address & Network Patterns](#10-ip-address--network-patterns)
- [11. Credit Card & Sensitive Data Patterns](#11-credit-card--sensitive-data-patterns)
- [12. Free-Text Extraction](#12-free-text-extraction)
- [13. Regex-Based Data Profiling Queries](#13-regex-based-data-profiling-queries)
- [14. Regex Quick Reference Card](#14-regex-quick-reference-card)

---

## 1. REGEX Foundations in PostgreSQL

### Operators

```sql
-- ~ (tilde)         → case-sensitive match
-- ~* (tilde-star)   → case-insensitive match
-- !~ (bang-tilde)   → case-sensitive NOT match
-- !~* (bang-tilde-star) → case-insensitive NOT match

-- Examples:
SELECT * FROM customers WHERE email ~ '@gmail\.com$';       -- exact case
SELECT * FROM customers WHERE email ~* '@GMAIL\.com$';      -- case-insensitive
SELECT * FROM customers WHERE email !~ '@gmail\.com$';      -- does NOT match
SELECT * FROM customers WHERE notes !~* 'urgent|priority';  -- does not contain either

-- REGEXP_REPLACE(string, pattern, replacement, flags)
-- flags: 'g' = global (replace all), 'i' = case-insensitive
SELECT REGEXP_REPLACE('Hello World 123', '[0-9]+', 'NUM', 'g');  -- 'Hello World NUM'
SELECT REGEXP_REPLACE('  hello  ',       '\s+', ' ', 'g');       -- ' hello '

-- REGEXP_MATCHES(string, pattern, flags) → returns ARRAY of matches
SELECT REGEXP_MATCHES('Order #12345 and #67890', '#(\d+)', 'g');
-- Returns: {12345}, {67890} (one row per match)

-- SUBSTRING(string FROM pattern) → returns first match
SELECT SUBSTRING('Contact: john@mail.com for info', '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}');
-- Returns: john@mail.com

-- REGEXP_SPLIT_TO_TABLE(string, pattern) → splits into rows
SELECT REGEXP_SPLIT_TO_TABLE('one,two,,three', ',+');  -- 'one', 'two', 'three'

-- REGEXP_SPLIT_TO_ARRAY(string, pattern) → splits into array
SELECT REGEXP_SPLIT_TO_ARRAY('a1b2c3', '[0-9]');  -- {a, b, c, ''}
```

### Essential Regex Syntax

```
.         Any single character (except newline)
\d        Digit [0-9]
\D        Non-digit
\w        Word character [a-zA-Z0-9_]
\W        Non-word character
\s        Whitespace (space, tab, newline)
\S        Non-whitespace
^         Start of string
$         End of string
[abc]     Any of a, b, c
[^abc]    NOT a, b, or c
[a-z]     Range: a to z
*         0 or more of previous
+         1 or more of previous
?         0 or 1 of previous (optional)
{n}       Exactly n of previous
{n,m}     Between n and m of previous
{n,}      n or more of previous
(abc)     Capture group
(?:abc)   Non-capturing group
a|b       a OR b
\b        Word boundary
\.        Literal dot (escaped)
\@        Literal @ (escaped)
```

---

## 2. Email Patterns

```sql
-- ─────────────────────────────────────────
-- SEARCH: Find rows that CONTAIN an email
-- ─────────────────────────────────────────
SELECT *
FROM customers
WHERE notes ~* '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}';

-- ─────────────────────────────────────────
-- VALIDATE: Is the email column a valid email?
-- ─────────────────────────────────────────

-- Basic: has @ and a dot after it
SELECT email FROM customers
WHERE email NOT LIKE '%@%.%';

-- Standard validation
SELECT email FROM customers
WHERE email !~ '^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$';

-- Strict validation (no consecutive dots, no leading/trailing dots in local part)
SELECT email FROM customers
WHERE email !~ '^[a-zA-Z0-9]([a-zA-Z0-9._%+\-]*[a-zA-Z0-9])?@[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?(\.[a-zA-Z]{2,})+$'
   OR email ~ '\.\.';  -- consecutive dots are invalid

-- ─────────────────────────────────────────
-- EXTRACT: Pull email out of a messy text column
-- ─────────────────────────────────────────

-- From free text like "Contact me at john@example.com or call 555-1234"
SELECT
  notes,
  SUBSTRING(notes FROM '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
    AS extracted_email
FROM customers
WHERE notes ~* '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}';

-- Extract ALL emails from a text blob (returns one row per email)
SELECT
  customer_id,
  UNNEST(REGEXP_MATCHES(notes, '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', 'g'))
    AS email_found
FROM customers
WHERE notes ~* '@';

-- ─────────────────────────────────────────
-- CLEAN: Fix common email typos
-- ─────────────────────────────────────────

-- Detect which domain typos exist
SELECT
  SUBSTRING(email FROM '@(.+)$') AS domain,
  COUNT(*) AS count
FROM customers
WHERE email IS NOT NULL
GROUP BY domain
ORDER BY count DESC;

-- Fix common domain misspellings
UPDATE customers SET email = REGEXP_REPLACE(email, '@gmial\.com$',    '@gmail.com',   'i') WHERE email ~* '@gmial\.com$';
UPDATE customers SET email = REGEXP_REPLACE(email, '@gmai\.com$',     '@gmail.com',   'i') WHERE email ~* '@gmai\.com$';
UPDATE customers SET email = REGEXP_REPLACE(email, '@gamil\.com$',    '@gmail.com',   'i') WHERE email ~* '@gamil\.com$';
UPDATE customers SET email = REGEXP_REPLACE(email, '@gnail\.com$',    '@gmail.com',   'i') WHERE email ~* '@gnail\.com$';
UPDATE customers SET email = REGEXP_REPLACE(email, '@hotmial\.com$',  '@hotmail.com', 'i') WHERE email ~* '@hotmial\.com$';
UPDATE customers SET email = REGEXP_REPLACE(email, '@hotmail\.co$',   '@hotmail.com', 'i') WHERE email ~* '@hotmail\.co$';
UPDATE customers SET email = REGEXP_REPLACE(email, '@yahooo?\.com$',  '@yahoo.com',   'i') WHERE email ~* '@yahooo?\.com$';
UPDATE customers SET email = REGEXP_REPLACE(email, '@outlok\.com$',   '@outlook.com', 'i') WHERE email ~* '@outlok\.com$';

-- Remove mailto: prefix
UPDATE customers SET email = REGEXP_REPLACE(email, '^mailto:', '', 'i') WHERE email ~* '^mailto:';

-- ─────────────────────────────────────────
-- PARSE: Extract parts of email
-- ─────────────────────────────────────────
SELECT
  email,
  SUBSTRING(email FROM '^(.+)@')            AS local_part,   -- before @
  SUBSTRING(email FROM '@(.+)$')            AS domain,       -- after @
  SUBSTRING(email FROM '@([^.]+)\.')        AS domain_name,  -- e.g. 'gmail'
  SUBSTRING(email FROM '\.([^.]+)$')        AS tld           -- e.g. 'com'
FROM customers
WHERE email IS NOT NULL;

-- Categorize email domains
SELECT
  CASE
    WHEN email ~* '@(gmail|googlemail)\.com$'                    THEN 'Google'
    WHEN email ~* '@(yahoo|ymail|rocketmail)\.com$'              THEN 'Yahoo'
    WHEN email ~* '@(hotmail|outlook|live|msn)\.com$'            THEN 'Microsoft'
    WHEN email ~* '@(icloud|me|mac)\.com$'                       THEN 'Apple'
    WHEN email ~* '@[a-z0-9\-]+\.(edu)$'                         THEN 'Educational'
    WHEN email ~* '@[a-z0-9\-]+\.(gov|mil)$'                     THEN 'Government'
    ELSE 'Business/Other'
  END AS email_provider,
  COUNT(*) AS count
FROM customers
GROUP BY email_provider
ORDER BY count DESC;
```

---

## 3. Phone Number Patterns

```sql
-- ─────────────────────────────────────────
-- SEARCH: Find rows containing a phone-like pattern
-- ─────────────────────────────────────────

-- US phone: (555) 123-4567 or 555-123-4567 or 5551234567
SELECT * FROM customers
WHERE notes ~ '\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}';

-- International: +63 912 345 6789 or +1-800-555-1234
SELECT * FROM customers
WHERE notes ~ '\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{1,4}[\s\-]?\d{1,9}';

-- ─────────────────────────────────────────
-- VALIDATE: Is it a valid phone format?
-- ─────────────────────────────────────────

-- US 10-digit validation (digits only, after stripping formatting)
SELECT phone,
  REGEXP_REPLACE(phone, '[^0-9]', '', 'g') AS digits_only,
  LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) AS digit_count,
  CASE
    WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) = 10 THEN '✅ Valid US'
    WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) = 11
     AND REGEXP_REPLACE(phone, '[^0-9]', '', 'g') LIKE '1%' THEN '✅ Valid US+1'
    WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) BETWEEN 7 AND 15 THEN '⚠️  Possible Intl'
    ELSE '❌ Invalid'
  END AS phone_validity
FROM customers
WHERE phone IS NOT NULL;

-- Detect what formats are in use
SELECT
  CASE
    WHEN phone ~ '^\(\d{3}\) \d{3}-\d{4}$'  THEN '(XXX) XXX-XXXX'
    WHEN phone ~ '^\d{3}-\d{3}-\d{4}$'       THEN 'XXX-XXX-XXXX'
    WHEN phone ~ '^\d{3}\.\d{3}\.\d{4}$'     THEN 'XXX.XXX.XXXX'
    WHEN phone ~ '^\d{10}$'                   THEN 'XXXXXXXXXX'
    WHEN phone ~ '^\+\d{1,3}[\s\-]\d+'       THEN '+X XXX...'
    WHEN phone ~ '^\d{3} \d{3} \d{4}$'       THEN 'XXX XXX XXXX'
    ELSE 'Other/Unknown'
  END AS format_detected,
  COUNT(*) AS count
FROM customers
WHERE phone IS NOT NULL
GROUP BY format_detected
ORDER BY count DESC;

-- ─────────────────────────────────────────
-- EXTRACT: Pull phone from free text
-- ─────────────────────────────────────────
SELECT
  notes,
  SUBSTRING(notes FROM '\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}')
    AS extracted_phone
FROM customers
WHERE notes ~ '\d{3}[\s.\-]?\d{3}[\s.\-]?\d{4}';

-- ─────────────────────────────────────────
-- CLEAN & STANDARDIZE: All phones → (XXX) XXX-XXXX
-- ─────────────────────────────────────────
SELECT
  phone,
  digits,
  CASE WHEN LENGTH(digits) = 10
    THEN '(' || SUBSTRING(digits,1,3) || ') '
             || SUBSTRING(digits,4,3) || '-'
             || SUBSTRING(digits,7,4)
    WHEN LENGTH(digits) = 11 AND digits LIKE '1%'
    THEN '(' || SUBSTRING(digits,2,3) || ') '
             || SUBSTRING(digits,5,3) || '-'
             || SUBSTRING(digits,8,4)
    ELSE phone
  END AS standardized_phone
FROM (
  SELECT phone, REGEXP_REPLACE(phone, '[^0-9]', '', 'g') AS digits
  FROM customers
  WHERE phone IS NOT NULL
) AS stripped;
```

---

## 4. URL & Domain Patterns

```sql
-- ─────────────────────────────────────────
-- SEARCH: Find rows containing a URL
-- ─────────────────────────────────────────
SELECT * FROM products
WHERE description ~* 'https?://[^\s]+';

SELECT * FROM posts
WHERE content ~* 'www\.[a-zA-Z0-9\-]+\.[a-zA-Z]{2,}';

-- ─────────────────────────────────────────
-- EXTRACT: Pull URLs from text
-- ─────────────────────────────────────────

-- Extract first URL from a text column
SELECT
  content,
  SUBSTRING(content FROM 'https?://[^\s"''<>]+') AS first_url
FROM posts
WHERE content ~* 'https?://';

-- Extract ALL URLs (one row per URL)
SELECT
  post_id,
  UNNEST(REGEXP_MATCHES(content, 'https?://[^\s"''<>]+', 'g')) AS url
FROM posts
WHERE content ~* 'https?://';

-- ─────────────────────────────────────────
-- VALIDATE: Check URL format
-- ─────────────────────────────────────────
SELECT website_url,
  CASE
    WHEN website_url ~* '^https?://([a-zA-Z0-9\-]+\.)+[a-zA-Z]{2,}(/.*)?$'
    THEN '✅ Valid URL'
    ELSE '❌ Invalid URL'
  END AS url_status
FROM companies
WHERE website_url IS NOT NULL;

-- ─────────────────────────────────────────
-- PARSE: Extract URL components
-- ─────────────────────────────────────────
SELECT
  url,
  SUBSTRING(url FROM '^(https?)')                          AS scheme,
  SUBSTRING(url FROM '^https?://([^/]+)')                  AS hostname,
  SUBSTRING(url FROM '^https?://[^/]+(/.*)$')              AS path,
  SUBSTRING(url FROM '\?(.+)$')                            AS query_string,
  SUBSTRING(url FROM '#(.+)$')                             AS fragment
FROM (VALUES
  ('https://shop.example.com/products/123?color=red#top'),
  ('http://www.test.co.uk/blog/post-title')
) AS t(url);

-- Extract domain from email or URL
SELECT
  SUBSTRING(email FROM '@(.+)$')                AS email_domain,
  SUBSTRING(url   FROM '^https?://([^/]+)')     AS url_domain,
  REGEXP_REPLACE(
    SUBSTRING(url FROM '^https?://([^/]+)'),
    '^www\.', ''
  )                                              AS clean_domain
FROM contacts;

-- ─────────────────────────────────────────
-- CLEAN: Strip URLs from text
-- ─────────────────────────────────────────
UPDATE posts
SET content = TRIM(REGEXP_REPLACE(content, 'https?://[^\s"''<>]+', '[link removed]', 'gi'));
```

---

## 5. Postal Code & Address Patterns

```sql
-- ─────────────────────────────────────────
-- US ZIP codes
-- ─────────────────────────────────────────

-- Validate: 5-digit or ZIP+4
SELECT zip_code,
  CASE
    WHEN zip_code ~ '^\d{5}(-\d{4})?$' THEN '✅ Valid US ZIP'
    ELSE '❌ Invalid'
  END AS zip_status
FROM customers;

-- Extract ZIP from full address string
SELECT
  address,
  SUBSTRING(address FROM '\b\d{5}(-\d{4})?\b') AS zip_extracted
FROM customers
WHERE address ~ '\d{5}';

-- Standardize ZIP: strip +4, keep only 5 digits
UPDATE customers
SET zip_code = SUBSTRING(zip_code FROM '^\d{5}')
WHERE zip_code ~ '^\d{5}-\d{4}$';

-- ─────────────────────────────────────────
-- Philippine ZIP codes (4 digits)
-- ─────────────────────────────────────────
SELECT zip_code,
  CASE
    WHEN zip_code ~ '^\d{4}$' THEN '✅ Valid PH ZIP'
    ELSE '❌ Invalid'
  END AS zip_status
FROM customers;

-- ─────────────────────────────────────────
-- UK Postcodes
-- ─────────────────────────────────────────
SELECT postcode,
  CASE
    WHEN UPPER(TRIM(postcode)) ~ '^[A-Z]{1,2}[0-9][0-9A-Z]?\s?[0-9][A-Z]{2}$'
    THEN '✅ Valid UK Postcode'
    ELSE '❌ Invalid'
  END AS postcode_status
FROM customers;

-- ─────────────────────────────────────────
-- Extract address components
-- ─────────────────────────────────────────

-- Street number from address
SELECT
  address,
  SUBSTRING(address FROM '^\d+[A-Za-z]?\b')       AS street_number,
  REGEXP_REPLACE(address, '^\d+[A-Za-z]?\s*', '')  AS street_name
FROM customers
WHERE address ~ '^\d+';

-- Detect apartment/unit numbers
SELECT address,
  SUBSTRING(address FROM '(?i)(apt|unit|suite|ste|#|floor|fl)\.?\s*([a-zA-Z0-9\-]+)') AS apt_unit
FROM customers
WHERE address ~* '(apt|unit|suite|ste|#|floor)';

-- State abbreviation detection (US)
SELECT
  address,
  SUBSTRING(address FROM '\b([A-Z]{2})\b\s+\d{5}') AS state_code
FROM customers
WHERE address ~ '\b[A-Z]{2}\b\s+\d{5}';
```

---

## 6. ID & Code Patterns

```sql
-- ─────────────────────────────────────────
-- UUID validation
-- ─────────────────────────────────────────
SELECT id,
  CASE
    WHEN id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    THEN '✅ Valid UUID'
    ELSE '❌ Invalid UUID'
  END AS uuid_status
FROM entities;

-- Fix UUID casing
UPDATE entities
SET id = LOWER(id)
WHERE id !~ '^[0-9a-f\-]+$' AND id ~ '^[0-9A-Fa-f\-]+$';

-- ─────────────────────────────────────────
-- SKU / Product Code validation
-- ─────────────────────────────────────────

-- Pattern: CATEGORY-YEAR-SEQUENCE (e.g. "ELEC-2024-001")
SELECT sku,
  CASE
    WHEN sku ~ '^[A-Z]{2,6}-\d{4}-\d{3,6}$' THEN '✅ Valid SKU format'
    ELSE '❌ Non-standard'
  END AS sku_status
FROM products;

-- Extract parts of SKU
SELECT
  sku,
  SPLIT_PART(sku, '-', 1) AS category_code,
  SPLIT_PART(sku, '-', 2) AS year_code,
  SPLIT_PART(sku, '-', 3) AS sequence_code
FROM products
WHERE sku ~ '^[A-Z]+-\d{4}-\d+$';

-- ─────────────────────────────────────────
-- SSN / National ID (detection for PII scrubbing)
-- ─────────────────────────────────────────

-- Detect US SSN patterns (to redact, not to validate)
SELECT *
FROM customer_notes
WHERE notes ~ '\b\d{3}-\d{2}-\d{4}\b'         -- 123-45-6789
   OR notes ~ '\b\d{9}\b'                       -- 123456789 (9 consecutive digits)
   OR notes ~ '\b\d{3}\s\d{2}\s\d{4}\b';       -- 123 45 6789

-- Redact SSN from free text
UPDATE customer_notes
SET notes = REGEXP_REPLACE(notes, '\b\d{3}-\d{2}-\d{4}\b', '[SSN REDACTED]', 'g')
WHERE notes ~ '\b\d{3}-\d{2}-\d{4}\b';

-- ─────────────────────────────────────────
-- Order / Transaction ID formats
-- ─────────────────────────────────────────

-- Validate format: ORD-YYYYMMDD-XXXXX
SELECT order_ref,
  CASE
    WHEN order_ref ~ '^ORD-\d{8}-[A-Z0-9]{5}$' THEN '✅ Valid'
    ELSE '❌ Invalid format'
  END AS format_check
FROM orders;

-- Extract numeric ID from mixed references
-- "Order #12345", "REF: 12345", "#12345"
SELECT
  reference,
  SUBSTRING(reference FROM '\d{4,}') AS numeric_id
FROM orders
WHERE reference ~ '\d{4,}';

-- ─────────────────────────────────────────
-- IBAN / Bank Account (detection)
-- ─────────────────────────────────────────
SELECT notes,
  SUBSTRING(notes FROM '\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}\b') AS iban_found
FROM transactions
WHERE notes ~ '[A-Z]{2}\d{2}[A-Z0-9]+';
```

---

## 7. Name Patterns

```sql
-- ─────────────────────────────────────────
-- VALIDATE: Is it a plausible name?
-- ─────────────────────────────────────────

-- Flag names with numbers in them
SELECT customer_id, first_name
FROM customers
WHERE first_name ~ '\d';

-- Flag names with special characters (not hyphen or apostrophe)
SELECT customer_id, first_name
FROM customers
WHERE first_name ~ '[^a-zA-Z \-''\.]';

-- Flag suspiciously short names
SELECT customer_id, first_name, last_name
FROM customers
WHERE LENGTH(TRIM(first_name)) < 2
   OR LENGTH(TRIM(last_name)) < 2;

-- Flag names that are clearly test data
SELECT customer_id, first_name, last_name, email
FROM customers
WHERE first_name ~* '^(test|dummy|fake|sample|user|admin|null|anonymous|xxx|aaa|asdf)'
   OR last_name  ~* '^(test|dummy|fake|sample|user|admin|null|anonymous)';

-- ─────────────────────────────────────────
-- CLEAN: Strip titles and suffixes
-- ─────────────────────────────────────────

-- Remove titles from first name
UPDATE customers
SET first_name = TRIM(REGEXP_REPLACE(
  first_name,
  '^(Mr\.?|Mrs\.?|Ms\.?|Miss\.?|Dr\.?|Prof\.?|Rev\.?|Sir\.?)\s+',
  '', 'i'
));

-- Remove suffixes from last name
UPDATE customers
SET last_name = TRIM(REGEXP_REPLACE(
  last_name,
  '\s+(Jr\.?|Sr\.?|II|III|IV|V|Esq\.?|PhD\.?|MD\.?|DDS\.?)$',
  '', 'i'
));

-- ─────────────────────────────────────────
-- PARSE: Split full_name into components
-- ─────────────────────────────────────────

-- Two-part name: "John Smith"
SELECT
  full_name,
  SPLIT_PART(TRIM(full_name), ' ', 1)                                    AS first_name,
  TRIM(SUBSTRING(TRIM(full_name) FROM POSITION(' ' IN TRIM(full_name)))) AS last_name
FROM customers
WHERE full_name ~ '^\S+ \S+$';

-- Three-part name: "John Michael Smith"
SELECT
  full_name,
  SPLIT_PART(full_name, ' ', 1) AS first_name,
  SPLIT_PART(full_name, ' ', 2) AS middle_name,
  SPLIT_PART(full_name, ' ', 3) AS last_name
FROM customers
WHERE ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(full_name), '\s+'), 1) = 3;

-- "Last, First" format
SELECT
  full_name,
  TRIM(SPLIT_PART(full_name, ',', 1)) AS last_name,
  TRIM(SPLIT_PART(full_name, ',', 2)) AS first_name
FROM customers
WHERE full_name ~ '^[^,]+, ?[^,]+$';
```

---

## 8. Numeric & Currency Patterns

```sql
-- ─────────────────────────────────────────
-- SEARCH: Find rows with numeric patterns
-- ─────────────────────────────────────────

-- Find prices hidden in a text description
SELECT description,
  SUBSTRING(description FROM '\$([0-9,]+(\.[0-9]{1,2})?)') AS price_found
FROM products
WHERE description ~ '\$[0-9]';

-- Find percentages in text
SELECT notes,
  SUBSTRING(notes FROM '(\d+(\.\d+)?)%') AS percentage_found
FROM orders
WHERE notes ~ '\d+%';

-- ─────────────────────────────────────────
-- VALIDATE: Number format checks
-- ─────────────────────────────────────────

-- Is it a valid positive decimal?
SELECT raw_price,
  CASE
    WHEN raw_price ~ '^\d+(\.\d{1,4})?$'        THEN '✅ Valid positive decimal'
    WHEN raw_price ~ '^-\d+(\.\d{1,4})?$'        THEN '⚠️  Negative number'
    WHEN raw_price ~ '^\$[\d,]+(\.\d{1,2})?$'    THEN '⚠️  Has currency symbol'
    WHEN raw_price ~ '^\d{1,3}(,\d{3})*(\.\d+)?$' THEN '⚠️  Has thousand separator'
    ELSE '❌ Not a number'
  END AS number_status
FROM staging_products;

-- Detect European vs US number formatting
SELECT
  raw_amount,
  CASE
    WHEN raw_amount ~ '^\d{1,3}(\.\d{3})*(,\d{2})?$'  THEN 'European (. thousands, , decimal)'
    WHEN raw_amount ~ '^\d{1,3}(,\d{3})*(\.\d+)?$'    THEN 'US (comma thousands, . decimal)'
    WHEN raw_amount ~ '^\d+(\.\d+)?$'                  THEN 'Plain decimal'
    ELSE 'Unknown format'
  END AS number_format
FROM staging_eu_orders;

-- ─────────────────────────────────────────
-- EXTRACT & CLEAN
-- ─────────────────────────────────────────

-- Strip everything except digits and decimal point
SELECT
  raw_price,
  REGEXP_REPLACE(raw_price, '[^0-9.]', '', 'g') AS cleaned
FROM staging_products
WHERE raw_price IS NOT NULL;

-- Extract just the numeric part from "12.50 USD" or "USD 12.50"
SELECT
  raw_price,
  SUBSTRING(raw_price FROM '\d+(\.\d+)?') AS numeric_part
FROM staging_products;
```

---

## 9. Date String Patterns

```sql
-- ─────────────────────────────────────────
-- DETECT: What date format is each row in?
-- ─────────────────────────────────────────
SELECT
  raw_date,
  CASE
    WHEN raw_date ~ '^\d{4}-\d{2}-\d{2}$'                        THEN 'YYYY-MM-DD (ISO)'
    WHEN raw_date ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'      THEN 'ISO 8601 with time'
    WHEN raw_date ~ '^\d{2}/\d{2}/\d{4}$'                        THEN 'MM/DD/YYYY (US)'
    WHEN raw_date ~ '^\d{2}-\d{2}-\d{4}$'                        THEN 'DD-MM-YYYY (EU)'
    WHEN raw_date ~ '^\d{2}\.\d{2}\.\d{4}$'                      THEN 'DD.MM.YYYY (EU)'
    WHEN raw_date ~ '^\d{8}$'                                     THEN 'YYYYMMDD (compact)'
    WHEN raw_date ~ '^\d{1,2}\s[A-Za-z]+\s\d{4}$'               THEN 'D Month YYYY'
    WHEN raw_date ~ '^[A-Za-z]+\s\d{1,2},?\s\d{4}$'             THEN 'Month D, YYYY'
    WHEN raw_date ~ '^\d{2}-[A-Za-z]{3}-\d{4}$'                  THEN 'DD-Mon-YYYY'
    WHEN raw_date ~ '^\d{10}$'                                    THEN 'Unix seconds'
    WHEN raw_date ~ '^\d{13}$'                                    THEN 'Unix milliseconds'
    WHEN raw_date IN ('', 'N/A', 'NULL', 'null', '-', 'TBD', 'n/a') THEN 'NULL placeholder'
    ELSE 'Unknown / Invalid'
  END AS detected_format,
  COUNT(*) AS count
FROM staging_data
GROUP BY raw_date, detected_format
ORDER BY detected_format, count DESC;

-- ─────────────────────────────────────────
-- EXTRACT: Pull dates from free text
-- ─────────────────────────────────────────

-- ISO date from text
SELECT notes,
  SUBSTRING(notes FROM '\d{4}-\d{2}-\d{2}') AS date_found
FROM customer_notes
WHERE notes ~ '\d{4}-\d{2}-\d{2}';

-- US format date from text
SELECT notes,
  SUBSTRING(notes FROM '\d{1,2}/\d{1,2}/\d{4}') AS date_found
FROM customer_notes
WHERE notes ~ '\d{1,2}/\d{1,2}/\d{4}';

-- Natural language month extraction: "March 15, 2024"
SELECT notes,
  SUBSTRING(notes FROM '(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}')
    AS date_found
FROM customer_notes
WHERE notes ~* '(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2}';
```

---

## 10. IP Address & Network Patterns

```sql
-- ─────────────────────────────────────────
-- VALIDATE: IPv4 format
-- ─────────────────────────────────────────
SELECT ip_address,
  CASE
    WHEN ip_address ~ '^(25[0-5]|2[0-4]\d|[01]?\d\d?)\.(25[0-5]|2[0-4]\d|[01]?\d\d?)\.(25[0-5]|2[0-4]\d|[01]?\d\d?)\.(25[0-5]|2[0-4]\d|[01]?\d\d?)$'
    THEN '✅ Valid IPv4'
    ELSE '❌ Invalid'
  END AS ip_status
FROM web_events;

-- Quick (less strict) IPv4 check
SELECT ip_address,
  CASE
    WHEN ip_address ~ '^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$' THEN 'IPv4'
    WHEN ip_address ~ '^[0-9a-fA-F:]{3,39}$'                   THEN 'Possibly IPv6'
    ELSE 'Invalid'
  END AS ip_type
FROM web_events;

-- Find internal/private IPs
SELECT ip_address
FROM web_events
WHERE ip_address ~ '^(10\.|172\.(1[6-9]|2[0-9]|3[01])\.|192\.168\.)';
-- 10.x.x.x, 172.16–31.x.x, 192.168.x.x are RFC1918 private ranges

-- Extract IP from log string: "2024-01-15 14:32:01 - IP: 192.168.1.1 - GET /page"
SELECT
  log_line,
  SUBSTRING(log_line FROM '(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})') AS ip_extracted
FROM server_logs
WHERE log_line ~ '\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}';
```

---

## 11. Credit Card & Sensitive Data Patterns

```sql
-- ─────────────────────────────────────────
-- DETECT PII: Search for credit card patterns
-- (for redaction — NOT for storage or validation)
-- ─────────────────────────────────────────

-- Detect potential CC numbers in free text (16 digits, various formats)
SELECT *
FROM customer_notes
WHERE notes ~ '\b\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}\b'
   OR notes ~ '\b\d{16}\b';

-- Detect Amex (15 digits, starts with 3)
SELECT * FROM customer_notes
WHERE notes ~ '\b3[47]\d{2}[\s\-]?\d{6}[\s\-]?\d{5}\b';

-- ─────────────────────────────────────────
-- REDACT: Replace sensitive patterns
-- ─────────────────────────────────────────

-- Redact credit card numbers
UPDATE customer_notes
SET notes = REGEXP_REPLACE(
  notes,
  '\b(\d{4})[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?(\d{4})\b',
  '\1-XXXX-XXXX-\2',
  'g'
)
WHERE notes ~ '\b\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}\b';

-- Redact SSN
UPDATE customer_notes
SET notes = REGEXP_REPLACE(
  notes, '\b\d{3}-\d{2}-\d{4}\b', '[SSN REDACTED]', 'g'
)
WHERE notes ~ '\b\d{3}-\d{2}-\d{4}\b';

-- Redact phone numbers from free text
UPDATE customer_notes
SET notes = REGEXP_REPLACE(
  notes,
  '\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}',
  '[PHONE REDACTED]',
  'g'
)
WHERE notes ~ '\d{3}[\s.\-]?\d{3}[\s.\-]?\d{4}';

-- Redact email addresses from free text
UPDATE customer_notes
SET notes = REGEXP_REPLACE(
  notes,
  '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
  '[EMAIL REDACTED]',
  'gi'
)
WHERE notes ~* '@';

-- Full PII scrub pass (all at once)
UPDATE customer_notes
SET notes =
  REGEXP_REPLACE(
  REGEXP_REPLACE(
  REGEXP_REPLACE(
  REGEXP_REPLACE(notes,
    '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',   '[EMAIL REDACTED]',  'gi'),
    '\b\d{3}-\d{2}-\d{4}\b',                                '[SSN REDACTED]',    'g'),
    '\b\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}\b',       '[CC REDACTED]',     'g'),
    '\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}',                '[PHONE REDACTED]',  'g');
```

---

## 12. Free-Text Extraction

```sql
-- ─────────────────────────────────────────
-- Extract hashtags from social text
-- ─────────────────────────────────────────
SELECT
  post_id,
  UNNEST(REGEXP_MATCHES(content, '#[a-zA-Z][a-zA-Z0-9_]*', 'g')) AS hashtag
FROM social_posts;

-- ─────────────────────────────────────────
-- Extract @mentions
-- ─────────────────────────────────────────
SELECT
  post_id,
  UNNEST(REGEXP_MATCHES(content, '@[a-zA-Z][a-zA-Z0-9_.]*', 'g')) AS mention
FROM social_posts;

-- ─────────────────────────────────────────
-- Extract key-value pairs from log strings
-- e.g. "user_id=123 action=login duration=450ms"
-- ─────────────────────────────────────────
SELECT
  log_line,
  SUBSTRING(log_line FROM 'user_id=(\d+)')       AS user_id,
  SUBSTRING(log_line FROM 'action=(\w+)')        AS action,
  SUBSTRING(log_line FROM 'duration=(\d+)')      AS duration_ms,
  SUBSTRING(log_line FROM 'status=(\d{3})')      AS http_status
FROM server_logs;

-- ─────────────────────────────────────────
-- Extract quantities and units from product text
-- e.g. "500ml bottle", "2.5kg bag", "100 tablets"
-- ─────────────────────────────────────────
SELECT
  description,
  SUBSTRING(description FROM '(\d+(\.\d+)?)\s*(ml|l|kg|g|mg|lb|oz|cm|mm|m|in|ft)') AS measurement,
  SUBSTRING(description FROM '(\d+(\.\d+)?)(?=\s*(ml|l|kg|g|mg|lb|oz|cm|mm|m|in|ft))') AS quantity,
  SUBSTRING(description FROM '\d+(?:\.\d+)?\s*(ml|l|kg|g|mg|lb|oz|cm|mm|m|in|ft)') AS unit
FROM products
WHERE description ~* '\d+\s*(ml|l|kg|g|mg|lb|oz|cm|mm|m|in|ft)';

-- ─────────────────────────────────────────
-- Extract version numbers
-- e.g. "v2.3.1", "version 1.0", "2.3.1-beta"
-- ─────────────────────────────────────────
SELECT
  app_name,
  SUBSTRING(version_string FROM 'v?(\d+\.\d+(\.\d+)?)') AS version_clean,
  SPLIT_PART(SUBSTRING(version_string FROM 'v?(\d+\.\d+(\.\d+)?)'), '.', 1)::int AS major,
  SPLIT_PART(SUBSTRING(version_string FROM 'v?(\d+\.\d+(\.\d+)?)'), '.', 2)::int AS minor
FROM app_versions;
```

---

## 13. Regex-Based Data Profiling Queries

Use these to rapidly understand what's in a column before cleaning.

```sql
-- Universal column profiler: classify every value in a text column
SELECT
  col_value,
  CASE
    WHEN col_value IS NULL                                                             THEN 'NULL'
    WHEN col_value = ''                                                                THEN 'Empty string'
    WHEN col_value ~* '^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'         THEN 'Email'
    WHEN col_value ~ '^\+?\d[\d\s\-\(\)]{7,14}\d$'                                   THEN 'Phone'
    WHEN col_value ~ '^\d{4}-\d{2}-\d{2}$'                                            THEN 'Date (ISO)'
    WHEN col_value ~ '^\d{1,2}/\d{1,2}/\d{4}$'                                       THEN 'Date (US)'
    WHEN col_value ~ '^https?://'                                                      THEN 'URL'
    WHEN col_value ~ '^\d+(\.\d+)?$'                                                  THEN 'Positive number'
    WHEN col_value ~ '^-\d+(\.\d+)?$'                                                 THEN 'Negative number'
    WHEN col_value ~ '^\$[\d,]+(\.\d{2})?$'                                           THEN 'Currency (USD)'
    WHEN col_value ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}'                               THEN 'ISO Timestamp'
    WHEN col_value ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN 'UUID'
    WHEN col_value ~ '^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'                          THEN 'IP Address'
    WHEN col_value ~* '^(true|false|yes|no|y|n|1|0)$'                                 THEN 'Boolean-like'
    WHEN col_value ~ '^[A-Za-z ]+$'                                                    THEN 'Alpha only'
    WHEN col_value ~ '^\d+$'                                                           THEN 'Digits only'
    WHEN col_value ~ '^[A-Za-z0-9 ]+$'                                                THEN 'Alphanumeric'
    ELSE 'Mixed/Other'
  END AS detected_type,
  COUNT(*) AS count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct
FROM (
  SELECT your_column AS col_value FROM your_table
) AS raw
GROUP BY col_value, detected_type
ORDER BY count DESC;

-- Pattern fingerprint: replace each character class with a symbol
-- Helps visualize the "shape" of data in a column
SELECT
  REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(col,
      '[A-Z]', 'A', 'g'),
      '[a-z]', 'a', 'g'),
      '[0-9]', '9', 'g'),
      '\s', '_', 'g'),
      '[^Aa9_@.\-]', '?', 'g'
  ) AS pattern_shape,
  COUNT(*) AS count
FROM (SELECT your_column AS col FROM your_table) t
GROUP BY pattern_shape
ORDER BY count DESC
LIMIT 30;
/*
Example output:
pattern_shape     | count
------------------+-------
aaa@aaaa.aaa      | 45231  ← most common email pattern
aaa.aaa@aaaa.aaa  | 12402
Aaa Aaa           |  8901  ← "First Last" name pattern
*/
```

---

## 14. Regex Quick Reference Card

```
PATTERN         MATCHES                          EXAMPLE
─────────────────────────────────────────────────────────────────
\d              Any digit 0–9                    '5'
\d+             One or more digits               '123'
\d{10}          Exactly 10 digits                '1234567890'
\d{3,5}         3 to 5 digits                    '123', '12345'
[A-Z]           Uppercase letter                 'A'
[a-z]+          One or more lowercase letters    'hello'
[A-Za-z]+       One or more letters              'Hello'
[A-Za-z0-9_]+   Word characters                  'user_123'
\w+             Word characters (same as above)  'hello'
\s              Whitespace (space/tab/newline)   ' '
\s+             One or more whitespace           '   '
\.              Literal dot                      '.'
@               Literal @                        '@'
^               Start of string                  '^hello' matches 'hello world'
$               End of string                    'com$' matches '@gmail.com'
.*              Any characters (greedy)          matches anything
.+              One or more of any char          at least one char
[^abc]          Not a, b, or c                   any other character
a|b             a OR b                           'cat|dog' matches either
(abc)           Group                            capture 'abc'
\b              Word boundary                    '\bword\b' exact word
(?i)            Case-insensitive flag inline     '(?i)hello' = HELLO/hello

COMMON PATTERNS
─────────────────────────────────────────────────────────────────
Email     : [a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}
Phone US  : \(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}
Phone Intl: \+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{1,9}
URL       : https?://[^\s"'<>]+
IPv4      : \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}
UUID      : [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}
Date ISO  : \d{4}-\d{2}-\d{2}
Date US   : \d{2}/\d{2}/\d{4}
ZIP US    : \d{5}(-\d{4})?
CC number : \d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}
SSN       : \d{3}-\d{2}-\d{4}
```

---

*Part of the SQL Data Cleaning series → Next: DC_09_Embedded_Mixed_Data.md*