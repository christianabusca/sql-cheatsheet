# 🧹 SQL Data Cleaning — Part 5: Duplicate Detection & Deduplication
> *Find and eliminate exact duplicates, near-duplicates, event duplicates, and cross-system record merges*

---

## Table of Contents
- [1. Detecting Exact Duplicates](#1-detecting-exact-duplicates)
- [2. Detecting Duplicate Primary Keys](#2-detecting-duplicate-primary-keys)
- [3. Duplicate Events & Transactions](#3-duplicate-events--transactions)
- [4. Fuzzy / Near-Duplicate Detection](#4-fuzzy--near-duplicate-detection)
- [5. Choosing Which Duplicate to Keep](#5-choosing-which-duplicate-to-keep)
- [6. Deduplication Strategies](#6-deduplication-strategies)
- [7. Cross-System Entity Resolution](#7-cross-system-entity-resolution)
- [8. Deduplication Audit & Verification](#8-deduplication-audit--verification)

---

## 1. Detecting Exact Duplicates

```sql
-- Find completely identical rows (every column matches)
SELECT
  customer_id, first_name, last_name, email, phone,
  COUNT(*) AS duplicate_count
FROM customers
GROUP BY customer_id, first_name, last_name, email, phone
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Count total duplicate rows in a table
SELECT
  COUNT(*) AS total_rows,
  COUNT(*) - COUNT(DISTINCT (customer_id, email, first_name)) AS duplicate_rows,
  COUNT(DISTINCT (customer_id, email, first_name)) AS unique_rows
FROM customers;

-- Find the actual duplicate row pairs with ROW_NUMBER
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER(
      PARTITION BY first_name, last_name, email  -- key to define "same record"
      ORDER BY created_at ASC                     -- keep the oldest
    ) AS rn
  FROM customers
)
SELECT * FROM ranked WHERE rn > 1;  -- these are the duplicates to remove
```

---

## 2. Detecting Duplicate Primary Keys

```sql
-- Check for duplicate PKs (should never happen, but import bugs cause it)
SELECT customer_id, COUNT(*) AS count
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY count DESC;

-- Find all rows involved in PK duplicates
SELECT *
FROM customers
WHERE customer_id IN (
  SELECT customer_id
  FROM customers
  GROUP BY customer_id
  HAVING COUNT(*) > 1
)
ORDER BY customer_id, created_at;

-- Across foreign key relationships — find orphan FK references
SELECT DISTINCT o.customer_id
FROM orders AS o
LEFT JOIN customers AS c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;  -- orders referencing non-existent customers

-- Find FK pointing to a duplicate PK (ambiguous join)
SELECT
  o.order_id,
  COUNT(*) AS matched_customer_rows
FROM orders AS o
INNER JOIN customers AS c ON o.customer_id = c.customer_id
GROUP BY o.order_id
HAVING COUNT(*) > 1;  -- this order joins to >1 customer = PK duplicate problem
```

---

## 3. Duplicate Events & Transactions

```sql
-- Find duplicate order submissions (same customer, same amount, same day)
SELECT
  customer_id,
  DATE(order_date) AS order_day,
  total_amount,
  COUNT(*) AS submissions
FROM orders
GROUP BY customer_id, DATE(order_date), total_amount
HAVING COUNT(*) > 1
ORDER BY submissions DESC;

-- Find double-charged transactions within a short time window
SELECT
  a.transaction_id AS txn_a,
  b.transaction_id AS txn_b,
  a.customer_id,
  a.amount,
  a.merchant,
  a.transaction_ts,
  b.transaction_ts,
  EXTRACT(EPOCH FROM (b.transaction_ts - a.transaction_ts)) AS seconds_apart
FROM transactions AS a
INNER JOIN transactions AS b
  ON a.customer_id = b.customer_id
  AND a.amount = b.amount
  AND a.merchant = b.merchant
  AND a.transaction_id < b.transaction_id       -- avoid self-match and duplication
  AND ABS(EXTRACT(EPOCH FROM (b.transaction_ts - a.transaction_ts))) < 300  -- within 5 min
ORDER BY seconds_apart;

-- Find duplicate page view events (same user, same page, within 1 second)
SELECT
  a.session_id,
  a.page_url,
  a.event_ts,
  b.event_ts AS dup_ts,
  EXTRACT(MILLISECONDS FROM (b.event_ts - a.event_ts)) AS ms_apart
FROM page_views AS a
INNER JOIN page_views AS b
  ON a.session_id = b.session_id
  AND a.page_url = b.page_url
  AND a.event_id < b.event_id
  AND b.event_ts - a.event_ts < INTERVAL '1 second';

-- Deduplicate events: keep first occurrence per session+page+second
DELETE FROM page_views
WHERE event_id NOT IN (
  SELECT MIN(event_id)
  FROM page_views
  GROUP BY session_id, page_url, DATE_TRUNC('second', event_ts)
);
```

---

## 4. Fuzzy / Near-Duplicate Detection

```sql
-- Enable trigram extension for fuzzy text matching
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Find customers with similar names and emails (likely duplicates)
SELECT
  a.customer_id,
  a.first_name || ' ' || a.last_name AS name_a,
  a.email                            AS email_a,
  b.customer_id                      AS dup_candidate_id,
  b.first_name || ' ' || b.last_name AS name_b,
  b.email                            AS email_b,
  ROUND(SIMILARITY(
    a.first_name || ' ' || a.last_name,
    b.first_name || ' ' || b.last_name
  )::numeric, 3) AS name_similarity,
  ROUND(SIMILARITY(a.email, b.email)::numeric, 3) AS email_similarity
FROM customers AS a
INNER JOIN customers AS b
  ON a.customer_id < b.customer_id
  AND (
    SIMILARITY(a.email, b.email) > 0.8           -- very similar email
    OR (
      SIMILARITY(a.first_name || a.last_name,
                 b.first_name || b.last_name) > 0.85  -- very similar name
      AND a.phone = b.phone                            -- AND same phone
    )
  )
ORDER BY email_similarity DESC, name_similarity DESC;

-- Block matching: only compare within same first-letter + ZIP (scalable)
SELECT
  a.customer_id,
  b.customer_id AS candidate_id,
  ROUND(SIMILARITY(a.full_name, b.full_name)::numeric, 3) AS similarity,
  a.full_name,
  b.full_name
FROM customers AS a
INNER JOIN customers AS b
  ON a.customer_id < b.customer_id
  AND LEFT(a.last_name, 1) = LEFT(b.last_name, 1)  -- blocking key
  AND a.zip_code = b.zip_code                        -- blocking key
  AND SIMILARITY(a.full_name, b.full_name) > 0.75
ORDER BY similarity DESC
LIMIT 500;

-- Soundex matching (phonetic similarity — "Smith" matches "Smyth")
SELECT
  a.customer_id,
  a.last_name,
  b.customer_id AS candidate_id,
  b.last_name AS candidate_name
FROM customers AS a
INNER JOIN customers AS b
  ON a.customer_id < b.customer_id
  AND SOUNDEX(a.last_name) = SOUNDEX(b.last_name)
  AND a.last_name != b.last_name   -- different spelling but same sound
  AND a.zip_code = b.zip_code;
```

---

## 5. Choosing Which Duplicate to Keep

```sql
-- Strategy: keep the MOST COMPLETE record (fewest NULLs)
WITH scored AS (
  SELECT
    *,
    (CASE WHEN email      IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN phone      IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN address    IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN birth_date IS NOT NULL THEN 1 ELSE 0 END) AS completeness_score,
    ROW_NUMBER() OVER(
      PARTITION BY LOWER(email)
      ORDER BY
        (CASE WHEN phone IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN address IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN birth_date IS NOT NULL THEN 1 ELSE 0 END) DESC,
        created_at ASC  -- tiebreak: oldest record
    ) AS rn
  FROM customers
  WHERE email IS NOT NULL
)
SELECT * FROM scored WHERE rn = 1;  -- keepers

-- Strategy: keep the MOST RECENT record
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY email ORDER BY updated_at DESC) AS rn
  FROM customers
)
SELECT * FROM ranked WHERE rn = 1;

-- Strategy: keep the record with the MOST ACTIVITY (most orders)
WITH ranked AS (
  SELECT
    c.*,
    COALESCE(o.order_count, 0) AS order_count,
    ROW_NUMBER() OVER(
      PARTITION BY LOWER(c.email)
      ORDER BY COALESCE(o.order_count, 0) DESC, c.created_at ASC
    ) AS rn
  FROM customers AS c
  LEFT JOIN (
    SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id
  ) AS o ON c.customer_id = o.customer_id
)
SELECT * FROM ranked WHERE rn = 1;

-- Strategy: MERGE the best fields from all duplicates into one record
WITH grouped AS (
  SELECT
    MIN(customer_id) AS master_id,  -- use lowest ID as canonical
    -- Take first non-null value from any duplicate
    COALESCE(MAX(first_name), 'Unknown')   AS first_name,
    COALESCE(MAX(last_name),  'Unknown')   AS last_name,
    MAX(phone)                             AS phone,
    MAX(address)                           AS address,
    MAX(birth_date)                        AS birth_date,
    LOWER(email)                           AS email,
    MIN(created_at)                        AS original_created_at
  FROM customers
  GROUP BY LOWER(email)
)
SELECT * FROM grouped;
```

---

## 6. Deduplication Strategies

### Strategy A: Delete Duplicates In-Place

```sql
-- Safe delete: keep lowest customer_id per email
DELETE FROM customers
WHERE customer_id NOT IN (
  SELECT MIN(customer_id)
  FROM customers
  GROUP BY LOWER(email)
);

-- Alternative using ctid (PostgreSQL internal row identifier — fastest)
DELETE FROM customers
WHERE ctid NOT IN (
  SELECT MIN(ctid)
  FROM customers
  GROUP BY LOWER(email)
);
```

### Strategy B: Create a Clean Copy

```sql
-- Create deduplicated table from scratch
CREATE TABLE customers_clean AS
SELECT DISTINCT ON (LOWER(email))
  *
FROM customers
ORDER BY LOWER(email), created_at ASC;  -- keep oldest

-- Verify counts
SELECT
  (SELECT COUNT(*) FROM customers)       AS original_count,
  (SELECT COUNT(*) FROM customers_clean) AS clean_count,
  (SELECT COUNT(*) FROM customers)
  - (SELECT COUNT(*) FROM customers_clean) AS rows_removed;

-- Add proper constraint to prevent future duplicates
ALTER TABLE customers_clean
  ADD CONSTRAINT uq_customer_email UNIQUE (email);
```

### Strategy C: Mark & Exclude (Non-Destructive)

```sql
-- Add a column marking the canonical record
ALTER TABLE customers ADD COLUMN IF NOT EXISTS is_master_record BOOLEAN DEFAULT TRUE;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS merged_into_id   BIGINT;

-- Mark duplicates (non-masters) pointing to their master
WITH ranked AS (
  SELECT customer_id,
    ROW_NUMBER() OVER(PARTITION BY LOWER(email) ORDER BY created_at ASC) AS rn,
    FIRST_VALUE(customer_id) OVER(PARTITION BY LOWER(email) ORDER BY created_at ASC) AS master_id
  FROM customers
)
UPDATE customers AS c
SET
  is_master_record = (r.rn = 1),
  merged_into_id   = CASE WHEN r.rn > 1 THEN r.master_id ELSE NULL END
FROM ranked AS r
WHERE c.customer_id = r.customer_id;

-- Query that automatically uses only master records
SELECT * FROM customers WHERE is_master_record = TRUE;
```

---

## 7. Cross-System Entity Resolution

When the same customer exists in multiple source systems with different IDs.

```sql
-- Build a cross-reference map between two systems
WITH matches AS (
  SELECT
    crm.customer_id   AS crm_id,
    shop.customer_id  AS shop_id,
    CASE
      -- Exact email match → definite same person
      WHEN LOWER(crm.email) = LOWER(shop.email)
        THEN 'email_match'
      -- Same name + phone → very likely same
      WHEN LOWER(crm.first_name) = LOWER(shop.first_name)
       AND LOWER(crm.last_name)  = LOWER(shop.last_name)
       AND crm.phone = shop.phone
        THEN 'name_phone_match'
      -- Same name + address → likely same
      WHEN LOWER(crm.first_name) = LOWER(shop.first_name)
       AND LOWER(crm.last_name)  = LOWER(shop.last_name)
       AND crm.zip_code = shop.zip_code
        THEN 'name_zip_match'
      ELSE NULL
    END AS match_type,
    CASE
      WHEN LOWER(crm.email) = LOWER(shop.email)                THEN 1.00
      WHEN LOWER(crm.first_name) = LOWER(shop.first_name)
       AND LOWER(crm.last_name)  = LOWER(shop.last_name)
       AND crm.phone = shop.phone                              THEN 0.95
      WHEN LOWER(crm.first_name) = LOWER(shop.first_name)
       AND LOWER(crm.last_name)  = LOWER(shop.last_name)
       AND crm.zip_code = shop.zip_code                        THEN 0.80
      ELSE 0
    END AS confidence_score
  FROM crm_customers AS crm
  CROSS JOIN shopify_customers AS shop
  WHERE LOWER(crm.email) = LOWER(shop.email)
     OR (LOWER(crm.first_name) = LOWER(shop.first_name)
         AND LOWER(crm.last_name) = LOWER(shop.last_name)
         AND (crm.phone = shop.phone OR crm.zip_code = shop.zip_code))
)
SELECT *
FROM matches
WHERE match_type IS NOT NULL
ORDER BY confidence_score DESC;

-- Create the master entity table with source system IDs
CREATE TABLE master_customers AS
SELECT
  ROW_NUMBER() OVER(ORDER BY crm_id) AS master_customer_id,
  crm_id,
  shop_id,
  match_type,
  confidence_score
FROM matches
WHERE confidence_score >= 0.95;
```

---

## 8. Deduplication Audit & Verification

```sql
-- Before/after comparison
SELECT 'before' AS stage, COUNT(*) AS total, COUNT(DISTINCT email) AS unique_emails FROM customers_raw
UNION ALL
SELECT 'after',  COUNT(*),               COUNT(DISTINCT email) FROM customers_clean;

-- Confirm no duplicates remain
SELECT
  LOWER(email) AS email,
  COUNT(*) AS count
FROM customers_clean
GROUP BY LOWER(email)
HAVING COUNT(*) > 1;
-- Should return 0 rows

-- Confirm no data loss: every email in original exists in clean
SELECT COUNT(*) AS emails_lost
FROM (
  SELECT DISTINCT LOWER(email) FROM customers_raw
  EXCEPT
  SELECT DISTINCT LOWER(email) FROM customers_clean
) AS missing;

-- Confirm orders still point to valid customers
SELECT COUNT(*) AS orphan_orders
FROM orders AS o
LEFT JOIN customers_clean AS c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Summary report
SELECT
  (SELECT COUNT(*) FROM customers_raw)                   AS original_rows,
  (SELECT COUNT(*) FROM customers_clean)                  AS clean_rows,
  (SELECT COUNT(*) FROM customers_raw)
    - (SELECT COUNT(*) FROM customers_clean)              AS rows_removed,
  ROUND(
    100.0 * ((SELECT COUNT(*) FROM customers_raw)
             - (SELECT COUNT(*) FROM customers_clean))
    / NULLIF((SELECT COUNT(*) FROM customers_raw), 0), 2) AS pct_removed;
```

---

## 📌 Deduplication Quick Reference

| Scenario | Technique |
|----------|----------|
| Exact row duplicates | `GROUP BY all cols HAVING COUNT > 1` |
| Same email, different records | `ROW_NUMBER() OVER(PARTITION BY email)` |
| Near-duplicate names | `SIMILARITY()` from `pg_trgm` extension |
| Phonetic name matching | `SOUNDEX()` function |
| Duplicate events (time window) | Self-join with time gap condition |
| Keep most complete | Order by `completeness_score DESC` in ROW_NUMBER |
| Keep most recent | `ORDER BY updated_at DESC` in ROW_NUMBER |
| Non-destructive dedup | `is_master_record` + `merged_into_id` columns |
| Cross-system matching | CROSS JOIN with multi-field confidence scoring |
| Prevent future duplicates | `ADD CONSTRAINT ... UNIQUE (email)` |

---

*Part of the SQL Data Cleaning series → Next: Part 6 — Schema, Referential Integrity & Constraint Cleaning*