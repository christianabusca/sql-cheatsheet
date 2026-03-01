# 🧹 SQL Data Cleaning — Part 6: Schema, Referential Integrity & Constraint Cleaning
> *Fix orphan records, broken foreign keys, wrong data types, constraint violations, and schema inconsistencies*

---

## Table of Contents
- [1. Auditing Column Data Types](#1-auditing-column-data-types)
- [2. Fixing Wrong Data Types](#2-fixing-wrong-data-types)
- [3. Referential Integrity Checks](#3-referential-integrity-checks)
- [4. Fixing Orphan Records](#4-fixing-orphan-records)
- [5. Constraint Violation Detection](#5-constraint-violation-detection)
- [6. Standardizing Lookup / Enum Columns](#6-standardizing-lookup--enum-columns)
- [7. Column Naming & Structure Cleanup](#7-column-naming--structure-cleanup)
- [8. Adding Protective Constraints](#8-adding-protective-constraints)
- [9. Final Schema Audit Checklist](#9-final-schema-audit-checklist)

---

## 1. Auditing Column Data Types

```sql
-- List all columns and their types for a table
SELECT
  column_name,
  data_type,
  character_maximum_length,
  is_nullable,
  column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'orders'
ORDER BY ordinal_position;

-- Find columns that look numeric but are stored as text
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'products'
  AND data_type IN ('character varying', 'text')
  AND column_name ~* '(price|amount|quantity|weight|age|score|rate|count|total)';

-- Find TEXT columns with suspiciously high cardinality (should they be numeric?)
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'orders'
  AND data_type IN ('text', 'character varying');
-- Then for each, run: SELECT COUNT(DISTINCT col) FROM table;

-- Detect misused VARCHAR for dates
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'orders'
  AND data_type IN ('character varying', 'text')
  AND column_name ~* '(date|time|at|on|created|updated|deleted|shipped|delivered)';
```

---

## 2. Fixing Wrong Data Types

```sql
-- Text price → NUMERIC
-- Step 1: check for non-numeric values first
SELECT raw_price FROM products
WHERE raw_price !~ '^[0-9]+(\.[0-9]{1,4})?$';

-- Step 2: clean, then alter
UPDATE products
SET raw_price = REGEXP_REPLACE(raw_price, '[^0-9.]', '', 'g')
WHERE raw_price ~ '[^0-9.]';

-- Step 3: add new column with correct type
ALTER TABLE products ADD COLUMN price NUMERIC(12, 2);
UPDATE products SET price = raw_price::NUMERIC;
ALTER TABLE products DROP COLUMN raw_price;
ALTER TABLE products RENAME COLUMN price TO price;

-- Text date → DATE (see DC_02 for format parsing)
ALTER TABLE orders ADD COLUMN order_date DATE;
UPDATE orders SET order_date = order_date_raw::DATE
WHERE order_date_raw ~ '^\d{4}-\d{2}-\d{2}$';
ALTER TABLE orders DROP COLUMN order_date_raw;

-- VARCHAR status → ENUM type (enforces valid values at DB level)
CREATE TYPE order_status_enum AS ENUM (
  'pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'refunded'
);

-- Clean values before converting
UPDATE orders
SET status = LOWER(TRIM(status));

UPDATE orders
SET status = 'pending'
WHERE status NOT IN ('pending','confirmed','shipped','delivered','cancelled','refunded');

-- Convert to ENUM
ALTER TABLE orders ALTER COLUMN status TYPE order_status_enum
  USING status::order_status_enum;

-- VARCHAR boolean → actual BOOLEAN
-- Common patterns: 'Y'/'N', 'yes'/'no', '1'/'0', 'true'/'false', 'T'/'F'
ALTER TABLE products ADD COLUMN is_active BOOLEAN;

UPDATE products
SET is_active = CASE LOWER(TRIM(active_flag))
  WHEN '1'     THEN TRUE
  WHEN 'y'     THEN TRUE
  WHEN 'yes'   THEN TRUE
  WHEN 'true'  THEN TRUE
  WHEN 't'     THEN TRUE
  WHEN 'on'    THEN TRUE
  WHEN '0'     THEN FALSE
  WHEN 'n'     THEN FALSE
  WHEN 'no'    THEN FALSE
  WHEN 'false' THEN FALSE
  WHEN 'f'     THEN FALSE
  WHEN 'off'   THEN FALSE
  ELSE NULL
END;

ALTER TABLE products DROP COLUMN active_flag;
```

---

## 3. Referential Integrity Checks

```sql
-- Find orders referencing non-existent customers (broken FK)
SELECT o.order_id, o.customer_id
FROM orders AS o
LEFT JOIN customers AS c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Find order items referencing non-existent orders
SELECT oi.order_item_id, oi.order_id
FROM order_items AS oi
LEFT JOIN orders AS o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Find order items referencing non-existent products
SELECT oi.order_item_id, oi.product_id
FROM order_items AS oi
LEFT JOIN products AS p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Multi-table cascade check
SELECT
  'orders → customers'          AS relationship,
  COUNT(*) AS orphan_count
FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id WHERE c.customer_id IS NULL

UNION ALL SELECT
  'order_items → orders',
  COUNT(*)
FROM order_items oi LEFT JOIN orders o ON oi.order_id = o.order_id WHERE o.order_id IS NULL

UNION ALL SELECT
  'order_items → products',
  COUNT(*)
FROM order_items oi LEFT JOIN products p ON oi.product_id = p.product_id WHERE p.product_id IS NULL

UNION ALL SELECT
  'reviews → customers',
  COUNT(*)
FROM reviews r LEFT JOIN customers c ON r.customer_id = c.customer_id WHERE c.customer_id IS NULL;
```

---

## 4. Fixing Orphan Records

```sql
-- Option 1: Delete orphan records
DELETE FROM orders
WHERE customer_id NOT IN (SELECT customer_id FROM customers);

DELETE FROM order_items
WHERE order_id NOT IN (SELECT order_id FROM orders);

-- Option 2: Assign to a placeholder "unknown" record
-- First create a catch-all customer
INSERT INTO customers (customer_id, first_name, last_name, email, created_at)
VALUES (-1, 'Unknown', 'Customer', 'unknown@placeholder.com', NOW())
ON CONFLICT (customer_id) DO NOTHING;

-- Then reassign orphans to the placeholder
UPDATE orders
SET customer_id = -1
WHERE customer_id NOT IN (SELECT customer_id FROM customers);

-- Option 3: Move to quarantine table
CREATE TABLE IF NOT EXISTS orphan_orders AS
SELECT *, CURRENT_TIMESTAMP AS flagged_at
FROM orders WHERE FALSE;

INSERT INTO orphan_orders
SELECT *, CURRENT_TIMESTAMP
FROM orders
WHERE customer_id NOT IN (SELECT customer_id FROM customers);

DELETE FROM orders
WHERE customer_id NOT IN (SELECT customer_id FROM customers);

-- Option 4: Restore missing parent record from backup/archive
INSERT INTO customers (customer_id, first_name, email, created_at)
SELECT DISTINCT o.customer_id, 'Recovered', 'recovered_' || o.customer_id || '@unknown.com', MIN(o.order_date)
FROM orders AS o
LEFT JOIN customers AS c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
GROUP BY o.customer_id;
```

---

## 5. Constraint Violation Detection

```sql
-- Check NOT NULL violations (data that should never be NULL)
SELECT
  COUNT(*) FILTER (WHERE customer_id IS NULL)   AS null_pk,
  COUNT(*) FILTER (WHERE email IS NULL)          AS null_email,
  COUNT(*) FILTER (WHERE created_at IS NULL)     AS null_created_at
FROM customers;

-- Check UNIQUE violations
SELECT email, COUNT(*) AS count
FROM customers
GROUP BY email
HAVING COUNT(*) > 1;

-- Check CHECK constraint violations (before adding the constraint)
-- Example: price must be > 0
SELECT product_id, price
FROM products
WHERE price IS NOT NULL AND price <= 0;

-- Example: end_date must be >= start_date
SELECT subscription_id, start_date, end_date
FROM subscriptions
WHERE end_date < start_date;

-- Example: discount must be between 0 and 1
SELECT order_id, discount_rate
FROM orders
WHERE discount_rate NOT BETWEEN 0 AND 1;

-- Example: age must be reasonable (18-120)
SELECT customer_id, age
FROM customers
WHERE age IS NOT NULL AND (age < 0 OR age > 120);

-- Example: quantity must be a positive integer
SELECT order_item_id, quantity
FROM order_items
WHERE quantity IS NULL OR quantity <= 0 OR quantity != FLOOR(quantity);

-- Fix all violations before adding constraints
UPDATE products SET price = NULL WHERE price <= 0;
UPDATE orders SET discount_rate = GREATEST(0, LEAST(1, discount_rate)) WHERE discount_rate NOT BETWEEN 0 AND 1;
UPDATE order_items SET quantity = ABS(ROUND(quantity))::int WHERE quantity <= 0 OR quantity != FLOOR(quantity);
```

---

## 6. Standardizing Lookup / Enum Columns

```sql
-- Audit all distinct values in a status column
SELECT status, COUNT(*) AS count
FROM orders
GROUP BY status
ORDER BY count DESC;
-- Might show: 'completed', 'Completed', 'COMPLETED', 'complete', 'done', 'finished'

-- Create the canonical value map
WITH status_map(raw, canonical) AS (
  VALUES
    ('completed',  'completed'),
    ('Completed',  'completed'),
    ('COMPLETED',  'completed'),
    ('complete',   'completed'),
    ('done',       'completed'),
    ('finished',   'completed'),
    ('pending',    'pending'),
    ('Pending',    'pending'),
    ('PENDING',    'pending'),
    ('in progress','pending'),
    ('processing', 'pending'),
    ('cancelled',  'cancelled'),
    ('Cancelled',  'cancelled'),
    ('canceled',   'cancelled'),   -- American spelling
    ('CANCELLED',  'cancelled'),
    ('refunded',   'refunded'),
    ('returned',   'refunded')
)
UPDATE orders AS o
SET status = sm.canonical
FROM status_map AS sm
WHERE o.status = sm.raw;

-- Audit for any remaining unmapped values
SELECT DISTINCT status
FROM orders
WHERE status NOT IN ('completed','pending','shipped','delivered','cancelled','refunded');

-- Set remaining unknowns to a safe default
UPDATE orders
SET status = 'pending'
WHERE status NOT IN ('completed','pending','shipped','delivered','cancelled','refunded');

-- Add a proper constraint to enforce valid values going forward
ALTER TABLE orders
  ADD CONSTRAINT chk_order_status
  CHECK (status IN ('completed','pending','shipped','delivered','cancelled','refunded'));
```

---

## 7. Column Naming & Structure Cleanup

```sql
-- Rename poorly named columns
ALTER TABLE orders RENAME COLUMN dt TO order_date;
ALTER TABLE customers RENAME COLUMN nm TO full_name;
ALTER TABLE products RENAME COLUMN prc TO price;
ALTER TABLE orders RENAME COLUMN amt TO total_amount;

-- Add missing standard columns
ALTER TABLE customers
  ADD COLUMN IF NOT EXISTS created_at  TIMESTAMPTZ DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS updated_at  TIMESTAMPTZ DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS is_deleted  BOOLEAN     DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS deleted_at  TIMESTAMPTZ;

ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS created_at  TIMESTAMPTZ DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS updated_at  TIMESTAMPTZ DEFAULT NOW();

-- Populate missing timestamps from surrogate data
UPDATE customers
SET created_at = (
  SELECT MIN(order_date) FROM orders WHERE orders.customer_id = customers.customer_id
)
WHERE created_at IS NULL;

-- Add auto-update trigger for updated_at
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE ON customers
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Normalize column name casing and separators (snake_case standard)
-- Rename camelCase → snake_case
ALTER TABLE orders RENAME COLUMN "orderDate"     TO order_date;
ALTER TABLE orders RENAME COLUMN "totalAmount"   TO total_amount;
ALTER TABLE orders RENAME COLUMN "customerId"    TO customer_id;
ALTER TABLE customers RENAME COLUMN "firstName"  TO first_name;
ALTER TABLE customers RENAME COLUMN "lastName"   TO last_name;
```

---

## 8. Adding Protective Constraints

Prevent bad data from entering in the future.

```sql
-- Primary key constraint
ALTER TABLE customers ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);

-- Unique constraints
ALTER TABLE customers ADD CONSTRAINT uq_customer_email UNIQUE (email);
ALTER TABLE products  ADD CONSTRAINT uq_product_sku    UNIQUE (sku);

-- Foreign key constraints
ALTER TABLE orders ADD CONSTRAINT fk_orders_customer
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
  ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE order_items ADD CONSTRAINT fk_items_order
  FOREIGN KEY (order_id) REFERENCES orders(order_id)
  ON DELETE CASCADE ON UPDATE CASCADE;

-- NOT NULL constraints
ALTER TABLE customers ALTER COLUMN email      SET NOT NULL;
ALTER TABLE customers ALTER COLUMN first_name SET NOT NULL;
ALTER TABLE orders    ALTER COLUMN order_date SET NOT NULL;
ALTER TABLE orders    ALTER COLUMN customer_id SET NOT NULL;

-- CHECK constraints
ALTER TABLE products
  ADD CONSTRAINT chk_price_positive CHECK (price > 0);

ALTER TABLE orders
  ADD CONSTRAINT chk_discount_range CHECK (discount_rate BETWEEN 0 AND 1);

ALTER TABLE subscriptions
  ADD CONSTRAINT chk_date_order CHECK (end_date >= start_date);

ALTER TABLE customers
  ADD CONSTRAINT chk_email_format
  CHECK (email ~* '^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$');

-- Default values for frequently NULL'd columns
ALTER TABLE orders    ALTER COLUMN discount_rate SET DEFAULT 0;
ALTER TABLE customers ALTER COLUMN is_active     SET DEFAULT TRUE;
ALTER TABLE products  ALTER COLUMN is_featured   SET DEFAULT FALSE;
```

---

## 9. Final Schema Audit Checklist

```sql
-- Check all tables for missing primary keys
SELECT
  t.table_name,
  CASE WHEN tc.table_name IS NOT NULL THEN 'Has PK' ELSE '⚠️  Missing PK' END AS pk_status
FROM information_schema.tables AS t
LEFT JOIN information_schema.table_constraints AS tc
  ON t.table_name = tc.table_name
  AND t.table_schema = tc.table_schema
  AND tc.constraint_type = 'PRIMARY KEY'
WHERE t.table_schema = 'public'
  AND t.table_type = 'BASE TABLE'
ORDER BY pk_status, t.table_name;

-- Check for nullable columns that should be NOT NULL
SELECT column_name, table_name, is_nullable, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('orders', 'customers', 'products')
  AND is_nullable = 'YES'
  AND column_name IN (
    'customer_id', 'order_id', 'product_id',  -- ID columns
    'email', 'status', 'order_date',           -- business-critical columns
    'price', 'total_amount'                    -- financial columns
  )
ORDER BY table_name, column_name;

-- Check FK coverage
SELECT
  tc.table_name,
  kcu.column_name,
  ccu.table_name AS references_table,
  ccu.column_name AS references_column
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'public'
ORDER BY tc.table_name;
```

---

## 📌 Schema Cleaning Quick Reference

| Problem | Fix |
|---------|-----|
| Wrong column data type | `ALTER COLUMN ... TYPE ... USING` |
| Text booleans | `CASE WHEN val IN ('Y','1','yes') THEN TRUE` |
| Orphan foreign keys | Delete, reassign to placeholder, or quarantine |
| Duplicate PKs | `DELETE WHERE ctid NOT IN (SELECT MIN(ctid)...)` |
| Invalid enum values | `CASE LOWER(TRIM(col)) WHEN ...` map to canonical |
| Missing timestamps | `ADD COLUMN created_at TIMESTAMPTZ DEFAULT NOW()` |
| Prevent future bad data | `ADD CONSTRAINT CHECK / UNIQUE / NOT NULL` |
| Snake_case rename | `ALTER TABLE ... RENAME COLUMN camelCase TO snake_case` |
| Referential integrity | `ADD FOREIGN KEY ... REFERENCES` |

---

*Part of the SQL Data Cleaning series → Next: Part 7 — Master Cleaning Pipeline & Final Audit*