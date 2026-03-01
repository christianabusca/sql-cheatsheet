# 📊 SQL for Data Analysis — Level 5: Expert Patterns & Performance
> *Recursive queries, graph traversal, AB testing, pivot tables, query optimization*

---

## Table of Contents
- [1. Recursive CTEs](#1-recursive-ctes)
- [2. AB Test Analysis in SQL](#2-ab-test-analysis-in-sql)
- [3. Dynamic Pivot Tables](#3-dynamic-pivot-tables)
- [4. Graph & Hierarchy Traversal](#4-graph--hierarchy-traversal)
- [5. Advanced String & JSON Analytics](#5-advanced-string--json-analytics)
- [6. Query Performance & Optimization](#6-query-performance--optimization)
- [7. Data Quality Checks](#7-data-quality-checks)
- [8. Expert Pattern Library](#8-expert-pattern-library)

---

## 1. Recursive CTEs

Recursive CTEs solve problems that require iterative or self-referencing logic — like org charts, date spines, and cumulative calculations.

### Date Spine Generator

```sql
-- Generate a complete date series (no gaps, guaranteed every day)
WITH RECURSIVE date_spine AS (
  -- Anchor: start date
  SELECT '2024-01-01'::date AS dt

  UNION ALL

  -- Recursive: add one day each iteration
  SELECT dt + 1
  FROM date_spine
  WHERE dt < '2024-12-31'::date
)
SELECT ds.dt AS date, COALESCE(SUM(o.total_amount), 0) AS revenue
FROM date_spine AS ds
LEFT JOIN orders AS o ON DATE(o.order_date) = ds.dt
GROUP BY ds.dt
ORDER BY ds.dt;

-- Without recursive CTE (PostgreSQL shortcut)
SELECT
  generate_series::date AS date
FROM generate_series('2024-01-01'::date, '2024-12-31'::date, '1 day') AS gs;
```

### Org Chart / Hierarchy Traversal

```sql
-- Employee table: id, name, manager_id (NULL = CEO)
WITH RECURSIVE org_tree AS (
  -- Anchor: top-level employees (no manager)
  SELECT
    id,
    name,
    manager_id,
    name AS path,
    0 AS depth
  FROM employees
  WHERE manager_id IS NULL

  UNION ALL

  -- Recursive: find direct reports
  SELECT
    e.id,
    e.name,
    e.manager_id,
    org_tree.path || ' → ' || e.name AS path,
    org_tree.depth + 1 AS depth
  FROM employees AS e
  INNER JOIN org_tree ON e.manager_id = org_tree.id
)
SELECT
  LPAD(' ', depth * 4, ' ') || name AS indented_name,
  path,
  depth AS level
FROM org_tree
ORDER BY path;

/*
CEO
    VP Sales → Manager A → John
    VP Sales → Manager A → Jane
    VP Engineering → Manager B → Bob
*/
```

### Bill of Materials (Multi-Level Parts Explosion)

```sql
-- Find all components (at any depth) needed to build a product
WITH RECURSIVE bom AS (
  -- Anchor: direct components
  SELECT parent_id, child_id, quantity, 1 AS level
  FROM parts_hierarchy
  WHERE parent_id = 'PRODUCT_X'

  UNION ALL

  -- Recursive: components of components
  SELECT ph.parent_id, ph.child_id, ph.quantity * bom.quantity, bom.level + 1
  FROM parts_hierarchy AS ph
  INNER JOIN bom ON ph.parent_id = bom.child_id
  WHERE bom.level < 10  -- depth limit to prevent infinite loops
)
SELECT child_id AS part, SUM(quantity) AS total_needed
FROM bom
GROUP BY child_id
ORDER BY total_needed DESC;
```

### Fibonacci / Numeric Series

```sql
-- Generate Fibonacci sequence
WITH RECURSIVE fib AS (
  SELECT 0 AS n, 0 AS val, 1 AS next_val
  UNION ALL
  SELECT n + 1, next_val, val + next_val
  FROM fib
  WHERE n < 20
)
SELECT n, val FROM fib;
```

---

## 2. AB Test Analysis in SQL

Properly analyze experiment results with statistical significance.

```sql
-- Step 1: Aggregate experiment metrics
WITH experiment_results AS (
  SELECT
    variant,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT CASE WHEN converted THEN user_id END) AS conversions,
    SUM(revenue) AS total_revenue
  FROM ab_experiment
  WHERE experiment_name = 'checkout_button_test'
    AND event_date BETWEEN '2024-06-01' AND '2024-06-30'
  GROUP BY variant
),

-- Step 2: Calculate rates
rates AS (
  SELECT
    variant,
    users,
    conversions,
    total_revenue,
    ROUND(100.0 * conversions / NULLIF(users, 0), 3) AS conversion_rate_pct,
    ROUND(total_revenue / NULLIF(users, 0), 2) AS revenue_per_user
  FROM experiment_results
),

-- Step 3: Compute uplift vs control
control AS (
  SELECT conversion_rate_pct, revenue_per_user
  FROM rates WHERE variant = 'control'
)

SELECT
  r.variant,
  r.users,
  r.conversions,
  r.conversion_rate_pct,
  r.revenue_per_user,
  ROUND(r.conversion_rate_pct - c.conversion_rate_pct, 3) AS absolute_lift_pct,
  ROUND(100.0 * (r.conversion_rate_pct - c.conversion_rate_pct)
                / NULLIF(c.conversion_rate_pct, 0), 1) AS relative_lift_pct,
  ROUND(r.revenue_per_user - c.revenue_per_user, 2) AS revenue_lift_per_user
FROM rates AS r
CROSS JOIN control AS c;
```

### Z-Test for Statistical Significance

```sql
WITH stats AS (
  SELECT
    SUM(CASE WHEN variant = 'control'   AND converted THEN 1 ELSE 0 END) AS ctrl_conv,
    SUM(CASE WHEN variant = 'control'                   THEN 1 ELSE 0 END) AS ctrl_n,
    SUM(CASE WHEN variant = 'treatment' AND converted THEN 1 ELSE 0 END) AS trt_conv,
    SUM(CASE WHEN variant = 'treatment'                 THEN 1 ELSE 0 END) AS trt_n
  FROM ab_experiment
),
rates AS (
  SELECT
    ctrl_conv::numeric / ctrl_n AS p_ctrl,
    trt_conv::numeric  / trt_n  AS p_trt,
    ctrl_n,
    trt_n
  FROM stats
),
z_calc AS (
  SELECT
    p_ctrl,
    p_trt,
    p_trt - p_ctrl AS delta,
    -- Pooled proportion
    (p_ctrl * ctrl_n + p_trt * trt_n) / (ctrl_n + trt_n) AS p_pool,
    ctrl_n,
    trt_n
  FROM rates
)
SELECT
  ROUND(p_ctrl * 100, 3) AS ctrl_rate_pct,
  ROUND(p_trt  * 100, 3) AS trt_rate_pct,
  ROUND(delta  * 100, 3) AS lift_pct,
  -- Z-score
  ROUND(
    delta / SQRT(p_pool * (1 - p_pool) * (1.0/ctrl_n + 1.0/trt_n)),
    3
  ) AS z_score,
  -- Interpretation: |z| > 1.96 → significant at 95%,  |z| > 2.576 → 99%
  CASE
    WHEN ABS(delta / SQRT(p_pool * (1 - p_pool) * (1.0/ctrl_n + 1.0/trt_n))) > 2.576
      THEN '✅ Significant at 99%'
    WHEN ABS(delta / SQRT(p_pool * (1 - p_pool) * (1.0/ctrl_n + 1.0/trt_n))) > 1.960
      THEN '✅ Significant at 95%'
    WHEN ABS(delta / SQRT(p_pool * (1 - p_pool) * (1.0/ctrl_n + 1.0/trt_n))) > 1.645
      THEN '⚠️  Significant at 90%'
    ELSE '❌ Not Significant'
  END AS significance
FROM z_calc;
```

---

## 3. Dynamic Pivot Tables

Transform rows into columns for crosstab reporting.

### CROSSTAB (PostgreSQL `tablefunc`)

```sql
-- Enable the extension once per database
CREATE EXTENSION IF NOT EXISTS tablefunc;

-- Monthly revenue by product category (rows → columns)
SELECT * FROM CROSSTAB(
  $$
  SELECT
    DATE_TRUNC('month', order_date)::date AS month,
    category,
    SUM(revenue)::numeric AS revenue
  FROM sales
  INNER JOIN products USING(product_id)
  WHERE EXTRACT(YEAR FROM order_date) = 2024
  GROUP BY 1, 2
  ORDER BY 1, 2
  $$,
  $$ VALUES ('Electronics'), ('Apparel'), ('Home'), ('Sports') $$
) AS ct(
  month         DATE,
  electronics   NUMERIC,
  apparel       NUMERIC,
  home          NUMERIC,
  sports        NUMERIC
);
```

### Manual Pivot with CASE (Works Everywhere)

```sql
-- Pivot: region as rows, year as columns
SELECT
  region,
  SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2022 THEN revenue ELSE 0 END) AS "2022",
  SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2023 THEN revenue ELSE 0 END) AS "2023",
  SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2024 THEN revenue ELSE 0 END) AS "2024",
  SUM(revenue) AS total,
  ROUND(
    100.0 * (
      SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2024 THEN revenue ELSE 0 END) -
      SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2023 THEN revenue ELSE 0 END)
    ) / NULLIF(
      SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2023 THEN revenue ELSE 0 END), 0
    ),
    1
  ) AS yoy_growth_pct
FROM sales
GROUP BY region
ORDER BY "2024" DESC;
```

---

## 4. Graph & Hierarchy Traversal

### Find All Paths Between Nodes

```sql
-- Find all referral chains (who referred whom, all levels)
WITH RECURSIVE referral_chain AS (
  SELECT
    referrer_id AS root,
    referred_id AS node,
    1 AS depth,
    ARRAY[referrer_id, referred_id] AS path
  FROM referrals
  WHERE referrer_id = 'USER_001'

  UNION ALL

  SELECT
    rc.root,
    r.referred_id,
    rc.depth + 1,
    rc.path || r.referred_id
  FROM referrals AS r
  INNER JOIN referral_chain AS rc ON r.referrer_id = rc.node
  WHERE rc.depth < 5  -- max 5 levels deep
    AND r.referred_id != ALL(rc.path)  -- prevent cycles
)
SELECT node, depth, array_to_string(path, ' → ') AS full_path
FROM referral_chain
ORDER BY depth, node;
```

### Category Hierarchy Rollup

```sql
-- Roll up sales from leaf categories to parent categories
WITH RECURSIVE category_rollup AS (
  -- Start with leaf-level sales
  SELECT c.id, c.name, c.parent_id, s.revenue, 0 AS depth
  FROM categories AS c
  INNER JOIN sales AS s ON c.id = s.category_id

  UNION ALL

  -- Bubble revenue up to parents
  SELECT c.id, c.name, c.parent_id, cr.revenue, cr.depth + 1
  FROM categories AS c
  INNER JOIN category_rollup AS cr ON cr.parent_id = c.id
)
SELECT name, SUM(revenue) AS total_revenue, MAX(depth) AS max_depth
FROM category_rollup
GROUP BY id, name
ORDER BY total_revenue DESC;
```

---

## 5. Advanced String & JSON Analytics

### JSON Unnesting & Analysis

```sql
-- Extract fields from JSON column
SELECT
  order_id,
  metadata->>'source'           AS acquisition_source,
  metadata->>'coupon_code'      AS coupon_used,
  (metadata->>'discount')::numeric AS discount_pct,
  jsonb_array_length(metadata->'tags') AS tag_count
FROM orders
WHERE metadata IS NOT NULL;

-- Explode JSON array into rows
SELECT
  order_id,
  jsonb_array_elements_text(metadata->'tags') AS tag
FROM orders;

-- Aggregate back to JSON
SELECT
  region,
  jsonb_build_object(
    'total_revenue', SUM(revenue),
    'order_count', COUNT(*),
    'avg_order_value', ROUND(AVG(revenue), 2)
  ) AS metrics
FROM orders
GROUP BY region;
```

### Text Analytics

```sql
-- Find most common words in product descriptions
WITH words AS (
  SELECT regexp_split_to_table(
    LOWER(REGEXP_REPLACE(description, '[^a-zA-Z ]', '', 'g')),
    '\s+'
  ) AS word
  FROM products
  WHERE LENGTH(description) > 0
)
SELECT word, COUNT(*) AS frequency
FROM words
WHERE LENGTH(word) > 3
  AND word NOT IN ('with', 'that', 'this', 'from', 'have', 'been', 'will', 'into')
GROUP BY word
ORDER BY frequency DESC
LIMIT 30;

-- Full-text search ranking
SELECT
  product_id,
  product_name,
  description,
  ts_rank(
    to_tsvector('english', product_name || ' ' || COALESCE(description, '')),
    to_tsquery('english', 'wireless & headphones')
  ) AS relevance_score
FROM products
WHERE to_tsvector('english', product_name || ' ' || COALESCE(description, ''))
      @@ to_tsquery('english', 'wireless & headphones')
ORDER BY relevance_score DESC
LIMIT 20;
```

---

## 6. Query Performance & Optimization

### Diagnosing Slow Queries

```sql
-- Analyze a query's execution plan
EXPLAIN ANALYZE
SELECT *
FROM orders AS o
INNER JOIN customers AS c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01';

-- Key things to look for:
-- Seq Scan = full table scan (consider adding index)
-- Nested Loop with large rows = may need HASH JOIN
-- High "actual time" rows = bottleneck
-- "rows=X" vs "actual rows=Y" — large difference = stale statistics

-- Update statistics (run after large data loads)
ANALYZE orders;
ANALYZE customers;
```

### Index Strategy

```sql
-- Single column index
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);

-- Composite index (order matters — most selective first)
CREATE INDEX idx_orders_customer_date ON orders(customer_id, order_date);

-- Partial index (index only the rows you query)
CREATE INDEX idx_active_orders ON orders(customer_id)
WHERE status = 'active';

-- Expression index
CREATE INDEX idx_orders_year ON orders(EXTRACT(YEAR FROM order_date));

-- Covering index (avoids heap fetch entirely)
CREATE INDEX idx_orders_covering ON orders(customer_id, order_date)
INCLUDE (total_amount, status);
```

### Rewriting for Performance

```sql
-- SLOW: Correlated subquery executes once per row
SELECT *
FROM products AS p
WHERE price > (SELECT AVG(price) FROM products WHERE category = p.category);

-- FAST: Window function executes once
SELECT * FROM (
  SELECT *, AVG(price) OVER(PARTITION BY category) AS cat_avg_price
  FROM products
) AS sub
WHERE price > cat_avg_price;

-- SLOW: IN with large subquery
SELECT * FROM orders
WHERE customer_id IN (SELECT customer_id FROM vip_customers);

-- FAST: EXISTS (stops scanning after first match)
SELECT * FROM orders AS o
WHERE EXISTS (SELECT 1 FROM vip_customers AS v WHERE v.customer_id = o.customer_id);

-- SLOW: OR conditions (can't use index efficiently)
SELECT * FROM customers WHERE city = 'NYC' OR city = 'LA';

-- FAST: IN with index
SELECT * FROM customers WHERE city IN ('NYC', 'LA');

-- Avoid functions on indexed columns in WHERE
-- SLOW (can't use index on order_date):
WHERE EXTRACT(YEAR FROM order_date) = 2024
-- FAST (can use range scan):
WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01'
```

---

## 7. Data Quality Checks

Automated checks to monitor data pipeline health.

```sql
-- Comprehensive data quality report
WITH checks AS (
  SELECT 'orders' AS table_name, 'row_count' AS check_name,
    COUNT(*) AS value, NULL AS expected, COUNT(*) > 0 AS passed
  FROM orders WHERE order_date = CURRENT_DATE - 1

  UNION ALL SELECT 'orders', 'null_customer_id',
    COUNT(*) FILTER (WHERE customer_id IS NULL), 0,
    COUNT(*) FILTER (WHERE customer_id IS NULL) = 0
  FROM orders

  UNION ALL SELECT 'orders', 'negative_amounts',
    COUNT(*) FILTER (WHERE total_amount < 0), 0,
    COUNT(*) FILTER (WHERE total_amount < 0) = 0
  FROM orders

  UNION ALL SELECT 'orders', 'future_dates',
    COUNT(*) FILTER (WHERE order_date > CURRENT_DATE), 0,
    COUNT(*) FILTER (WHERE order_date > CURRENT_DATE) = 0
  FROM orders

  UNION ALL SELECT 'orders', 'duplicate_order_ids',
    COUNT(*) - COUNT(DISTINCT order_id), 0,
    COUNT(*) = COUNT(DISTINCT order_id)
  FROM orders
)
SELECT
  table_name,
  check_name,
  value,
  CASE WHEN passed THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM checks
ORDER BY passed ASC, table_name, check_name;
```

---

## 8. Expert Pattern Library

### Session Analysis (Sessionize Raw Events)

```sql
-- Create sessions from raw events (30-min inactivity = new session)
WITH events_with_gaps AS (
  SELECT
    user_id,
    event_ts,
    LAG(event_ts) OVER(PARTITION BY user_id ORDER BY event_ts) AS prev_event_ts,
    CASE
      WHEN event_ts - LAG(event_ts) OVER(PARTITION BY user_id ORDER BY event_ts)
           > INTERVAL '30 minutes'
        OR LAG(event_ts) OVER(PARTITION BY user_id ORDER BY event_ts) IS NULL
      THEN 1 ELSE 0
    END AS is_new_session
  FROM raw_events
),
sessions AS (
  SELECT
    user_id,
    event_ts,
    SUM(is_new_session) OVER(PARTITION BY user_id ORDER BY event_ts) AS session_id
  FROM events_with_gaps
)
SELECT
  user_id,
  session_id,
  MIN(event_ts) AS session_start,
  MAX(event_ts) AS session_end,
  COUNT(*) AS events_in_session,
  MAX(event_ts) - MIN(event_ts) AS session_duration
FROM sessions
GROUP BY user_id, session_id
ORDER BY user_id, session_start;
```

### Slowly Changing Dimensions (Type 2 History)

```sql
-- Find what a customer's plan was at the time of each purchase
SELECT
  o.order_id,
  o.customer_id,
  o.order_date,
  p.plan_name AS plan_at_purchase,
  p.plan_price
FROM orders AS o
INNER JOIN customer_plan_history AS p
  ON o.customer_id = p.customer_id
  AND o.order_date >= p.effective_from
  AND o.order_date <  COALESCE(p.effective_to, '9999-12-31')  -- open-ended records
ORDER BY o.customer_id, o.order_date;
```

### Outlier Detection with IQR

```sql
WITH stats AS (
  SELECT
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY order_value) AS q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY order_value) AS q3
  FROM orders
),
bounds AS (
  SELECT
    q1,
    q3,
    q1 - 1.5 * (q3 - q1) AS lower_fence,
    q3 + 1.5 * (q3 - q1) AS upper_fence
  FROM stats
)
SELECT
  o.order_id,
  o.order_value,
  b.lower_fence,
  b.upper_fence,
  CASE
    WHEN o.order_value < b.lower_fence THEN '⬇ Low Outlier'
    WHEN o.order_value > b.upper_fence THEN '⬆ High Outlier'
    ELSE 'Normal'
  END AS outlier_status
FROM orders AS o
CROSS JOIN bounds AS b
ORDER BY order_value DESC;
```

### Attribution Modeling (Last-Touch)

```sql
-- Attribute each conversion to the last marketing touchpoint
WITH touchpoints AS (
  SELECT
    user_id,
    channel,
    event_ts,
    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY event_ts DESC) AS recency_rank
  FROM marketing_events
  WHERE event_type = 'touchpoint'
),
last_touch AS (
  SELECT user_id, channel AS attributed_channel
  FROM touchpoints
  WHERE recency_rank = 1
)
SELECT
  lt.attributed_channel,
  COUNT(DISTINCT c.conversion_id) AS conversions,
  SUM(c.revenue) AS attributed_revenue,
  ROUND(AVG(c.revenue), 2) AS avg_conversion_value
FROM conversions AS c
INNER JOIN last_touch AS lt ON c.user_id = lt.user_id
GROUP BY lt.attributed_channel
ORDER BY attributed_revenue DESC;
```

---

## 📌 Expert Quick Reference

| Pattern | Key Technique |
|---------|--------------|
| Org chart traversal | Recursive CTE + `depth` counter |
| AB significance | Z-test with `SQRT(pooled_p * (1-p))` |
| Pivot table | `CASE WHEN month = X` or `CROSSTAB()` |
| Session detection | LAG + 30min gap + `SUM() OVER` |
| Slowly changing dims | Join on date range `BETWEEN effective_from AND effective_to` |
| Outlier detection | IQR fences with `PERCENTILE_CONT` |
| Attribution | `ROW_NUMBER() OVER(PARTITION BY user ORDER BY ts DESC)` |
| Query tuning | `EXPLAIN ANALYZE` + index on JOIN/WHERE columns |
| Data quality | `UNION ALL` of assertion checks |
| Text search | `to_tsvector()` + `to_tsquery()` |

---

## 🎓 Full Learning Path Summary

| Level | File | Focus |
|-------|------|-------|
| 1 — Beginner | `README_01_Beginner_SQL_DA.md` | SELECT, WHERE, GROUP BY, basic JOINs |
| 2 — Intermediate | `README_02_Intermediate_Aggregations.md` | CTEs, subqueries, ROLLUP, conditional agg |
| 3 — Window Functions | `README_03_Window_Functions.md` | Rankings, moving averages, LAG/LEAD |
| 4 — Advanced Analytics | `README_04_Advanced_Analytics.md` | Cohorts, funnels, RFM, retention, stats |
| 5 — Expert | `README_05_Expert_Patterns.md` | Recursion, AB testing, pivots, optimization |

---

*← Previous: README_04_Advanced_Analytics.md*
