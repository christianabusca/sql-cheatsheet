# 📊 SQL for Data Analysis — Master Summary
## Complete Reference Across All 8 Cheat Sheets
> *From SELECT to statistical modeling — every pattern, formula, and concept in one place*

---

## 📚 Series Index

| # | File | Level | Focus Area |
|---|------|-------|-----------|
| 01 | `01_Beginner_SQL_DA.md` | 🟢 Beginner | Filtering, aggregations, GROUP BY, JOINs, CASE |
| 02 | `02_Intermediate_Aggregations.md` | 🟡 Intermediate | CTEs, subqueries, ROLLUP, conditional agg, set ops |
| 03 | `03_Window_Functions.md` | 🟠 Intermediate+ | Rankings, running totals, moving averages, LAG/LEAD |
| 04 | `04_Advanced_Analytics.md` | 🔴 Advanced | Cohorts, funnels, RFM, retention, MRR, statistics |
| 05 | `05_Expert_Patterns.md` | 🔥 Expert | Recursion, AB testing, pivots, optimization |
| 06 | `06_Period_Comparisons_YoY_MoM.md` | 🔴 Advanced | YoY, MoM, WoW, DoD, SPLY, CAGR, indexed growth |
| 07 | `07_Moving_Rolling_Computations.md` | 🔴 Advanced | SMA, EMA, rolling stddev, Bollinger, momentum |
| 08 | `08_Essential_DA_Computations.md` | 🟠 Intermediate+ | LTV, market basket, Pareto, dedup, pivot, NPS |

---

## Table of Contents
- [Part 1 — Foundations (README 01)](#part-1--foundations)
- [Part 2 — Intermediate Aggregations (README 02)](#part-2--intermediate-aggregations)
- [Part 3 — Window Functions (README 03)](#part-3--window-functions)
- [Part 4 — Advanced Business Analytics (README 04)](#part-4--advanced-business-analytics)
- [Part 5 — Expert Patterns (README 05)](#part-5--expert-patterns)
- [Part 6 — Period-over-Period Comparisons (README 06)](#part-6--period-over-period-comparisons)
- [Part 7 — Moving & Rolling Computations (README 07)](#part-7--moving--rolling-computations)
- [Part 8 — Essential DA Computations (README 08)](#part-8--essential-da-computations)
- [Master Quick Reference Tables](#master-quick-reference-tables)
- [The Universal DA Query Template](#the-universal-da-query-template)

---

## Part 1 — Foundations
> 📄 `README_01_Beginner_SQL_DA.md`

The building blocks every analyst needs before anything else.

### SQL Execution Order
SQL processes clauses in this order — not the order you write them:
```
FROM → JOIN → WHERE → GROUP BY → HAVING → SELECT → DISTINCT → ORDER BY → LIMIT
```

### Filtering & Sorting
```sql
WHERE col = 'value'                          -- exact match
WHERE col IN ('a', 'b', 'c')                 -- list match
WHERE col NOT IN ('x', 'y')                  -- exclusion
WHERE col BETWEEN 10 AND 100                 -- range (inclusive)
WHERE col LIKE '%pattern%'                   -- pattern search
WHERE col IS NULL / IS NOT NULL              -- null checks
ORDER BY col DESC                            -- sort descending
LIMIT 10                                     -- cap rows returned
```

### Core Aggregation Functions
```sql
COUNT(*)                    -- total row count
COUNT(col)                  -- non-NULL count
COUNT(DISTINCT col)         -- unique value count
SUM(col)                    -- total
AVG(col)                    -- mean
MIN(col) / MAX(col)         -- extremes
ROUND(AVG(col), 2)          -- rounded average
```

### Grouping & Filtering Groups
```sql
GROUP BY category                            -- segment data
HAVING COUNT(*) > 10                         -- filter after aggregation
-- Rule: WHERE filters rows, HAVING filters groups
```

### CASE — Conditional Logic
```sql
CASE
  WHEN col < 50   THEN 'Low'
  WHEN col < 200  THEN 'Medium'
  ELSE 'High'
END AS tier

-- Conditional COUNT (pivot pattern)
COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed_count
```

### NULL Handling
```sql
COALESCE(col, 0)            -- replace NULL with default
NULLIF(col, 0)              -- return NULL if col = 0 (prevents ÷ 0)
```

### Join Types at a Glance
```sql
INNER JOIN   -- only matching rows in both tables
LEFT JOIN    -- all rows from left, NULLs where no match on right
FULL JOIN    -- all rows from both, NULLs where no match
CROSS JOIN   -- every combination (Cartesian product)
SELF JOIN    -- join table to itself for comparisons
```

---

## Part 2 — Intermediate Aggregations
> 📄 `README_02_Intermediate_Aggregations.md`

Multi-step analysis, reusable query logic, and advanced grouping.

### Subquery Locations
```sql
-- WHERE subquery: filter by a dynamic value
WHERE price > (SELECT AVG(price) FROM products)

-- FROM subquery: query pre-aggregated data (must be aliased)
SELECT AVG(team_avg) FROM (SELECT AVG(goals) AS team_avg FROM matches GROUP BY team) AS t

-- EXISTS: faster than IN for existence checks on large tables
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)
```

### Common Table Expressions (CTEs)
```sql
WITH step_one AS (
  SELECT customer_id, SUM(total_amount) AS ltv FROM orders GROUP BY customer_id
),
step_two AS (
  SELECT *, NTILE(4) OVER(ORDER BY ltv DESC) AS quartile FROM step_one
)
SELECT * FROM step_two WHERE quartile = 1;
-- ✅ Prefer CTEs over nested subqueries for readability and debugging
```

### Hierarchical Aggregations
```sql
-- ROLLUP: subtotals at each hierarchy level
GROUP BY ROLLUP(region, category)
-- Produces: region+category rows, region subtotals, grand total

-- CUBE: every possible combination of subtotals
GROUP BY CUBE(region, category, channel)

-- COALESCE on ROLLUP/CUBE results for readable labels
COALESCE(region, 'All Regions') AS region
```

### Conditional Aggregation (Pivot in SQL)
```sql
-- Revenue by quarter in one query
SUM(CASE WHEN EXTRACT(QUARTER FROM dt) = 1 THEN amount ELSE 0 END) AS Q1,
SUM(CASE WHEN EXTRACT(QUARTER FROM dt) = 2 THEN amount ELSE 0 END) AS Q2,
SUM(CASE WHEN EXTRACT(QUARTER FROM dt) = 3 THEN amount ELSE 0 END) AS Q3,
SUM(CASE WHEN EXTRACT(QUARTER FROM dt) = 4 THEN amount ELSE 0 END) AS Q4
```

### Set Operations
```sql
UNION       -- combine + remove duplicates (slower)
UNION ALL   -- combine + keep duplicates (faster, preferred)
INTERSECT   -- rows that exist in BOTH queries
EXCEPT      -- rows in first query but NOT in second
```

---

## Part 3 — Window Functions
> 📄 `README_03_Window_Functions.md`

Calculations across rows without collapsing them — the most powerful DA tool in SQL.

### Window Function Syntax
```sql
function() OVER (
  PARTITION BY group_col     -- separate calculation per group (optional)
  ORDER BY time_col          -- row sequence (optional)
  ROWS BETWEEN n PRECEDING   -- frame definition (optional)
       AND CURRENT ROW
)
```

### Ranking Functions
```sql
ROW_NUMBER() OVER(ORDER BY val DESC)   -- 1,2,3,4,5       (always unique)
RANK()       OVER(ORDER BY val DESC)   -- 1,2,2,4,5       (gaps on ties)
DENSE_RANK() OVER(ORDER BY val DESC)   -- 1,2,2,3,4       (no gaps)
NTILE(4)     OVER(ORDER BY val DESC)   -- quartile bucket  (1=top 25%)
PERCENT_RANK() OVER(ORDER BY val)      -- 0.0 to 1.0 percentile
```

### Running / Cumulative Calculations
```sql
SUM(amount)  OVER(ORDER BY date)  -- running total
MAX(value)   OVER(ORDER BY date)  -- all-time high watermark
AVG(amount)  OVER(PARTITION BY region ORDER BY date)  -- running avg per region
```

### Frame Clause Reference
```sql
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW   -- running total
ROWS BETWEEN 6 PRECEDING AND CURRENT ROW           -- 7-row sliding window
ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING           -- centered 7-row window
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  -- whole partition
```

### LAG & LEAD — Access Other Rows
```sql
LAG(col, 1)  OVER(ORDER BY date)  -- previous row value
LEAD(col, 1) OVER(ORDER BY date)  -- next row value
LAG(col, 12) OVER(ORDER BY date)  -- same month last year (monthly data)
LAG(col, 1, 0)                    -- default value 0 if NULL
```

### Moving Average
```sql
-- N-day SMA: ROWS BETWEEN (N-1) PRECEDING AND CURRENT ROW
AVG(revenue) OVER(ORDER BY date ROWS BETWEEN 6  PRECEDING AND CURRENT ROW) AS sma_7
AVG(revenue) OVER(ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sma_30
AVG(revenue) OVER(ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS sma_90
```

### Top N Per Group Pattern
```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY category ORDER BY revenue DESC) AS rn
  FROM sales
) ranked WHERE rn <= 3;
```

---

## Part 4 — Advanced Business Analytics
> 📄 `README_04_Advanced_Analytics.md`

Core frameworks used in product analytics, growth analysis, and business intelligence.

### Cohort Analysis (Retention Table)
```sql
-- 1. Assign cohort month (first order date)
-- 2. Calculate period_number = months since cohort
-- 3. Count active customers per cohort × period
-- 4. Divide by cohort_size → retention_rate_pct

-- Key formula:
EXTRACT(YEAR FROM AGE(order_month, cohort_month)) * 12 +
EXTRACT(MONTH FROM AGE(order_month, cohort_month)) AS period_number
```

### Funnel Analysis
```sql
-- Count distinct sessions/users at each step
COUNT(DISTINCT CASE WHEN event = 'page_view'   THEN session_id END) AS step1,
COUNT(DISTINCT CASE WHEN event = 'add_to_cart' THEN session_id END) AS step2,
COUNT(DISTINCT CASE WHEN event = 'purchase'    THEN session_id END) AS step3
-- Conversion rate = step2 / step1 × 100
```

### RFM Segmentation
```sql
-- Score customers 1-5 on each dimension
NTILE(5) OVER(ORDER BY days_since_last ASC)  AS r_score  -- Recency
NTILE(5) OVER(ORDER BY order_count DESC)     AS f_score  -- Frequency
NTILE(5) OVER(ORDER BY total_spent DESC)     AS m_score  -- Monetary

-- Combine: r+f+m ≥ 12 → Champions; r=1,f=1 → Lost
```

### Retention & Churn
```sql
-- MoM retention: self-join monthly active users
LEFT JOIN monthly_active m2
  ON m1.customer_id = m2.customer_id
  AND m2.month = m1.month + INTERVAL '1 month'

retention_rate = retained_next_month / base_users × 100
churn_rate     = 100 - retention_rate
```

### Revenue Metrics
```sql
-- MRR: sum of all active subscriptions' monthly amounts
-- ARR: MRR × 12
-- NRR (Net Revenue Retention): ending_mrr / starting_mrr × 100
--   NRR > 100% = expansion exceeds churn ✅

-- LTV (simple): avg_order_value × purchase_frequency × avg_customer_lifespan
```

### Statistical Functions
```sql
PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY val)  -- median
PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY val)  -- p95
STDDEV(val)                                         -- standard deviation
CORR(x, y)                                          -- Pearson correlation
REGR_R2(y, x)                                       -- R-squared
REGR_SLOPE(y, x)                                    -- linear regression slope
```

---

## Part 5 — Expert Patterns
> 📄 `README_05_Expert_Patterns.md`

Advanced patterns for complex recursive, statistical, and optimization problems.

### Recursive CTEs
```sql
WITH RECURSIVE tree AS (
  SELECT id, name, parent_id, 0 AS depth FROM table WHERE parent_id IS NULL  -- anchor
  UNION ALL
  SELECT t.id, t.name, t.parent_id, r.depth + 1
  FROM table t INNER JOIN tree r ON t.parent_id = r.id   -- recursive step
)
SELECT * FROM tree;
-- Use for: org charts, bill-of-materials, date spines, category hierarchies
```

### AB Test Statistical Significance (Z-Test)
```sql
-- Z-score formula:
z = (p_treatment - p_control)
    / SQRT(p_pool × (1 - p_pool) × (1/n_ctrl + 1/n_trt))

-- p_pool = (ctrl_conversions + trt_conversions) / (ctrl_n + trt_n)
-- |z| > 1.960 → 95% significance ✅
-- |z| > 2.576 → 99% significance ✅✅
```

### Dynamic Pivot (CROSSTAB)
```sql
CREATE EXTENSION IF NOT EXISTS tablefunc;
SELECT * FROM CROSSTAB('SELECT row, col, val FROM data ORDER BY 1,2')
  AS ct(row TEXT, col1 NUMERIC, col2 NUMERIC);
```

### Session Detection
```sql
-- New session = gap > 30 minutes from previous event
CASE WHEN event_ts - LAG(event_ts) OVER(PARTITION BY user_id ORDER BY event_ts)
          > INTERVAL '30 minutes' THEN 1 ELSE 0 END AS is_new_session
-- Then: SUM(is_new_session) OVER(...) = session_id
```

### Query Optimization Rules
```sql
-- 1. EXPLAIN ANALYZE before optimizing
-- 2. Index JOIN, WHERE, ORDER BY columns
-- 3. Use EXISTS instead of IN for large subqueries
-- 4. Avoid functions on indexed columns in WHERE
-- 5. Filter early with WHERE before expensive operations
-- 6. UNION ALL is faster than UNION (no deduplication)
-- 7. Window functions beat correlated subqueries
-- 8. Partial indexes for frequently filtered subsets
```

---

## Part 6 — Period-over-Period Comparisons
> 📄 `README_06_Period_Comparisons_YoY_MoM.md`

Every time-period comparison pattern used in reporting and dashboards.

### Universal Pattern
```sql
-- Step 1: aggregate to the right grain (daily/weekly/monthly)
-- Step 2: apply LAG with correct offset
-- Step 3: compute absolute change and % change

current_value - LAG(current_value, N) OVER(ORDER BY period) AS abs_change,
ROUND(
  100.0 * (current_value - LAG(current_value, N) OVER(ORDER BY period))
  / NULLIF(LAG(current_value, N) OVER(ORDER BY period), 0),
  2
) AS pct_change
```

### LAG Offset Reference
| Comparison | Data Grain | LAG Offset |
|-----------|-----------|-----------|
| Day-over-Day (DoD) | Daily | `LAG(val, 1)` |
| Week-over-Week (WoW) | Weekly | `LAG(val, 1)` |
| Month-over-Month (MoM) | Monthly | `LAG(val, 1)` |
| Quarter-over-Quarter (QoQ) | Quarterly | `LAG(val, 1)` |
| Year-over-Year (YoY) | Yearly | `LAG(val, 1)` |
| Same Month Last Year (SPLY) | Monthly | `LAG(val, 12)` |
| Same Week Last Year | Weekly | `LAG(val, 52)` |
| Same Day Last Year | Daily | `LAG(val, 365)` |

### CAGR Formula
```sql
-- Compound Annual Growth Rate
ROUND(
  (POWER(end_revenue / NULLIF(start_revenue, 0), 1.0 / NULLIF(years, 0)) - 1) * 100,
  2
) AS cagr_pct
```

### Indexed Growth (Normalize to Base 100)
```sql
ROUND(100.0 * current_value / FIRST_VALUE(current_value) OVER(PARTITION BY series ORDER BY date), 1)
AS index_value
-- All series start at 100 → compare growth rates regardless of scale
```

### YTD Running Total (Resets Each Year)
```sql
SUM(revenue) OVER(
  PARTITION BY EXTRACT(YEAR FROM date)
  ORDER BY date
) AS ytd_revenue
```

### Gap-Safe Comparison (Date Spine Required)
```sql
-- If your data has missing dates/months, LAG gives wrong results
-- Solution: left join actuals to a complete date spine first, fill gaps with 0
LEFT JOIN monthly_actuals USING(month)
COALESCE(revenue, 0) AS revenue  -- fill missing periods
```

---

## Part 7 — Moving & Rolling Computations
> 📄 `README_07_Moving_Rolling_Computations.md`

Every sliding-window calculation for time-series smoothing and signal detection.

### Simple Moving Average (SMA) Formula
```sql
AVG(value) OVER(
  ORDER BY date
  ROWS BETWEEN (N - 1) PRECEDING AND CURRENT ROW
)
-- 7-day:  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
-- 30-day: ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
-- 90-day: ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
```

### Exponential Moving Average (EMA)
```sql
-- Uses recursive CTE; α = 2/(N+1)
-- α = 0.1818 for 10-period EMA
EMA(today) = α × price(today) + (1 − α) × EMA(yesterday)
-- More responsive to recent data than SMA
```

### Rolling Statistics
```sql
-- Rolling standard deviation (volatility)
STDDEV(value) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)

-- Z-score (how many stddevs is today from rolling mean?)
(value - AVG(value) OVER(window)) / NULLIF(STDDEV(value) OVER(window), 0)
-- Z > 2 = anomaly flag, Z > 3 = extreme outlier

-- Coefficient of variation (normalized volatility)
STDDEV(v) OVER(window) / NULLIF(AVG(v) OVER(window), 0)
```

### Bollinger Bands (20-day)
```sql
AVG(price) OVER(20-row window)                         AS bb_middle
AVG(price) OVER(...) + 2 * STDDEV(price) OVER(...)    AS bb_upper
AVG(price) OVER(...) - 2 * STDDEV(price) OVER(...)    AS bb_lower
-- Above upper band = unusually high; below lower = unusually low
```

### Momentum & Rate of Change (ROC)
```sql
-- Momentum: current minus N periods ago
value - LAG(value, N) OVER(ORDER BY date) AS momentum

-- ROC: % change vs N periods ago
100.0 * (value - LAG(value, N) OVER(ORDER BY date))
        / NULLIF(LAG(value, N) OVER(ORDER BY date), 0) AS roc_pct
```

### Running Extremes
```sql
-- All-time high watermark
MAX(value) OVER(ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- % drawdown from all-time high
100.0 * (value - MAX(value) OVER(... UNBOUNDED PRECEDING ...))
        / NULLIF(MAX(value) OVER(... UNBOUNDED PRECEDING ...), 0)
```

### ROWS vs RANGE
```sql
ROWS  -- physical row count (use this for most cases)
RANGE -- logical value range (use for date gaps with INTERVAL)
-- Example: RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW
```

---

## Part 8 — Essential DA Computations
> 📄 `README_08_Essential_DA_Computations.md`

The complete toolkit of business-critical analytical formulas.

### Share of Total & Contribution
```sql
-- Share of grand total
100.0 * SUM(revenue) / SUM(SUM(revenue)) OVER()

-- Share within group
100.0 * SUM(revenue) / SUM(SUM(revenue)) OVER(PARTITION BY category)

-- Cumulative running share
SUM(share_pct) OVER(ORDER BY revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
```

### Customer Lifetime Value (LTV)
```sql
-- Historic LTV
SUM(total_amount) per customer

-- Predicted LTV
avg_order_value
  × (order_count / customer_lifespan_days)   -- daily purchase rate
  × 365                                       -- project to 1 year
  × gross_margin_pct                          -- account for costs

-- Risk-adjusted LTV
× 0.20 if churned (180d+ inactive)
× 0.60 if at-risk (90-180d inactive)
× 1.00 if active (< 90d)
```

### Market Basket (Association Rules)
```sql
-- Support    = co_occurrences / total_orders × 100
-- Confidence = co_occurrences / orders_with_A × 100  (A → B probability)
-- Lift       = (co_occ / A_count) / (B_count / total)
-- Lift > 1 = positive association, = 1 = independent, < 1 = negative
```

### Deduplication
```sql
-- Keep most recent per key
ROW_NUMBER() OVER(PARTITION BY key_col ORDER BY updated_at DESC) AS rn
→ WHERE rn = 1

-- Keep earliest per key
ROW_NUMBER() OVER(PARTITION BY email ORDER BY created_at ASC) AS rn
→ WHERE rn = 1
```

### Pareto / ABC Classification
```sql
-- Cumulative revenue share
SUM(revenue) OVER(ORDER BY revenue DESC) / SUM(revenue) OVER() AS cum_share
-- A = cum_share ≤ 80%  (vital few)
-- B = cum_share ≤ 95%  (important middle)
-- C = cum_share > 95%  (trivial many)
```

### Net Promoter Score (NPS)
```sql
NPS = % Promoters (score 9–10) − % Detractors (score 0–6)
-- Passives (7–8) are excluded from calculation
-- Range: −100 to +100; above 50 = excellent
```

### Weighted Average
```sql
SUM(price × units_sold) / NULLIF(SUM(units_sold), 0)
-- vs simple AVG(price) which ignores sales volume
```

### Fill Forward (Last Observation Carried Forward)
```sql
FIRST_VALUE(col) OVER(
  PARTITION BY entity_id, count_group
  ORDER BY date
)
-- count_group = COUNT(col) OVER(... ORDER BY date) — increments only on non-NULL
```

### Gap & Island Detection
```sql
-- Islands (consecutive sequences): subtract ROW_NUMBER from date → same value = same island
-- Gaps (missing dates): full date spine LEFT JOIN actuals → WHERE actuals IS NULL
```

### Date Arithmetic
```sql
CURRENT_DATE - order_date::date              -- days since order
order_date + INTERVAL '30 days'              -- 30 days from now
DATE_TRUNC('month', date)                    -- first of month
EXTRACT(DOW FROM date)                       -- 0=Sunday, 6=Saturday
AGE(timestamp1, timestamp2)                  -- human-readable interval
```

---

## Master Quick Reference Tables

### All Aggregation Functions
| Function | Description | NULL Behavior |
|----------|------------|--------------|
| `COUNT(*)` | Total rows | Counts NULLs |
| `COUNT(col)` | Non-null count | Ignores NULLs |
| `COUNT(DISTINCT col)` | Unique values | Ignores NULLs |
| `SUM(col)` | Total | Ignores NULLs |
| `AVG(col)` | Mean | Ignores NULLs |
| `MIN(col)` | Smallest | Ignores NULLs |
| `MAX(col)` | Largest | Ignores NULLs |
| `STDDEV(col)` | Standard deviation | Ignores NULLs |
| `VARIANCE(col)` | Variance | Ignores NULLs |
| `PERCENTILE_CONT(p)` | Continuous percentile | — |
| `CORR(x,y)` | Pearson correlation | — |
| `STRING_AGG(col, sep)` | Concatenate rows | Ignores NULLs |

### All Window Functions
| Function | Category | Description |
|----------|---------|------------|
| `ROW_NUMBER()` | Ranking | Unique sequential number |
| `RANK()` | Ranking | Rank with gaps on ties |
| `DENSE_RANK()` | Ranking | Rank without gaps |
| `NTILE(n)` | Ranking | Split into n buckets |
| `PERCENT_RANK()` | Ranking | 0 to 1 percentile |
| `CUME_DIST()` | Ranking | Cumulative distribution |
| `LAG(col, n)` | Navigation | Value n rows before |
| `LEAD(col, n)` | Navigation | Value n rows after |
| `FIRST_VALUE(col)` | Navigation | First in window |
| `LAST_VALUE(col)` | Navigation | Last in window |
| `NTH_VALUE(col, n)` | Navigation | Nth in window |
| `SUM() OVER(ORDER BY)` | Aggregate | Running total |
| `AVG() OVER(ROWS n)` | Aggregate | Moving average |
| `MAX() OVER(UNBOUNDED)` | Aggregate | Running maximum |
| `STDDEV() OVER(ROWS n)` | Aggregate | Rolling volatility |

### JOIN Decision Guide
| JOIN Type | When to Use | Returns |
|----------|-----------|--------|
| `INNER JOIN` | Need only matching rows | Intersection |
| `LEFT JOIN` | Keep all left rows | Left + matched right |
| `RIGHT JOIN` | Keep all right rows | Right + matched left |
| `FULL JOIN` | Keep all rows both sides | Union of both |
| `CROSS JOIN` | All combinations | Cartesian product |
| `SELF JOIN` | Compare rows in same table | Varies |
| `LEFT JOIN ... WHERE right IS NULL` | Anti-join: unmatched only | Left minus right |

### Subquery Location Guide
| Location | Use For | Returns |
|---------|---------|--------|
| `WHERE (subquery)` | Dynamic filter values | Single value or list |
| `WHERE EXISTS (subquery)` | Existence check (fast) | Boolean |
| `FROM (subquery) AS t` | Pre-aggregated data | Table |
| `SELECT (subquery)` | Per-row calculation | Single value per row |
| `WITH name AS (...)` | Reusable named result | Table |

### Period Comparison Formulas
| Metric | Formula |
|--------|--------|
| Absolute change | `current - prior` |
| % change | `100 × (current - prior) / NULLIF(prior, 0)` |
| CAGR | `POWER(end/start, 1/years) - 1` × 100 |
| Indexed growth | `100 × value / FIRST_VALUE(value) OVER(...)` |
| Rolling 30d vs prior 30d | `ROWS BETWEEN 29 PRECEDING AND CURRENT ROW` vs `ROWS BETWEEN 59 PRECEDING AND 30 PRECEDING` |

### Moving Window Formulas
| Computation | Window Size | SQL |
|------------|------------|-----|
| 7-day SMA | 7 rows | `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` |
| 30-day SMA | 30 rows | `ROWS BETWEEN 29 PRECEDING AND CURRENT ROW` |
| Bollinger Upper | 20 rows | `AVG + 2 × STDDEV` |
| Bollinger Lower | 20 rows | `AVG - 2 × STDDEV` |
| Z-score | N rows | `(val - AVG) / STDDEV` |
| Momentum | N lag | `val - LAG(val, N)` |
| All-time high | All prior rows | `MAX OVER(UNBOUNDED PRECEDING)` |

### Business Metric Formulas
| Metric | Formula |
|--------|--------|
| Conversion rate | `conversions / sessions × 100` |
| Retention rate | `retained_next_period / base_cohort × 100` |
| Churn rate | `100 − retention_rate` |
| NRR | `ending_MRR / starting_MRR × 100` (>100% = expansion) |
| NPS | `% Promoters − % Detractors` |
| Lift (basket) | `(A∩B / A) / (B / total_orders)` |
| RFM Score | `r_score + f_score + m_score` (each 1–5) |
| LTV | `AOV × purchase_frequency × lifespan × gross_margin` |
| ARPU | `total_revenue / unique_users` |
| AOV | `total_revenue / order_count` |

---

## The Universal DA Query Template

This template covers 90% of all data analysis queries. Swap in your table names, metrics, and dimensions.

```sql
/*
  ============================================================
  UNIVERSAL DATA ANALYSIS QUERY TEMPLATE
  ============================================================
  Purpose  : [describe what this query answers]
  Author   : [your name]
  Updated  : [date]
  ============================================================
*/

WITH
-- STEP 1: Base data — apply initial filters, join required tables
base AS (
  SELECT
    f.id,
    f.date,
    f.amount,
    f.status,
    d.category,
    d.region
  FROM fact_table AS f
  INNER JOIN dimension_table AS d ON f.dim_id = d.id
  WHERE f.date >= '2024-01-01'
    AND f.status = 'completed'
),

-- STEP 2: Aggregate to the desired grain
aggregated AS (
  SELECT
    DATE_TRUNC('month', date) AS period,
    region,
    category,
    COUNT(*)              AS order_count,
    COUNT(DISTINCT id)    AS unique_ids,
    SUM(amount)           AS revenue,
    ROUND(AVG(amount), 2) AS avg_value
  FROM base
  GROUP BY DATE_TRUNC('month', date), region, category
),

-- STEP 3: Add window calculations (rankings, trends, comparisons)
enriched AS (
  SELECT
    *,
    -- Period-over-period
    LAG(revenue, 1) OVER(PARTITION BY region, category ORDER BY period)
      AS prev_period_revenue,
    ROUND(
      100.0 * (revenue - LAG(revenue, 1) OVER(PARTITION BY region, category ORDER BY period))
              / NULLIF(LAG(revenue, 1) OVER(PARTITION BY region, category ORDER BY period), 0),
      2
    ) AS pct_change,

    -- Running total
    SUM(revenue) OVER(PARTITION BY region, category ORDER BY period) AS running_total,

    -- Share of total for this period
    ROUND(100.0 * revenue / SUM(revenue) OVER(PARTITION BY period), 2) AS share_of_period,

    -- Ranking within period
    RANK() OVER(PARTITION BY period ORDER BY revenue DESC) AS revenue_rank
  FROM aggregated
)

-- STEP 4: Final output — filter, label, and format
SELECT
  TO_CHAR(period, 'YYYY-MM') AS month,
  region,
  category,
  order_count,
  revenue,
  avg_value,
  pct_change,
  running_total,
  share_of_period,
  revenue_rank,
  CASE
    WHEN pct_change > 10  THEN '📈 Strong Growth'
    WHEN pct_change > 0   THEN '↗ Growth'
    WHEN pct_change = 0   THEN '→ Flat'
    WHEN pct_change > -10 THEN '↘ Decline'
    ELSE '📉 Strong Decline'
  END AS trend_label
FROM enriched
WHERE revenue_rank <= 10         -- top 10 per period
ORDER BY period DESC, revenue DESC;
```

---

## Common Gotchas & Rules to Remember

**Division by zero** — always wrap denominators: `NULLIF(denominator, 0)`

**LAST_VALUE** — requires explicit frame extension or it returns current row:
`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`

**LAG on missing periods** — join to a date spine first to ensure correct prior-period matching

**HAVING vs WHERE** — `WHERE` filters before grouping; `HAVING` filters aggregated results

**Window functions and GROUP BY** — window functions see the post-`GROUP BY` result set, not raw rows

**ROWS vs RANGE** — `ROWS` counts physical rows; `RANGE` groups identical ORDER BY values together

**UNION vs UNION ALL** — `UNION ALL` is always faster; use `UNION` only when you need deduplication

**Correlated subqueries** — execute once per outer row; replace with window functions or joins when possible

**Partial periods in YoY** — cap current year with `WHERE date <= CURRENT_DATE` to avoid unfair comparisons

**NULL in aggregations** — `COUNT(col)` ignores NULLs; `COUNT(*)` does not; `AVG` ignores NULLs (denominator = non-null rows only)

---

*This summary covers all 8 README files in the SQL Data Analysis Cheat Sheet series.*
*For full examples, edge cases, and complete code, refer to the individual README files.*
