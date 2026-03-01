# 📊 SQL for Data Analysis — Level 3: Window Functions & Moving Computations
> *Calculate rankings, running totals, moving averages, and period-over-period changes*

---

## Table of Contents
- [1. Window Function Anatomy](#1-window-function-anatomy)
- [2. Ranking Functions](#2-ranking-functions)
- [3. Running Totals & Cumulative Metrics](#3-running-totals--cumulative-metrics)
- [4. Moving Averages](#4-moving-averages)
- [5. LAG & LEAD — Period Comparisons](#5-lag--lead--period-comparisons)
- [6. FIRST_VALUE / LAST_VALUE / NTH_VALUE](#6-first_value--last_value--nth_value)
- [7. NTILE — Percentile Buckets](#7-ntile--percentile-buckets)
- [8. Frame Clauses Deep Dive](#8-frame-clauses-deep-dive)
- [9. Window Function Practice Patterns](#9-window-function-practice-patterns)

---

## 1. Window Function Anatomy

Window functions compute values across a set of rows **without collapsing** them like GROUP BY would. Every row keeps its identity.

```sql
-- Full syntax
function_name(expression) OVER (
  PARTITION BY col1, col2     -- define groups (optional)
  ORDER BY col3 ASC/DESC      -- define row sequence (optional)
  ROWS BETWEEN ... AND ...    -- define the frame (optional)
)

-- Minimal window (entire result set as one window)
AVG(price) OVER()

-- Partitioned (separate window per group)
AVG(price) OVER(PARTITION BY category)

-- Ordered (running calculation)
SUM(amount) OVER(ORDER BY date)

-- Framed (only specific nearby rows)
AVG(amount) OVER(ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
```

> 💡 **Key insight:** Window functions execute AFTER `WHERE`, `GROUP BY`, and `HAVING`, but BEFORE the final `ORDER BY`. They always see the full filtered result set.

---

## 2. Ranking Functions

### ROW_NUMBER, RANK, DENSE_RANK

```sql
SELECT
  athlete,
  country,
  gold_medals,
  ROW_NUMBER() OVER(ORDER BY gold_medals DESC) AS row_num,
  RANK()       OVER(ORDER BY gold_medals DESC) AS rank,
  DENSE_RANK() OVER(ORDER BY gold_medals DESC) AS dense_rank
FROM olympians
ORDER BY gold_medals DESC;

/*
athlete   | medals | row_num | rank | dense_rank
----------|--------|---------|------|----------
Phelps    |    23  |       1 |    1 |          1
Williams  |    22  |       2 |    2 |          2
Bolt      |    22  |       3 |    2 |          2  ← tie
Ledecky   |    20  |       4 |    4 |          3  ← RANK skips 3, DENSE doesn't
*/

-- ROW_NUMBER: 1,2,3,4,5   (always unique)
-- RANK:       1,2,2,4,5   (skips 3)
-- DENSE_RANK: 1,2,2,3,4   (no gaps)
```

### Top N Per Group

```sql
-- Top 3 products by revenue per category
SELECT category, product_name, revenue
FROM (
  SELECT
    category,
    product_name,
    SUM(revenue) AS revenue,
    RANK() OVER(PARTITION BY category ORDER BY SUM(revenue) DESC) AS rnk
  FROM sales
  INNER JOIN products USING(product_id)
  GROUP BY category, product_name
) AS ranked
WHERE rnk <= 3
ORDER BY category, rnk;
```

---

## 3. Running Totals & Cumulative Metrics

```sql
-- Running (cumulative) total
SELECT
  order_date,
  daily_revenue,
  SUM(daily_revenue) OVER(ORDER BY order_date) AS running_total
FROM daily_sales;

-- Running total per region (resets for each region)
SELECT
  region,
  order_date,
  daily_revenue,
  SUM(daily_revenue) OVER(
    PARTITION BY region
    ORDER BY order_date
  ) AS regional_running_total
FROM daily_sales;

-- Running count of orders
SELECT
  order_date,
  order_id,
  COUNT(*) OVER(ORDER BY order_date) AS cumulative_order_count
FROM orders;

-- Cumulative revenue as percentage of final total
SELECT
  order_date,
  daily_revenue,
  SUM(daily_revenue) OVER(ORDER BY order_date) AS running_total,
  ROUND(
    100.0 * SUM(daily_revenue) OVER(ORDER BY order_date)
            / SUM(daily_revenue) OVER(),
    2
  ) AS cumulative_pct
FROM daily_sales;

-- Running maximum (watermark / all-time high)
SELECT
  date,
  daily_price,
  MAX(daily_price) OVER(ORDER BY date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS all_time_high
FROM stock_prices;
```

---

## 4. Moving Averages

Moving averages smooth out noise in time-series data.

```sql
-- 7-day simple moving average (SMA)
SELECT
  sale_date,
  daily_revenue,
  ROUND(
    AVG(daily_revenue) OVER(
      ORDER BY sale_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2
  ) AS sma_7day
FROM daily_sales;

-- 30-day moving average
SELECT
  sale_date,
  daily_revenue,
  AVG(daily_revenue) OVER(
    ORDER BY sale_date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS sma_30day
FROM daily_sales;

-- Multiple moving averages together (short vs long term)
SELECT
  sale_date,
  daily_revenue,
  ROUND(AVG(daily_revenue) OVER(
    ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 2) AS sma_7,
  ROUND(AVG(daily_revenue) OVER(
    ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ), 2) AS sma_30,
  ROUND(AVG(daily_revenue) OVER(
    ORDER BY sale_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
  ), 2) AS sma_90
FROM daily_sales
ORDER BY sale_date;

-- Centered moving average (uses rows before AND after — for analysis only)
SELECT
  sale_date,
  daily_revenue,
  AVG(daily_revenue) OVER(
    ORDER BY sale_date
    ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
  ) AS centered_7day_avg
FROM daily_sales;

-- Moving average per product (resets per group)
SELECT
  product_id,
  sale_date,
  daily_units,
  AVG(daily_units) OVER(
    PARTITION BY product_id
    ORDER BY sale_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS product_7day_avg
FROM product_daily_sales;
```

### Moving Standard Deviation (Volatility)

```sql
SELECT
  date,
  price,
  AVG(price) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    AS ma_20,
  STDDEV(price) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    AS rolling_stddev_20
FROM stock_prices;
```

---

## 5. LAG & LEAD — Period Comparisons

Access values from other rows without a self-join.

```sql
-- Day-over-day revenue change
SELECT
  sale_date,
  daily_revenue,
  LAG(daily_revenue, 1) OVER(ORDER BY sale_date) AS yesterday_revenue,
  daily_revenue - LAG(daily_revenue, 1) OVER(ORDER BY sale_date) AS dod_change,
  ROUND(
    100.0 * (daily_revenue - LAG(daily_revenue, 1) OVER(ORDER BY sale_date))
            / NULLIF(LAG(daily_revenue, 1) OVER(ORDER BY sale_date), 0),
    2
  ) AS dod_pct_change
FROM daily_sales;

-- Month-over-month growth
WITH monthly AS (
  SELECT
    DATE_TRUNC('month', order_date) AS month,
    SUM(total_amount) AS revenue
  FROM orders
  GROUP BY DATE_TRUNC('month', order_date)
)
SELECT
  month,
  revenue,
  LAG(revenue) OVER(ORDER BY month) AS prev_month_revenue,
  ROUND(
    100.0 * (revenue - LAG(revenue) OVER(ORDER BY month))
            / NULLIF(LAG(revenue) OVER(ORDER BY month), 0),
    1
  ) AS mom_growth_pct
FROM monthly;

-- Year-over-year comparison (lag by 12 months)
WITH monthly AS (
  SELECT DATE_TRUNC('month', order_date) AS month, SUM(total_amount) AS revenue
  FROM orders GROUP BY 1
)
SELECT
  month,
  revenue,
  LAG(revenue, 12) OVER(ORDER BY month) AS same_month_last_year,
  revenue - LAG(revenue, 12) OVER(ORDER BY month) AS yoy_change
FROM monthly;

-- LEAD: Look forward (useful for churn analysis)
SELECT
  customer_id,
  order_date,
  LEAD(order_date, 1) OVER(PARTITION BY customer_id ORDER BY order_date)
    AS next_order_date,
  LEAD(order_date, 1) OVER(PARTITION BY customer_id ORDER BY order_date)
    - order_date AS days_to_next_order
FROM orders;

-- Default value when no prior/next row exists
SELECT
  date,
  revenue,
  LAG(revenue, 1, 0) OVER(ORDER BY date) AS prev_revenue  -- default 0 if NULL
FROM daily_sales;
```

---

## 6. FIRST_VALUE / LAST_VALUE / NTH_VALUE

```sql
-- Compare each day's revenue to the month's first and last day
SELECT
  sale_date,
  daily_revenue,
  FIRST_VALUE(daily_revenue) OVER(
    PARTITION BY DATE_TRUNC('month', sale_date)
    ORDER BY sale_date
  ) AS month_first_day_revenue,
  LAST_VALUE(daily_revenue) OVER(
    PARTITION BY DATE_TRUNC('month', sale_date)
    ORDER BY sale_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  -- required!
  ) AS month_last_day_revenue
FROM daily_sales;

-- Show who holds first place next to every row
SELECT
  athlete,
  medals,
  FIRST_VALUE(athlete) OVER(ORDER BY medals DESC) AS current_leader,
  medals - FIRST_VALUE(medals) OVER(ORDER BY medals DESC) AS gap_to_leader
FROM athletes;

-- Get the 2nd highest value (silver medal)
SELECT
  category,
  product_name,
  revenue,
  NTH_VALUE(product_name, 2) OVER(
    PARTITION BY category
    ORDER BY revenue DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS second_best_product
FROM product_revenue;
```

---

## 7. NTILE — Percentile Buckets

```sql
-- Quartiles (4 equal buckets)
SELECT
  customer_id,
  lifetime_value,
  NTILE(4) OVER(ORDER BY lifetime_value DESC) AS quartile
FROM customer_metrics;
-- Q1 = top 25%, Q4 = bottom 25%

-- Deciles (10 buckets)
SELECT
  product_id,
  total_revenue,
  NTILE(10) OVER(ORDER BY total_revenue DESC) AS decile
FROM product_revenue;

-- Percentile with label
SELECT
  customer_id,
  lifetime_value,
  CASE NTILE(100) OVER(ORDER BY lifetime_value DESC)
    WHEN 1 THEN 'Top 1%'
    ELSE NTILE(100) OVER(ORDER BY lifetime_value DESC)::TEXT || 'th percentile'
  END AS percentile_label
FROM customer_metrics;

-- Find customers in top 10% of spend
SELECT customer_id, lifetime_value
FROM (
  SELECT customer_id, lifetime_value,
         NTILE(10) OVER(ORDER BY lifetime_value DESC) AS decile
  FROM customer_metrics
) AS bucketed
WHERE decile = 1;
```

---

## 8. Frame Clauses Deep Dive

The frame defines which rows are **included** in each window calculation.

```sql
-- Default frame when ORDER BY is used:
-- RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW

-- Explicit frame options:

-- 1. All rows from start to current (running total)
SUM(amount) OVER(ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- 2. Fixed look-back window (7-day moving avg)
AVG(amount) OVER(ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)

-- 3. Centered window (smoothing)
AVG(amount) OVER(ORDER BY date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING)

-- 4. Entire partition (global average alongside each row)
AVG(amount) OVER(PARTITION BY category ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)

-- 5. Next 3 rows (forward-looking window)
AVG(amount) OVER(ORDER BY date ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING)

-- ROWS vs RANGE:
-- ROWS: physical row positions
-- RANGE: logical value ranges (useful for dates with duplicate values)
AVG(amount) OVER(ORDER BY date RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW)
```

---

## 9. Window Function Practice Patterns

### Sales Trend Analysis Dashboard
```sql
WITH daily AS (
  SELECT
    sale_date,
    SUM(revenue) AS revenue
  FROM sales
  GROUP BY sale_date
)
SELECT
  sale_date,
  revenue,
  -- Smoothed trend lines
  AVG(revenue) OVER(ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS ma_7,
  AVG(revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ma_30,
  -- Growth signals
  LAG(revenue, 7)  OVER(ORDER BY sale_date) AS same_day_last_week,
  LAG(revenue, 28) OVER(ORDER BY sale_date) AS same_day_last_month,
  -- Cumulative
  SUM(revenue) OVER(ORDER BY sale_date) AS ytd_revenue,
  -- Rank
  RANK() OVER(ORDER BY revenue DESC) AS revenue_rank_all_time
FROM daily
ORDER BY sale_date DESC;
```

### Customer Purchase Gap Analysis
```sql
SELECT
  customer_id,
  order_date,
  LAG(order_date) OVER(PARTITION BY customer_id ORDER BY order_date)
    AS previous_order,
  order_date - LAG(order_date) OVER(PARTITION BY customer_id ORDER BY order_date)
    AS days_between_orders,
  ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_date) AS purchase_number
FROM orders
ORDER BY customer_id, order_date;
```

### Detect Revenue Anomalies (Z-Score over rolling window)
```sql
SELECT
  sale_date,
  revenue,
  AVG(revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ma_30,
  STDDEV(revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS std_30,
  ROUND(
    (revenue - AVG(revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW))
    / NULLIF(STDDEV(revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0),
    2
  ) AS z_score
FROM daily_sales
ORDER BY sale_date;
-- z_score > 2 or < -2 signals an anomaly
```

---

## 📌 Window Functions Quick Reference

| Function | Description |
|----------|-------------|
| `ROW_NUMBER()` | Unique sequential number |
| `RANK()` | Rank with gaps on ties |
| `DENSE_RANK()` | Rank without gaps on ties |
| `NTILE(n)` | Split into n equal buckets |
| `LAG(col, n)` | Value from n rows before |
| `LEAD(col, n)` | Value from n rows after |
| `FIRST_VALUE(col)` | First value in window frame |
| `LAST_VALUE(col)` | Last value in window frame |
| `NTH_VALUE(col, n)` | Nth value in window frame |
| `SUM() OVER(ORDER BY)` | Running total |
| `AVG() OVER(ROWS n PRECEDING)` | Moving average |

---

*← Previous: README_02_Intermediate_Aggregations.md | Next → README_04_Advanced_Analytics.md*
