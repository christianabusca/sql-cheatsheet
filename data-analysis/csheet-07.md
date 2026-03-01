# 📊 SQL for Data Analysis — Moving & Rolling Computations
## Moving Averages, Running Totals, Rolling Sums, Volatility & More
> *Every sliding-window calculation pattern you'll need for time-series analysis*

---

## Table of Contents
- [1. Window Frame Anatomy](#1-window-frame-anatomy)
- [2. Simple Moving Average (SMA)](#2-simple-moving-average-sma)
- [3. Exponential Moving Average (EMA)](#3-exponential-moving-average-ema)
- [4. Moving Sum & Running Total](#4-moving-sum--running-total)
- [5. Moving Min / Max (Rolling Extremes)](#5-moving-min--max-rolling-extremes)
- [6. Moving Standard Deviation & Variance (Volatility)](#6-moving-standard-deviation--variance-volatility)
- [7. Moving Count & Rolling Distinct Count](#7-moving-count--rolling-distinct-count)
- [8. Moving Median & Percentiles](#8-moving-median--percentiles)
- [9. Bollinger Bands](#9-bollinger-bands)
- [10. Momentum & Rate of Change (ROC)](#10-momentum--rate-of-change-roc)
- [11. Moving Aggregations per Group (PARTITION BY)](#11-moving-aggregations-per-group-partition-by)
- [12. Gap-Safe Rolling Windows (with Date Spine)](#12-gap-safe-rolling-windows-with-date-spine)
- [13. Comparing Multiple Window Lengths](#13-comparing-multiple-window-lengths)

---

## 1. Window Frame Anatomy

Every rolling calculation uses a **frame clause** inside `OVER()`.

```sql
function() OVER (
  PARTITION BY group_col      -- optional: reset per group
  ORDER BY time_col           -- defines row sequence
  ROWS BETWEEN start AND end  -- defines which rows to include
)

-- Common frame boundaries:
--  UNBOUNDED PRECEDING  → first row of partition
--  N PRECEDING          → N rows before current row
--  CURRENT ROW          → the current row itself
--  N FOLLOWING          → N rows after current row
--  UNBOUNDED FOLLOWING  → last row of partition

-- Most common patterns:
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  -- running total
ROWS BETWEEN 6 PRECEDING AND CURRENT ROW          -- 7-day window (incl. today)
ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING          -- 3-row centered window
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING -- whole partition
```

> 💡 **ROWS vs RANGE:**
> - `ROWS` counts physical row positions (what you almost always want)
> - `RANGE` groups rows with identical ORDER BY values together
> - For date-based windows with possible duplicate dates, use `RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW`

---

## 2. Simple Moving Average (SMA)

```sql
-- 7-day SMA
SELECT
  sale_date,
  daily_revenue,
  ROUND(
    AVG(daily_revenue) OVER(
      ORDER BY sale_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2
  ) AS sma_7
FROM daily_sales;

-- 14-day SMA
ROUND(AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW), 2) AS sma_14

-- 30-day SMA
ROUND(AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) AS sma_30

-- 90-day SMA
ROUND(AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW), 2) AS sma_90

-- Multiple SMAs in one query — compare short and long-term trends
SELECT
  sale_date,
  daily_revenue,
  ROUND(AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN  6 PRECEDING AND CURRENT ROW), 2) AS sma_7,
  ROUND(AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) AS sma_30,
  ROUND(AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW), 2) AS sma_90,
  -- Signal: when short MA crosses above long MA → uptrend
  CASE
    WHEN AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
       > AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    THEN 'Above 30d avg'
    ELSE 'Below 30d avg'
  END AS trend_signal
FROM daily_sales
ORDER BY sale_date;

-- Centered SMA (uses future rows — analysis only, not real-time forecasting)
ROUND(AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING), 2) AS centered_7

-- Weighted moving average (more recent rows = higher weight)
-- For a 3-period WMA: weights 3, 2, 1 for current, -1, -2
SELECT
  sale_date,
  daily_revenue,
  ROUND(
    (  3.0 * daily_revenue
     + 2.0 * LAG(daily_revenue, 1) OVER(ORDER BY sale_date)
     + 1.0 * LAG(daily_revenue, 2) OVER(ORDER BY sale_date)
    ) / NULLIF(
      3.0
      + CASE WHEN LAG(daily_revenue, 1) OVER(ORDER BY sale_date) IS NOT NULL THEN 2.0 ELSE 0 END
      + CASE WHEN LAG(daily_revenue, 2) OVER(ORDER BY sale_date) IS NOT NULL THEN 1.0 ELSE 0 END,
      0
    ), 2
  ) AS wma_3
FROM daily_sales;
```

---

## 3. Exponential Moving Average (EMA)

EMA gives more weight to recent data. SQL lacks native EMA support, so we use a recursive CTE.

```sql
-- EMA with smoothing factor α (alpha)
-- α = 2 / (N + 1) where N is the period
-- α = 0.1818 for 10-period EMA
-- α = 0.0645 for 30-period EMA

WITH RECURSIVE ema_calc AS (
  -- Anchor: use the first day's value as the starting EMA
  SELECT
    sale_date,
    daily_revenue,
    daily_revenue::numeric AS ema,
    ROW_NUMBER() OVER(ORDER BY sale_date) AS rn
  FROM daily_sales
  WHERE sale_date = (SELECT MIN(sale_date) FROM daily_sales)

  UNION ALL

  SELECT
    ds.sale_date,
    ds.daily_revenue,
    -- EMA formula: EMA(today) = α * price(today) + (1-α) * EMA(yesterday)
    ROUND(0.1818 * ds.daily_revenue + (1 - 0.1818) * ec.ema, 4) AS ema,
    ec.rn + 1
  FROM daily_sales AS ds
  INNER JOIN ema_calc AS ec
    ON ds.sale_date = (SELECT MIN(sale_date) FROM daily_sales WHERE sale_date > ec.sale_date)
)
SELECT
  sale_date,
  daily_revenue,
  ROUND(ema, 2) AS ema_10
FROM ema_calc
ORDER BY sale_date;
```

> 💡 For most DA use cases, a **simple moving average (SMA)** is sufficient and far simpler to write. Use EMA when you need to weight recent data more heavily (e.g., for financial analysis or real-time alerting).

---

## 4. Moving Sum & Running Total

```sql
-- Rolling 7-day revenue sum
SELECT
  sale_date,
  daily_revenue,
  SUM(daily_revenue) OVER(
    ORDER BY sale_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7d_sum,

  -- Running total from day 1 to current
  SUM(daily_revenue) OVER(
    ORDER BY sale_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_total,

  -- Running total that resets each year
  SUM(daily_revenue) OVER(
    PARTITION BY EXTRACT(YEAR FROM sale_date)
    ORDER BY sale_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS ytd_running_total,

  -- Rolling 30-day sum
  SUM(daily_revenue) OVER(
    ORDER BY sale_date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS rolling_30d_sum

FROM daily_sales
ORDER BY sale_date;

-- Cumulative percentage of total
SELECT
  sale_date,
  daily_revenue,
  SUM(daily_revenue) OVER(ORDER BY sale_date) AS running_total,
  ROUND(
    100.0 * SUM(daily_revenue) OVER(ORDER BY sale_date)
            / SUM(daily_revenue) OVER(),
    2
  ) AS cumulative_pct_of_total
FROM daily_sales;
```

---

## 5. Moving Min / Max (Rolling Extremes)

```sql
SELECT
  sale_date,
  daily_revenue,

  -- Rolling 7-day low and high
  MIN(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_low,
  MAX(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_high,

  -- Rolling 30-day low and high
  MIN(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30d_low,
  MAX(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30d_high,

  -- All-time high watermark (running max)
  MAX(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS all_time_high,

  -- How far is today from the all-time high?
  ROUND(
    100.0 * (daily_revenue - MAX(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))
            / NULLIF(MAX(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0),
    2
  ) AS pct_from_ath,

  -- Position within 30-day range (0 = at low, 1 = at high)
  ROUND(
    (daily_revenue - MIN(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW))
    / NULLIF(
      MAX(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) -
      MIN(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
      0
    ), 2
  ) AS position_in_30d_range  -- RSI-like indicator

FROM daily_sales
ORDER BY sale_date;
```

---

## 6. Moving Standard Deviation & Variance (Volatility)

```sql
SELECT
  sale_date,
  daily_revenue,

  -- 20-day rolling standard deviation (volatility)
  ROUND(STDDEV(daily_revenue) OVER(
    ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ), 2) AS rolling_stddev_20,

  -- 20-day rolling variance
  ROUND(VARIANCE(daily_revenue) OVER(
    ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ), 2) AS rolling_variance_20,

  -- Coefficient of variation (stddev / mean) — normalized volatility
  ROUND(
    STDDEV(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    / NULLIF(AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 0),
    4
  ) AS coeff_of_variation_20,

  -- Z-score: how many stddevs is today from the rolling mean?
  ROUND(
    (daily_revenue - AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW))
    / NULLIF(STDDEV(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0),
    2
  ) AS z_score_30d

FROM daily_sales
ORDER BY sale_date;

-- Flag anomalies using rolling Z-score
SELECT *,
  CASE
    WHEN ABS(z_score) > 3 THEN '🔴 Extreme outlier'
    WHEN ABS(z_score) > 2 THEN '🟡 Anomaly'
    ELSE '🟢 Normal'
  END AS anomaly_flag
FROM (
  SELECT
    sale_date,
    daily_revenue,
    ROUND(
      (daily_revenue - AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW))
      / NULLIF(STDDEV(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0),
      2
    ) AS z_score
  FROM daily_sales
) AS zscored;
```

---

## 7. Moving Count & Rolling Distinct Count

```sql
-- Rolling 7-day order count
SELECT
  sale_date,
  orders_today,
  SUM(orders_today) OVER(ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
    AS rolling_7d_order_count
FROM daily_order_counts;

-- Rolling 30-day unique customer count (approximate — requires pre-aggregation trick)
-- Step 1: flag each customer's first appearance in each 30d window
WITH customer_daily AS (
  SELECT
    customer_id,
    DATE(order_date) AS day
  FROM orders
  GROUP BY 1, 2
),
customer_window AS (
  SELECT
    cd.day,
    cd.customer_id,
    -- Was this customer seen in the prior 29 days?
    MAX(other.day) OVER(
      PARTITION BY cd.customer_id
      ORDER BY cd.day
      ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
    ) AS last_seen_before_window
  FROM customer_daily AS cd
)
SELECT
  day,
  COUNT(DISTINCT customer_id) AS new_to_window_customers
FROM customer_window
WHERE last_seen_before_window IS NULL   -- first appearance in this 30d window
   OR last_seen_before_window < day - 29
GROUP BY day
ORDER BY day;
```

---

## 8. Moving Median & Percentiles

Standard SQL doesn't support window-framed `PERCENTILE_CONT`, but there are workarounds.

```sql
-- Approach 1: Per-period percentiles (not rolling, but common)
SELECT
  DATE_TRUNC('month', order_date) AS month,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY order_value) AS p50_median,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY order_value) AS p25,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY order_value) AS p75,
  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY order_value) AS p90,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY order_value) AS p95
FROM orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;

-- Approach 2: Rolling percentile approximation using AVG + STDDEV
-- (assumes normal distribution; good enough for most business use cases)
SELECT
  sale_date,
  daily_revenue,
  AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS approx_median,
  AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    + 1.28 * STDDEV(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    AS approx_p90,
  AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    + 1.645 * STDDEV(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    AS approx_p95
FROM daily_sales;
```

---

## 9. Bollinger Bands

Bollinger Bands = 20-day SMA ± 2 standard deviations. Identifies overbought/oversold conditions.

```sql
SELECT
  sale_date,
  daily_revenue,

  -- Middle band: 20-day SMA
  ROUND(AVG(daily_revenue) OVER(
    ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ), 2) AS bb_middle,

  -- Upper band: SMA + 2σ
  ROUND(
    AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    + 2 * STDDEV(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    2
  ) AS bb_upper,

  -- Lower band: SMA - 2σ
  ROUND(
    AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    - 2 * STDDEV(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    2
  ) AS bb_lower,

  -- Band width (high = volatile, low = consolidating)
  ROUND(
    4 * STDDEV(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    / NULLIF(AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 0),
    4
  ) AS band_width,

  -- Signal: where is today relative to bands?
  CASE
    WHEN daily_revenue > AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                       + 2 * STDDEV(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    THEN '🔴 Above upper band'
    WHEN daily_revenue < AVG(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                       - 2 * STDDEV(daily_revenue) OVER(ORDER BY sale_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    THEN '🔵 Below lower band'
    ELSE '🟢 Within bands'
  END AS bb_signal

FROM daily_sales
ORDER BY sale_date;
```

---

## 10. Momentum & Rate of Change (ROC)

```sql
SELECT
  sale_date,
  daily_revenue,

  -- Momentum: current minus N periods ago
  daily_revenue - LAG(daily_revenue, 7)  OVER(ORDER BY sale_date) AS momentum_7d,
  daily_revenue - LAG(daily_revenue, 30) OVER(ORDER BY sale_date) AS momentum_30d,

  -- Rate of Change (ROC): % change vs N periods ago
  ROUND(
    100.0 * (daily_revenue - LAG(daily_revenue, 7) OVER(ORDER BY sale_date))
            / NULLIF(LAG(daily_revenue, 7) OVER(ORDER BY sale_date), 0),
    2
  ) AS roc_7d_pct,

  ROUND(
    100.0 * (daily_revenue - LAG(daily_revenue, 30) OVER(ORDER BY sale_date))
            / NULLIF(LAG(daily_revenue, 30) OVER(ORDER BY sale_date), 0),
    2
  ) AS roc_30d_pct,

  -- Acceleration: is momentum increasing or decreasing?
  (daily_revenue - LAG(daily_revenue, 7) OVER(ORDER BY sale_date))
  - LAG(daily_revenue - LAG(daily_revenue, 7) OVER(ORDER BY sale_date), 7) OVER(ORDER BY sale_date)
    AS momentum_acceleration

FROM daily_sales
ORDER BY sale_date;
```

---

## 11. Moving Aggregations per Group (PARTITION BY)

Partition resets the window for each group — essential for per-product or per-region rolling calculations.

```sql
-- Per-product 7-day moving average (each product gets its own window)
SELECT
  product_id,
  sale_date,
  daily_units,
  ROUND(
    AVG(daily_units) OVER(
      PARTITION BY product_id          -- ← resets for each product
      ORDER BY sale_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2
  ) AS product_sma_7,
  SUM(daily_units) OVER(
    PARTITION BY product_id
    ORDER BY sale_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS product_cumulative_units
FROM product_daily_sales
ORDER BY product_id, sale_date;

-- Per-region running total (resets each year AND region)
SELECT
  region,
  sale_date,
  daily_revenue,
  SUM(daily_revenue) OVER(
    PARTITION BY region, EXTRACT(YEAR FROM sale_date)
    ORDER BY sale_date
  ) AS region_ytd_revenue,
  AVG(daily_revenue) OVER(
    PARTITION BY region
    ORDER BY sale_date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS region_sma_30
FROM regional_daily_sales
ORDER BY region, sale_date;
```

---

## 12. Gap-Safe Rolling Windows (with Date Spine)

If your data has missing dates, `ROWS BETWEEN N PRECEDING` counts **present rows**, not calendar days. Use a date spine to ensure true calendar-based windows.

```sql
-- Build a complete date spine and join actuals to it
WITH date_spine AS (
  SELECT generate_series(
    '2024-01-01'::date,
    '2024-12-31'::date,
    '1 day'
  )::date AS day
),
filled AS (
  SELECT
    ds.day,
    COALESCE(SUM(o.total_amount), 0) AS revenue
  FROM date_spine AS ds
  LEFT JOIN orders AS o ON DATE(o.order_date) = ds.day
  GROUP BY ds.day
)
-- Now ROWS BETWEEN 6 PRECEDING = true 7 calendar days
SELECT
  day,
  revenue,
  ROUND(AVG(revenue) OVER(ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS sma_7_calendar
FROM filled
ORDER BY day;

-- Alternative: use RANGE with INTERVAL for date-based frames
SELECT
  sale_date,
  daily_revenue,
  AVG(daily_revenue) OVER(
    ORDER BY sale_date::timestamp
    RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
  ) AS sma_7_range_based  -- groups by actual date range, not row count
FROM daily_sales;
```

---

## 13. Comparing Multiple Window Lengths

Use this pattern to find the "best" window length for smoothing your data.

```sql
WITH daily AS (
  SELECT DATE(order_date) AS day, SUM(total_amount) AS revenue
  FROM orders GROUP BY 1
)
SELECT
  day,
  revenue                                                                                      AS raw,
  ROUND(AVG(revenue) OVER(ORDER BY day ROWS BETWEEN  2 PRECEDING AND CURRENT ROW), 2)        AS sma_3,
  ROUND(AVG(revenue) OVER(ORDER BY day ROWS BETWEEN  6 PRECEDING AND CURRENT ROW), 2)        AS sma_7,
  ROUND(AVG(revenue) OVER(ORDER BY day ROWS BETWEEN 13 PRECEDING AND CURRENT ROW), 2)        AS sma_14,
  ROUND(AVG(revenue) OVER(ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2)        AS sma_30,
  ROUND(AVG(revenue) OVER(ORDER BY day ROWS BETWEEN 89 PRECEDING AND CURRENT ROW), 2)        AS sma_90,
  -- How much does 7d MA differ from 30d MA? (trend divergence signal)
  ROUND(
    AVG(revenue) OVER(ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) -
    AVG(revenue) OVER(ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
    2
  ) AS ma_divergence_7_30
FROM daily
ORDER BY day DESC;
```

---

## 📌 Moving Computation Quick Reference

| Computation | SQL Pattern |
|-------------|------------|
| N-day SMA | `AVG(val) OVER(ORDER BY d ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW)` |
| Running Total | `SUM(val) OVER(ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` |
| Rolling Sum (N) | `SUM(val) OVER(ORDER BY d ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW)` |
| Rolling Max | `MAX(val) OVER(ORDER BY d ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW)` |
| All-Time High | `MAX(val) OVER(ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` |
| Rolling Stddev | `STDDEV(val) OVER(ORDER BY d ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW)` |
| Z-Score | `(val - AVG OVER window) / NULLIF(STDDEV OVER window, 0)` |
| Bollinger Upper | `AVG + 2 * STDDEV over 20-row window` |
| Momentum | `val - LAG(val, N) OVER(ORDER BY d)` |
| Rate of Change | `100 * (val - LAG(val,N)) / NULLIF(LAG(val,N), 0)` |
| Centered Window | `ROWS BETWEEN N PRECEDING AND N FOLLOWING` |

---

*Part of the SQL Data Analysis Cheat Sheet series*
