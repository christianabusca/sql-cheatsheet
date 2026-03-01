# 📊 SQL for Data Analysis — Period-over-Period Comparisons
## YoY, MoM, WoW, DoD, QoQ & Rolling Period Growth
> *Every pattern you need for comparing performance across time periods*

---

## Table of Contents
- [1. The Core Pattern: LAG for Period Comparisons](#1-the-core-pattern-lag-for-period-comparisons)
- [2. Day-over-Day (DoD)](#2-day-over-day-dod)
- [3. Week-over-Week (WoW)](#3-week-over-week-wow)
- [4. Month-over-Month (MoM)](#4-month-over-month-mom)
- [5. Quarter-over-Quarter (QoQ)](#5-quarter-over-quarter-qoq)
- [6. Year-over-Year (YoY)](#6-year-over-year-yoy)
- [7. Same Period Last Year (SPLY)](#7-same-period-last-year-sply)
- [8. Rolling Period Comparisons](#8-rolling-period-comparisons)
- [9. Multi-Period Dashboard in One Query](#9-multi-period-dashboard-in-one-query)
- [10. Indexed Growth (Base = 100)](#10-indexed-growth-base--100)
- [11. Compound Annual Growth Rate (CAGR)](#11-compound-annual-growth-rate-cagr)
- [12. Common Pitfalls & Edge Cases](#12-common-pitfalls--edge-cases)

---

## 1. The Core Pattern: LAG for Period Comparisons

All period comparisons follow the same three-step formula:

```sql
-- Universal pattern
SELECT
  period,
  current_value,
  LAG(current_value, N) OVER(ORDER BY period) AS prior_value,

  -- Absolute change
  current_value - LAG(current_value, N) OVER(ORDER BY period) AS abs_change,

  -- Percentage change
  ROUND(
    100.0 * (current_value - LAG(current_value, N) OVER(ORDER BY period))
            / NULLIF(LAG(current_value, N) OVER(ORDER BY period), 0),
    2
  ) AS pct_change

FROM aggregated_table
ORDER BY period;

-- N = 1  → compare to previous period (DoD, WoW, MoM, QoQ, YoY)
-- N = 12 → compare to same month last year (SPLY for monthly data)
-- N = 4  → compare to same quarter last year (SPLY for quarterly data)
-- N = 52 → compare to same week last year (SPLY for weekly data)
-- N = 365 → compare to same day last year (SPLY for daily data)
```

> 💡 **NULLIF(value, 0)** prevents division-by-zero errors when prior period had zero revenue.

---

## 2. Day-over-Day (DoD)

```sql
-- Step 1: Aggregate to daily grain
WITH daily AS (
  SELECT
    DATE(order_date)       AS day,
    COUNT(*)               AS orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(total_amount)      AS revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
  FROM orders
  GROUP BY DATE(order_date)
),

-- Step 2: Add previous-day comparisons
dod AS (
  SELECT
    day,
    orders,
    revenue,
    avg_order_value,
    LAG(revenue, 1) OVER(ORDER BY day) AS yesterday_revenue,
    LAG(orders,  1) OVER(ORDER BY day) AS yesterday_orders
  FROM daily
)

-- Step 3: Final output with changes
SELECT
  day,
  revenue,
  yesterday_revenue,
  revenue - yesterday_revenue AS dod_abs_change,
  ROUND(
    100.0 * (revenue - yesterday_revenue)
            / NULLIF(yesterday_revenue, 0),
    2
  ) AS dod_pct_change,
  CASE
    WHEN revenue > yesterday_revenue THEN '▲'
    WHEN revenue < yesterday_revenue THEN '▼'
    ELSE '→'
  END AS trend_arrow
FROM dod
ORDER BY day DESC;
```

---

## 3. Week-over-Week (WoW)

```sql
-- Weekly revenue with WoW comparison
WITH weekly AS (
  SELECT
    DATE_TRUNC('week', order_date)::date AS week_start,
    SUM(total_amount) AS revenue,
    COUNT(*) AS orders,
    COUNT(DISTINCT customer_id) AS customers
  FROM orders
  GROUP BY DATE_TRUNC('week', order_date)
)
SELECT
  week_start,
  revenue,
  orders,
  customers,
  LAG(revenue, 1) OVER(ORDER BY week_start)   AS prev_week_rev,
  LAG(orders,  1) OVER(ORDER BY week_start)   AS prev_week_orders,

  -- WoW revenue change
  revenue - LAG(revenue, 1) OVER(ORDER BY week_start)  AS wow_rev_change,
  ROUND(
    100.0 * (revenue - LAG(revenue, 1) OVER(ORDER BY week_start))
            / NULLIF(LAG(revenue, 1) OVER(ORDER BY week_start), 0), 2
  ) AS wow_rev_pct,

  -- WoW order change
  ROUND(
    100.0 * (orders - LAG(orders, 1) OVER(ORDER BY week_start))
            / NULLIF(LAG(orders, 1) OVER(ORDER BY week_start), 0), 2
  ) AS wow_orders_pct,

  -- Same week last year (lag 52 weeks)
  LAG(revenue, 52) OVER(ORDER BY week_start)  AS same_week_ly,
  ROUND(
    100.0 * (revenue - LAG(revenue, 52) OVER(ORDER BY week_start))
            / NULLIF(LAG(revenue, 52) OVER(ORDER BY week_start), 0), 2
  ) AS wow_vs_ly_pct

FROM weekly
ORDER BY week_start DESC;
```

---

## 4. Month-over-Month (MoM)

```sql
-- Full MoM analysis with multiple metrics
WITH monthly AS (
  SELECT
    DATE_TRUNC('month', order_date)::date    AS month,
    SUM(total_amount)                         AS revenue,
    COUNT(*)                                  AS orders,
    COUNT(DISTINCT customer_id)               AS unique_customers,
    ROUND(AVG(total_amount), 2)               AS aov,         -- Average Order Value
    SUM(total_amount) / NULLIF(COUNT(DISTINCT customer_id), 0) AS arpu  -- Avg Revenue Per User
  FROM orders
  WHERE status = 'completed'
  GROUP BY DATE_TRUNC('month', order_date)
)
SELECT
  TO_CHAR(month, 'YYYY-MM')                  AS month,
  revenue,
  orders,
  unique_customers,
  aov,
  arpu,

  -- MoM absolute changes
  revenue          - LAG(revenue,          1) OVER(ORDER BY month) AS rev_mom_abs,
  orders           - LAG(orders,           1) OVER(ORDER BY month) AS ord_mom_abs,
  unique_customers - LAG(unique_customers, 1) OVER(ORDER BY month) AS cust_mom_abs,

  -- MoM percentage changes
  ROUND(100.0 * (revenue - LAG(revenue, 1) OVER(ORDER BY month))
        / NULLIF(LAG(revenue, 1) OVER(ORDER BY month), 0), 2)          AS rev_mom_pct,
  ROUND(100.0 * (orders - LAG(orders, 1) OVER(ORDER BY month))
        / NULLIF(LAG(orders, 1) OVER(ORDER BY month), 0), 2)           AS ord_mom_pct,
  ROUND(100.0 * (unique_customers - LAG(unique_customers, 1) OVER(ORDER BY month))
        / NULLIF(LAG(unique_customers, 1) OVER(ORDER BY month), 0), 2) AS cust_mom_pct,

  -- Running YTD total
  SUM(revenue) OVER(
    PARTITION BY EXTRACT(YEAR FROM month)
    ORDER BY month
  ) AS ytd_revenue

FROM monthly
ORDER BY month DESC;
```

---

## 5. Quarter-over-Quarter (QoQ)

```sql
WITH quarterly AS (
  SELECT
    EXTRACT(YEAR    FROM order_date) AS yr,
    EXTRACT(QUARTER FROM order_date) AS qtr,
    DATE_TRUNC('quarter', order_date)::date AS quarter_start,
    SUM(total_amount) AS revenue,
    COUNT(*) AS orders
  FROM orders
  GROUP BY 1, 2, 3
)
SELECT
  yr,
  'Q' || qtr AS quarter,
  quarter_start,
  revenue,
  orders,

  -- QoQ change (vs previous quarter)
  LAG(revenue, 1) OVER(ORDER BY quarter_start) AS prev_quarter_rev,
  ROUND(
    100.0 * (revenue - LAG(revenue, 1) OVER(ORDER BY quarter_start))
            / NULLIF(LAG(revenue, 1) OVER(ORDER BY quarter_start), 0), 2
  ) AS qoq_pct,

  -- Same quarter last year (lag 4 quarters)
  LAG(revenue, 4) OVER(ORDER BY quarter_start) AS same_qtr_ly,
  ROUND(
    100.0 * (revenue - LAG(revenue, 4) OVER(ORDER BY quarter_start))
            / NULLIF(LAG(revenue, 4) OVER(ORDER BY quarter_start), 0), 2
  ) AS qoq_vs_ly_pct,

  -- Quarter % of full year (using window)
  ROUND(
    100.0 * revenue / NULLIF(SUM(revenue) OVER(PARTITION BY yr), 0), 1
  ) AS pct_of_annual_rev

FROM quarterly
ORDER BY quarter_start DESC;
```

---

## 6. Year-over-Year (YoY)

```sql
-- Annual YoY summary
WITH yearly AS (
  SELECT
    EXTRACT(YEAR FROM order_date)::int AS yr,
    SUM(total_amount)                  AS revenue,
    COUNT(*)                           AS orders,
    COUNT(DISTINCT customer_id)        AS customers,
    ROUND(AVG(total_amount), 2)        AS aov
  FROM orders
  WHERE status = 'completed'
  GROUP BY EXTRACT(YEAR FROM order_date)
)
SELECT
  yr,
  revenue,
  orders,
  customers,
  aov,
  LAG(revenue,   1) OVER(ORDER BY yr) AS prev_yr_rev,
  LAG(orders,    1) OVER(ORDER BY yr) AS prev_yr_orders,
  LAG(customers, 1) OVER(ORDER BY yr) AS prev_yr_customers,

  -- YoY changes
  ROUND(100.0 * (revenue   - LAG(revenue,   1) OVER(ORDER BY yr))
                / NULLIF(LAG(revenue,   1) OVER(ORDER BY yr), 0), 2) AS rev_yoy_pct,
  ROUND(100.0 * (orders    - LAG(orders,    1) OVER(ORDER BY yr))
                / NULLIF(LAG(orders,    1) OVER(ORDER BY yr), 0), 2) AS ord_yoy_pct,
  ROUND(100.0 * (customers - LAG(customers, 1) OVER(ORDER BY yr))
                / NULLIF(LAG(customers, 1) OVER(ORDER BY yr), 0), 2) AS cust_yoy_pct,
  ROUND(100.0 * (aov       - LAG(aov,       1) OVER(ORDER BY yr))
                / NULLIF(LAG(aov,       1) OVER(ORDER BY yr), 0), 2) AS aov_yoy_pct

FROM yearly
ORDER BY yr DESC;
```

---

## 7. Same Period Last Year (SPLY)

The most useful pattern for seasonality-aware reporting.

```sql
-- Monthly revenue vs same month last year
WITH monthly AS (
  SELECT
    DATE_TRUNC('month', order_date)::date AS month,
    SUM(total_amount) AS revenue
  FROM orders
  GROUP BY DATE_TRUNC('month', order_date)
)
SELECT
  month,
  TO_CHAR(month, 'Mon YYYY')             AS month_label,
  revenue                                AS this_year_rev,
  LAG(revenue, 12) OVER(ORDER BY month) AS same_month_last_year,

  -- Absolute and percentage change vs SPLY
  revenue - LAG(revenue, 12) OVER(ORDER BY month) AS sply_abs_change,
  ROUND(
    100.0 * (revenue - LAG(revenue, 12) OVER(ORDER BY month))
            / NULLIF(LAG(revenue, 12) OVER(ORDER BY month), 0), 2
  ) AS sply_pct_change

FROM monthly
WHERE month >= DATE_TRUNC('month', NOW()) - INTERVAL '24 months'
ORDER BY month DESC;

-- ----------------------------------------------------------
-- ALTERNATIVE: Join approach (more flexible, handles irregular data)
-- ----------------------------------------------------------
WITH monthly AS (
  SELECT
    DATE_TRUNC('month', order_date)::date AS month,
    SUM(total_amount) AS revenue
  FROM orders
  GROUP BY 1
)
SELECT
  cy.month,
  cy.revenue AS current_year,
  py.revenue AS prior_year,
  cy.revenue - py.revenue AS sply_change,
  ROUND(100.0 * (cy.revenue - py.revenue) / NULLIF(py.revenue, 0), 2) AS sply_pct
FROM monthly AS cy
LEFT JOIN monthly AS py
  ON cy.month = py.month + INTERVAL '1 year'  -- match same month, prior year
ORDER BY cy.month DESC;
```

---

## 8. Rolling Period Comparisons

Compare rolling windows instead of calendar periods — avoids boundary effects.

```sql
-- Rolling 30-day revenue vs prior rolling 30-day
WITH daily AS (
  SELECT DATE(order_date) AS day, SUM(total_amount) AS revenue
  FROM orders GROUP BY DATE(order_date)
),
rolling AS (
  SELECT
    day,
    revenue,
    SUM(revenue) OVER(ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      AS rolling_30d,
    SUM(revenue) OVER(ORDER BY day ROWS BETWEEN 59 PRECEDING AND 30 PRECEDING)
      AS prev_rolling_30d
  FROM daily
)
SELECT
  day,
  rolling_30d,
  prev_rolling_30d,
  rolling_30d - prev_rolling_30d AS abs_change,
  ROUND(
    100.0 * (rolling_30d - prev_rolling_30d)
            / NULLIF(prev_rolling_30d, 0), 2
  ) AS rolling_30d_pct_change
FROM rolling
ORDER BY day DESC;

-- Rolling 90-day vs same rolling window last year
WITH daily AS (
  SELECT DATE(order_date) AS day, SUM(total_amount) AS revenue
  FROM orders GROUP BY 1
),
rolling AS (
  SELECT day, revenue,
    SUM(revenue) OVER(ORDER BY day ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
      AS rolling_90d
  FROM daily
)
SELECT
  cy.day,
  cy.rolling_90d AS this_period,
  py.rolling_90d AS last_year_same_period,
  ROUND(
    100.0 * (cy.rolling_90d - py.rolling_90d) / NULLIF(py.rolling_90d, 0), 2
  ) AS yoy_rolling_pct
FROM rolling AS cy
LEFT JOIN rolling AS py ON cy.day = py.day + INTERVAL '365 days'
ORDER BY cy.day DESC;
```

---

## 9. Multi-Period Dashboard in One Query

Get DoD, WoW, MoM, YoY and YTD all at once for a single snapshot date.

```sql
WITH snapshot AS (
  SELECT CURRENT_DATE AS today
),
-- Pre-aggregate for each comparison window
periods AS (
  SELECT
    -- Current periods
    SUM(CASE WHEN order_date = s.today - 1                                   THEN total_amount END) AS yesterday,
    SUM(CASE WHEN order_date >= s.today - 7  AND order_date < s.today        THEN total_amount END) AS last_7d,
    SUM(CASE WHEN order_date >= s.today - 30 AND order_date < s.today        THEN total_amount END) AS last_30d,
    SUM(CASE WHEN DATE_TRUNC('month', order_date) = DATE_TRUNC('month', s.today - INTERVAL '1 month')
                                                                             THEN total_amount END) AS last_full_month,
    SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = EXTRACT(YEAR FROM s.today) THEN total_amount END) AS ytd,

    -- Prior comparison periods
    SUM(CASE WHEN order_date = s.today - 2                                   THEN total_amount END) AS day_before_yesterday,
    SUM(CASE WHEN order_date >= s.today - 14 AND order_date < s.today - 7   THEN total_amount END) AS prev_7d,
    SUM(CASE WHEN order_date >= s.today - 60 AND order_date < s.today - 30  THEN total_amount END) AS prev_30d,
    SUM(CASE WHEN DATE_TRUNC('month', order_date) = DATE_TRUNC('month', s.today - INTERVAL '2 months')
                                                                             THEN total_amount END) AS prev_full_month,
    SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = EXTRACT(YEAR FROM s.today) - 1
              AND order_date <= s.today - INTERVAL '1 year'                  THEN total_amount END) AS ytd_last_year
  FROM orders
  CROSS JOIN snapshot AS s
  WHERE status = 'completed'
)
SELECT
  ROUND(yesterday, 2)                                                          AS yesterday_rev,
  ROUND(100.0 * (yesterday - day_before_yesterday) / NULLIF(day_before_yesterday, 0), 1) AS dod_pct,

  ROUND(last_7d, 2)                                                            AS last_7d_rev,
  ROUND(100.0 * (last_7d - prev_7d) / NULLIF(prev_7d, 0), 1)                  AS wow_pct,

  ROUND(last_30d, 2)                                                           AS last_30d_rev,
  ROUND(100.0 * (last_30d - prev_30d) / NULLIF(prev_30d, 0), 1)               AS mom_pct,

  ROUND(last_full_month, 2)                                                    AS last_month_rev,
  ROUND(100.0 * (last_full_month - prev_full_month) / NULLIF(prev_full_month, 0), 1) AS full_mom_pct,

  ROUND(ytd, 2)                                                                AS ytd_rev,
  ROUND(100.0 * (ytd - ytd_last_year) / NULLIF(ytd_last_year, 0), 1)          AS ytd_yoy_pct

FROM periods;
```

---

## 10. Indexed Growth (Base = 100)

Normalize all series to the same starting point to compare growth rates across products, regions, or cohorts.

```sql
-- Index revenue to 100 at the first month of data
WITH monthly AS (
  SELECT
    product_id,
    DATE_TRUNC('month', order_date)::date AS month,
    SUM(revenue) AS revenue
  FROM sales
  GROUP BY 1, 2
),
base_values AS (
  SELECT product_id,
         FIRST_VALUE(revenue) OVER(PARTITION BY product_id ORDER BY month) AS base_revenue
  FROM monthly
),
indexed AS (
  SELECT
    m.product_id,
    m.month,
    m.revenue,
    b.base_revenue,
    ROUND(100.0 * m.revenue / NULLIF(b.base_revenue, 0), 1) AS index_value
  FROM monthly AS m
  INNER JOIN base_values AS b USING(product_id, month) -- actually just needs product_id
)
SELECT * FROM indexed ORDER BY product_id, month;

-- At index month: all products = 100
-- At later months: 120 = grew 20%, 80 = shrank 20%
-- Great for charts comparing relative growth of different-sized products
```

---

## 11. Compound Annual Growth Rate (CAGR)

CAGR answers: "What steady annual growth rate would take us from A to B in N years?"

```sql
-- CAGR between first and last year of data
WITH yearly AS (
  SELECT
    EXTRACT(YEAR FROM order_date)::int AS yr,
    SUM(total_amount) AS revenue
  FROM orders
  GROUP BY 1
),
endpoints AS (
  SELECT
    MIN(yr)     AS start_year,
    MAX(yr)     AS end_year,
    MAX(yr) - MIN(yr) AS years,
    FIRST_VALUE(revenue) OVER(ORDER BY yr) AS start_revenue,
    LAST_VALUE(revenue)  OVER(ORDER BY yr ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_revenue
  FROM yearly
  LIMIT 1
)
SELECT
  start_year,
  end_year,
  years,
  ROUND(start_revenue, 2) AS start_revenue,
  ROUND(end_revenue, 2) AS end_revenue,
  -- CAGR = (end/start)^(1/years) - 1
  ROUND(
    (POWER(end_revenue / NULLIF(start_revenue, 0), 1.0 / NULLIF(years, 0)) - 1) * 100,
    2
  ) AS cagr_pct
FROM endpoints;

-- Per-product CAGR
WITH yearly AS (
  SELECT product_id, EXTRACT(YEAR FROM order_date)::int AS yr, SUM(revenue) AS revenue
  FROM sales GROUP BY 1, 2
),
product_endpoints AS (
  SELECT
    product_id,
    MAX(yr) - MIN(yr) AS years,
    FIRST_VALUE(revenue) OVER(PARTITION BY product_id ORDER BY yr) AS start_rev,
    LAST_VALUE(revenue)  OVER(PARTITION BY product_id ORDER BY yr
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_rev
  FROM yearly
)
SELECT DISTINCT
  product_id,
  years,
  ROUND(start_rev, 2) AS start_revenue,
  ROUND(end_rev, 2) AS end_revenue,
  ROUND(
    (POWER(end_rev / NULLIF(start_rev, 0), 1.0 / NULLIF(years, 0)) - 1) * 100, 2
  ) AS cagr_pct
FROM product_endpoints
ORDER BY cagr_pct DESC;
```

---

## 12. Common Pitfalls & Edge Cases

### Missing Periods (Gaps in Data)

```sql
-- Problem: LAG gives WRONG prior period if a month has no data
-- Solution: Join to a complete date spine first

WITH date_spine AS (
  SELECT generate_series(
    DATE_TRUNC('month', MIN(order_date)),
    DATE_TRUNC('month', MAX(order_date)),
    '1 month'::interval
  )::date AS month
  FROM orders
),
monthly_actuals AS (
  SELECT DATE_TRUNC('month', order_date)::date AS month, SUM(total_amount) AS revenue
  FROM orders GROUP BY 1
),
filled AS (
  SELECT
    ds.month,
    COALESCE(ma.revenue, 0) AS revenue  -- fill gaps with 0
  FROM date_spine AS ds
  LEFT JOIN monthly_actuals AS ma USING(month)
)
SELECT
  month,
  revenue,
  LAG(revenue, 1) OVER(ORDER BY month) AS prev_month,  -- now guaranteed correct
  LAG(revenue, 12) OVER(ORDER BY month) AS same_month_last_year
FROM filled
ORDER BY month;
```

### Division by Zero

```sql
-- Always wrap the denominator in NULLIF
-- BAD  → crashes when prior period = 0
100.0 * (current - prior) / prior

-- GOOD → returns NULL instead of error
100.0 * (current - prior) / NULLIF(prior, 0)
```

### Partial Periods

```sql
-- YoY comparison should only use completed months
-- or compare same number of days to avoid "unfair" comparison

SELECT
  yr,
  revenue,
  -- Only compare through the same calendar day as today
  CASE
    WHEN yr < EXTRACT(YEAR FROM CURRENT_DATE)
    THEN 'Full Year'
    ELSE 'YTD (partial)'
  END AS period_type
FROM (
  SELECT EXTRACT(YEAR FROM order_date)::int AS yr, SUM(total_amount) AS revenue
  FROM orders
  WHERE order_date <= CURRENT_DATE  -- cap current year to today
  GROUP BY 1
) annual
ORDER BY yr;
```

---

## 📌 Period Comparison Quick Reference

| Comparison | LAG offset | Grain |
|-----------|-----------|-------|
| Day-over-Day (DoD) | `LAG(val, 1)` | Daily |
| Week-over-Week (WoW) | `LAG(val, 1)` | Weekly |
| Month-over-Month (MoM) | `LAG(val, 1)` | Monthly |
| Quarter-over-Quarter (QoQ) | `LAG(val, 1)` | Quarterly |
| Year-over-Year (YoY) | `LAG(val, 1)` | Yearly |
| Same Month Last Year (SPLY) | `LAG(val, 12)` | Monthly |
| Same Week Last Year | `LAG(val, 52)` | Weekly |
| Same Day Last Year | `LAG(val, 365)` | Daily |
| CAGR | `POWER(end/start, 1/n) - 1` | Yearly |
| Indexed Growth | `100 * val / FIRST_VALUE(val)` | Any |

---

*Part of the SQL Data Analysis Cheat Sheet series*
