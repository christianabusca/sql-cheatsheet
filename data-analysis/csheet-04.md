# 📊 SQL for Data Analysis — Level 4: Advanced Analytics & Business Metrics
> *Cohort analysis, funnel metrics, RFM segmentation, forecasting prep, and more*

---

## Table of Contents
- [1. Cohort Analysis](#1-cohort-analysis)
- [2. Funnel Analysis](#2-funnel-analysis)
- [3. RFM Customer Segmentation](#3-rfm-customer-segmentation)
- [4. Retention & Churn Analysis](#4-retention--churn-analysis)
- [5. Revenue Metrics (MRR, ARR, LTV)](#5-revenue-metrics-mrr-arr-ltv)
- [6. Statistical Functions](#6-statistical-functions)
- [7. Time-Series Analysis](#7-time-series-analysis)
- [8. Advanced Practice Patterns](#8-advanced-practice-patterns)

---

## 1. Cohort Analysis

Cohort analysis groups users by a shared starting event (e.g., signup month) and tracks their behavior over time.

```sql
-- Step 1: Assign each customer their cohort (month they first ordered)
WITH cohort_assignment AS (
  SELECT
    customer_id,
    DATE_TRUNC('month', MIN(order_date)) AS cohort_month
  FROM orders
  GROUP BY customer_id
),

-- Step 2: Calculate how many months after cohort each order occurred
order_periods AS (
  SELECT
    o.customer_id,
    ca.cohort_month,
    DATE_TRUNC('month', o.order_date) AS order_month,
    EXTRACT(YEAR FROM AGE(
      DATE_TRUNC('month', o.order_date),
      ca.cohort_month
    )) * 12 +
    EXTRACT(MONTH FROM AGE(
      DATE_TRUNC('month', o.order_date),
      ca.cohort_month
    )) AS period_number  -- 0 = acquisition month, 1 = 1 month later, etc.
  FROM orders AS o
  INNER JOIN cohort_assignment AS ca ON o.customer_id = ca.customer_id
),

-- Step 3: Count cohort size
cohort_sizes AS (
  SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_size
  FROM cohort_assignment
  GROUP BY cohort_month
)

-- Step 4: Build retention table
SELECT
  op.cohort_month,
  cs.cohort_size,
  op.period_number,
  COUNT(DISTINCT op.customer_id) AS active_customers,
  ROUND(
    100.0 * COUNT(DISTINCT op.customer_id) / cs.cohort_size, 1
  ) AS retention_rate_pct
FROM order_periods AS op
INNER JOIN cohort_sizes AS cs ON op.cohort_month = cs.cohort_month
GROUP BY op.cohort_month, cs.cohort_size, op.period_number
ORDER BY op.cohort_month, op.period_number;

/*
cohort_month | cohort_size | period | active | retention_pct
-------------|-------------|--------|--------|-------------
2024-01-01   |         500 |      0 |    500 |       100.0
2024-01-01   |         500 |      1 |    325 |        65.0  ← month 2
2024-01-01   |         500 |      2 |    210 |        42.0  ← month 3
2024-02-01   |         480 |      0 |    480 |       100.0
*/
```

### Revenue Cohort Analysis

```sql
WITH cohorts AS (
  SELECT customer_id,
         DATE_TRUNC('month', MIN(order_date)) AS cohort_month
  FROM orders GROUP BY customer_id
)
SELECT
  c.cohort_month,
  EXTRACT(MONTH FROM AGE(DATE_TRUNC('month', o.order_date), c.cohort_month)) AS period,
  COUNT(DISTINCT o.customer_id) AS customers,
  SUM(o.total_amount) AS revenue,
  ROUND(AVG(o.total_amount), 2) AS avg_order_value
FROM orders AS o
INNER JOIN cohorts AS c USING(customer_id)
GROUP BY c.cohort_month, period
ORDER BY c.cohort_month, period;
```

---

## 2. Funnel Analysis

Track how users progress (or drop off) through a multi-step process.

```sql
-- E-commerce conversion funnel
WITH funnel AS (
  SELECT
    COUNT(DISTINCT CASE WHEN event = 'page_view'       THEN session_id END) AS step1_views,
    COUNT(DISTINCT CASE WHEN event = 'product_viewed'  THEN session_id END) AS step2_product,
    COUNT(DISTINCT CASE WHEN event = 'add_to_cart'     THEN session_id END) AS step3_cart,
    COUNT(DISTINCT CASE WHEN event = 'checkout_start'  THEN session_id END) AS step4_checkout,
    COUNT(DISTINCT CASE WHEN event = 'purchase'        THEN session_id END) AS step5_purchase
  FROM events
  WHERE event_date >= CURRENT_DATE - 30
)
SELECT
  'Page View'       AS step, step1_views AS users,
  100.0             AS conversion_from_top,
  NULL              AS conversion_from_prev
FROM funnel

UNION ALL SELECT 'Product Viewed', step2_product,
  ROUND(100.0 * step2_product / NULLIF(step1_views, 0), 1),
  ROUND(100.0 * step2_product / NULLIF(step1_views, 0), 1)
FROM funnel

UNION ALL SELECT 'Add to Cart', step3_cart,
  ROUND(100.0 * step3_cart / NULLIF(step1_views, 0), 1),
  ROUND(100.0 * step3_cart / NULLIF(step2_product, 0), 1)
FROM funnel

UNION ALL SELECT 'Checkout Start', step4_checkout,
  ROUND(100.0 * step4_checkout / NULLIF(step1_views, 0), 1),
  ROUND(100.0 * step4_checkout / NULLIF(step3_cart, 0), 1)
FROM funnel

UNION ALL SELECT 'Purchase', step5_purchase,
  ROUND(100.0 * step5_purchase / NULLIF(step1_views, 0), 1),
  ROUND(100.0 * step5_purchase / NULLIF(step4_checkout, 0), 1)
FROM funnel;
```

### Ordered Funnel (ensures proper step sequence per user)

```sql
WITH user_funnel AS (
  SELECT
    user_id,
    MIN(CASE WHEN event = 'signup'        THEN event_ts END) AS signup_ts,
    MIN(CASE WHEN event = 'onboarding'    THEN event_ts END) AS onboard_ts,
    MIN(CASE WHEN event = 'first_action'  THEN event_ts END) AS action_ts,
    MIN(CASE WHEN event = 'subscription'  THEN event_ts END) AS sub_ts
  FROM events
  GROUP BY user_id
)
SELECT
  COUNT(*) AS signups,
  COUNT(CASE WHEN onboard_ts > signup_ts THEN 1 END) AS completed_onboarding,
  COUNT(CASE WHEN action_ts > onboard_ts THEN 1 END) AS took_first_action,
  COUNT(CASE WHEN sub_ts > action_ts     THEN 1 END) AS subscribed
FROM user_funnel;
```

---

## 3. RFM Customer Segmentation

RFM scores customers on **Recency**, **Frequency**, and **Monetary** value.

```sql
-- Step 1: Calculate raw RFM values
WITH rfm_raw AS (
  SELECT
    customer_id,
    MAX(order_date) AS last_order_date,
    CURRENT_DATE - MAX(order_date) AS days_since_last_order,  -- Recency
    COUNT(DISTINCT order_id) AS order_count,                  -- Frequency
    SUM(total_amount) AS total_spent                          -- Monetary
  FROM orders
  WHERE status = 'completed'
  GROUP BY customer_id
),

-- Step 2: Score each dimension 1-5 using NTILE
rfm_scores AS (
  SELECT
    customer_id,
    days_since_last_order,
    order_count,
    total_spent,
    NTILE(5) OVER(ORDER BY days_since_last_order ASC)  AS r_score, -- lower recency = better
    NTILE(5) OVER(ORDER BY order_count DESC)            AS f_score,
    NTILE(5) OVER(ORDER BY total_spent DESC)            AS m_score
  FROM rfm_raw
),

-- Step 3: Combine scores
rfm_combined AS (
  SELECT *,
    CONCAT(r_score, f_score, m_score) AS rfm_code,
    (r_score + f_score + m_score) AS rfm_total
  FROM rfm_scores
)

-- Step 4: Assign segments
SELECT
  customer_id,
  r_score, f_score, m_score,
  rfm_total,
  CASE
    WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
    WHEN r_score >= 3 AND f_score >= 3                  THEN 'Loyal Customers'
    WHEN r_score >= 4                                   THEN 'Recent Customers'
    WHEN f_score >= 4 AND r_score <= 2                  THEN 'At Risk'
    WHEN r_score = 1 AND f_score = 1                    THEN 'Lost'
    WHEN m_score >= 4 AND r_score <= 2                  THEN 'Cant Lose Them'
    ELSE 'Needs Attention'
  END AS segment
FROM rfm_combined
ORDER BY rfm_total DESC;
```

---

## 4. Retention & Churn Analysis

### Month-over-Month Retention

```sql
WITH monthly_active AS (
  SELECT DISTINCT
    customer_id,
    DATE_TRUNC('month', order_date) AS active_month
  FROM orders
),
retention AS (
  SELECT
    m1.active_month AS base_month,
    COUNT(DISTINCT m1.customer_id) AS base_users,
    COUNT(DISTINCT m2.customer_id) AS retained_next_month
  FROM monthly_active AS m1
  LEFT JOIN monthly_active AS m2
    ON m1.customer_id = m2.customer_id
    AND m2.active_month = m1.active_month + INTERVAL '1 month'
  GROUP BY m1.active_month
)
SELECT
  base_month,
  base_users,
  retained_next_month,
  ROUND(100.0 * retained_next_month / NULLIF(base_users, 0), 1) AS retention_rate_pct,
  base_users - retained_next_month AS churned_users
FROM retention
ORDER BY base_month;
```

### Customer Lifetime Prediction (Simple)

```sql
WITH customer_history AS (
  SELECT
    customer_id,
    COUNT(*) AS total_orders,
    MIN(order_date) AS first_order,
    MAX(order_date) AS last_order,
    SUM(total_amount) AS total_spent,
    AVG(total_amount) AS avg_order_value,
    CURRENT_DATE - MAX(order_date) AS days_since_last,
    MAX(order_date)::date - MIN(order_date)::date AS customer_lifespan_days
  FROM orders
  GROUP BY customer_id
  HAVING COUNT(*) >= 2
)
SELECT
  customer_id,
  total_orders,
  avg_order_value,
  customer_lifespan_days,
  -- Estimated purchase frequency (orders per day)
  ROUND(total_orders::numeric / NULLIF(customer_lifespan_days, 0), 4) AS daily_purchase_rate,
  -- Projected LTV for 365 more days
  ROUND(
    avg_order_value * (total_orders::numeric / NULLIF(customer_lifespan_days, 0)) * 365,
    2
  ) AS projected_annual_ltv,
  -- Churn risk flag
  CASE
    WHEN days_since_last > 180 THEN 'High Risk'
    WHEN days_since_last > 90  THEN 'Medium Risk'
    ELSE 'Active'
  END AS churn_risk
FROM customer_history
ORDER BY projected_annual_ltv DESC;
```

---

## 5. Revenue Metrics (MRR, ARR, LTV)

### Monthly Recurring Revenue (MRR) Movements

```sql
WITH monthly_subs AS (
  SELECT
    customer_id,
    DATE_TRUNC('month', start_date) AS start_month,
    DATE_TRUNC('month', end_date)   AS end_month,
    monthly_amount
  FROM subscriptions
),
mrr_timeline AS (
  SELECT
    start_month AS month,
    SUM(monthly_amount) AS new_mrr
  FROM monthly_subs
  GROUP BY start_month

  UNION ALL

  SELECT
    end_month AS month,
    -SUM(monthly_amount) AS churned_mrr
  FROM monthly_subs
  WHERE end_month IS NOT NULL
  GROUP BY end_month
)
SELECT
  month,
  SUM(new_mrr + churned_mrr) AS net_new_mrr,
  SUM(SUM(new_mrr + churned_mrr)) OVER(ORDER BY month) AS total_mrr
FROM mrr_timeline
GROUP BY month
ORDER BY month;
```

### Net Revenue Retention (NRR)

```sql
WITH cohort_revenue AS (
  SELECT
    customer_id,
    DATE_TRUNC('month', charge_date) AS month,
    SUM(amount) AS mrr
  FROM charges
  GROUP BY customer_id, DATE_TRUNC('month', charge_date)
)
SELECT
  curr.month,
  SUM(prev.mrr) AS starting_mrr,
  SUM(curr.mrr) AS ending_mrr,
  ROUND(100.0 * SUM(curr.mrr) / NULLIF(SUM(prev.mrr), 0), 1) AS nrr_pct
FROM cohort_revenue AS curr
INNER JOIN cohort_revenue AS prev
  ON curr.customer_id = prev.customer_id
  AND curr.month = prev.month + INTERVAL '1 month'
GROUP BY curr.month
ORDER BY curr.month;
-- NRR > 100% = expansion revenue exceeds churn
```

---

## 6. Statistical Functions

```sql
-- Distribution analysis
SELECT
  ROUND(AVG(order_value), 2)          AS mean,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY order_value) AS median,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY order_value) AS p25,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY order_value) AS p75,
  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY order_value) AS p90,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY order_value) AS p95,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY order_value) AS p99,
  STDDEV(order_value)                  AS std_dev,
  VARIANCE(order_value)                AS variance,
  MIN(order_value)                     AS min_val,
  MAX(order_value)                     AS max_val
FROM orders;

-- Correlation between ad spend and revenue (Pearson)
SELECT
  CORR(ad_spend, revenue) AS correlation,
  REGR_SLOPE(revenue, ad_spend) AS slope,
  REGR_INTERCEPT(revenue, ad_spend) AS intercept,
  REGR_R2(revenue, ad_spend) AS r_squared
FROM marketing_data;
-- r_squared = 0.85 means ad spend explains 85% of revenue variance

-- Histogram buckets
SELECT
  bucket,
  COUNT(*) AS frequency,
  REPEAT('█', (COUNT(*) / 10)::INT) AS bar  -- visual bar
FROM (
  SELECT
    WIDTH_BUCKET(order_value, 0, 1000, 10) AS bucket
  FROM orders
) AS bucketed
GROUP BY bucket
ORDER BY bucket;
```

---

## 7. Time-Series Analysis

### Gap Detection (Missing Dates)

```sql
-- Find dates where no orders were placed
WITH date_spine AS (
  SELECT generate_series(
    '2024-01-01'::date,
    '2024-12-31'::date,
    '1 day'::interval
  )::date AS expected_date
),
order_dates AS (
  SELECT DISTINCT DATE(order_date) AS order_date FROM orders
)
SELECT ds.expected_date AS missing_date
FROM date_spine AS ds
LEFT JOIN order_dates AS od ON ds.expected_date = od.order_date
WHERE od.order_date IS NULL
ORDER BY missing_date;
```

### Seasonality Analysis

```sql
-- Day of week patterns
SELECT
  TO_CHAR(order_date, 'Day') AS day_of_week,
  EXTRACT(DOW FROM order_date) AS dow_num,
  COUNT(*) AS orders,
  SUM(total_amount) AS revenue,
  ROUND(AVG(total_amount), 2) AS avg_order_value
FROM orders
GROUP BY TO_CHAR(order_date, 'Day'), EXTRACT(DOW FROM order_date)
ORDER BY dow_num;

-- Hour of day patterns
SELECT
  EXTRACT(HOUR FROM created_at) AS hour_of_day,
  COUNT(*) AS events,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_of_daily
FROM user_events
GROUP BY EXTRACT(HOUR FROM created_at)
ORDER BY hour_of_day;
```

---

## 8. Advanced Practice Patterns

### Product-Level Growth Accounting
```sql
WITH monthly_product AS (
  SELECT
    p.product_id,
    p.product_name,
    DATE_TRUNC('month', o.order_date) AS month,
    SUM(oi.revenue) AS revenue
  FROM order_items AS oi
  INNER JOIN products AS p USING(product_id)
  INNER JOIN orders AS o USING(order_id)
  GROUP BY p.product_id, p.product_name, DATE_TRUNC('month', o.order_date)
)
SELECT
  product_name,
  month,
  revenue,
  LAG(revenue) OVER(PARTITION BY product_id ORDER BY month) AS prev_month_rev,
  ROUND(
    100.0 * (revenue - LAG(revenue) OVER(PARTITION BY product_id ORDER BY month))
            / NULLIF(LAG(revenue) OVER(PARTITION BY product_id ORDER BY month), 0),
    1
  ) AS mom_growth_pct,
  CASE
    WHEN LAG(revenue) OVER(PARTITION BY product_id ORDER BY month) IS NULL THEN 'New'
    WHEN revenue > LAG(revenue) OVER(PARTITION BY product_id ORDER BY month) THEN '📈 Growing'
    WHEN revenue < LAG(revenue) OVER(PARTITION BY product_id ORDER BY month) THEN '📉 Declining'
    ELSE '→ Stable'
  END AS trend
FROM monthly_product
ORDER BY product_name, month;
```

---

## 📌 Advanced Analytics Quick Reference

| Analysis Type | Key Technique |
|--------------|---------------|
| Cohort Retention | `AGE()` + `PARTITION BY cohort` |
| Funnel | `COUNT(DISTINCT CASE WHEN step)` |
| RFM Scoring | `NTILE(5)` on recency/frequency/monetary |
| Churn Rate | Left join month-over-month |
| MRR Movement | `UNION ALL` new + churned revenue |
| Distribution | `PERCENTILE_CONT()`, `STDDEV()` |
| Correlation | `CORR()`, `REGR_R2()` |
| Seasonality | `EXTRACT(DOW/HOUR)` + `GROUP BY` |
| Gap Detection | `generate_series()` + `LEFT JOIN` |

---

*← Previous: README_03_Window_Functions.md | Next → README_05_Expert_Patterns.md*
