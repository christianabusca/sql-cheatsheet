# 📊 SQL for Data Analysis — Essential DA Computations
## LTV, Percentile Scoring, Market Basket, Deduplication, Pivot, Contribution & More
> *The complete toolkit of must-know analytical calculations for data analysts*

---

## Table of Contents
- [1. Contribution & Share of Total](#1-contribution--share-of-total)
- [2. Running Rank & Percentile Rank](#2-running-rank--percentile-rank)
- [3. Customer Lifetime Value (LTV/CLV)](#3-customer-lifetime-value-ltvclv)
- [4. Market Basket Analysis](#4-market-basket-analysis)
- [5. Deduplication & Unique Record Identification](#5-deduplication--unique-record-identification)
- [6. Pivot & Unpivot](#6-pivot--unpivot)
- [7. Running Difference & Delta Analysis](#7-running-difference--delta-analysis)
- [8. Gap & Island Problems](#8-gap--island-problems)
- [9. Weighted Averages & Weighted Metrics](#9-weighted-averages--weighted-metrics)
- [10. Pareto / 80-20 Analysis](#10-pareto--80-20-analysis)
- [11. Fill Forward / Fill Backward (LOCF)](#11-fill-forward--fill-backward-locf)
- [12. Date & Time Calculations](#12-date--time-calculations)

---

## 1. Contribution & Share of Total

```sql
-- Each product's % of total revenue
SELECT
  product_name,
  category,
  SUM(revenue) AS product_revenue,

  -- Share of grand total
  ROUND(
    100.0 * SUM(revenue) / SUM(SUM(revenue)) OVER(),
    2
  ) AS pct_of_total,

  -- Share within category
  ROUND(
    100.0 * SUM(revenue) / SUM(SUM(revenue)) OVER(PARTITION BY category),
    2
  ) AS pct_of_category,

  -- Rank within category by revenue
  RANK() OVER(PARTITION BY category ORDER BY SUM(revenue) DESC) AS rank_in_category

FROM sales
INNER JOIN products USING(product_id)
GROUP BY product_name, category
ORDER BY product_revenue DESC;

-- Revenue contribution: running cumulative %
SELECT
  product_name,
  revenue,
  ROUND(100.0 * revenue / SUM(revenue) OVER(), 2) AS share_pct,
  ROUND(
    SUM(100.0 * revenue / SUM(revenue) OVER())
    OVER(ORDER BY revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS cumulative_share_pct
FROM (
  SELECT product_name, SUM(revenue) AS revenue FROM sales GROUP BY product_name
) AS product_totals
ORDER BY revenue DESC;

-- Category mix shift: how did the share change vs last year?
WITH category_yearly AS (
  SELECT
    category,
    EXTRACT(YEAR FROM order_date)::int AS yr,
    SUM(revenue) AS revenue
  FROM sales GROUP BY 1, 2
),
with_share AS (
  SELECT *,
    ROUND(100.0 * revenue / SUM(revenue) OVER(PARTITION BY yr), 2) AS share_pct
  FROM category_yearly
)
SELECT
  cy.category,
  cy.yr AS year,
  cy.share_pct AS current_share,
  py.share_pct AS prior_share,
  ROUND(cy.share_pct - py.share_pct, 2) AS share_point_change
FROM with_share AS cy
LEFT JOIN with_share AS py
  ON cy.category = py.category AND cy.yr = py.yr + 1
ORDER BY cy.yr, share_point_change DESC;
```

---

## 2. Running Rank & Percentile Rank

```sql
-- Percentile rank: what % of values are below this row?
SELECT
  customer_id,
  lifetime_value,
  PERCENT_RANK() OVER(ORDER BY lifetime_value)          AS percent_rank,   -- 0 to 1
  CUME_DIST()    OVER(ORDER BY lifetime_value)          AS cumulative_dist, -- 0 to 1
  NTILE(100)     OVER(ORDER BY lifetime_value DESC)     AS percentile,     -- 1=top 1%
  NTILE(10)      OVER(ORDER BY lifetime_value DESC)     AS decile,         -- 1=top 10%
  NTILE(4)       OVER(ORDER BY lifetime_value DESC)     AS quartile        -- 1=top 25%
FROM customer_metrics;

-- Label percentile groups
SELECT
  customer_id,
  lifetime_value,
  CASE NTILE(100) OVER(ORDER BY lifetime_value DESC)
    WHEN 1  THEN 'Top 1%'
    WHEN 2  THEN 'Top 2%'
    WHEN 3  THEN 'Top 3%'
    WHEN 4  THEN 'Top 4%'
    WHEN 5  THEN 'Top 5%'
    ELSE         'Remaining 95%'
  END AS tier
FROM customer_metrics;

-- Conditional rank (rank only within a filtered set)
SELECT
  product_id,
  category,
  revenue,
  RANK() OVER(PARTITION BY category ORDER BY revenue DESC) AS rank_in_cat,
  ROUND(PERCENT_RANK() OVER(PARTITION BY category ORDER BY revenue), 4) AS pct_rank_in_cat
FROM product_revenue;
```

---

## 3. Customer Lifetime Value (LTV/CLV)

```sql
-- Historic LTV (what they've actually spent)
SELECT
  customer_id,
  MIN(order_date) AS first_order,
  MAX(order_date) AS last_order,
  COUNT(*)        AS total_orders,
  SUM(total_amount) AS historic_ltv,
  ROUND(AVG(total_amount), 2) AS avg_order_value,
  CURRENT_DATE - MIN(order_date)::date AS customer_age_days
FROM orders
WHERE status = 'completed'
GROUP BY customer_id
ORDER BY historic_ltv DESC;

-- Predicted LTV using BG/NBD-inspired SQL approximation
-- Formula: Predicted purchases × AOV × Gross Margin
WITH customer_stats AS (
  SELECT
    customer_id,
    COUNT(*) AS purchase_count,
    SUM(total_amount) AS total_spent,
    ROUND(AVG(total_amount), 2) AS aov,
    MIN(order_date) AS first_order,
    MAX(order_date) AS last_order,
    CURRENT_DATE - MAX(order_date)::date AS days_since_last,
    MAX(order_date)::date - MIN(order_date)::date AS lifespan_days
  FROM orders
  WHERE status = 'completed'
  GROUP BY customer_id
  HAVING COUNT(*) >= 2
),
ltv_model AS (
  SELECT *,
    -- Purchase frequency per day
    ROUND(purchase_count::numeric / NULLIF(lifespan_days, 0), 6) AS daily_freq,
    -- Projected purchases in next 365 days
    ROUND(purchase_count::numeric / NULLIF(lifespan_days, 0) * 365, 2) AS projected_annual_purchases
  FROM customer_stats
)
SELECT
  customer_id,
  purchase_count,
  aov,
  days_since_last,
  projected_annual_purchases,
  -- Predicted 1-year LTV (assuming 40% gross margin)
  ROUND(projected_annual_purchases * aov * 0.40, 2) AS predicted_annual_ltv,
  -- Predicted 3-year LTV
  ROUND(projected_annual_purchases * aov * 0.40 * 3, 2) AS predicted_3yr_ltv,
  -- Churn risk adjustment
  CASE
    WHEN days_since_last > 180 THEN ROUND(projected_annual_purchases * aov * 0.40 * 0.2, 2)
    WHEN days_since_last > 90  THEN ROUND(projected_annual_purchases * aov * 0.40 * 0.6, 2)
    ELSE                            ROUND(projected_annual_purchases * aov * 0.40, 2)
  END AS risk_adjusted_ltv
FROM ltv_model
ORDER BY predicted_annual_ltv DESC;

-- LTV by acquisition channel (to calculate payback period)
SELECT
  acquisition_channel,
  COUNT(DISTINCT customer_id) AS customers,
  ROUND(AVG(historic_ltv), 2) AS avg_ltv,
  ROUND(AVG(aov), 2) AS avg_aov,
  ROUND(AVG(total_orders), 1) AS avg_orders
FROM (
  SELECT
    c.acquisition_channel,
    c.customer_id,
    COUNT(o.order_id) AS total_orders,
    SUM(o.total_amount) AS historic_ltv,
    AVG(o.total_amount) AS aov
  FROM customers AS c
  LEFT JOIN orders AS o ON c.customer_id = o.customer_id
  WHERE o.status = 'completed'
  GROUP BY c.acquisition_channel, c.customer_id
) AS customer_ltv
GROUP BY acquisition_channel
ORDER BY avg_ltv DESC;
```

---

## 4. Market Basket Analysis

Find which products are most frequently bought together.

```sql
-- Pairwise product affinity (items bought in the same order)
WITH order_products AS (
  SELECT DISTINCT order_id, product_id
  FROM order_items
),
product_pairs AS (
  SELECT
    a.product_id AS product_a,
    b.product_id AS product_b,
    COUNT(DISTINCT a.order_id) AS co_occurrences
  FROM order_products AS a
  INNER JOIN order_products AS b
    ON a.order_id = b.order_id
    AND a.product_id < b.product_id  -- avoid duplicates like (A,B) and (B,A)
  GROUP BY a.product_id, b.product_id
),
product_counts AS (
  SELECT product_id, COUNT(DISTINCT order_id) AS order_count
  FROM order_items
  GROUP BY product_id
)
SELECT
  pp.product_a,
  pp.product_b,
  pp.co_occurrences,
  pc_a.order_count AS orders_with_a,
  pc_b.order_count AS orders_with_b,

  -- Support: how often does this pair appear?
  ROUND(100.0 * pp.co_occurrences / (SELECT COUNT(DISTINCT order_id) FROM orders), 4)
    AS support_pct,

  -- Confidence: given customer bought A, how likely are they to buy B?
  ROUND(100.0 * pp.co_occurrences / NULLIF(pc_a.order_count, 0), 2)
    AS confidence_a_to_b_pct,

  -- Lift: how much more likely is B given A, vs B in general?
  ROUND(
    (pp.co_occurrences::numeric / NULLIF(pc_a.order_count, 0))
    / NULLIF(pc_b.order_count::numeric / (SELECT COUNT(DISTINCT order_id) FROM orders), 0),
    3
  ) AS lift
  -- Lift > 1 = positively associated, < 1 = negatively associated, = 1 = independent

FROM product_pairs AS pp
INNER JOIN product_counts AS pc_a ON pp.product_a = pc_a.product_id
INNER JOIN product_counts AS pc_b ON pp.product_b = pc_b.product_id
WHERE pp.co_occurrences >= 10    -- minimum support threshold
ORDER BY lift DESC
LIMIT 50;
```

---

## 5. Deduplication & Unique Record Identification

```sql
-- Keep only the most recent record per customer
SELECT *
FROM (
  SELECT *,
    ROW_NUMBER() OVER(
      PARTITION BY customer_id
      ORDER BY updated_at DESC    -- most recent first
    ) AS rn
  FROM customers_raw
) AS deduped
WHERE rn = 1;

-- Find exact duplicate rows
SELECT
  email,
  first_name,
  last_name,
  COUNT(*) AS duplicate_count
FROM customers
GROUP BY email, first_name, last_name
HAVING COUNT(*) > 1;

-- Soft deduplication: keep first, flag later duplicates
SELECT
  customer_id,
  email,
  created_at,
  ROW_NUMBER() OVER(PARTITION BY email ORDER BY created_at) AS occurrence,
  CASE WHEN ROW_NUMBER() OVER(PARTITION BY email ORDER BY created_at) = 1
       THEN 'original' ELSE 'duplicate' END AS record_type
FROM customers;

-- Deduplicate events: keep only distinct user-event-date combinations
SELECT DISTINCT ON (user_id, event_type, DATE(event_ts))
  user_id,
  event_type,
  DATE(event_ts) AS event_date,
  event_ts       AS first_occurrence
FROM events
ORDER BY user_id, event_type, DATE(event_ts), event_ts ASC;

-- Delete duplicates (keep lowest id)
DELETE FROM customers
WHERE customer_id NOT IN (
  SELECT MIN(customer_id)
  FROM customers
  GROUP BY email
);
```

---

## 6. Pivot & Unpivot

### Pivot (Rows → Columns)

```sql
-- Revenue by month as columns
SELECT
  product_name,
  SUM(CASE WHEN TO_CHAR(order_date,'YYYY-MM') = '2024-01' THEN revenue ELSE 0 END) AS "Jan 2024",
  SUM(CASE WHEN TO_CHAR(order_date,'YYYY-MM') = '2024-02' THEN revenue ELSE 0 END) AS "Feb 2024",
  SUM(CASE WHEN TO_CHAR(order_date,'YYYY-MM') = '2024-03' THEN revenue ELSE 0 END) AS "Mar 2024",
  SUM(CASE WHEN TO_CHAR(order_date,'YYYY-MM') = '2024-04' THEN revenue ELSE 0 END) AS "Apr 2024",
  SUM(CASE WHEN TO_CHAR(order_date,'YYYY-MM') = '2024-05' THEN revenue ELSE 0 END) AS "May 2024",
  SUM(CASE WHEN TO_CHAR(order_date,'YYYY-MM') = '2024-06' THEN revenue ELSE 0 END) AS "Jun 2024",
  SUM(revenue) AS total
FROM sales
INNER JOIN products USING(product_id)
GROUP BY product_name
ORDER BY total DESC;

-- Status breakdown pivot
SELECT
  region,
  COUNT(CASE WHEN status = 'completed'  THEN 1 END) AS completed,
  COUNT(CASE WHEN status = 'pending'    THEN 1 END) AS pending,
  COUNT(CASE WHEN status = 'cancelled'  THEN 1 END) AS cancelled,
  COUNT(CASE WHEN status = 'refunded'   THEN 1 END) AS refunded,
  COUNT(*) AS total,
  ROUND(100.0 * COUNT(CASE WHEN status = 'completed' THEN 1 END) / COUNT(*), 1) AS completion_rate
FROM orders
GROUP BY region;
```

### Unpivot (Columns → Rows)

```sql
-- Turn multiple metric columns into rows using CROSS JOIN LATERAL
SELECT
  customer_id,
  metric_name,
  metric_value
FROM customer_metrics
CROSS JOIN LATERAL (VALUES
  ('revenue_q1',  revenue_q1),
  ('revenue_q2',  revenue_q2),
  ('revenue_q3',  revenue_q3),
  ('revenue_q4',  revenue_q4),
  ('order_count', order_count::numeric)
) AS metrics(metric_name, metric_value);
```

---

## 7. Running Difference & Delta Analysis

```sql
-- Day-by-day change in inventory levels
SELECT
  product_id,
  date,
  stock_level,
  stock_level - LAG(stock_level) OVER(PARTITION BY product_id ORDER BY date)
    AS daily_change,
  CASE
    WHEN stock_level < LAG(stock_level) OVER(PARTITION BY product_id ORDER BY date) THEN 'Sold'
    WHEN stock_level > LAG(stock_level) OVER(PARTITION BY product_id ORDER BY date) THEN 'Restocked'
    ELSE 'No Change'
  END AS movement_type
FROM inventory_history;

-- Detect sign changes (transitions from positive to negative growth)
WITH monthly AS (
  SELECT DATE_TRUNC('month', order_date)::date AS month, SUM(revenue) AS revenue
  FROM orders GROUP BY 1
),
with_mom AS (
  SELECT month, revenue,
    revenue - LAG(revenue) OVER(ORDER BY month) AS mom_change
  FROM monthly
)
SELECT
  month,
  revenue,
  mom_change,
  SIGN(mom_change::numeric) AS direction,
  CASE
    WHEN SIGN(mom_change::numeric) != SIGN(LAG(mom_change) OVER(ORDER BY month)::numeric)
    THEN '🔄 Trend Reversal'
    ELSE NULL
  END AS reversal_flag
FROM with_mom;
```

---

## 8. Gap & Island Problems

Find consecutive sequences and breaks in time-series or ordered data.

```sql
-- ISLANDS: Find consecutive days where revenue > threshold
WITH daily AS (
  SELECT sale_date, SUM(revenue) AS revenue FROM sales GROUP BY 1
),
flagged AS (
  SELECT sale_date, revenue,
    CASE WHEN revenue >= 1000 THEN 1 ELSE 0 END AS above_threshold
  FROM daily
),
islands AS (
  SELECT sale_date, revenue, above_threshold,
    sale_date - (ROW_NUMBER() OVER(ORDER BY sale_date) * INTERVAL '1 day')::interval AS grp
  FROM flagged
  WHERE above_threshold = 1
)
SELECT
  MIN(sale_date) AS streak_start,
  MAX(sale_date) AS streak_end,
  COUNT(*) AS consecutive_days,
  SUM(revenue) AS total_revenue_in_streak
FROM islands
GROUP BY grp
ORDER BY consecutive_days DESC;

-- GAPS: Find periods where there were NO orders
WITH date_spine AS (
  SELECT generate_series('2024-01-01'::date, '2024-12-31'::date, '1 day')::date AS day
),
order_days AS (
  SELECT DISTINCT DATE(order_date) AS day FROM orders
),
gaps AS (
  SELECT ds.day,
    CASE WHEN od.day IS NULL THEN 1 ELSE 0 END AS is_gap,
    SUM(CASE WHEN od.day IS NOT NULL THEN 1 ELSE 0 END)
      OVER(ORDER BY ds.day) AS gap_group
  FROM date_spine AS ds
  LEFT JOIN order_days AS od USING(day)
)
SELECT
  MIN(day) AS gap_start,
  MAX(day) AS gap_end,
  COUNT(*) AS gap_days
FROM gaps
WHERE is_gap = 1
GROUP BY gap_group
ORDER BY gap_days DESC;
```

---

## 9. Weighted Averages & Weighted Metrics

```sql
-- Weighted average price (weights by units sold, not just count of products)
SELECT
  category,
  -- Unweighted average (wrong: treats a $5 and a $500 product equally)
  ROUND(AVG(price), 2) AS simple_avg_price,
  -- Weighted average (weights by actual units sold)
  ROUND(SUM(price * units_sold) / NULLIF(SUM(units_sold), 0), 2) AS weighted_avg_price
FROM product_sales
GROUP BY category;

-- Net Promoter Score (NPS)
-- Scores 9-10 = Promoters, 7-8 = Passives, 0-6 = Detractors
-- NPS = % Promoters - % Detractors
SELECT
  survey_date,
  COUNT(*) AS total_responses,
  COUNT(CASE WHEN score >= 9 THEN 1 END) AS promoters,
  COUNT(CASE WHEN score BETWEEN 7 AND 8 THEN 1 END) AS passives,
  COUNT(CASE WHEN score <= 6 THEN 1 END) AS detractors,
  ROUND(
    100.0 * COUNT(CASE WHEN score >= 9 THEN 1 END) / NULLIF(COUNT(*), 0) -
    100.0 * COUNT(CASE WHEN score <= 6 THEN 1 END) / NULLIF(COUNT(*), 0),
    1
  ) AS nps_score
FROM nps_survey
GROUP BY survey_date
ORDER BY survey_date;

-- Weighted rolling average (more recent days carry more weight in the mean)
SELECT
  sale_date,
  daily_revenue,
  SUM(daily_revenue * weight) OVER(ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
  / NULLIF(SUM(weight) OVER(ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0)
    AS weighted_avg
FROM (
  SELECT sale_date, daily_revenue, ROW_NUMBER() OVER(ORDER BY sale_date) AS weight
  FROM daily_sales
) AS weighted;
```

---

## 10. Pareto / 80-20 Analysis

Identify the 20% of products/customers driving 80% of revenue.

```sql
WITH product_revenue AS (
  SELECT product_id, SUM(revenue) AS revenue
  FROM sales GROUP BY product_id
),
ranked AS (
  SELECT
    product_id,
    revenue,
    SUM(revenue) OVER() AS total_revenue,
    RANK() OVER(ORDER BY revenue DESC) AS rank,
    COUNT(*) OVER() AS total_products
  FROM product_revenue
),
cumulative AS (
  SELECT *,
    ROUND(100.0 * revenue / total_revenue, 2) AS share_pct,
    ROUND(
      100.0 * SUM(revenue) OVER(ORDER BY revenue DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      / total_revenue, 2
    ) AS cumulative_share_pct,
    ROUND(100.0 * rank / total_products, 1) AS product_percentile
  FROM ranked
)
SELECT
  product_id,
  revenue,
  share_pct,
  cumulative_share_pct,
  product_percentile,
  CASE
    WHEN cumulative_share_pct <= 80 THEN 'A: Top 80% Revenue'
    WHEN cumulative_share_pct <= 95 THEN 'B: Next 15% Revenue'
    ELSE 'C: Bottom 5% Revenue'
  END AS pareto_class
FROM cumulative
ORDER BY revenue DESC;

-- Summary: what % of products drive 80% of revenue?
SELECT
  COUNT(*) AS products_in_top_80pct,
  ROUND(100.0 * COUNT(*) / MAX(total_products), 1) AS pct_of_product_catalog
FROM (
  SELECT *, COUNT(*) OVER() AS total_products,
    SUM(revenue) OVER(ORDER BY revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      / NULLIF(SUM(revenue) OVER(), 0) AS cum_share
  FROM product_revenue
) AS x
WHERE cum_share <= 0.80;
```

---

## 11. Fill Forward / Fill Backward (LOCF)

Last Observation Carried Forward — fill in missing values using the most recent known value.

```sql
-- Fill forward NULL prices using last non-null value
WITH daily_prices AS (
  SELECT
    product_id,
    price_date,
    price,
    -- Create groups: same group = same "last known value"
    COUNT(price) OVER(PARTITION BY product_id ORDER BY price_date) AS fill_group
  FROM product_price_history
),
filled AS (
  SELECT
    product_id,
    price_date,
    price,
    fill_group,
    FIRST_VALUE(price) OVER(
      PARTITION BY product_id, fill_group
      ORDER BY price_date
    ) AS price_filled_forward
  FROM daily_prices
)
SELECT product_id, price_date, price, price_filled_forward
FROM filled
ORDER BY product_id, price_date;

-- Fill backward (carry NEXT known value backward into NULLs)
SELECT
  product_id,
  price_date,
  price,
  LAST_VALUE(price IGNORE NULLS) OVER(
    PARTITION BY product_id
    ORDER BY price_date DESC   -- reverse order = fill backward
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS price_filled_backward
FROM product_price_history
ORDER BY product_id, price_date;
```

---

## 12. Date & Time Calculations

```sql
-- Time between events
SELECT
  order_id,
  customer_id,
  order_date,
  shipped_date,
  delivered_date,
  shipped_date - order_date::date   AS days_to_ship,
  delivered_date - shipped_date     AS days_in_transit,
  delivered_date - order_date::date AS total_fulfillment_days
FROM orders;

-- Business days between two dates (excluding weekends)
SELECT
  order_id,
  order_date,
  delivered_date,
  -- Count weekdays only
  (
    SELECT COUNT(*)
    FROM generate_series(order_date, delivered_date - 1, '1 day'::interval) AS d
    WHERE EXTRACT(DOW FROM d) NOT IN (0, 6)   -- 0=Sunday, 6=Saturday
  ) AS business_days_to_deliver
FROM orders;

-- Age buckets (how old are our customers?)
SELECT
  CASE
    WHEN age_years < 25 THEN '18–24'
    WHEN age_years < 35 THEN '25–34'
    WHEN age_years < 45 THEN '35–44'
    WHEN age_years < 55 THEN '45–54'
    WHEN age_years < 65 THEN '55–64'
    ELSE '65+'
  END AS age_group,
  COUNT(*) AS customer_count,
  ROUND(AVG(lifetime_value), 2) AS avg_ltv
FROM (
  SELECT customer_id, lifetime_value,
         EXTRACT(YEAR FROM AGE(birthdate)) AS age_years
  FROM customers
) AS aged
GROUP BY age_group
ORDER BY MIN(age_years);

-- Time-of-day analysis
SELECT
  CASE
    WHEN EXTRACT(HOUR FROM order_time) BETWEEN  0 AND  5 THEN 'Late Night (0–5)'
    WHEN EXTRACT(HOUR FROM order_time) BETWEEN  6 AND 11 THEN 'Morning (6–11)'
    WHEN EXTRACT(HOUR FROM order_time) BETWEEN 12 AND 17 THEN 'Afternoon (12–17)'
    WHEN EXTRACT(HOUR FROM order_time) BETWEEN 18 AND 21 THEN 'Evening (18–21)'
    ELSE 'Night (22–23)'
  END AS time_of_day,
  COUNT(*) AS orders,
  ROUND(AVG(total_amount), 2) AS avg_order_value
FROM orders
GROUP BY time_of_day
ORDER BY MIN(EXTRACT(HOUR FROM order_time));

-- Days since last purchase (recency)
SELECT
  customer_id,
  MAX(order_date) AS last_purchase,
  CURRENT_DATE - MAX(order_date)::date AS days_since_purchase,
  CASE
    WHEN CURRENT_DATE - MAX(order_date)::date <= 30  THEN 'Active (< 30d)'
    WHEN CURRENT_DATE - MAX(order_date)::date <= 90  THEN 'At Risk (30–90d)'
    WHEN CURRENT_DATE - MAX(order_date)::date <= 180 THEN 'Lapsed (90–180d)'
    ELSE 'Churned (180d+)'
  END AS recency_segment
FROM orders
GROUP BY customer_id
ORDER BY days_since_purchase;
```

---

## 📌 Essential DA Computations Quick Reference

| Computation | Key SQL |
|------------|---------|
| Share of total | `SUM(val) / SUM(SUM(val)) OVER()` |
| Share within group | `SUM(val) / SUM(SUM(val)) OVER(PARTITION BY grp)` |
| Percentile rank | `PERCENT_RANK() OVER(ORDER BY val)` |
| Ntile bucket | `NTILE(n) OVER(ORDER BY val)` |
| Weighted average | `SUM(val * weight) / SUM(weight)` |
| Pareto ABC class | Cumulative SUM + CASE threshold |
| Market basket lift | `(AB/A) / (B/total)` |
| Deduplication | `ROW_NUMBER() OVER(PARTITION BY key ORDER BY ts DESC) = 1` |
| Fill forward | `FIRST_VALUE(val) OVER(PARTITION BY grp ORDER BY ts)` |
| NPS | `% Promoters − % Detractors` |
| CAGR | `POWER(end/start, 1/n) − 1` |
| Gap detection | date spine `LEFT JOIN` + `WHERE IS NULL` |
| Islands | Row number subtraction grouping trick |

---

*Part of the SQL Data Analysis Cheat Sheet series*
