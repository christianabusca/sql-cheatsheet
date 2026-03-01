# 📊 SQL for Data Analysis — Level 2: Intermediate Aggregations & Subqueries
> *Unlock multi-step analysis, grouping hierarchies, and reusable query logic*

---

## Table of Contents
- [1. Subqueries in WHERE](#1-subqueries-in-where)
- [2. Subqueries in FROM](#2-subqueries-in-from)
- [3. Common Table Expressions (CTEs)](#3-common-table-expressions-ctes)
- [4. Advanced GROUP BY](#4-advanced-group-by)
- [5. Conditional Aggregations](#5-conditional-aggregations)
- [6. Date-Based Aggregations](#6-date-based-aggregations)
- [7. Set Operations](#7-set-operations)
- [8. Intermediate Practice Patterns](#8-intermediate-practice-patterns)

---

## 1. Subqueries in WHERE

Filter your results based on dynamic calculated values from another query.

```sql
-- Find products priced above the category average
SELECT product_name, price, category
FROM products
WHERE price > (SELECT AVG(price) FROM products);

-- Find customers who placed orders last month
SELECT first_name, last_name, email
FROM customers
WHERE customer_id IN (
  SELECT DISTINCT customer_id
  FROM orders
  WHERE order_date >= DATE_TRUNC('month', NOW() - INTERVAL '1 month')
    AND order_date < DATE_TRUNC('month', NOW())
);

-- Find products that have NEVER been ordered (anti-join)
SELECT product_name, category, price
FROM products
WHERE product_id NOT IN (
  SELECT DISTINCT product_id FROM order_items
);

-- EXISTS — faster than IN for large datasets
SELECT c.first_name, c.last_name
FROM customers AS c
WHERE EXISTS (
  SELECT 1 FROM orders AS o
  WHERE o.customer_id = c.customer_id
    AND o.total_amount > 500
);

-- Find orders with above-average total for THEIR specific region
SELECT order_id, region, total_amount
FROM orders AS o1
WHERE total_amount > (
  SELECT AVG(total_amount)
  FROM orders AS o2
  WHERE o2.region = o1.region  -- correlated: references outer query
);
```

---

## 2. Subqueries in FROM

Pre-aggregate data, then query the result as if it were a table.

```sql
-- Average of averages — find the overall average customer spend
SELECT ROUND(AVG(customer_total), 2) AS avg_customer_value
FROM (
  SELECT customer_id, SUM(total_amount) AS customer_total
  FROM orders
  GROUP BY customer_id
) AS customer_totals;

-- Rank customers by spend per region
SELECT region, customer_id, customer_spend
FROM (
  SELECT
    region,
    customer_id,
    SUM(total_amount) AS customer_spend,
    RANK() OVER(PARTITION BY region ORDER BY SUM(total_amount) DESC) AS rank
  FROM orders
  GROUP BY region, customer_id
) AS ranked_customers
WHERE rank <= 5;

-- Find months where revenue exceeded the yearly average
SELECT month, monthly_revenue
FROM (
  SELECT
    DATE_TRUNC('month', order_date) AS month,
    SUM(total_amount) AS monthly_revenue
  FROM orders
  GROUP BY DATE_TRUNC('month', order_date)
) AS monthly_totals
WHERE monthly_revenue > (
  SELECT AVG(monthly_revenue)
  FROM (
    SELECT SUM(total_amount) AS monthly_revenue
    FROM orders
    GROUP BY DATE_TRUNC('month', order_date)
  ) AS all_months
);
```

---

## 3. Common Table Expressions (CTEs)

CTEs make complex multi-step queries readable and maintainable.

```sql
-- Single CTE: Top customers per region
WITH customer_spend AS (
  SELECT
    customer_id,
    region,
    SUM(total_amount) AS total_spent,
    COUNT(*) AS order_count
  FROM orders
  GROUP BY customer_id, region
)
SELECT *
FROM customer_spend
WHERE total_spent > 1000
ORDER BY region, total_spent DESC;

-- Chained CTEs: Step-by-step pipeline
WITH 
-- Step 1: Get completed orders with customer info
completed_orders AS (
  SELECT o.order_id, o.customer_id, o.total_amount, o.order_date,
         c.region, c.customer_since
  FROM orders AS o
  INNER JOIN customers AS c ON o.customer_id = c.customer_id
  WHERE o.status = 'completed'
),
-- Step 2: Aggregate by customer
customer_metrics AS (
  SELECT
    customer_id,
    region,
    COUNT(*) AS total_orders,
    SUM(total_amount) AS lifetime_value,
    MAX(order_date) AS last_order_date,
    MIN(order_date) AS first_order_date
  FROM completed_orders
  GROUP BY customer_id, region
),
-- Step 3: Segment customers
customer_segments AS (
  SELECT *,
    CASE
      WHEN lifetime_value >= 10000 THEN 'Platinum'
      WHEN lifetime_value >= 5000  THEN 'Gold'
      WHEN lifetime_value >= 1000  THEN 'Silver'
      ELSE 'Bronze'
    END AS segment
  FROM customer_metrics
)
-- Final: Summarize by segment
SELECT
  segment,
  COUNT(*) AS customer_count,
  ROUND(AVG(lifetime_value), 2) AS avg_ltv,
  ROUND(AVG(total_orders), 1) AS avg_orders
FROM customer_segments
GROUP BY segment
ORDER BY AVG(lifetime_value) DESC;
```

---

## 4. Advanced GROUP BY

### ROLLUP — Hierarchical Subtotals

```sql
-- Sales with subtotals at each level: region → category → grand total
SELECT
  COALESCE(region, '── ALL REGIONS ──') AS region,
  COALESCE(category, '── ALL CATEGORIES ──') AS category,
  COUNT(*) AS orders,
  SUM(revenue) AS total_revenue
FROM sales
GROUP BY ROLLUP(region, category)
ORDER BY region NULLS LAST, category NULLS LAST;

/*
Result looks like:
region    | category     | orders | revenue
----------|-------------|--------|--------
East      | Electronics  |    120 |  48,000
East      | Apparel      |     80 |  16,000
East      | ALL CATS     |    200 |  64,000   ← region subtotal
West      | Electronics  |     90 |  36,000
West      | ALL CATS     |    150 |  48,000   ← region subtotal
ALL       | ALL CATS     |    350 | 112,000   ← grand total
*/
```

### CUBE — All Possible Groupings

```sql
-- Generate every possible combination of aggregations
SELECT
  COALESCE(region, 'All Regions') AS region,
  COALESCE(category, 'All Categories') AS category,
  COALESCE(channel, 'All Channels') AS channel,
  SUM(revenue) AS total_revenue
FROM sales
GROUP BY CUBE(region, category, channel)
ORDER BY region NULLS LAST, category NULLS LAST, channel NULLS LAST;

-- Useful for building dashboards where users can slice any dimension
```

### GROUPING SETS — Custom Aggregations

```sql
-- Only specific grouping combinations you need
SELECT
  region,
  category,
  year,
  SUM(revenue) AS revenue
FROM sales
GROUP BY GROUPING SETS (
  (region, year),       -- subtotal by region + year
  (category, year),     -- subtotal by category + year
  (year),               -- just by year
  ()                    -- grand total
);
```

---

## 5. Conditional Aggregations

Calculate multiple metrics from one dataset in a single pass.

```sql
-- Order status matrix
SELECT
  region,
  COUNT(*) AS total_orders,
  COUNT(CASE WHEN status = 'completed'  THEN 1 END) AS completed,
  COUNT(CASE WHEN status = 'pending'    THEN 1 END) AS pending,
  COUNT(CASE WHEN status = 'cancelled'  THEN 1 END) AS cancelled,
  ROUND(100.0 * COUNT(CASE WHEN status = 'completed' THEN 1 END)
        / COUNT(*), 1) AS completion_rate_pct
FROM orders
GROUP BY region;

-- Revenue by quarter (pivot-style)
SELECT
  EXTRACT(YEAR FROM order_date) AS year,
  SUM(CASE WHEN EXTRACT(QUARTER FROM order_date) = 1 THEN total_amount ELSE 0 END) AS Q1,
  SUM(CASE WHEN EXTRACT(QUARTER FROM order_date) = 2 THEN total_amount ELSE 0 END) AS Q2,
  SUM(CASE WHEN EXTRACT(QUARTER FROM order_date) = 3 THEN total_amount ELSE 0 END) AS Q3,
  SUM(CASE WHEN EXTRACT(QUARTER FROM order_date) = 4 THEN total_amount ELSE 0 END) AS Q4,
  SUM(total_amount) AS annual_total
FROM orders
GROUP BY EXTRACT(YEAR FROM order_date)
ORDER BY year;

-- High-value vs low-value order split
SELECT
  category,
  COUNT(*) AS total,
  SUM(CASE WHEN total_amount >= 500 THEN 1 ELSE 0 END) AS high_value,
  SUM(CASE WHEN total_amount < 500  THEN 1 ELSE 0 END) AS low_value,
  ROUND(AVG(CASE WHEN total_amount >= 500 THEN 1.0 ELSE 0.0 END) * 100, 1)
    AS high_value_pct
FROM orders AS o
INNER JOIN products AS p ON o.product_id = p.product_id
GROUP BY category;
```

---

## 6. Date-Based Aggregations

```sql
-- Week-over-week revenue comparison
WITH weekly_revenue AS (
  SELECT
    DATE_TRUNC('week', order_date) AS week_start,
    SUM(total_amount) AS revenue
  FROM orders
  GROUP BY DATE_TRUNC('week', order_date)
)
SELECT
  week_start,
  revenue AS this_week,
  LAG(revenue) OVER(ORDER BY week_start) AS last_week,
  revenue - LAG(revenue) OVER(ORDER BY week_start) AS wow_change,
  ROUND(100.0 * (revenue - LAG(revenue) OVER(ORDER BY week_start))
        / NULLIF(LAG(revenue) OVER(ORDER BY week_start), 0), 1) AS wow_pct_change
FROM weekly_revenue
ORDER BY week_start;

-- Cohort size: How many customers joined each month?
SELECT
  DATE_TRUNC('month', customer_since) AS cohort_month,
  COUNT(*) AS new_customers
FROM customers
GROUP BY DATE_TRUNC('month', customer_since)
ORDER BY cohort_month;

-- Orders in the last 7, 30, 90 days
SELECT
  COUNT(CASE WHEN order_date >= CURRENT_DATE - 7  THEN 1 END) AS last_7_days,
  COUNT(CASE WHEN order_date >= CURRENT_DATE - 30 THEN 1 END) AS last_30_days,
  COUNT(CASE WHEN order_date >= CURRENT_DATE - 90 THEN 1 END) AS last_90_days
FROM orders;
```

---

## 7. Set Operations

Combine results from multiple queries vertically.

```sql
-- UNION: All unique customers from two systems
SELECT customer_id, email FROM crm_customers
UNION
SELECT customer_id, email FROM ecommerce_customers;

-- UNION ALL: Keep all records including duplicates (faster)
SELECT 'web' AS channel, order_id, total_amount FROM web_orders
UNION ALL
SELECT 'mobile' AS channel, order_id, total_amount FROM mobile_orders
UNION ALL
SELECT 'store' AS channel, order_id, total_amount FROM store_orders;

-- INTERSECT: Customers who bought in both Q1 and Q2
SELECT DISTINCT customer_id FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-03-31'
INTERSECT
SELECT DISTINCT customer_id FROM orders
WHERE order_date BETWEEN '2024-04-01' AND '2024-06-30';

-- EXCEPT: Customers from Q1 who did NOT return in Q2 (churned)
SELECT DISTINCT customer_id FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-03-31'
EXCEPT
SELECT DISTINCT customer_id FROM orders
WHERE order_date BETWEEN '2024-04-01' AND '2024-06-30';
```

---

## 8. Intermediate Practice Patterns

### Customer Retention: First vs. Repeat Buyers
```sql
WITH order_sequence AS (
  SELECT
    customer_id,
    order_id,
    order_date,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_date) AS order_num
  FROM orders
)
SELECT
  DATE_TRUNC('month', order_date) AS month,
  COUNT(CASE WHEN order_num = 1 THEN 1 END) AS new_customers,
  COUNT(CASE WHEN order_num > 1 THEN 1 END) AS repeat_orders
FROM order_sequence
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;
```

### Product Affinity: Which Categories Co-exist in Orders
```sql
WITH order_categories AS (
  SELECT DISTINCT o.order_id, p.category
  FROM order_items AS oi
  INNER JOIN orders AS o ON oi.order_id = o.order_id
  INNER JOIN products AS p ON oi.product_id = p.product_id
)
SELECT
  a.category AS category_a,
  b.category AS category_b,
  COUNT(*) AS times_bought_together
FROM order_categories AS a
INNER JOIN order_categories AS b 
  ON a.order_id = b.order_id
  AND a.category < b.category  -- avoid duplicates
GROUP BY a.category, b.category
ORDER BY times_bought_together DESC
LIMIT 20;
```

### Revenue Contribution by Product (ABC Analysis)
```sql
WITH product_revenue AS (
  SELECT
    product_id,
    SUM(revenue) AS product_revenue,
    SUM(SUM(revenue)) OVER() AS total_revenue
  FROM sales
  GROUP BY product_id
),
ranked AS (
  SELECT
    product_id,
    product_revenue,
    100.0 * product_revenue / total_revenue AS pct_of_total,
    SUM(100.0 * product_revenue / total_revenue) 
      OVER(ORDER BY product_revenue DESC) AS cumulative_pct
  FROM product_revenue
)
SELECT
  product_id,
  ROUND(product_revenue, 2),
  ROUND(pct_of_total, 2) AS pct,
  ROUND(cumulative_pct, 2) AS cumulative_pct,
  CASE
    WHEN cumulative_pct <= 80 THEN 'A — Top Performers'
    WHEN cumulative_pct <= 95 THEN 'B — Mid Performers'
    ELSE 'C — Low Performers'
  END AS abc_class
FROM ranked
ORDER BY product_revenue DESC;
```

---

## 📌 Intermediate Quick Reference

| Technique | When to Use |
|-----------|------------|
| `WHERE` subquery | Filter by dynamic calculated values |
| `FROM` subquery | Query pre-aggregated data |
| CTE (`WITH`) | Multi-step logic, readability |
| `ROLLUP` | Hierarchical subtotals |
| `CUBE` | All combination subtotals |
| `CASE` in `COUNT`/`SUM` | Conditional aggregation pivot |
| `UNION` / `EXCEPT` / `INTERSECT` | Combine/compare datasets |
| `COALESCE` | Replace NULLs in ROLLUP/CUBE labels |

---

*← Previous: README_01_Beginner_SQL_DA.md | Next → README_03_Window_Functions.md*
