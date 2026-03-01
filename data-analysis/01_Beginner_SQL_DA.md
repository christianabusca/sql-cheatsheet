# 📊 SQL for Data Analysis — Level 1: Beginner Foundations
> *Master the building blocks of data analysis with SQL*

---

## Table of Contents
- [1. Filtering & Sorting Data](#1-filtering--sorting-data)
- [2. Basic Aggregations](#2-basic-aggregations)
- [3. Grouping Data](#3-grouping-data)
- [4. Conditional Logic with CASE](#4-conditional-logic-with-case)
- [5. Working with NULLs](#5-working-with-nulls)
- [6. Basic String & Date Operations](#6-basic-string--date-operations)
- [7. Simple Joins](#7-simple-joins)
- [8. Practice Patterns](#8-practice-patterns)

---

## 1. Filtering & Sorting Data

The foundation of any data analysis is selecting and filtering the right rows.

```sql
-- Select specific columns
SELECT product_name, price, category
FROM products;

-- Filter with WHERE
SELECT * FROM orders
WHERE status = 'completed';

-- Multiple conditions
SELECT * FROM orders
WHERE status = 'completed'
  AND order_date >= '2024-01-01'
  AND total_amount > 100;

-- Filter with OR
SELECT * FROM products
WHERE category = 'Electronics'
   OR category = 'Accessories';

-- Filter with IN (cleaner than multiple OR)
SELECT * FROM products
WHERE category IN ('Electronics', 'Accessories', 'Gadgets');

-- Exclude values with NOT IN
SELECT * FROM products
WHERE category NOT IN ('Clearance', 'Discontinued');

-- Range filter with BETWEEN
SELECT * FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31';

-- Pattern matching with LIKE
SELECT * FROM customers
WHERE email LIKE '%@gmail.com';    -- ends with @gmail.com
WHERE name LIKE 'John%';           -- starts with John
WHERE name LIKE '%son%';           -- contains "son"

-- Sort results
SELECT product_name, price
FROM products
ORDER BY price DESC;               -- highest first

-- Sort by multiple columns
SELECT customer_id, order_date, total_amount
FROM orders
ORDER BY customer_id ASC, order_date DESC;

-- Limit results
SELECT * FROM products
ORDER BY price DESC
LIMIT 10;                          -- top 10 most expensive
```

---

## 2. Basic Aggregations

Aggregations summarize your data into meaningful metrics.

```sql
-- COUNT: How many rows?
SELECT COUNT(*) AS total_orders FROM orders;
SELECT COUNT(customer_id) AS orders_with_customer FROM orders;  -- ignores NULLs
SELECT COUNT(DISTINCT customer_id) AS unique_customers FROM orders;

-- SUM: Total values
SELECT SUM(total_amount) AS total_revenue FROM orders;
SELECT SUM(quantity) AS units_sold FROM order_items;

-- AVG: Mean value
SELECT AVG(total_amount) AS average_order_value FROM orders;
SELECT ROUND(AVG(price), 2) AS avg_price FROM products;

-- MIN / MAX: Extremes
SELECT 
  MIN(price) AS cheapest,
  MAX(price) AS most_expensive,
  MAX(price) - MIN(price) AS price_range
FROM products;

-- Combine multiple metrics
SELECT
  COUNT(*)                          AS total_orders,
  COUNT(DISTINCT customer_id)       AS unique_customers,
  SUM(total_amount)                 AS total_revenue,
  ROUND(AVG(total_amount), 2)       AS avg_order_value,
  MIN(total_amount)                 AS smallest_order,
  MAX(total_amount)                 AS largest_order
FROM orders
WHERE status = 'completed';
```

---

## 3. Grouping Data

GROUP BY breaks your data into segments and applies aggregations to each.

```sql
-- Sales by category
SELECT 
  category,
  COUNT(*) AS product_count,
  ROUND(AVG(price), 2) AS avg_price
FROM products
GROUP BY category
ORDER BY product_count DESC;

-- Revenue by month
SELECT
  DATE_TRUNC('month', order_date) AS month,
  COUNT(*) AS total_orders,
  SUM(total_amount) AS monthly_revenue
FROM orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;

-- Revenue by region and product category
SELECT
  region,
  category,
  SUM(revenue) AS total_revenue,
  COUNT(*) AS transaction_count
FROM sales
GROUP BY region, category
ORDER BY region, total_revenue DESC;

-- HAVING: Filter groups (like WHERE but for aggregated data)
-- Find categories with more than 50 products
SELECT 
  category,
  COUNT(*) AS product_count
FROM products
GROUP BY category
HAVING COUNT(*) > 50;

-- Find customers who spent more than $1,000 total
SELECT
  customer_id,
  SUM(total_amount) AS lifetime_value
FROM orders
GROUP BY customer_id
HAVING SUM(total_amount) > 1000
ORDER BY lifetime_value DESC;
```

> 💡 **Rule:** Use `WHERE` to filter rows **before** grouping. Use `HAVING` to filter **after** grouping.

---

## 4. Conditional Logic with CASE

CASE lets you create new categories and labels from existing data.

```sql
-- Categorize order sizes
SELECT
  order_id,
  total_amount,
  CASE
    WHEN total_amount < 50   THEN 'Small'
    WHEN total_amount < 200  THEN 'Medium'
    WHEN total_amount < 1000 THEN 'Large'
    ELSE 'Enterprise'
  END AS order_size
FROM orders;

-- Count how many orders fall in each category
SELECT
  CASE
    WHEN total_amount < 50   THEN 'Small'
    WHEN total_amount < 200  THEN 'Medium'
    WHEN total_amount < 1000 THEN 'Large'
    ELSE 'Enterprise'
  END AS order_size,
  COUNT(*) AS order_count,
  SUM(total_amount) AS total_revenue
FROM orders
GROUP BY order_size
ORDER BY MIN(total_amount);

-- Create a simple pass/fail flag
SELECT
  student_id,
  score,
  CASE WHEN score >= 60 THEN 'Pass' ELSE 'Fail' END AS result
FROM exam_results;

-- Conditional counting (count only rows that match a condition)
SELECT
  region,
  COUNT(*) AS total_orders,
  COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed_orders,
  COUNT(CASE WHEN status = 'cancelled' THEN 1 END) AS cancelled_orders
FROM orders
GROUP BY region;
```

---

## 5. Working with NULLs

NULLs represent missing data and require special handling.

```sql
-- Find rows with missing data
SELECT * FROM customers WHERE phone IS NULL;
SELECT * FROM orders WHERE shipped_date IS NOT NULL;

-- Replace NULL with a default value using COALESCE
SELECT
  customer_id,
  COALESCE(phone, 'No phone on file') AS phone_number,
  COALESCE(discount, 0) AS discount_applied
FROM customers;

-- NULLIF: Return NULL when two values match (avoid division by zero)
SELECT
  product_id,
  total_revenue / NULLIF(units_sold, 0) AS revenue_per_unit
FROM product_sales;

-- Count NULLs vs non-NULLs
SELECT
  COUNT(*) AS total_rows,
  COUNT(email) AS rows_with_email,
  COUNT(*) - COUNT(email) AS rows_missing_email
FROM customers;
```

---

## 6. Basic String & Date Operations

```sql
-- Combine first and last name
SELECT 
  first_name || ' ' || last_name AS full_name
FROM customers;

-- Extract part of a string
SELECT 
  UPPER(first_name) AS first_name_upper,
  LOWER(email) AS email_lower,
  LENGTH(phone) AS phone_length
FROM customers;

-- Extract email username (everything before @)
SELECT
  email,
  SUBSTRING(email FROM 1 FOR POSITION('@' IN email) - 1) AS username
FROM customers;

-- Current date and time
SELECT 
  NOW()          AS current_timestamp,
  CURRENT_DATE   AS today,
  CURRENT_DATE - INTERVAL '30 days' AS thirty_days_ago;

-- Extract year, month, day from a date
SELECT
  order_date,
  EXTRACT(YEAR FROM order_date)  AS order_year,
  EXTRACT(MONTH FROM order_date) AS order_month,
  EXTRACT(DAY FROM order_date)   AS order_day
FROM orders;

-- Calculate how many days since an event
SELECT
  order_id,
  order_date,
  CURRENT_DATE - order_date::date AS days_since_order
FROM orders;

-- Group by year-month
SELECT
  TO_CHAR(order_date, 'YYYY-MM') AS year_month,
  COUNT(*) AS orders,
  SUM(total_amount) AS revenue
FROM orders
GROUP BY TO_CHAR(order_date, 'YYYY-MM')
ORDER BY year_month;
```

---

## 7. Simple Joins

Joins combine data from multiple tables.

```sql
-- INNER JOIN: Only rows that match in both tables
SELECT
  o.order_id,
  c.first_name,
  c.last_name,
  o.total_amount,
  o.order_date
FROM orders AS o
INNER JOIN customers AS c ON o.customer_id = c.customer_id;

-- LEFT JOIN: All orders even if customer is missing
SELECT
  o.order_id,
  c.first_name,
  o.total_amount
FROM orders AS o
LEFT JOIN customers AS c ON o.customer_id = c.customer_id;

-- Find orphan records (orders with no matching customer)
SELECT o.order_id, o.total_amount
FROM orders AS o
LEFT JOIN customers AS c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Join three tables
SELECT
  o.order_id,
  c.first_name,
  p.product_name,
  oi.quantity,
  oi.unit_price
FROM orders AS o
INNER JOIN customers AS c ON o.customer_id = c.customer_id
INNER JOIN order_items AS oi ON o.order_id = oi.order_id
INNER JOIN products AS p ON oi.product_id = p.product_id;
```

---

## 8. Practice Patterns

### Monthly Sales Summary
```sql
SELECT
  DATE_TRUNC('month', order_date) AS month,
  COUNT(*) AS total_orders,
  COUNT(DISTINCT customer_id) AS unique_customers,
  SUM(total_amount) AS revenue,
  ROUND(AVG(total_amount), 2) AS avg_order_value
FROM orders
WHERE status = 'completed'
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;
```

### Top 10 Products by Revenue
```sql
SELECT
  p.product_name,
  p.category,
  SUM(oi.quantity * oi.unit_price) AS total_revenue,
  SUM(oi.quantity) AS units_sold
FROM order_items AS oi
INNER JOIN products AS p ON oi.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10;
```

### Customer Segmentation
```sql
SELECT
  customer_id,
  COUNT(*) AS total_orders,
  SUM(total_amount) AS lifetime_value,
  CASE
    WHEN SUM(total_amount) >= 5000 THEN 'VIP'
    WHEN SUM(total_amount) >= 1000 THEN 'Regular'
    ELSE 'New'
  END AS customer_segment
FROM orders
WHERE status = 'completed'
GROUP BY customer_id
ORDER BY lifetime_value DESC;
```

---

## 📌 Beginner Quick Reference

| Goal | Clause to Use |
|------|--------------|
| Filter rows | `WHERE` |
| Filter groups | `HAVING` |
| Count rows | `COUNT(*)` |
| Total values | `SUM()` |
| Group data | `GROUP BY` |
| Sort results | `ORDER BY` |
| Handle NULLs | `COALESCE()` |
| Create categories | `CASE WHEN` |
| Combine tables | `JOIN` |

---

*Next Level → README_02_Intermediate_Aggregations.md*
