
/*
This SQL script showcased exploratory data analysis (EDA) using the GOLD schema views to explore 
the dimensions and key business metrics utilizing different SQL techniques  
*/


==========================================================
-- DATABASE EXPLORATION
==========================================================
--Explore all objects in the database
SELECT * FROM INFORMATION_SCHEMA.TABLES


--Explore all columns in the database
SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers'

-- Explore all countries our customers are from
SELECT DISTINCT country FROM gold.dim_customers;

-- Explore all product categories 
SELECT DISTINCT category, subcategory, product_name FROM gold.dim_products
ORDER BY 1,2,3;

==========================================================
--DATE EXPLORATION
==========================================================
-- Date of the first and last order
-- How many years of sales available
SELECT  
MIN(order_date) AS first_order_data,
MAX(order_date) AS last_order_data,
DATEDIFF(year, MIN(order_date), MAX(order_date)) AS  order_range_year
FROM gold.fact_sales

-- Find the youngest and oldest customer
SELECT
MIN(birthdate) AS oldest_birthdate,
DATEDIFF(year, MIN(birthdate), GETDATE()) AS  oldest_age,
MAX(birthdate) AS youngest_birthdate,
DATEDIFF(year, MAX(birthdate), GETDATE()) AS  youngest_age
FROM gold.dim_customers
  
==========================================================
--MEASURES EXPLORATION
==========================================================
--Find:

-- Total Sales
SELECT SUM(sales_amount) AS total_sales FROM gold.fact_sales

-- How many items were sold
SELECT SUM(quantity) AS total_quantity FROM gold.fact_sales

-- Average selling price
SELECT AVG(price) AS avg_price FROM gold.fact_sales

-- Total number of orders
SELECT COUNT(order_number) AS total_order FROM gold.fact_sales
SELECT COUNT( DISTINCT order_number) AS total_order FROM gold.fact_sales

-- Total number of products
SELECT COUNT(product_key) AS total_products FROM gold.dim_products
SELECT COUNT( DISTINCT product_key) AS total_products FROM gold.dim_products

-- Total number of customers
SELECT COUNT(customer_key) AS total_customers FROM gold.dim_customers

-- Total number of customers that has placed an order
SELECT COUNT(customer_key) AS total_customers FROM gold.fact_sales
SELECT COUNT( DISTINCT customer_key) AS total_customers FROM gold.fact_sales

-- Generate a Report showcasing the key business metrics
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity' AS measure_name, SUM(quantity) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Average Price' AS measure_name, AVG(price) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Orders' AS measure_name, COUNT( DISTINCT order_number) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Products' AS measure_name, COUNT( DISTINCT product_key) AS measure_value FROM gold.dim_products
UNION ALL
SELECT 'Total Nr. Customers' AS measure_name, COUNT(customer_key) AS measure_value FROM gold.dim_customers

==========================================================
-- MAGNITUDE ANALYSIS
==========================================================
--Find total customers by countries
SELECT 
country, 
COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY COUNT(customer_key) DESC

--Find total customers by gender
SELECT 
gender, 
COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY COUNT(customer_key) DESC

-- Find total products by category
SELECT
category,
COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC

--What is the average cost in each category
SELECT
category,
AVG(cost) AS avg_costs
FROM gold.dim_products
GROUP BY category
ORDER BY avg_costs DESC

--What is the total revenue generated for each category
SELECT
p.category,
SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY total_revenue DESC

--What is the total revenue generated for each customer
SELECT
c.customer_key,
c.first_name,
c.last_name,
SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f 
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY 
c.customer_key,
c.first_name,
c.last_name
ORDER BY total_revenue DESC

-- What is the distribution of sold items across countries
SELECT 
c.country,
SUM(f.quantity) AS total_quantity_sold
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY 
c.country
ORDER BY total_quantity_sold DESC
  
==========================================================
--RANKING ANALYSIS
==========================================================
-- Which 5 products generates the highest revenue
SELECT TOP 5
p.product_name,
SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC 

--What is the 5 worst-performing products in terms of sales?
SELECT TOP 5
p.product_name,
SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue 

-- using window function
SELECT *
FROM (
	SELECT 
	p.product_name,
	SUM(f.sales_amount) AS total_revenue,
	ROW_NUMBER () OVER (ORDER BY SUM(f.sales_amount) DESC) AS rank_products
	FROM gold.fact_sales f 
	LEFT JOIN gold.dim_products p
	ON p.product_key = f.product_key
	GROUP BY p.product_name	
	) t
WHERE rank_products <= 5


--What is the TOP 10 revenue generated customer
SELECT TOP 10
c.customer_key,
c.first_name,
c.last_name,
SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f 
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY 
c.customer_key,
c.first_name,
c.last_name
ORDER BY total_revenue DESC

-- The 3 customers with the fewest orders placed
SELECT TOP 10
c.customer_key,
c.first_name,
c.last_name,
COUNT(DISTINCT f.order_number) AS total_order
FROM gold.fact_sales f 
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY 
c.customer_key,
c.first_name,
c.last_name
ORDER BY total_order
