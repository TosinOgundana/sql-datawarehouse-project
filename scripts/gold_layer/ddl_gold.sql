/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
	
CREATE VIEW gold.dim_customers AS 
SELECT
  	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- assigning surrogate key
  	cs.cst_id 	      AS customer_id,
  	cs.cst_key 	      AS customer_number,
  	cs.cst_firstname      AS first_name,
  	cs.cst_lastname       AS last_name,
  	lo.cntry 	      AS country,
  	cs.cst_marital_status AS marital_status,
  	CASE WHEN cs.cst_gndr != 'n/a' THEN cs.cst_gndr -- CRM is the Master for Gender info
  		 ELSE COALESCE(cu.gen, 'n/a')
  	END AS gender,
  	cu.bdate AS birthdate,
  	cs.cst_create_date AS create_date
FROM silver.crm_cust_info cs
LEFT JOIN silver.erp_cust_az12 cu
ON	      cs.cst_key = cu.cid
LEFT JOIN silver.erp_loc_a101 lo
ON		  cs.cst_key = lo.cid;

GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
	
CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY pr.prd_start_dt,pr.prd_key ) AS product_key, -- assigning surrogate key
	pr.prd_id  	AS product_id,
	pr.prd_key 	AS product_number,
	pr.prd_nm 	AS product_name,
	pr.cat_id	AS category_id,
	ep.cat  	AS category,
	ep.subcat       AS subcategory,
	ep.maintenance  AS maintenance,
	pr.prd_line  	AS product_line,
	pr.prd_cost 	AS cost,
	pr.prd_start_dt AS start_date
FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_px_cat_g1v2 ep
ON pr.cat_id = ep.id
WHERE prd_end_dt IS NULL;

GO

-- =============================================================================
-- Create Dimension: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;

GO

CREATE VIEW gold.fact_sales AS
SELECT
	sa.sls_ord_num  AS order_number,
	pr.product_key,
	cu.customer_key,
	sa.sls_order_dt AS order_date,
	sa.sls_ship_dt  AS shipping_date,
	sa.sls_due_dt   AS due_date,
	sa.sls_sales    AS sales_amount,
	sa.sls_quantity AS quantity,
	sa.sls_price    AS price
FROM silver.crm_sales_details sa
LEFT JOIN gold.dim_products pr
ON		  sa.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON		  sa.sls_cust_id = cu.customer_id;

GO

