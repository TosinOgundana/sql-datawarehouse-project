-- Data Quality Check

-- Check for duplicates in the cust_id after left join
SELECT cst_id, COUNT(*) AS duplicate_count
FROM(
	SELECT 
  	cs.cst_id,
  	cs.cst_key,
  	cs.cst_firstname,
  	cs.cst_lastname,
  	cs.cst_marital_status,
  	cs.cst_gndr,
  	cu.gen,
  	cu.bdate,
  	lo.cntry,
  	cs.cst_create_date
	FROM silver.crm_cust_info cs
	LEFT JOIN silver.erp_cust_az12 cu
	ON	      cs.cst_key = cu.cid
	LEFT JOIN silver.erp_loc_a101 lo
	ON cs.cst_key = lo.cid
  ) x 
GROUP BY cst_id
HAVING COUNT(*) > 1

-- Gender checks
SELECT DISTINCT
  cs.cst_gndr,
  cu.gen,
  CASE WHEN cs.cst_gndr != 'n/a' THEN cs.cst_gndr --CRM is the Master data for Customer
  	   ELSE COALESCE( cu.gen, 'n/a')
  END AS new_gen
FROM silver.crm_cust_info cs
LEFT JOIN silver.erp_cust_az12 cu
ON	      cs.cst_key = cu.cid
ORDER BY 1,2



--Quality check for gold.dim_customers
SELECT DISTINCT gender
FROM gold.dim_customers

SELECT DISTINCT *
FROM gold.dim_customers


-- Check for duplicates in the prd_key after left join
SELECT prd_key, COUNT(*) AS duplicate_count
FROM (
	SELECT
  	pr.prd_id,
  	pr.cat_id,
  	pr.prd_key,
  	pr.prd_nm,
  	pr.prd_cost,
  	pr.prd_line,
  	pr.prd_start_dt,
  	ep.cat,
  	ep.subcat,
  	ep.maintenance
	FROM silver.crm_prd_info pr
	LEFT JOIN silver.erp_px_cat_g1v2 ep
	ON pr.cat_id = ep.id
	WHERE prd_end_dt IS NULL
	) x
	GROUP BY prd_key
	HAVING COUNT(*) > 1

--Quality check for fact_sales
SELECT * FROM gold.fact_sales

-- Foreign key integrity check
SELECT *
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON		  s.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
ON		  s.product_key = p.product_key
WHERE	  c.customer_key IS NULL OR p.product_key IS NULL








