/*
=============================================================================
Quality Checks
=============================================================================
This script performs quality checks for data consistency, accuracy, and 
standardization across the silver schema. Below are the various checks:
- Null or duplicate primary keys
- Invalid date range and orders
- Data consistency and standardization
- Unwanted spaces in string fields
- Extracted relevant IDs and Keys
=============================================================================
*\


-- Bronze crm_cust_info data cleaning and transformation
-- Check for Nulls or duplicates primary key
-- Expectation: No result

SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL 

-- Check for unwanted spaces
SELECT cst_material_status
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Data standardization and Consistency
SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info


-- Silver crm_cust_info quality checks --
-- Check for Nulls or duplicates primary key

SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL 


-- Check for unwanted spaces
SELECT cst_lastname			-- lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


-- Data standardization and Consistency
SELECT DISTINCT cst_marital_status --marital status
FROM silver.crm_cust_info

SELECT DISTINCT cst_gndr --gender
FROM silver.crm_cust_info

-- Final check
SELECT *
FROM silver.crm_cust_info
=============================================================================

-- Bronze crm_prd_info data cleaning and transformation
-->>> Check for Nulls or duplicates primary key

SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL 

-- Check for unwanted spaces
SELECT prd_nm			-- product name
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for nulls or negative cost
SELECT prd_cost			-- product name
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL


-- Data standardization and Consistency for prd_line
SELECT DISTINCT prd_line --product line
FROM bronze.crm_prd_info

-- Check for invalid product Date Orders 
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- Sampled
SELECT  
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('CO-RF-FR-R92R-56', 'CL-BS-SB-M891-M') 


-->>> SILVER crm_prd_info data quality checks <<<----

-- Check for Nulls or duplicates of the primary key
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL 

-- Check for unwanted spaces
SELECT prd_nm			-- product name
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for nulls or negative cost
SELECT prd_cost			-- product name
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL


-- Data standardization and Consistency for prd_line
SELECT DISTINCT prd_line --product line
FROM silver.crm_prd_info

-- Check for invalid product Date Orders 
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt
--WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN

--(SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)
=============================================================================
  
-->> Bronze crm_sales_details data cleaning and transformation <<--
--Quality checks

-- Check for unwanted spaces
SELECT  
sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

--Check for  inconsistencies in sales product key and product product key & customer ID
SELECT  
sls_ord_num
,sls_prd_key
,sls_cust_id
,sls_order_dt
,sls_shipt_dt
,sls_due_dt
,sls_sales
,sls_quantity
,sls_price
 FROM bronze.crm_sales_details
 --WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
 WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
 

-- Check for invalid dates
SELECT  
sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt >20500101
OR sls_order_dt < 19000000

-- Check for invalid dates
SELECT  
sls_shipt_dt
FROM bronze.crm_sales_details
WHERE sls_shipt_dt <= 0 
OR LEN(sls_shipt_dt) != 8
OR sls_shipt_dt >20500101
OR sls_shipt_dt < 19000000

-- Check for invalid dates
SELECT  
sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8
OR sls_due_dt >20500101
OR sls_due_dt < 19000000

-- Replace zero dates with a null value
SELECT  
NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0

-- Check for invalid dates 
SELECT  
*
FROM bronze.crm_sales_details
WHERE sls_order_dt >  sls_shipt_dt
OR sls_order_dt > sls_due_dt


-- Check data consistency: between sales, quantity and price
-->> Sales = Quantity * Price
-->> No nulls, negatives or zero values

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS  NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


SELECT DISTINCT
sls_sales AS old_sales,
sls_quantity,
sls_price AS Old_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0)
	 ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS  NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


--SILVER.crm_sales_details Quality checks--
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS  NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


SELECT  
*
FROM silver.crm_sales_details
WHERE sls_order_dt >  sls_ship_dt
OR sls_order_dt > sls_due_dt
=============================================================================

-->>> Bronze erp_cust_az12 data cleaning and transformation <<--
SELECT 
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4,LEN(cid))
	   ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4,LEN(cid))
	 ELSE cid
END  NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


-- Check for out-of-range Birth dates
SELECT 
bdate,
CASE WHEN bdate > GETDATE() THEN NULL
	 ELSE bdate
END AS bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1925-02-01' OR bdate > GETDATE()

--Check for data standardization and consistency
SELECT DISTINCT
gen,
CASE WHEN UPPER(TRIM(gen)) IN('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

-->> Check silver.erp_cust_az12 data quality <<--

-- Check out-of-range Birth dates
SELECT 
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1925-02-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
SELECT DISTINCT
gen
FROM silver.erp_cust_az12

SELECT 
*
FROM silver.erp_cust_az12
=============================================================================
  
-->> bronze.erp_loc_a101 data cleaning and transformation <<--
SELECT 
REPLACE(cid,'-', '') cid, -- Replaced unwanted values
cntry
FROM bronze.erp_loc_a101
--WHERE REPLACE(cid,'-', '') NOT IN
--(select cst_key from silver.crm_cust_info)

-- Data Standardization & Consistency
SELECT DISTINCT
cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany' 
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' THEN 'n/a'
	 WHEN TRIM(cntry) IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry -- Normalize and handle missing or blank country abbreviations
FROM bronze.erp_loc_a101

--Check silver.erp_loc_a101 data quality--
SELECT DISTINCT
cntry
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT
*
FROM silver.erp_loc_a101

=============================================================================
--> bronze.erp_px_cat_g1v2 data cleaning and transformation <<--
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

-- Check unwanted spaces
SELECT 
*
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

---- Data Standardization & Consistency
SELECT DISTINCT
--id,
--cat
--subcat
maintenance
FROM bronze.erp_px_cat_g1v2
  

-->> Check silver.erp_px_cat_g1v2 data quality <<--
SELECT 
*
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

SELECT 
*
FROM silver.erp_px_cat_g1v2



