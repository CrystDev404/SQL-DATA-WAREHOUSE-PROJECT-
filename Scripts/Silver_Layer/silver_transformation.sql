EXEC silver.load_silver
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	TRUNCATE TABLE silver.crm_cust_info
	INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_firstname,
	cst_lastname,
	cst_key,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

	SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname ) AS cst_lastname, 
	CASE WHEN UPPER(cst_marital_status) = 'S' THEN 'SINGLE'
		WHEN UPPER(cst_marital_status)  = 'M' THEN 'MARRIED'
		ELSE 'N/A'
	END cst_marital_status,
	CASE WHEN UPPER(cst_gndr) = 'F' THEN 'FEMALE'
		WHEN UPPER(cst_gndr) =  'M' THEN 'MALE'
		ELSE 'N/A'
	END cst_gndr,
	cst_create_date
	FROM(
	SELECT * , 
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info)t WHERE flag_last = 1

	TRUNCATE TABLE silver.crm_prd_info
	INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	SELECT
	prd_id,
	REPLACE (SUBSTRING (prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, 
	prd_nm,	
	ISNULL (prd_cost, 0) AS pro_cost,
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(
	LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE 
	)
	FROM bronze.crm_prd_info

	TRUNCATE TABLE silver.crm_sales_details
	INSERT INTO silver.crm_sales_details (
	sls_ord_num ,
	sls_prd_key	,
	sls_cust_id	,
	sls_order_dt ,
	sls_ship_dt	 ,
	sls_due_dt	,
	sls_sales	,
	sls_quantity,
	sls_price 
	)
	SELECT
	sls_ord_num	,
	sls_prd_key	,
	sls_cust_id	,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,

	CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales 
	END AS sls_sales,
	sls_quantity,	
	CASE WHEN sls_price IS NULL OR sls_price <=0
		THEN sls_sales/NULLIF(sls_quantity,0)
	ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details


	TRUNCATE TABLE silver.erp_cust_az12
	INSERT INTO silver.erp_cust_az12(
	CID,
	BDATE,	
	GEN
	)
	SELECT 
	CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4 , len(CID))
		ELSE CID
	END AS CID,
	CASE WHEN BDATE> GETDATE() THEN NULL
		ELSE BDATE
		END AS BDATE,
	CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'N/A'
	END AS GEN
	FROM bronze.erp_cust_az12

	TRUNCATE TABLE silver.erp_px_cat_g1v2
	INSERT INTO silver.erp_px_cat_g1v2(Id,
	cat,
	subcat,
	maintenance)
	SELECT 
	Id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2

	TRUNCATE TABLE silver.erp_loc_a101
	INSERT INTO silver.erp_loc_a101(
	CID, cntry)
	SELECT 
	REPLACE(CID, '-' , ' ') AS CID,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'GERMANY'
		WHEN TRIM(cntry)  IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
		ELSE TRIM(cntry)
	END AS cntry
	FROM bronze.erp_loc_a101
END

