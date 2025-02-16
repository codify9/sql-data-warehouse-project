INSERT INTO silver.crm_sales_details(
	sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
)
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	
	CASE WHEN sls_order_dt <= 0 OR LENGTH(CAST(sls_order_dt AS text)) <> 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
	END AS sls_order_dt,
	
	CASE WHEN sls_ship_dt <= 0 OR LENGTH(CAST(sls_ship_dt AS text)) <> 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
	END AS sls_ship_dt,
	
	CASE WHEN sls_due_dt <= 0 OR LENGTH(CAST(sls_due_dt AS text)) <> 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
	END AS sls_due_dt,
	
	Case when sls_sales IS Null or sls_sales <=0 OR sls_sales <> sls_quantity * ABS(sls_price)
		then sls_quantity * ABS(sls_price)
		else sls_sales
	end as sls_sales,
	sls_quantity,
	Case when sls_price IS Null or sls_price <=0 
		then sls_sales / NULLIF(sls_quantity, 0)
		else sls_price
	end as sls_price
FROM bronze.crm_sales_details;

Select * from silver.crm_sales_details;