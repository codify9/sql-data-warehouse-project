CREATE OR REPLACE PROCEDURE silver.load_data()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Truncate all target tables in the silver schema
    TRUNCATE TABLE silver.crm_cust_info;
    TRUNCATE TABLE silver.crm_prd_info;
    TRUNCATE TABLE silver.crm_sales_details;
    TRUNCATE TABLE silver.erp_cust_az12;
    TRUNCATE TABLE silver.erp_loc_a101;
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    -- Insert into silver.crm_cust_info
    INSERT INTO silver.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'n/a' 
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT 
            *, 
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
    ) x
    WHERE flag_last = 1 AND cst_id IS NOT NULL;

    -- Insert into silver.crm_prd_info
    INSERT INTO silver.crm_prd_info (
        prd_id, prd_key, cat_id, prd_key_short, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    WITH ranked_products AS (
        SELECT 
            prd_id,
            prd_key AS original_prd_key,  -- Rename the original prd_key
            REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key FROM 7) AS prd_key_short,  -- Rename the substring result
            prd_nm,
            COALESCE(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'R' THEN 'Road'
                WHEN 'M' THEN 'Mountain'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            LEAD(CAST(prd_start_dt AS DATE)) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS next_prd_start_dt
        FROM bronze.crm_prd_info
    )
    SELECT 
        prd_id,
        original_prd_key AS prd_key,  -- Use the renamed original_prd_key
        cat_id,
        prd_key_short,  -- Use the renamed prd_key_short
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        (next_prd_start_dt - INTERVAL '1 day')::DATE AS prd_end_dt
    FROM ranked_products;

    -- Insert into silver.crm_sales_details
    INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE 
            WHEN sls_order_dt <= 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) <> 8 THEN NULL
            ELSE TO_DATE(CAST(sls_order_dt AS TEXT), 'YYYYMMDD')
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt <= 0 OR LENGTH(CAST(sls_ship_dt AS TEXT)) <> 8 THEN NULL
            ELSE TO_DATE(CAST(sls_ship_dt AS TEXT), 'YYYYMMDD')
        END AS sls_ship_dt,
        CASE 
            WHEN sls_due_dt <= 0 OR LENGTH(CAST(sls_due_dt AS TEXT)) <> 8 THEN NULL
            ELSE TO_DATE(CAST(sls_due_dt AS TEXT), 'YYYYMMDD')
        END AS sls_due_dt,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;

    -- Insert into silver.erp_cust_az12
    INSERT INTO silver.erp_cust_az12 (
        cid, bdate, gen
    )
    SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
            ELSE cid
        END AS cid,
        CASE 
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;

    -- Insert into silver.erp_loc_a101
    INSERT INTO silver.erp_loc_a101 (
        cid, cntry
    )
    SELECT 
        REPLACE(cid, '-', '') AS cid,
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;

    -- Insert into silver.erp_px_cat_g1v2
    INSERT INTO silver.erp_px_cat_g1v2 (
        id, cat, subcat, maintenance
    )
    SELECT 
        TRIM(id) AS id, 
        TRIM(cat) AS cat,
        TRIM(subcat) AS subcat,
        TRIM(maintenance) AS maintenance
    FROM bronze.erp_px_cat_g1v2;

    -- Log completion
    RAISE NOTICE 'Data loaded into silver schema successfully.';
END;
$$;

CALL silver.load_data();