INSERT INTO silver.crm_prd_info(
prd_id, prd_key, cat_id, prd_key_short, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
)
WITH ranked_products AS (
    SELECT 
        prd_id,
        prd_key AS original_prd_key,  -- Rename the original prd_key
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7) AS prd_key_short,  -- Rename the substring result
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
	ORDER BY prd_id
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