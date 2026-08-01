-- Data Quality Test for {dataset_name}

-- Schema validation test
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN id IS NULL THEN 1 END) as null_id_count,
    COUNT(CASE WHEN name IS NULL THEN 1 END) as null_name_count
FROM {dataset_name}_table
WHERE 
    -- Add your schema validation conditions here
    -- Example:
    -- id IS NOT NULL
    -- AND name IS NOT NULL
    1=0; -- Placeholder - replace with actual validation conditions


-- Completeness test
SELECT 
    COUNT(*) as total_records,
    COUNT(*) - COUNT(id) as missing_id_count
FROM {dataset_name}_table
WHERE 
    -- Add your completeness validation conditions here
    -- Example:
    -- id IS NOT NULL
    1=0; -- Placeholder - replace with actual validation conditions;


-- Uniqueness test
SELECT 
    id,
    COUNT(*) as count
FROM {dataset_name}_table
WHERE 
    -- Add your uniqueness validation conditions here
    -- Example:
    -- id IS NOT NULL
    1=0 -- Placeholder - replace with actual validation conditions
GROUP BY id
HAVING COUNT(*) > 1;


-- Timeliness test (example for timestamp field)
SELECT 
    COUNT(*) as total_records,
    MAX(created_at) as latest_timestamp,
    MIN(created_at) as earliest_timestamp
FROM {dataset_name}_table
WHERE 
    -- Add your timeliness validation conditions here
    -- Example:
    -- created_at >= DATEADD(HOUR, -24, GETDATE())
    1=0; -- Placeholder - replace with actual validation conditions;