{# Test for {dataset_name} model #}

-- Test that {dataset_name} model has no null values in required fields

SELECT *
FROM {{ ref('{dataset_name}') }}
WHERE 
    -- Add your null checks here
    -- Example:
    -- id IS NULL
    -- OR name IS NULL
    1=0 -- Placeholder - replace with actual test conditions