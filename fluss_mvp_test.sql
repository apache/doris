
-- Integration test for Doris-Fluss MVP (Phase 1)

-- 1. Create a Fluss table
-- This should succeed if the FE changes are correct.
CREATE TABLE fluss_test_table (
    `id` INT,
    `data` VARCHAR(255)
)
ENGINE=Fluss
PROPERTIES (
    "fluss.stream" = "my_test_stream"
);

-- 2. Describe the table
-- This will show that Doris has correctly created the table with the 'Fluss' type.
DESCRIBE fluss_test_table;

-- 3. Attempt to select from the table
-- This query is expected to FAIL. We have not implemented the BE part yet.
-- The failure will prove that the FE has correctly identified the table as a Fluss
-- table and is trying to use a non-existent execution path.
-- Look for an error message like "Not implemented yet" or "Unsupported table type".
SELECT * FROM fluss_test_table LIMIT 10;
