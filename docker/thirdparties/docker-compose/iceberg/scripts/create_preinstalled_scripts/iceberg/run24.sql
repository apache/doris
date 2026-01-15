use demo.test_db;

DROP TABLE IF EXISTS test_struct_evolution;

-- Test case for struct schema evolution bug
-- Bug scenario: When querying a struct field after schema evolution, if all queried fields are missing
-- in old Parquet files, the code tries to find a reference column from file schema. However, if the
-- reference column (e.g., 'removed') was dropped from table schema, accessing it via root_node will fail.
--
-- Steps to reproduce:
-- 1. Create table with struct containing: removed, rename, keep, drop_and_add
-- 2. Insert data (creates Parquet file with these fields)
-- 3. DROP a_struct.removed - removes field from table schema
-- 4. DROP a_struct.drop_and_add then ADD a_struct.drop_and_add - gets new field ID
-- 5. ADD a_struct.added - adds new field
-- 6. Query struct_element(a_struct, 'drop_and_add') or struct_element(a_struct, 'added')
--    -> This will fail because all queried fields are missing in old file, and the reference
--       column 'removed' doesn't exist in root_node (it was dropped from table schema)

-- Step 1: Create table
CREATE TABLE test_struct_evolution (
    id BIGINT,
    a_struct STRUCT<removed: BIGINT, rename: BIGINT, keep: BIGINT, drop_and_add: BIGINT>
) USING ICEBERG
TBLPROPERTIES ('write.format.default' = 'parquet', 'format-version' = 2);

-- Step 2: Insert data (creates Parquet file with original schema)
INSERT INTO test_struct_evolution 
SELECT 1, named_struct('removed', 10, 'rename', 11, 'keep', 12, 'drop_and_add', 13);

-- Step 3: Schema evolution - drop removed field
ALTER TABLE test_struct_evolution DROP COLUMN a_struct.removed;

-- Step 4: Rename field (field ID stays the same)
ALTER TABLE test_struct_evolution RENAME COLUMN a_struct.rename TO renamed;

-- Step 5: Drop and add drop_and_add (new field ID)
ALTER TABLE test_struct_evolution DROP COLUMN a_struct.drop_and_add;
ALTER TABLE test_struct_evolution ADD COLUMN a_struct.drop_and_add BIGINT;

-- Step 6: Add new field
ALTER TABLE test_struct_evolution ADD COLUMN a_struct.added BIGINT;

-- Step 7: Insert new data after schema evolution (creates new Parquet file)
INSERT INTO test_struct_evolution 
SELECT 2, named_struct('renamed', 21, 'keep', 22, 'drop_and_add', 23, 'added', 24);

-- Now the table contains two Parquet files:
-- - Old file: contains removed, rename, keep, drop_and_add (old field ID)
-- - New file: contains renamed, keep, drop_and_add (new field ID), added
--
-- Querying struct_element(a_struct, 'drop_and_add') or struct_element(a_struct, 'added')
-- on the old file will trigger the bug

-- ============================================================
-- ORC format test table (for completeness, though ORC doesn't have the same bug)
-- ============================================================
DROP TABLE IF EXISTS test_struct_evolution_orc;

-- Create ORC format table with same schema evolution scenario
CREATE TABLE test_struct_evolution_orc (
    id BIGINT,
    a_struct STRUCT<removed: BIGINT, rename: BIGINT, keep: BIGINT, drop_and_add: BIGINT>
) USING ICEBERG
TBLPROPERTIES ('write.format.default' = 'orc', 'format-version' = 2);

-- Insert initial data (creates ORC file with original schema)
INSERT INTO test_struct_evolution_orc 
SELECT 1, named_struct('removed', 10, 'rename', 11, 'keep', 12, 'drop_and_add', 13);

-- Schema evolution - same operations as Parquet table
ALTER TABLE test_struct_evolution_orc DROP COLUMN a_struct.removed;
ALTER TABLE test_struct_evolution_orc RENAME COLUMN a_struct.rename TO renamed;
ALTER TABLE test_struct_evolution_orc DROP COLUMN a_struct.drop_and_add;
ALTER TABLE test_struct_evolution_orc ADD COLUMN a_struct.drop_and_add BIGINT;
ALTER TABLE test_struct_evolution_orc ADD COLUMN a_struct.added BIGINT;

-- Insert new data after schema evolution (creates new ORC file)
INSERT INTO test_struct_evolution_orc 
SELECT 2, named_struct('renamed', 21, 'keep', 22, 'drop_and_add', 23, 'added', 24);

-- ============================================================
-- Case sensitivity test table (mixed case field names)
-- ============================================================
DROP TABLE IF EXISTS test_struct_evolution_case;

-- Test case for struct schema evolution with mixed case field names
-- This tests that case sensitivity is handled correctly when:
-- - Field names have mixed case (e.g., REMOVED, rename, keep, drop_and_add)
-- - Schema evolution operations are performed
-- - Querying struct fields with different case patterns

-- Step 1: Create table with mixed case field names
CREATE TABLE test_struct_evolution_case (
    id BIGINT,
    a_struct STRUCT<REMOVED: BIGINT, rename: BIGINT, keep: BIGINT, drop_and_add: BIGINT>
) USING ICEBERG
TBLPROPERTIES ('write.format.default' = 'parquet', 'format-version' = 2);

-- Step 2: Insert data (creates Parquet file with original schema)
INSERT INTO test_struct_evolution_case 
SELECT 1, named_struct('REMOVED', 10, 'rename', 11, 'keep', 12, 'drop_and_add', 13);

-- Step 3: Schema evolution - drop REMOVED field (uppercase)
ALTER TABLE test_struct_evolution_case DROP COLUMN a_struct.REMOVED;

-- Step 4: Rename field (field ID stays the same)
ALTER TABLE test_struct_evolution_case RENAME COLUMN a_struct.rename TO renamed;

-- Step 5: Drop and add drop_and_add with case change (new field ID)
-- Initial: drop_and_add (lowercase), after re-add: DROP_AND_ADD (uppercase)
ALTER TABLE test_struct_evolution_case DROP COLUMN a_struct.drop_and_add;
ALTER TABLE test_struct_evolution_case ADD COLUMN a_struct.DROP_AND_ADD BIGINT;

-- Step 6: Add new field
ALTER TABLE test_struct_evolution_case ADD COLUMN a_struct.added BIGINT;

-- Step 7: Insert new data after schema evolution (creates new Parquet file)
-- Note: Use DROP_AND_ADD (uppercase) in the new data
INSERT INTO test_struct_evolution_case 
SELECT 2, named_struct('renamed', 21, 'keep', 22, 'DROP_AND_ADD', 23, 'added', 24);

-- ============================================================
-- ORC format test table with mixed case (for completeness)
-- ============================================================
DROP TABLE IF EXISTS test_struct_evolution_case_orc;

-- Create ORC format table with same schema evolution scenario and mixed case
CREATE TABLE test_struct_evolution_case_orc (
    id BIGINT,
    a_struct STRUCT<REMOVED: BIGINT, rename: BIGINT, keep: BIGINT, drop_and_add: BIGINT>
) USING ICEBERG
TBLPROPERTIES ('write.format.default' = 'orc', 'format-version' = 2);

-- Insert initial data (creates ORC file with original schema)
INSERT INTO test_struct_evolution_case_orc 
SELECT 1, named_struct('REMOVED', 10, 'rename', 11, 'keep', 12, 'drop_and_add', 13);

-- Schema evolution - same operations as Parquet table
ALTER TABLE test_struct_evolution_case_orc DROP COLUMN a_struct.REMOVED;
ALTER TABLE test_struct_evolution_case_orc RENAME COLUMN a_struct.rename TO renamed;
-- Drop and add with case change: drop_and_add (lowercase) -> DROP_AND_ADD (uppercase)
ALTER TABLE test_struct_evolution_case_orc DROP COLUMN a_struct.drop_and_add;
ALTER TABLE test_struct_evolution_case_orc ADD COLUMN a_struct.DROP_AND_ADD BIGINT;
ALTER TABLE test_struct_evolution_case_orc ADD COLUMN a_struct.added BIGINT;

-- Insert new data after schema evolution (creates new ORC file)
-- Note: Use DROP_AND_ADD (uppercase) in the new data
INSERT INTO test_struct_evolution_case_orc 
SELECT 2, named_struct('renamed', 21, 'keep', 22, 'DROP_AND_ADD', 23, 'added', 24);

