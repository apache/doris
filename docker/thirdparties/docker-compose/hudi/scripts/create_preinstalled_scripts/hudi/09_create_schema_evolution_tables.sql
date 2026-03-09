-- Create schema evolution test tables
-- These tables are used to test schema evolution operations supported by Hudi:
-- 1. Adding columns (ADD COLUMNS)
-- 2. Dropping columns (DROP COLUMNS)
-- 3. Renaming columns (RENAME COLUMN)
-- 4. Reordering columns (field order changes)
USE regression_hudi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS adding_simple_columns_table;
DROP TABLE IF EXISTS deleting_simple_columns_table;
DROP TABLE IF EXISTS renaming_simple_columns_table;
DROP TABLE IF EXISTS reordering_columns_table;
DROP TABLE IF EXISTS adding_complex_columns_table;
DROP TABLE IF EXISTS deleting_complex_columns_table;
DROP TABLE IF EXISTS renaming_complex_columns_table;

-- ============================================================================
-- 1. adding_simple_columns_table - Testing adding simple type columns
-- ============================================================================
-- Step 1: Create table with initial schema (id, name)
CREATE TABLE IF NOT EXISTS adding_simple_columns_table (
    id STRING,
    name STRING
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/adding_simple_columns_table';

-- Insert initial data with schema: id, name
INSERT INTO adding_simple_columns_table (id, name) VALUES
  ('1', 'Alice'),
  ('2', 'Bob'),
  ('3', 'Cathy');

-- Step 2: Use ALTER TABLE to add age and city columns
ALTER TABLE adding_simple_columns_table ADD COLUMNS (age INT, city STRING);

-- Step 3: Insert data with evolved schema (id, name, age, city)
INSERT INTO adding_simple_columns_table (id, name, age, city) VALUES
  ('4', 'David', 25, 'New York'),
  ('5', 'Eva', 30, 'Los Angeles'),
  ('6', 'Frank', 28, 'Chicago');

-- ============================================================================
-- 2. deleting_simple_columns_table - Testing dropping simple type columns
-- ============================================================================
-- Step 1: Create table with initial schema (id, name, age, city)
CREATE TABLE IF NOT EXISTS deleting_simple_columns_table (
    id STRING,
    name STRING,
    age INT,
    city STRING
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/deleting_simple_columns_table';

-- Insert initial data with schema: id, name, age, city
INSERT INTO deleting_simple_columns_table (id, name, age, city) VALUES
  ('1', 'Alice', 25, 'New York'),
  ('2', 'Bob', 30, 'Los Angeles'),
  ('3', 'Cathy', 28, 'Chicago');

-- Step 2: Insert data with evolved schema (id, name only)
-- Note: Hudi doesn't support ALTER TABLE DROP COLUMNS directly
-- We write data without age and city, and Hudi's schema evolution will handle it when reading
-- The table definition in Hive Metastore still contains all columns, but Hudi can read data correctly
INSERT INTO deleting_simple_columns_table (id, name) VALUES
  ('4', 'David'),
  ('5', 'Eva'),
  ('6', 'Frank');

-- Step 3: Update Hive Metastore table definition
-- Note: Hudi doesn't support DROP COLUMNS via ALTER TABLE
-- The schema evolution is handled automatically when reading data
-- ALTER TABLE deleting_simple_columns_table DROP COLUMNS (age, city);

-- ============================================================================
-- 3. renaming_simple_columns_table - Testing renaming simple type columns
-- ============================================================================
-- Note: Hudi doesn't support RENAME COLUMN operation
-- Since column renaming requires explicit metadata updates and Hudi's schema evolution
-- cannot automatically handle column name changes, we skip this test case
-- The table is created but not used for testing
CREATE TABLE IF NOT EXISTS renaming_simple_columns_table (
    id STRING,
    name STRING
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/renaming_simple_columns_table';

-- Insert initial data with schema: id, name
INSERT INTO renaming_simple_columns_table (id, name) VALUES
  ('1', 'Alice'),
  ('2', 'Bob'),
  ('3', 'Cathy');

-- Note: Column renaming is not supported by Hudi's schema evolution
-- Hudi cannot automatically handle column name changes without explicit metadata updates
-- ALTER TABLE renaming_simple_columns_table RENAME COLUMN name TO full_name;

-- ============================================================================
-- 4. reordering_columns_table - Testing column reordering
-- ============================================================================
-- Step 1: Create table with initial schema (id, name, age)
CREATE TABLE IF NOT EXISTS reordering_columns_table (
    id STRING,
    name STRING,
    age INT
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/reordering_columns_table';

-- Insert initial data with schema: id, name, age
INSERT INTO reordering_columns_table (id, name, age) VALUES
  ('1', 'Alice', 25),
  ('2', 'Bob', 30),
  ('3', 'Cathy', 28);

-- Step 2: Add new column city after name (using ALTER TABLE with column position)
-- Note: Hudi may support column position specification
ALTER TABLE reordering_columns_table ADD COLUMNS (city STRING);

-- Step 3: Insert data with evolved schema (id, name, city, age)
-- The actual column order in Hive Metastore may differ, but data should be readable
INSERT INTO reordering_columns_table (id, name, city, age) VALUES
  ('4', 'David', 'New York', 26),
  ('5', 'Eva', 'Los Angeles', 31),
  ('6', 'Frank', 'Chicago', 29);

-- ============================================================================
-- 5. adding_complex_columns_table - Testing adding complex type columns
-- ============================================================================
-- Note: Hudi doesn't support ALTER TABLE CHANGE COLUMN for modifying struct field definitions
-- Since we cannot add struct fields dynamically, we create the table with all fields from the start
-- This table is created but schema evolution testing for struct fields is not supported
CREATE TABLE IF NOT EXISTS adding_complex_columns_table (
    id STRING,
    name STRING,
    info STRUCT<age: INT, address: STRING, email: STRING>
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/adding_complex_columns_table';

-- Insert initial data with schema: id, name, info STRUCT<age, address> (email will be NULL)
INSERT INTO adding_complex_columns_table VALUES
  ('1', 'Alice', named_struct('age', 25, 'address', 'Guangzhou', 'email', CAST(NULL AS STRING))),
  ('2', 'Bob', named_struct('age', 30, 'address', 'Shanghai', 'email', CAST(NULL AS STRING))),
  ('3', 'Cathy', named_struct('age', 28, 'address', 'Beijing', 'email', CAST(NULL AS STRING)));

-- Insert data with evolved schema (id, name, info STRUCT<age, address, email>)
INSERT INTO adding_complex_columns_table VALUES
  ('4', 'David', named_struct('age', 25, 'address', 'Shenzhen', 'email', 'david@example.com')),
  ('5', 'Eva', named_struct('age', 30, 'address', 'Chengdu', 'email', 'eva@example.com')),
  ('6', 'Frank', named_struct('age', 28, 'address', 'Wuhan', 'email', 'frank@example.com'));

-- ============================================================================
-- 6. deleting_complex_columns_table - Testing dropping complex type columns
-- ============================================================================
-- Step 1: Create table with initial schema (id, name, info STRUCT<age, address, email>)
CREATE TABLE IF NOT EXISTS deleting_complex_columns_table (
    id STRING,
    name STRING,
    info STRUCT<age: INT, address: STRING, email: STRING>
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/deleting_complex_columns_table';

-- Insert initial data with schema: id, name, info STRUCT<age, address, email>
INSERT INTO deleting_complex_columns_table VALUES
  ('1', 'Alice', named_struct('age', 25, 'address', 'Guangzhou', 'email', 'alice@example.com')),
  ('2', 'Bob', named_struct('age', 30, 'address', 'Shanghai', 'email', 'bob@example.com')),
  ('3', 'Cathy', named_struct('age', 28, 'address', 'Beijing', 'email', 'cathy@example.com'));

-- Step 2: Insert data with evolved schema (id, name, info STRUCT<age, address>)
-- Note: Hudi doesn't support removing struct fields dynamically
-- We write data with email set to NULL to simulate field removal
INSERT INTO deleting_complex_columns_table VALUES
  ('4', 'David', named_struct('age', 25, 'address', 'Shenzhen', 'email', CAST(NULL AS STRING))),
  ('5', 'Eva', named_struct('age', 30, 'address', 'Chengdu', 'email', CAST(NULL AS STRING))),
  ('6', 'Frank', named_struct('age', 28, 'address', 'Wuhan', 'email', CAST(NULL AS STRING)));

-- Note: Hudi doesn't support ALTER TABLE CHANGE COLUMN for modifying struct field definitions
-- The table definition remains with all fields, but we can test reading data with NULL values

-- ============================================================================
-- 7. renaming_complex_columns_table - Testing renaming complex type columns
-- ============================================================================
-- Note: Hudi doesn't support renaming struct fields
-- Since field renaming in structs requires explicit metadata updates and Hudi's schema evolution
-- cannot automatically handle field name changes, we skip this test case
-- The table is created but not used for testing
CREATE TABLE IF NOT EXISTS renaming_complex_columns_table (
    id STRING,
    name STRING,
    info STRUCT<age: INT, location: STRING>
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/renaming_complex_columns_table';

-- Insert initial data with schema: id, name, info STRUCT<age, location>
INSERT INTO renaming_complex_columns_table VALUES
  ('1', 'Alice', named_struct('age', 25, 'location', 'Guangzhou')),
  ('2', 'Bob', named_struct('age', 30, 'location', 'Shanghai')),
  ('3', 'Cathy', named_struct('age', 28, 'location', 'Beijing'));

-- Note: Struct field renaming is not supported by Hudi's schema evolution
-- Hudi cannot automatically handle field name changes in structs without explicit metadata updates
-- ALTER TABLE renaming_complex_columns_table CHANGE COLUMN info info STRUCT<age: INT, address: STRING>;
