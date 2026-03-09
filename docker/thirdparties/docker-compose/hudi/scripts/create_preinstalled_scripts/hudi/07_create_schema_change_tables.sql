-- Create schema change test tables
-- These tables are used to test schema evolution capabilities
-- Reference: test_hudi_schema_change.groovy (p2) for the complete schema evolution process
-- Reference: https://hudi.apache.org/docs/schema_evolution
USE regression_hudi;

-- Set configuration to allow DROP COLUMN and RENAME COLUMN operations
-- According to Hudi docs, when using hive metastore, disable this config if encountering
-- "The following columns have types incompatible with the existing columns in their respective positions"
SET spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes=false;
SET hoodie.schema.on.read.enable=true;
SET hoodie.metadata.enable=false;
SET hoodie.parquet.small.file.limit=100;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS hudi_sc_orc_cow;
DROP TABLE IF EXISTS hudi_sc_parquet_cow;

-- ============================================================================
-- hudi_sc_orc_cow table - Complete schema evolution process
-- ============================================================================

-- Step 1: Create table with initial schema (id, name, age)
CREATE TABLE IF NOT EXISTS hudi_sc_orc_cow (
  id INT,
  name STRING,
  age INT
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.base.file.format = 'orc',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.parquet.small.file.limit = '100',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/hudi_sc_orc_cow';

-- Step 2: Insert initial data
INSERT INTO hudi_sc_orc_cow VALUES (1, 'Alice', 25);
INSERT INTO hudi_sc_orc_cow VALUES (2, 'Bob', 30);

-- Step 3: Add city column (schema: id, name, age, city)
ALTER TABLE hudi_sc_orc_cow ADD COLUMNS (city STRING);
INSERT INTO hudi_sc_orc_cow VALUES (3, 'Charlie', 28, 'New York');

-- Step 4: Drop age column (schema: id, name, city)
-- Note: DROP COLUMN operation is disabled in current version
-- ALTER TABLE hudi_sc_orc_cow DROP COLUMN age;
-- Schema remains: id, name, age, city
INSERT INTO hudi_sc_orc_cow VALUES (4, 'David', 35, 'Los Angeles');

-- Step 5: Rename name to full_name (schema: id, full_name, city)
-- Note: RENAME COLUMN operation is disabled in current version
-- ALTER TABLE hudi_sc_orc_cow RENAME COLUMN name TO full_name;
-- Schema remains: id, name, age, city
INSERT INTO hudi_sc_orc_cow VALUES (5, 'Eve', 28, 'Chicago');

-- Step 6: Add score column (schema: id, name, age, city, score)
-- Note: Column position (AFTER id) may not be supported, adding at the end
ALTER TABLE hudi_sc_orc_cow ADD COLUMNS (score FLOAT);
INSERT INTO hudi_sc_orc_cow VALUES (6, 'Frank', 32, 'San Francisco', 85.5);

-- Step 7: Change city position to after id (schema: id, city, score, full_name)
-- Note: REORDER operation (ALTER COLUMN ... AFTER) is disabled in current version
-- ALTER TABLE hudi_sc_orc_cow ALTER COLUMN city AFTER id;
-- Schema remains: id, name, age, city, score
INSERT INTO hudi_sc_orc_cow VALUES (7, 'Grace', 29, 'Seattle', 90.0);

-- Step 8: Change score type from float to double
-- According to Hudi docs: ALTER TABLE tableName ALTER COLUMN column_name TYPE type
ALTER TABLE hudi_sc_orc_cow ALTER COLUMN score TYPE DOUBLE;
INSERT INTO hudi_sc_orc_cow VALUES (8, 'Heidi', 31, 'Portland', 95.5);

-- Step 9: Rename city to location (schema: id, location, score, full_name)
-- Note: RENAME COLUMN operation is disabled in current version
-- ALTER TABLE hudi_sc_orc_cow RENAME COLUMN city TO location;
-- Schema remains: id, name, age, city, score
INSERT INTO hudi_sc_orc_cow VALUES (9, 'Ivan', 26, 'Denver', 88.0);

-- Step 10: Change location position to after full_name (schema: id, score, full_name, location)
-- Note: REORDER operation (ALTER COLUMN ... AFTER) is disabled in current version
-- ALTER TABLE hudi_sc_orc_cow ALTER COLUMN location AFTER full_name;
-- Schema remains: id, name, age, city, score
INSERT INTO hudi_sc_orc_cow VALUES (10, 'Judy', 27, 'Austin', 101.1);

-- Step 11: Add age column (final schema: id, name, age, city, score)
-- Note: age column already exists, so this step is skipped
-- ALTER TABLE hudi_sc_orc_cow ADD COLUMN age INT;
INSERT INTO hudi_sc_orc_cow VALUES (11, 'QQ', 24, 'cn', 222.2);

-- ============================================================================
-- hudi_sc_parquet_cow table - Same schema evolution process
-- ============================================================================

-- Step 1: Create table with initial schema (id, name, age)
CREATE TABLE IF NOT EXISTS hudi_sc_parquet_cow (
  id INT,
  name STRING,
  age INT
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.base.file.format = 'parquet',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.parquet.small.file.limit = '100',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/hudi_sc_parquet_cow';

-- Step 2: Insert initial data
INSERT INTO hudi_sc_parquet_cow VALUES (1, 'Alice', 25);
INSERT INTO hudi_sc_parquet_cow VALUES (2, 'Bob', 30);

-- Step 3: Add city column (schema: id, name, age, city)
ALTER TABLE hudi_sc_parquet_cow ADD COLUMNS (city STRING);
INSERT INTO hudi_sc_parquet_cow VALUES (3, 'Charlie', 28, 'New York');

-- Step 4: Drop age column (schema: id, name, city)
-- Note: DROP COLUMN operation is disabled in current version
-- ALTER TABLE hudi_sc_parquet_cow DROP COLUMN age;
-- Schema remains: id, name, age, city
INSERT INTO hudi_sc_parquet_cow VALUES (4, 'David', 35, 'Los Angeles');

-- Step 5: Rename name to full_name (schema: id, full_name, city)
-- Note: RENAME COLUMN operation is disabled in current version
-- ALTER TABLE hudi_sc_parquet_cow RENAME COLUMN name TO full_name;
-- Schema remains: id, name, age, city
INSERT INTO hudi_sc_parquet_cow VALUES (5, 'Eve', 28, 'Chicago');

-- Step 6: Add score column (schema: id, name, age, city, score)
-- Note: Column position (AFTER id) may not be supported, adding at the end
ALTER TABLE hudi_sc_parquet_cow ADD COLUMNS (score FLOAT);
INSERT INTO hudi_sc_parquet_cow VALUES (6, 'Frank', 32, 'San Francisco', 85.5);

-- Step 7: Change city position to after id (schema: id, city, score, full_name)
-- Note: REORDER operation (ALTER COLUMN ... AFTER) is disabled in current version
-- ALTER TABLE hudi_sc_parquet_cow ALTER COLUMN city AFTER id;
-- Schema remains: id, name, age, city, score
INSERT INTO hudi_sc_parquet_cow VALUES (7, 'Grace', 29, 'Seattle', 90.0);

-- Step 8: Change score type from float to double
-- According to Hudi docs: ALTER TABLE tableName ALTER COLUMN column_name TYPE type
ALTER TABLE hudi_sc_parquet_cow ALTER COLUMN score TYPE DOUBLE;
INSERT INTO hudi_sc_parquet_cow VALUES (8, 'Heidi', 31, 'Portland', 95.5);

-- Step 9: Rename city to location (schema: id, location, score, full_name)
-- Note: RENAME COLUMN operation is disabled in current version
-- ALTER TABLE hudi_sc_parquet_cow RENAME COLUMN city TO location;
-- Schema remains: id, name, age, city, score
INSERT INTO hudi_sc_parquet_cow VALUES (9, 'Ivan', 26, 'Denver', 88.0);

-- Step 10: Change location position to after full_name (schema: id, score, full_name, location)
-- Note: REORDER operation (ALTER COLUMN ... AFTER) is disabled in current version
-- ALTER TABLE hudi_sc_parquet_cow ALTER COLUMN location AFTER full_name;
-- Schema remains: id, name, age, city, score
INSERT INTO hudi_sc_parquet_cow VALUES (10, 'Judy', 27, 'Austin', 101.1);

-- Step 11: Add age column (final schema: id, name, age, city, score)
-- Note: age column already exists, so this step is skipped
-- ALTER TABLE hudi_sc_parquet_cow ADD COLUMN age INT;
INSERT INTO hudi_sc_parquet_cow VALUES (11, 'QQ', 24, 'cn', 222.2);
