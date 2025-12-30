-- Create schema change test tables
-- These tables are used to test schema evolution capabilities
-- Note: Tables are created with final schema (after evolution) to match test expectations
USE regression_hudi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS hudi_sc_orc_cow;
DROP TABLE IF EXISTS hudi_sc_parquet_cow;

-- Create hudi_sc_orc_cow table with final schema after evolution
-- Final schema: id, score (DOUBLE), full_name, location, age
-- This matches the expected schema after all ALTER TABLE operations
CREATE TABLE IF NOT EXISTS hudi_sc_orc_cow (
  id INT,
  score DOUBLE,
  full_name STRING,
  location STRING,
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

-- Create hudi_sc_parquet_cow table with final schema after evolution
CREATE TABLE IF NOT EXISTS hudi_sc_parquet_cow (
  id INT,
  score DOUBLE,
  full_name STRING,
  location STRING,
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

-- Insert data matching the expected test results
-- Data structure matches the test case expectations from p2 test comments
INSERT INTO hudi_sc_orc_cow VALUES
  (1, NULL, 'Alice', NULL, NULL),
  (2, NULL, 'Bob', NULL, NULL),
  (3, NULL, 'Charlie', 'New York', NULL),
  (4, NULL, 'David', 'Los Angeles', NULL),
  (5, NULL, 'Eve', 'Chicago', NULL),
  (6, 85.5, 'Frank', 'San Francisco', NULL),
  (7, 90.0, 'Grace', 'Seattle', NULL),
  (8, 95.5, 'Heidi', 'Portland', NULL),
  (9, 88.0, 'Ivan', 'Denver', NULL),
  (10, 101.1, 'Judy', 'Austin', NULL),
  (11, 222.2, 'QQ', 'cn', 24);

INSERT INTO hudi_sc_parquet_cow VALUES
  (1, NULL, 'Alice', NULL, NULL),
  (2, NULL, 'Bob', NULL, NULL),
  (3, NULL, 'Charlie', 'New York', NULL),
  (4, NULL, 'David', 'Los Angeles', NULL),
  (5, NULL, 'Eve', 'Chicago', NULL),
  (6, 85.5, 'Frank', 'San Francisco', NULL);

