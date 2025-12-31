-- Create schema change test tables
-- These tables are used to test schema evolution capabilities
-- Tables are created with initial schema, then ALTER TABLE is used to add columns
USE regression_hudi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS hudi_sc_orc_cow;
DROP TABLE IF EXISTS hudi_sc_parquet_cow;

-- Create hudi_sc_orc_cow table with initial schema (before evolution)
-- Initial schema: id, score, full_name, location
CREATE TABLE IF NOT EXISTS hudi_sc_orc_cow (
  id INT,
  score DOUBLE,
  full_name STRING,
  location STRING
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

-- Insert initial data (without age column)
INSERT INTO hudi_sc_orc_cow (id, score, full_name, location) VALUES
  (1, NULL, 'Alice', NULL),
  (2, NULL, 'Bob', NULL),
  (3, NULL, 'Charlie', 'New York'),
  (4, NULL, 'David', 'Los Angeles'),
  (5, NULL, 'Eve', 'Chicago'),
  (6, 85.5, 'Frank', 'San Francisco'),
  (7, 90.0, 'Grace', 'Seattle'),
  (8, 95.5, 'Heidi', 'Portland'),
  (9, 88.0, 'Ivan', 'Denver'),
  (10, 101.1, 'Judy', 'Austin');

-- Execute schema change: Add age column
ALTER TABLE hudi_sc_orc_cow ADD COLUMNS (age INT);

-- Insert data with the new age column
INSERT INTO hudi_sc_orc_cow (id, score, full_name, location, age) VALUES
  (11, 222.2, 'QQ', 'cn', 24);

-- Create hudi_sc_parquet_cow table with initial schema (before evolution)
CREATE TABLE IF NOT EXISTS hudi_sc_parquet_cow (
  id INT,
  score DOUBLE,
  full_name STRING,
  location STRING
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

-- Insert initial data (without age column)
INSERT INTO hudi_sc_parquet_cow (id, score, full_name, location) VALUES
  (1, NULL, 'Alice', NULL),
  (2, NULL, 'Bob', NULL),
  (3, NULL, 'Charlie', 'New York'),
  (4, NULL, 'David', 'Los Angeles'),
  (5, NULL, 'Eve', 'Chicago'),
  (6, 85.5, 'Frank', 'San Francisco');

-- Execute schema change: Add age column
ALTER TABLE hudi_sc_parquet_cow ADD COLUMNS (age INT);
