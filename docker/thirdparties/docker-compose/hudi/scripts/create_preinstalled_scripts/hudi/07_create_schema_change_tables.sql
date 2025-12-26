-- Create schema change test tables
-- These tables are used to test schema evolution capabilities
USE regression_hudi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS hudi_sc_orc_cow;
DROP TABLE IF EXISTS hudi_sc_parquet_cow;

-- Create hudi_sc_orc_cow table with initial schema
CREATE TABLE IF NOT EXISTS hudi_sc_orc_cow (
  id INT,
  name STRING,
  age INT
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.base.file.format = 'orc',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/hudi_sc_orc_cow';

-- Create hudi_sc_parquet_cow table with initial schema
CREATE TABLE IF NOT EXISTS hudi_sc_parquet_cow (
  id INT,
  name STRING,
  age INT
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.base.file.format = 'parquet',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/hudi_sc_parquet_cow';

-- Note: Schema changes (ALTER TABLE) are performed in test cases or separate scripts
-- This file only creates the initial table structure
-- The test cases will perform ALTER TABLE operations to test schema evolution

