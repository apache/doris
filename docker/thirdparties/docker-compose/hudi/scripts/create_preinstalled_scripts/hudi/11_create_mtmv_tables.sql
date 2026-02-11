-- Create MTMV test tables for Hudi materialized view tests
-- These tables are used by test_hudi_mtmv, test_hudi_rewrite_mtmv, and test_hudi_olap_rewrite_mtmv
USE regression_hudi;

-- Create database for MTMV tests if it doesn't exist
CREATE DATABASE IF NOT EXISTS hudi_mtmv_regression_test;
USE hudi_mtmv_regression_test;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS hudi_table_1;
DROP TABLE IF EXISTS hudi_table_two_partitions;
DROP TABLE IF EXISTS hudi_table_null_partition;

-- ============================================================================
-- hudi_table_1: Table with par partition (values: 'a', 'b')
-- Used for basic MTMV tests, partition refresh, and rewrite tests
-- ============================================================================
CREATE TABLE IF NOT EXISTS hudi_table_1 (
  id INT,
  age INT,
  par STRING
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.parquet.small.file.limit = '100',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
PARTITIONED BY (par)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/hudi_mtmv_regression_test/hudi_table_1';

-- Insert data for partition 'a'
INSERT INTO hudi_table_1 PARTITION (par='a') VALUES
  (1, 25),
  (2, 30),
  (3, 28),
  (4, 35),
  (5, 22);

-- Insert data for partition 'b'
INSERT INTO hudi_table_1 PARTITION (par='b') VALUES
  (6, 40),
  (7, 27),
  (8, 32),
  (9, 29),
  (10, 38);

-- ============================================================================
-- hudi_table_two_partitions: Table with create_date partition (DATE type)
-- Used for date partition tests and partition sync limit tests
-- Partition values: 2020-01-01, 2038-01-01, 2038-01-02
-- ============================================================================
CREATE TABLE IF NOT EXISTS hudi_table_two_partitions (
  id INT,
  name STRING,
  value INT,
  create_date DATE
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.parquet.small.file.limit = '100',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
PARTITIONED BY (create_date)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/hudi_mtmv_regression_test/hudi_table_two_partitions';

-- Insert data for partition 2020-01-01
INSERT INTO hudi_table_two_partitions PARTITION (create_date='2020-01-01') VALUES
  (1, 'name1', 100),
  (2, 'name2', 200),
  (3, 'name3', 300);

-- Insert data for partition 2038-01-01
INSERT INTO hudi_table_two_partitions PARTITION (create_date='2038-01-01') VALUES
  (4, 'name4', 400),
  (5, 'name5', 500),
  (6, 'name6', 600),
  (7, 'name7', 700);

-- Insert data for partition 2038-01-02
INSERT INTO hudi_table_two_partitions PARTITION (create_date='2038-01-02') VALUES
  (8, 'name8', 800),
  (9, 'name9', 900),
  (10, 'name10', 1000);

-- ============================================================================
-- hudi_table_null_partition: Table with region partition (contains NULL values)
-- Used for null partition handling tests
-- Partition values: NULL, 'bj'
-- ============================================================================
CREATE TABLE IF NOT EXISTS hudi_table_null_partition (
  id INT,
  name STRING,
  value INT,
  region STRING
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.parquet.small.file.limit = '100',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
PARTITIONED BY (region)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/hudi_mtmv_regression_test/hudi_table_null_partition';

-- Insert data for partition NULL (using __HIVE_DEFAULT_PARTITION__)
-- Note: Hudi/Hive uses '__HIVE_DEFAULT_PARTITION__' for NULL partition values
-- When queried, this will appear as 'NULL' or '__HIVE_DEFAULT_PARTITION__'
INSERT INTO hudi_table_null_partition PARTITION (region='__HIVE_DEFAULT_PARTITION__') VALUES
  (1, 'name1', 100),
  (2, 'name2', 200),
  (3, 'name3', 300);

-- Insert data for partition 'bj'
INSERT INTO hudi_table_null_partition PARTITION (region='bj') VALUES
  (4, 'name4', 400),
  (5, 'name5', 500),
  (6, 'name6', 600);

