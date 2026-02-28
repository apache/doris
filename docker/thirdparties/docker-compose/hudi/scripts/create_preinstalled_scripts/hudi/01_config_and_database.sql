-- Hudi configuration and database setup
SET hoodie.datasource.write.hive_style_partitioning = true;
SET hoodie.upsert.shuffle.parallelism = 2;
SET hoodie.insert.shuffle.parallelism = 2;

-- set time zone to UTC to ensure consistent timestamp handling across different machines
SET TIME ZONE 'UTC';

CREATE DATABASE IF NOT EXISTS regression_hudi;
USE regression_hudi;

