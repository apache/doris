-- Hudi configuration and database setup
SET hoodie.datasource.write.hive_style_partitioning = true;
SET hoodie.upsert.shuffle.parallelism = 2;
SET hoodie.insert.shuffle.parallelism = 2;

CREATE DATABASE IF NOT EXISTS regression_hudi;
USE regression_hudi;

