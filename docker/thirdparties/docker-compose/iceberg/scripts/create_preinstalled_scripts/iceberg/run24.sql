// New file: docker/thirdparties/docker-compose/iceberg/scripts/create_preinstalled_scripts/iceberg/run_mixed_format.sql
-- Pre-create mixed-format Iceberg table for Doris regression test
CREATE DATABASE IF NOT EXISTS test_mixed_format_db;

USE test_mixed_format_db;

DROP TABLE IF EXISTS mixed_format_table;

-- Create Iceberg table with default write format = parquet
CREATE TABLE mixed_format_table (
    id   INT,
    data STRING
)
USING iceberg
PARTITIONED BY (id)
TBLPROPERTIES (
    'write.format.default' = 'parquet'
);

-- First write: parquet data
INSERT INTO mixed_format_table VALUES (1, 'parquet_data');

-- Switch default write format to ORC for subsequent writes
ALTER TABLE test_mixed_format_db.mixed_format_table
SET TBLPROPERTIES ('write.format.default' = 'orc');

-- Second write: orc data
INSERT INTO mixed_format_table VALUES (2, 'orc_data');
