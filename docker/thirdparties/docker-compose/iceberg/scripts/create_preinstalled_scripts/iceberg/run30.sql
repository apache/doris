create database if not exists demo.test_db;
use demo.test_db;
CREATE TABLE test_variant_repro (
    id INT,
    v VARIANT
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '3',
    'write.format.default' = 'parquet'
);