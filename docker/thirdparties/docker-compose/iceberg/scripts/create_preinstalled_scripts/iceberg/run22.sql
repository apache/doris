
create database if not exists demo.test_varbinary;
USE demo.test_varbinary;

DROP TABLE IF EXISTS test_ice_uuid_orc;
DROP TABLE IF EXISTS test_ice_uuid_parquet;

CREATE TABLE test_ice_uuid_orc (
    id INT,
    col1 BINARY COMMENT 'UUID stored as 16-byte binary',
    col2 BINARY COMMENT 'Binary data'
)
USING iceberg
TBLPROPERTIES(
    'write.format.default' = 'orc',
    'format-version' = '1'
);

CREATE TABLE test_ice_uuid_parquet (
    id INT,
    col1 BINARY COMMENT 'UUID stored as 16-byte binary',
    col2 BINARY COMMENT 'Binary data'
)
USING iceberg
TBLPROPERTIES(
    'write.format.default' = 'parquet',
    'format-version' = '1'
);

INSERT INTO test_ice_uuid_orc VALUES
    (1, X'550e8400e29b41d4a716446655440000', X'0123456789ABCDEF'),
    (2, X'123e4567e89b12d3a456426614174000', X'FEDCBA9876543210'),
    (3, X'00000000000000000000000000000000', X'00');

INSERT INTO test_ice_uuid_parquet VALUES
    (1, X'550e8400e29b41d4a716446655440000', X'0123456789ABCDEF'),
    (2, X'123e4567e89b12d3a456426614174000', X'FEDCBA9876543210'),
    (3, X'00000000000000000000000000000000', X'00');

SELECT * FROM test_ice_uuid_orc;
SELECT * FROM test_ice_uuid_parquet;

DROP TABLE IF EXISTS test_ice_uuid_orc_write_no_mapping;
DROP TABLE IF EXISTS test_ice_uuid_parquet_write_no_mapping;

CREATE TABLE test_ice_uuid_orc_write_no_mapping (
    id INT,
    col1 BINARY COMMENT 'UUID stored as 16-byte binary',
    col2 BINARY COMMENT 'Binary data'
)
USING iceberg
TBLPROPERTIES(
    'write.format.default' = 'orc',
    'format-version' = '1'
);

CREATE TABLE test_ice_uuid_parquet_write_no_mapping (
    id INT,
    col1 BINARY COMMENT 'UUID stored as 16-byte binary',
    col2 BINARY COMMENT 'Binary data'
)
USING iceberg
TBLPROPERTIES(
    'write.format.default' = 'parquet',
    'format-version' = '1'
);

DROP TABLE IF EXISTS test_ice_uuid_orc_write_with_mapping;
DROP TABLE IF EXISTS test_ice_uuid_parquet_write_with_mapping;

CREATE TABLE test_ice_uuid_orc_write_with_mapping (
    id INT,
    col1 BINARY COMMENT 'UUID stored as 16-byte binary',
    col2 BINARY COMMENT 'Binary data'
)
USING iceberg
TBLPROPERTIES(
    'write.format.default' = 'orc',
    'format-version' = '1'
);

CREATE TABLE test_ice_uuid_parquet_write_with_mapping (
    id INT,
    col1 BINARY COMMENT 'UUID stored as 16-byte binary',
    col2 BINARY COMMENT 'Binary data'
)
USING iceberg
TBLPROPERTIES(
    'write.format.default' = 'parquet',
    'format-version' = '1'
);