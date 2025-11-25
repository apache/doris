create database if not exists test_varbinary;
use test_varbinary;

DROP TABLE IF EXISTS test_hive_binary_orc;
DROP TABLE IF EXISTS test_hive_binary_parquet;


CREATE TABLE test_hive_binary_orc (
  id INT COMMENT 'Primary key',
  col1 BINARY COMMENT 'UUID stored as 16-byte binary',
  col2 BINARY COMMENT 'Variable length binary data'
)
COMMENT 'Test table for BINARY type in ORC format'
STORED AS ORC;


INSERT INTO test_hive_binary_orc 
SELECT 1, unhex('550e8400e29b41d4a716446655440000'), unhex('0123456789ABCDEF');
  
INSERT INTO test_hive_binary_orc 
SELECT 2, unhex('123e4567e89b12d3a456426614174000'), unhex('FEDCBA9876543210');
  
INSERT INTO test_hive_binary_orc 
SELECT 3, unhex('00000000000000000000000000000000'), unhex('00');


INSERT INTO test_hive_binary_orc SELECT 4, NULL, NULL;


INSERT INTO test_hive_binary_orc 
SELECT 5, unhex('ABCDEF1234567890'), unhex('FFFF');


CREATE TABLE test_hive_binary_parquet (
  id INT COMMENT 'Primary key',
  col1 BINARY COMMENT 'UUID stored as 16-byte binary',
  col2 BINARY COMMENT 'Variable length binary data'
)
COMMENT 'Test table for BINARY type in Parquet format'
STORED AS PARQUET;


INSERT INTO test_hive_binary_parquet 
SELECT 1, unhex('550e8400e29b41d4a716446655440000'), unhex('0123456789ABCDEF');
  
INSERT INTO test_hive_binary_parquet 
SELECT 2, unhex('123e4567e89b12d3a456426614174000'), unhex('FEDCBA9876543210');
  
INSERT INTO test_hive_binary_parquet 
SELECT 3, unhex('00000000000000000000000000000000'), unhex('00');


INSERT INTO test_hive_binary_parquet SELECT 4, NULL, NULL;


INSERT INTO test_hive_binary_parquet 
SELECT 5, unhex('ABCDEF1234567890'), unhex('FFFF');


DROP TABLE IF EXISTS test_hive_binary_orc_write_no_mapping;
CREATE TABLE test_hive_binary_orc_write_no_mapping (
  id INT,
  col1 BINARY,
  col2 BINARY
)
STORED AS ORC;


DROP TABLE IF EXISTS test_hive_binary_parquet_write_no_mapping;
CREATE TABLE test_hive_binary_parquet_write_no_mapping (
  id INT,
  col1 BINARY,
  col2 BINARY
)
STORED AS PARQUET;


DROP TABLE IF EXISTS test_hive_binary_orc_write_with_mapping;
CREATE TABLE test_hive_binary_orc_write_with_mapping (
  id INT,
  col1 BINARY,
  col2 BINARY
)
STORED AS ORC;


DROP TABLE IF EXISTS test_hive_binary_parquet_write_with_mapping;
CREATE TABLE test_hive_binary_parquet_write_with_mapping (
  id INT,
  col1 BINARY,
  col2 BINARY
)
STORED AS PARQUET;


DROP TABLE IF EXISTS test_hive_uuid_fixed_orc;
CREATE TABLE test_hive_uuid_fixed_orc (
  id INT,
  uuid_col BINARY COMMENT '16-byte UUID',
  created_at STRING
)
STORED AS ORC;

INSERT INTO test_hive_uuid_fixed_orc 
SELECT 1, unhex('550e8400e29b41d4a716446655440000'), '2024-01-01';
INSERT INTO test_hive_uuid_fixed_orc 
SELECT 2, unhex('123e4567e89b12d3a456426614174000'), '2024-01-02';
INSERT INTO test_hive_uuid_fixed_orc 
SELECT 3, unhex('deadbeefcafebabeabcdef0123456789'), '2024-01-03';

DROP TABLE IF EXISTS test_hive_uuid_fixed_parquet;
CREATE TABLE test_hive_uuid_fixed_parquet (
  id INT,
  uuid_col BINARY COMMENT '16-byte UUID',
  created_at STRING
)
STORED AS PARQUET;

INSERT INTO test_hive_uuid_fixed_parquet 
SELECT 1, unhex('550e8400e29b41d4a716446655440000'), '2024-01-01';
INSERT INTO test_hive_uuid_fixed_parquet 
SELECT 2, unhex('123e4567e89b12d3a456426614174000'), '2024-01-02';
INSERT INTO test_hive_uuid_fixed_parquet 
SELECT 3, unhex('deadbeefcafebabeabcdef0123456789'), '2024-01-03';


DROP TABLE IF EXISTS test_hive_binary_edge_cases;
CREATE TABLE test_hive_binary_edge_cases (
  id INT,
  empty_binary BINARY,
  single_byte BINARY,
  max_length BINARY
)
STORED AS PARQUET;


INSERT INTO test_hive_binary_edge_cases 
SELECT 1, unhex(''), unhex('FF'), unhex('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF');

INSERT INTO test_hive_binary_edge_cases 
SELECT 2, NULL, unhex('00'), unhex('00000000000000000000000000000000');

INSERT INTO test_hive_binary_edge_cases 
SELECT 3, unhex(''), unhex('AB'), unhex('0123456789ABCDEF0123456789ABCDEF');
