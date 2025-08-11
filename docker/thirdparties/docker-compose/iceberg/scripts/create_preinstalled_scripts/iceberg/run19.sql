create database if not exists transform_partition_db;

use transform_partition_db;

-- set time zone for deterministic timestamp partitioning
SET TIME ZONE '+08:00';

-- =============================================
-- Bucket partition coverage across many types
-- =============================================
-- Bucket by INT but empty
CREATE TABLE bucket_int_empty (
    id BIGINT,
    name STRING,
    partition_key INT
) USING ICEBERG PARTITIONED BY (bucket (4, partition_key));

-- Bucket by INT
CREATE TABLE bucket_int_4 (
    id BIGINT,
    name STRING,
    partition_key INT
) USING ICEBERG PARTITIONED BY (bucket (4, partition_key));

CREATE TABLE bucket_int_4_copy AS
SELECT * FROM bucket_int_4;

INSERT INTO
    bucket_int_4
VALUES (1, 'n100', -100),
    (2, 'n1', -1),
    (3, 'z', 0),
    (4, 'p1', 1),
    (5, 'p2', 2),
    (6, 'p9', 9),
    (7, 'p16', 16),
    (8, 'null', NULL);

-- Bucket by BIGINT
CREATE TABLE bucket_bigint_4 (
    id BIGINT,
    name STRING,
    partition_key BIGINT
) USING ICEBERG PARTITIONED BY (bucket (4, partition_key));

CREATE TABLE bucket_bigint_4_copy AS
SELECT * FROM bucket_bigint_4;

INSERT INTO
    bucket_bigint_4
VALUES (
        1,
        'minish',
        -9223372036854775808
    ),
    (2, 'large-', -1234567890123),
    (3, 'neg1', -1),
    (4, 'zero', 0),
    (5, 'pos1', 1),
    (6, 'large+', 1234567890123),
    (
        7,
        'maxish',
        9223372036854775807
    ),
    (8, 'null', NULL);

-- Bucket by STRING
CREATE TABLE bucket_string_4 (
    id BIGINT,
    name STRING,
    partition_key STRING
) USING ICEBERG PARTITIONED BY (bucket (4, partition_key));

CREATE TABLE bucket_string_4_copy AS
SELECT * FROM bucket_string_4;

INSERT INTO
    bucket_string_4
VALUES (1, 'empty', ''),
    (2, 'a', 'a'),
    (3, 'abc', 'abc'),
    (4, 'unicode', '北'),
    (5, 'emoji', '😊'),
    (6, 'space', ' '),
    (
        7,
        'long',
        'this is a relatively long string for hashing'
    ),
    (8, 'null', NULL);

-- Bucket by DATE
CREATE TABLE bucket_date_4 (
    id BIGINT,
    name STRING,
    partition_key DATE
) USING ICEBERG PARTITIONED BY (bucket (4, partition_key));

CREATE TABLE bucket_date_4_copy AS
SELECT * FROM bucket_date_4;

INSERT INTO
    bucket_date_4
VALUES (1, 'd1', DATE '1970-01-01'),
    (2, 'd2', DATE '1999-12-31'),
    (3, 'd3', DATE '2000-01-01'),
    (4, 'd4', DATE '2024-02-29'),
    (5, 'd5', DATE '2024-03-01'),
    (6, 'd6', DATE '2038-01-19'),
    (7, 'null', NULL);

-- Bucket by TIMESTAMP (with time zone semantics)
CREATE TABLE bucket_timestamp_4 (
    id BIGINT,
    name STRING,
    partition_key TIMESTAMP
) USING ICEBERG PARTITIONED BY (bucket (4, partition_key));

CREATE TABLE bucket_timestamp_4_copy AS
SELECT * FROM bucket_timestamp_4;

INSERT INTO
    bucket_timestamp_4
VALUES (
        1,
        't1',
        TIMESTAMP '2024-01-15 08:00:00'
    ),
    (
        2,
        't2',
        TIMESTAMP '2024-01-15 09:00:00'
    ),
    (
        3,
        't3',
        TIMESTAMP '2024-06-30 23:59:59'
    ),
    (
        4,
        't4',
        TIMESTAMP '1970-01-01 00:00:00'
    ),
    (
        5,
        't5',
        TIMESTAMP '2030-12-31 23:59:59'
    ),
    (6, 'null', NULL);

-- Bucket by TIMESTAMP_NTZ
CREATE TABLE bucket_timestamp_ntz_4 (
    id BIGINT,
    name STRING,
    partition_key TIMESTAMP_NTZ
) USING ICEBERG PARTITIONED BY (bucket (4, partition_key));

CREATE TABLE bucket_timestamp_ntz_4_copy AS
SELECT * FROM bucket_timestamp_ntz_4;

INSERT INTO
    bucket_timestamp_ntz_4
VALUES (
        1,
        'ntz1',
        TIMESTAMP_NTZ '2024-01-15 08:00:00'
    ),
    (
        2,
        'ntz2',
        TIMESTAMP_NTZ '2024-01-15 09:00:00'
    ),
    (
        3,
        'ntz3',
        TIMESTAMP_NTZ '2024-06-30 23:59:59'
    ),
    (
        4,
        'ntz4',
        TIMESTAMP_NTZ '1970-01-01 00:00:00'
    ),
    (
        5,
        'ntz5',
        TIMESTAMP_NTZ '2030-12-31 23:59:59'
    ),
    (6, 'null', NULL);

-- Bucket by DECIMAL
CREATE TABLE bucket_decimal_4 (
    id BIGINT,
    name STRING,
    partition_key DECIMAL(10, 2)
) USING ICEBERG PARTITIONED BY (bucket (4, partition_key));

CREATE TABLE bucket_decimal_4_copy AS
SELECT * FROM bucket_decimal_4;

INSERT INTO
    bucket_decimal_4
VALUES (1, 'p1', 0.00),
    (2, 'p2', 1.00),
    (3, 'p3', 10.50),
    (4, 'n1', -1.25),
    (5, 'n2', -10.50),
    (6, 'big', 9999999.99),
    (7, 'null', NULL);

-- Bucket by BINARY
CREATE TABLE bucket_binary_4 (
    id BIGINT,
    name STRING,
    partition_key BINARY
) USING ICEBERG PARTITIONED BY (bucket (4, partition_key));

CREATE TABLE bucket_binary_4_copy AS
SELECT * FROM bucket_binary_4;

INSERT INTO
    bucket_binary_4
VALUES (1, 'b1', CAST('' AS BINARY)),
    (2, 'b2', CAST('a' AS BINARY)),
    (
        3,
        'b3',
        CAST('abc' AS BINARY)
    ),
    (4, 'b4', CAST('你好' AS BINARY)),
    (
        5,
        'b5',
        CAST('01010101' AS BINARY)
    ),
    (6, 'null', NULL);

-- =============================================
-- Truncate partition coverage for supported types
-- =============================================

-- Truncate STRING to length 3
CREATE TABLE truncate_string_3 (
    id BIGINT,
    name STRING,
    partition_key STRING
) USING ICEBERG PARTITIONED BY (
    truncate (3, partition_key)
);

CREATE TABLE truncate_string_3_copy AS
SELECT * FROM truncate_string_3;

INSERT INTO
    truncate_string_3
VALUES (1, 'empty', ''),
    (2, 'short', 'a'),
    (3, 'two', 'ab'),
    (4, 'three', 'abc'),
    (5, 'long', 'abcdef'),
    (6, 'unicode', '你好世界'),
    (7, 'space', '  ab'),
    (8, 'null', NULL);

-- Truncate BINARY to length 4 bytes
CREATE TABLE truncate_binary_4 (
    id BIGINT,
    name STRING,
    partition_key BINARY
) USING ICEBERG PARTITIONED BY (
    truncate (4, partition_key)
);

CREATE TABLE truncate_binary_4_copy AS
SELECT * FROM truncate_binary_4;

INSERT INTO
    truncate_binary_4
VALUES (1, 'b0', CAST('' AS BINARY)),
    (2, 'b1', CAST('a' AS BINARY)),
    (
        3,
        'b2',
        CAST('abcd' AS BINARY)
    ),
    (
        4,
        'b3',
        CAST('abcdef' AS BINARY)
    ),
    (5, 'b4', CAST('你好' AS BINARY)),
    (6, 'null', NULL);

-- Truncate INT by width 10
CREATE TABLE truncate_int_10 (
    id BIGINT,
    name STRING,
    partition_key INT
) USING ICEBERG PARTITIONED BY (
    truncate (10, partition_key)
);

CREATE TABLE truncate_int_10_copy AS
SELECT * FROM truncate_int_10;

INSERT INTO
    truncate_int_10
VALUES (1, 'n23', -23),
    (2, 'n1', -1),
    (3, 'z', 0),
    (4, 'p7', 7),
    (5, 'p10', 10),
    (6, 'p19', 19),
    (7, 'p20', 20),
    (8, 'p999', 999),
    (9, 'null', NULL);

-- Truncate BIGINT by width 100
CREATE TABLE truncate_bigint_100 (
    id BIGINT,
    name STRING,
    partition_key BIGINT
) USING ICEBERG PARTITIONED BY (
    truncate (100, partition_key)
);

CREATE TABLE truncate_bigint_100_copy AS
SELECT * FROM truncate_bigint_100;

INSERT INTO
    truncate_bigint_100
VALUES (1, 'n1001', -1001),
    (2, 'n1', -1),
    (3, 'z', 0),
    (4, 'p7', 7),
    (5, 'p100', 100),
    (6, 'p199', 199),
    (7, 'p200', 200),
    (8, 'p10101', 10101),
    (9, 'null', NULL);

-- Truncate DECIMAL(10,2) by width 10
CREATE TABLE truncate_decimal_10 (
    id BIGINT,
    name STRING,
    partition_key DECIMAL(10, 2)
) USING ICEBERG PARTITIONED BY (
    truncate (10, partition_key)
);

CREATE TABLE truncate_decimal_10_copy AS
SELECT * FROM truncate_decimal_10;

INSERT INTO
    truncate_decimal_10
VALUES (1, 'z', 0.00),
    (2, 'p1', 1.23),
    (3, 'p9', 9.99),
    (4, 'p10', 10.00),
    (5, 'p19', 19.99),
    (6, 'p20', 20.00),
    (7, 'n1', -1.23),
    (8, 'n20', -20.00),
    (9, 'big', 9999999.99),
    (10, 'null', NULL);