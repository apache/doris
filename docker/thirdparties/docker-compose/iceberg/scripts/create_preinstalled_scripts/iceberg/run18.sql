create database if not exists demo.partition_db;

use demo.partition_db;

-- set time zone to shanghai
SET TIME ZONE '+08:00';
-- Partition by date type
CREATE TABLE date_partitioned (
    id BIGINT,
    name STRING,
    partition_key DATE
) USING ICEBERG PARTITIONED BY (partition_key);
-- Insert data into date_partitioned table
INSERT INTO
    date_partitioned
VALUES (1, 'Alice', DATE '2024-01-01'),
    (2, 'Bob', DATE '2024-01-01'),
    (
        3,
        'Charlie',
        DATE '2024-02-01'
    ),
    (4, 'David', DATE '2024-02-01'),
    (5, 'Eve', DATE '2024-03-01'),
    (6, 'Null Date', NULL);

-- Partition by integer type
CREATE TABLE int_partitioned (
    id BIGINT,
    name STRING,
    partition_key INT
) USING ICEBERG PARTITIONED BY (partition_key);

-- Insert data into int_partitioned table
INSERT INTO
    int_partitioned
VALUES (1, 'Product A', 1),
    (2, 'Product B', 1),
    (3, 'Product C', 2),
    (4, 'Product D', 2),
    (5, 'Product E', 3),
    (6, 'Null Int', NULL);

-- Partition by float type
CREATE TABLE float_partitioned (
    id BIGINT,
    name STRING,
    partition_key FLOAT
) USING ICEBERG PARTITIONED BY (partition_key);

-- Insert data into float_partitioned table
INSERT INTO
    float_partitioned
VALUES (1, 'Item 1', 10.5),
    (2, 'Item 2', 20.75),
    (3, 'Item 3', 30.0),
    (4, 'Item 4', 40.25),
    (5, 'Item 5', 50.5),
    (6, 'Null Float', NULL);

-- Partition by string type
CREATE TABLE string_partitioned (
    id BIGINT,
    name STRING,
    partition_key STRING
) USING ICEBERG PARTITIONED BY (partition_key);

-- Insert data into string_partitioned table
INSERT INTO
    string_partitioned
VALUES (1, 'User1', 'North America'),
    (2, 'User2', 'North America'),
    (3, 'User3', 'Europe'),
    (4, 'User4', 'Europe'),
    (5, 'User5', 'Asia'),
    (6, 'User6', 'Asia'),
    (7, 'Null String', NULL);

-- Partition by timestamp type
CREATE TABLE timestamp_partitioned (
    id BIGINT,
    name STRING,
    partition_key TIMESTAMP
) USING ICEBERG PARTITIONED BY (partition_key);

-- Insert data into timestamp_partitioned table
INSERT INTO
    timestamp_partitioned
VALUES (
        1,
        'Event1',
        TIMESTAMP '2024-01-15 08:00:00'
    ),
    (
        2,
        'Event2',
        TIMESTAMP '2024-01-15 09:00:00'
    ),
    (
        3,
        'Event3',
        TIMESTAMP '2024-01-15 14:00:00'
    ),
    (
        4,
        'Event4',
        TIMESTAMP '2024-01-16 10:00:00'
    ),
    (
        5,
        'Event5',
        TIMESTAMP '2024-01-16 16:00:00'
    ),
    (
        6,
        'Null Timestamp',
        NULL
    );

-- Partition by timestamp_ntz type
CREATE TABLE timestamp_ntz_partitioned (
    id BIGINT,
    name STRING,
    partition_key TIMESTAMP_NTZ
) USING ICEBERG PARTITIONED BY (partition_key);

-- INSERT INTO timestamp_ntz_partitioned
INSERT INTO
    timestamp_ntz_partitioned
VALUES (
        1,
        'Event1',
        TIMESTAMP_NTZ '2024-01-15 08:00:00'
    ),
    (
        2,
        'Event2',
        TIMESTAMP_NTZ '2024-01-15 09:00:00'
    ),
    (
        3,
        'Event3',
        TIMESTAMP_NTZ '2024-01-15 14:00:00'
    ),
    (
        4,
        'Event4',
        TIMESTAMP_NTZ '2024-01-16 10:00:00'
    ),
    (
        5,
        'Event5',
        TIMESTAMP_NTZ '2024-01-16 16:00:00'
    ),
    (
        6,
        'Null Timestamp NTZ',
        NULL
    );

-- Partition by boolean type
CREATE TABLE boolean_partitioned (
    id BIGINT,
    name STRING,
    partition_key BOOLEAN
) USING ICEBERG PARTITIONED BY (partition_key);

-- Insert data into boolean_partitioned table
INSERT INTO
    boolean_partitioned
VALUES (1, 'Active User', true),
    (2, 'Active Admin', true),
    (3, 'Inactive User', false),
    (4, 'Inactive Guest', false),
    (5, 'Active Manager', true),
    (6, 'Null Boolean', NULL);

-- Partition by decimal type
CREATE TABLE decimal_partitioned (
    id BIGINT,
    name STRING,
    value FLOAT,
    partition_key DECIMAL(10, 2)
) USING ICEBERG PARTITIONED BY (partition_key);

-- Insert data into decimal_partitioned table
INSERT INTO
    decimal_partitioned
VALUES (1, 'Item A', 125.50, 10.50),
    (2, 'Item B', 200.75, 10.50),
    (3, 'Item C', 89.99, 25.25),
    (4, 'Item D', 156.80, 25.25),
    (5, 'Item E', 299.95, 50.00),
    (6, 'Item F', 399.99, 50.00),
    (7, 'Null Decimal', 0.0, NULL);
-- Partition by binary type
CREATE TABLE binary_partitioned (
    id BIGINT,
    name STRING,
    partition_key BINARY
) USING ICEBERG PARTITIONED BY (partition_key);

-- Insert data into binary_partitioned table
INSERT INTO
    binary_partitioned
VALUES (
        1,
        'Binary Data 1',
        CAST('01010101' AS BINARY)
    ),
    (
        2,
        'Binary Data 2',
        CAST('01010101' AS BINARY)
    ),
    (
        3,
        'Binary Data 3',
        CAST('11001100' AS BINARY)
    ),
    (
        4,
        'Binary Data 4',
        CAST('11001100' AS BINARY)
    ),
    (
        5,
        'Binary Data 5',
        CAST('11110000' AS BINARY)
    ),
    (
        6,
        'Null Binary',
        NULL
    );

-- Partition by string type with null values
CREATE TABLE null_str_partition_table (
    id BIGINT,
    category STRING,
    value DOUBLE
) USING iceberg PARTITIONED BY (category);

INSERT INTO
    null_str_partition_table
VALUES (1, NULL, 100.0),
    (2, 'NULL', 200.0),
    (3, '\\N', 300.0),
    (4, 'null', 400.0),
    (5, 'A', 500.0);