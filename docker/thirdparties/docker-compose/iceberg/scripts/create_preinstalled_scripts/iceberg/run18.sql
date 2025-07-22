create database if not exists demo.partition_db;

use demo.partition_db;

-- set time zone to shanghai
set spark.sql.session.timeZone = 'Asia/Shanghai';
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
    (5, 'Eve', DATE '2024-03-01');

-- Partition by time type
CREATE TABLE time_partitioned (
    id BIGINT,
    name STRING,
    partition_key TIME
) USING ICEBERG PARTITIONED BY (partition_key);

-- Insert data into time_partitioned table
INSERT INTO
    time_partitioned
VALUES (
        1,
        'Morning Shift',
        TIME '08:00:00'
    ),
    (
        2,
        'Afternoon Shift',
        TIME '12:00:00'
    ),
    (
        3,
        'Evening Shift',
        TIME '18:00:00'
    ),
    (
        4,
        'Night Shift',
        TIME '22:00:00'
    ),
    (
        5,
        'Overnight Shift',
        TIME '02:00:00'
    );

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
    (5, 'Product E', 3);

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
    (5, 'Item 5', 50.5);

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
    (6, 'User6', 'Asia');

-- Partition by fixed type
CREATE TABLE fixed_partitioned (
    id BIGINT,
    name STRING,
    partition_key FIXED(16)
) USING ICEBERG PARTITIONED BY (partition_key);

-- Insert data into fixed_partitioned table
INSERT INTO
    fixed_partitioned
VALUES (
        1,
        'Product A',
        CAST(1 AS FIXED(16))
    ),
    (
        2,
        'Product B',
        CAST(1 AS FIXED(16))
    ),
    (
        3,
        'Product C',
        CAST(2 AS FIXED(16))
    ),
    (
        4,
        'Product D',
        CAST(2 AS FIXED(16))
    ),
    (
        5,
        'Product E',
        CAST(3 AS FIXED(16))
    );

-- Partition by uuid type
CREATE TABLE uuid_partitioned (
    id BIGINT,
    name STRING,
    partition_key UUID
) USING ICEBERG PARTITIONED BY (partition_key);

-- Insert data into uuid_partitioned table
INSERT INTO
    uuid_partitioned
VALUES (
        1,
        'UUID User 1',
        UUID '123e4567-e89b-12d3-a456-426614174000'
    ),
    (
        2,
        'UUID User 2',
        UUID '123e4567-e89b-12d3-a456-426614174001'
    ),
    (
        3,
        'UUID User 3',
        UUID '123e4567-e89b-12d3-a456-426614174002'
    ),
    (
        4,
        'UUID User 4',
        UUID '123e4567-e89b-12d3-a456-426614174003'
    ),
    (
        5,
        'UUID User 5',
        UUID '123e4567-e89b-12d3-a456-426614174004'
    );

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
    (5, 'Active Manager', true);

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
    (6, 'Item F', 399.99, 50.00);
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
    );