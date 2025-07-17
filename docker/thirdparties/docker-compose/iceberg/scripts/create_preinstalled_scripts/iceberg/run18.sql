create database if not exists demo.partition_db;
use demo.partition_db;

-- Partition by date type
CREATE TABLE date_partitioned (
    id BIGINT,
    name STRING,
    partition_key DATE
) USING ICEBERG
PARTITIONED BY (partition_key);

-- Partition by integer type
CREATE TABLE int_partitioned (
    id BIGINT,
    name STRING,
    partition_key INT
) USING ICEBERG
PARTITIONED BY (partition_key);

-- Partition by string type
CREATE TABLE string_partitioned (
    id BIGINT,
    name STRING,
    partition_key STRING
) USING ICEBERG
PARTITIONED BY (partition_key);

-- Partition by timestamp type
CREATE TABLE timestamp_partitioned (
    id BIGINT,
    name STRING,
    partition_key TIMESTAMP
) USING ICEBERG
PARTITIONED BY (partition_key);

-- Partition by boolean type
CREATE TABLE boolean_partitioned (
    id BIGINT,
    name STRING,
    partition_key BOOLEAN
) USING ICEBERG
PARTITIONED BY (partition_key);

-- Partition by decimal type
CREATE TABLE decimal_partitioned (
    id BIGINT,
    name STRING,
    value FLOAT,
    partition_key DECIMAL(10, 2)
) USING ICEBERG
PARTITIONED BY (partition_key);

-- Insert data into date_partitioned table
INSERT INTO date_partitioned VALUES
(1, 'Alice', DATE '2024-01-01'),
(2, 'Bob', DATE '2024-01-01'),
(3, 'Charlie', DATE '2024-02-01'),
(4, 'David', DATE '2024-02-01'),
(5, 'Eve', DATE '2024-03-01');

-- Insert data into int_partitioned table
INSERT INTO int_partitioned VALUES
(1, 'Product A', 1),
(2, 'Product B', 1),
(3, 'Product C', 2),
(4, 'Product D', 2),
(5, 'Product E', 3);

-- Insert data into string_partitioned table
INSERT INTO string_partitioned VALUES
(1, 'User1', 'North America'),
(2, 'User2', 'North America'),
(3, 'User3', 'Europe'),
(4, 'User4', 'Europe'),
(5, 'User5', 'Asia'),
(6, 'User6', 'Asia');

-- Insert data into timestamp_partitioned table
INSERT INTO timestamp_partitioned VALUES
(1, 'Event1', TIMESTAMP '2024-01-15 08:00:00'),
(2, 'Event2', TIMESTAMP '2024-01-15 09:00:00'),
(3, 'Event3', TIMESTAMP '2024-01-15 14:00:00'),
(4, 'Event4', TIMESTAMP '2024-01-16 10:00:00'),
(5, 'Event5', TIMESTAMP '2024-01-16 16:00:00');

-- Insert data into boolean_partitioned table
INSERT INTO boolean_partitioned VALUES
(1, 'Active User', true),
(2, 'Active Admin', true),
(3, 'Inactive User', false),
(4, 'Inactive Guest', false),
(5, 'Active Manager', true);

-- Insert data into decimal_partitioned table
INSERT INTO decimal_partitioned VALUES
(1, 'Item A', 125.50, 10.50),
(2, 'Item B', 200.75, 10.50),
(3, 'Item C', 89.99, 25.25),
(4, 'Item D', 156.80, 25.25),
(5, 'Item E', 299.95, 50.00),
(6, 'Item F', 399.99, 50.00);