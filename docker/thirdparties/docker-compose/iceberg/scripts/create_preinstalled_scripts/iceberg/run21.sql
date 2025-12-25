create database if not exists demo.test_db;
use demo.test_db;

-- =====================================================================================
-- Tables for testing rewrite_data_files with WHERE conditions
-- These tables are created with Spark SQL to ensure min/max metadata is properly
-- generated, which is required for WHERE condition filtering to work correctly.
-- =====================================================================================

-- Table 1: For baseline test (rewrite without WHERE condition)
CREATE TABLE IF NOT EXISTS test_rewrite_where_conditions_baseline (
    id BIGINT,
    name STRING,
    age INT,
    salary DOUBLE
) USING iceberg;

-- Insert data in multiple batches to create multiple files
-- First batch: id 1-10
INSERT INTO test_rewrite_where_conditions_baseline VALUES
(1, 'Alice', 25, 50000.0),
(2, 'Bob', 30, 60000.0),
(3, 'Charlie', 35, 70000.0),
(4, 'David', 28, 55000.0),
(5, 'Eve', 32, 65000.0),
(6, 'Frank', 27, 52000.0),
(7, 'Grace', 29, 58000.0),
(8, 'Henry', 33, 72000.0),
(9, 'Ivy', 26, 48000.0),
(10, 'Jack', 31, 68000.0);

-- Second batch: id 11-20
INSERT INTO test_rewrite_where_conditions_baseline VALUES
(11, 'Kate', 34, 75000.0),
(12, 'Liam', 36, 80000.0),
(13, 'Mia', 38, 82000.0),
(14, 'Noah', 31, 71000.0),
(15, 'Olivia', 35, 76000.0),
(16, 'Peter', 37, 79000.0),
(17, 'Quinn', 32, 73000.0),
(18, 'Rachel', 39, 84000.0),
(19, 'Sam', 33, 74000.0),
(20, 'Tina', 36, 81000.0);

-- Third batch: id 21-30
INSERT INTO test_rewrite_where_conditions_baseline VALUES
(21, 'Uma', 41, 90000.0),
(22, 'Victor', 43, 92000.0),
(23, 'Wendy', 45, 95000.0),
(24, 'Xavier', 42, 91000.0),
(25, 'Yara', 44, 93000.0),
(26, 'Zoe', 46, 96000.0),
(27, 'Alex', 41, 89000.0),
(28, 'Blake', 48, 98000.0),
(29, 'Cameron', 47, 97000.0),
(30, 'Dana', 49, 99000.0);

-- Table 2: For test with WHERE condition matching subset (id >= 11 AND id <= 20)
CREATE TABLE IF NOT EXISTS test_rewrite_where_conditions_with_where (
    id BIGINT,
    name STRING,
    age INT,
    salary DOUBLE
) USING iceberg;

-- Insert data in multiple batches to create multiple files
-- First batch: id 1-10
INSERT INTO test_rewrite_where_conditions_with_where VALUES
(1, 'Alice', 25, 50000.0),
(2, 'Bob', 30, 60000.0),
(3, 'Charlie', 35, 70000.0),
(4, 'David', 28, 55000.0),
(5, 'Eve', 32, 65000.0),
(6, 'Frank', 27, 52000.0),
(7, 'Grace', 29, 58000.0),
(8, 'Henry', 33, 72000.0),
(9, 'Ivy', 26, 48000.0),
(10, 'Jack', 31, 68000.0);

-- Second batch: id 11-20
INSERT INTO test_rewrite_where_conditions_with_where VALUES
(11, 'Kate', 34, 75000.0),
(12, 'Liam', 36, 80000.0),
(13, 'Mia', 38, 82000.0),
(14, 'Noah', 31, 71000.0),
(15, 'Olivia', 35, 76000.0),
(16, 'Peter', 37, 79000.0),
(17, 'Quinn', 32, 73000.0),
(18, 'Rachel', 39, 84000.0),
(19, 'Sam', 33, 74000.0),
(20, 'Tina', 36, 81000.0);

-- Third batch: id 21-30
INSERT INTO test_rewrite_where_conditions_with_where VALUES
(21, 'Uma', 41, 90000.0),
(22, 'Victor', 43, 92000.0),
(23, 'Wendy', 45, 95000.0),
(24, 'Xavier', 42, 91000.0),
(25, 'Yara', 44, 93000.0),
(26, 'Zoe', 46, 96000.0),
(27, 'Alex', 41, 89000.0),
(28, 'Blake', 48, 98000.0),
(29, 'Cameron', 47, 97000.0),
(30, 'Dana', 49, 99000.0);

-- Table 3: For test with WHERE condition matching no data (id = 99999)
CREATE TABLE IF NOT EXISTS test_rewrite_where_conditions_no_match (
    id BIGINT,
    name STRING,
    age INT,
    salary DOUBLE
) USING iceberg;

-- Insert data in multiple batches to create multiple files
-- First batch: id 1-10
INSERT INTO test_rewrite_where_conditions_no_match VALUES
(1, 'Alice', 25, 50000.0),
(2, 'Bob', 30, 60000.0),
(3, 'Charlie', 35, 70000.0),
(4, 'David', 28, 55000.0),
(5, 'Eve', 32, 65000.0),
(6, 'Frank', 27, 52000.0),
(7, 'Grace', 29, 58000.0),
(8, 'Henry', 33, 72000.0),
(9, 'Ivy', 26, 48000.0),
(10, 'Jack', 31, 68000.0);

-- Second batch: id 11-20
INSERT INTO test_rewrite_where_conditions_no_match VALUES
(11, 'Kate', 34, 75000.0),
(12, 'Liam', 36, 80000.0),
(13, 'Mia', 38, 82000.0),
(14, 'Noah', 31, 71000.0),
(15, 'Olivia', 35, 76000.0),
(16, 'Peter', 37, 79000.0),
(17, 'Quinn', 32, 73000.0),
(18, 'Rachel', 39, 84000.0),
(19, 'Sam', 33, 74000.0),
(20, 'Tina', 36, 81000.0);

-- Third batch: id 21-30
INSERT INTO test_rewrite_where_conditions_no_match VALUES
(21, 'Uma', 41, 90000.0),
(22, 'Victor', 43, 92000.0),
(23, 'Wendy', 45, 95000.0),
(24, 'Xavier', 42, 91000.0),
(25, 'Yara', 44, 93000.0),
(26, 'Zoe', 46, 96000.0),
(27, 'Alex', 41, 89000.0),
(28, 'Blake', 48, 98000.0),
(29, 'Cameron', 47, 97000.0),
(30, 'Dana', 49, 99000.0);
