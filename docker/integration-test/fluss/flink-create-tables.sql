-- ================================================================
-- Flink SQL: Create Fluss Tables and Insert Test Data
-- ================================================================
-- Purpose: Create test tables in Fluss and populate with sample data
-- Prerequisites:
--   1. Fluss cluster running
--   2. Flink running with Fluss connector installed
-- Usage:
--   docker exec -it flink-jobmanager ./bin/sql-client.sh
--   Then paste this SQL
-- ================================================================

-- ----------------------------------------------------------------
-- STEP 1: Create Fluss Catalog in Flink
-- ----------------------------------------------------------------

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'fluss-coordinator:9123'
);

-- Switch to Fluss catalog
USE CATALOG fluss_catalog;

-- Verify catalog
SHOW CATALOGS;
SHOW CURRENT CATALOG;

-- ----------------------------------------------------------------
-- STEP 2: Create Demo Database
-- ----------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS demo_db;
USE demo_db;

-- Verify database
SHOW DATABASES;
SHOW CURRENT DATABASE;

-- ----------------------------------------------------------------
-- STEP 3: Create Users Table (Primary Key Table)
-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY NOT ENFORCED,
    name STRING,
    email STRING,
    age INT,
    city STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3)
) WITH (
    'bucket.num' = '4',
    'table.auto-partition.enabled' = 'false'
);

-- Verify table created
SHOW TABLES;
DESC users;

-- ----------------------------------------------------------------
-- STEP 4: Create Orders Table (Partitioned Primary Key Table)
-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS orders (
    order_id INT,
    user_id INT,
    product STRING,
    amount DOUBLE,
    status STRING,
    order_date STRING,
    created_at TIMESTAMP(3),
    PRIMARY KEY (order_id, order_date) NOT ENFORCED
) PARTITIONED BY (order_date) WITH (
    'bucket.num' = '4'
);

-- Verify table
DESC orders;

-- ----------------------------------------------------------------
-- STEP 5: Create Events Table (Log Table - Append Only)
-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS events (
    event_id INT,
    event_type STRING,
    event_data STRING,
    user_id INT,
    event_time TIMESTAMP(3)
) WITH (
    'bucket.num' = '2',
    'table.type' = 'log'
);

-- Verify table
DESC events;

-- ----------------------------------------------------------------
-- STEP 6: Insert Sample Data - Users
-- ----------------------------------------------------------------

INSERT INTO users VALUES
    (1, 'Alice Johnson', 'alice@example.com', 28, 'San Francisco',
     TIMESTAMP '2024-01-15 10:00:00', TIMESTAMP '2024-01-15 10:00:00'),
    (2, 'Bob Smith', 'bob@example.com', 35, 'New York',
     TIMESTAMP '2024-01-15 11:00:00', TIMESTAMP '2024-01-15 11:00:00'),
    (3, 'Carol White', 'carol@example.com', 42, 'Los Angeles',
     TIMESTAMP '2024-01-15 11:30:00', TIMESTAMP '2024-01-15 11:30:00'),
    (4, 'David Brown', 'david@example.com', 29, 'Chicago',
     TIMESTAMP '2024-01-15 12:00:00', TIMESTAMP '2024-01-15 12:00:00'),
    (5, 'Eve Davis', 'eve@example.com', 31, 'Boston',
     TIMESTAMP '2024-01-15 12:30:00', TIMESTAMP '2024-01-15 12:30:00'),
    (6, 'Frank Miller', 'frank@example.com', 38, 'Seattle',
     TIMESTAMP '2024-01-15 13:00:00', TIMESTAMP '2024-01-15 13:00:00'),
    (7, 'Grace Lee', 'grace@example.com', 26, 'Austin',
     TIMESTAMP '2024-01-15 13:30:00', TIMESTAMP '2024-01-15 13:30:00'),
    (8, 'Henry Wilson', 'henry@example.com', 45, 'Denver',
     TIMESTAMP '2024-01-15 14:00:00', TIMESTAMP '2024-01-15 14:00:00'),
    (9, 'Iris Chen', 'iris@example.com', 33, 'San Francisco',
     TIMESTAMP '2024-01-15 14:30:00', TIMESTAMP '2024-01-15 14:30:00'),
    (10, 'Jack Taylor', 'jack@example.com', 40, 'New York',
     TIMESTAMP '2024-01-15 15:00:00', TIMESTAMP '2024-01-15 15:00:00');

-- Verify insert
SELECT COUNT(*) as user_count FROM users;

-- ----------------------------------------------------------------
-- STEP 7: Insert Sample Data - Orders
-- ----------------------------------------------------------------

INSERT INTO orders VALUES
    (101, 1, 'Laptop', 1299.99, 'completed', '2024-01-15', TIMESTAMP '2024-01-15 10:30:00'),
    (102, 2, 'Mouse', 29.99, 'completed', '2024-01-15', TIMESTAMP '2024-01-15 11:15:00'),
    (103, 3, 'Keyboard', 89.99, 'completed', '2024-01-15', TIMESTAMP '2024-01-15 11:45:00'),
    (104, 1, 'Monitor', 399.99, 'completed', '2024-01-16', TIMESTAMP '2024-01-16 09:00:00'),
    (105, 4, 'Headphones', 149.99, 'completed', '2024-01-16', TIMESTAMP '2024-01-16 10:00:00'),
    (106, 5, 'Webcam', 79.99, 'processing', '2024-01-16', TIMESTAMP '2024-01-16 11:00:00'),
    (107, 2, 'USB Cable', 12.99, 'completed', '2024-01-17', TIMESTAMP '2024-01-17 09:30:00'),
    (108, 3, 'Laptop Stand', 45.99, 'completed', '2024-01-17', TIMESTAMP '2024-01-17 10:30:00'),
    (109, 6, 'Desk Lamp', 35.99, 'completed', '2024-01-17', TIMESTAMP '2024-01-17 11:30:00'),
    (110, 7, 'Phone Charger', 19.99, 'processing', '2024-01-18', TIMESTAMP '2024-01-18 09:00:00'),
    (111, 8, 'Mouse Pad', 15.99, 'completed', '2024-01-18', TIMESTAMP '2024-01-18 10:00:00'),
    (112, 9, 'HDMI Cable', 22.99, 'completed', '2024-01-18', TIMESTAMP '2024-01-18 11:00:00'),
    (113, 10, 'External SSD', 129.99, 'shipped', '2024-01-19', TIMESTAMP '2024-01-19 09:00:00'),
    (114, 1, 'Wireless Charger', 39.99, 'completed', '2024-01-19', TIMESTAMP '2024-01-19 10:00:00'),
    (115, 2, 'Laptop Bag', 59.99, 'processing', '2024-01-19', TIMESTAMP '2024-01-19 11:00:00');

-- Verify insert
SELECT COUNT(*) as order_count FROM orders;

-- ----------------------------------------------------------------
-- STEP 8: Insert Sample Data - Events (Log Table)
-- ----------------------------------------------------------------

INSERT INTO events VALUES
    (1, 'login', 'user_login_successful', 1, TIMESTAMP '2024-01-15 10:00:00'),
    (2, 'page_view', 'page=/products', 1, TIMESTAMP '2024-01-15 10:05:00'),
    (3, 'add_to_cart', 'product_id=laptop', 1, TIMESTAMP '2024-01-15 10:10:00'),
    (4, 'checkout', 'order_id=101', 1, TIMESTAMP '2024-01-15 10:30:00'),
    (5, 'logout', 'user_logout', 1, TIMESTAMP '2024-01-15 10:35:00'),
    (6, 'login', 'user_login_successful', 2, TIMESTAMP '2024-01-15 11:00:00'),
    (7, 'search', 'query=mouse', 2, TIMESTAMP '2024-01-15 11:10:00'),
    (8, 'add_to_cart', 'product_id=mouse', 2, TIMESTAMP '2024-01-15 11:12:00'),
    (9, 'checkout', 'order_id=102', 2, TIMESTAMP '2024-01-15 11:15:00'),
    (10, 'login', 'user_login_successful', 3, TIMESTAMP '2024-01-15 11:30:00'),
    (11, 'page_view', 'page=/products/keyboard', 3, TIMESTAMP '2024-01-15 11:35:00'),
    (12, 'add_to_cart', 'product_id=keyboard', 3, TIMESTAMP '2024-01-15 11:40:00'),
    (13, 'checkout', 'order_id=103', 3, TIMESTAMP '2024-01-15 11:45:00'),
    (14, 'page_view', 'page=/home', 4, TIMESTAMP '2024-01-16 09:00:00'),
    (15, 'search', 'query=headphones', 4, TIMESTAMP '2024-01-16 09:30:00');

-- Verify insert
SELECT COUNT(*) as event_count FROM events;

-- ----------------------------------------------------------------
-- STEP 9: Verify All Data
-- ----------------------------------------------------------------

-- Check users
SELECT 'Users' as table_name, COUNT(*) as row_count FROM users
UNION ALL
SELECT 'Orders' as table_name, COUNT(*) as row_count FROM orders
UNION ALL
SELECT 'Events' as table_name, COUNT(*) as row_count FROM events;

-- Sample queries to verify data
SELECT * FROM users LIMIT 5;
SELECT * FROM orders WHERE order_date = '2024-01-15' LIMIT 5;
SELECT * FROM events WHERE event_type = 'login' LIMIT 5;

-- ----------------------------------------------------------------
-- STEP 10: Test Update (Primary Key Tables)
-- ----------------------------------------------------------------

-- Update a user's information (upsert on primary key)
INSERT INTO users VALUES
    (1, 'Alice Johnson-Smith', 'alice.smith@example.com', 29, 'San Francisco',
     TIMESTAMP '2024-01-15 10:00:00', TIMESTAMP '2024-01-20 15:00:00');

-- Verify update
SELECT * FROM users WHERE id = 1;

-- Update order status
INSERT INTO orders VALUES
    (106, 5, 'Webcam', 79.99, 'completed', '2024-01-16', TIMESTAMP '2024-01-16 11:00:00');

-- Verify update
SELECT * FROM orders WHERE order_id = 106;

-- ----------------------------------------------------------------
-- Summary
-- ----------------------------------------------------------------

SELECT
    'Setup Complete' as status,
    '3 tables created' as tables,
    'Sample data loaded' as data;

-- Final verification
SHOW TABLES;
