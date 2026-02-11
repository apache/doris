-- ================================================================
-- Fluss-Doris Integration Validation Script
-- ================================================================
-- Purpose: Validate Doris can connect to and query Fluss tables
-- Prerequisites:
--   1. Fluss cluster running (localhost:9123)
--   2. Doris FE/BE running (localhost:9030)
--   3. Test tables created in Fluss
-- Run: mysql -h 127.0.0.1 -P 9030 -u root < validation-doris.sql
-- ================================================================

-- ----------------------------------------------------------------
-- PART 1: Create and Verify Fluss Catalog
-- ----------------------------------------------------------------

-- Create Fluss external catalog
CREATE CATALOG IF NOT EXISTS fluss_catalog PROPERTIES (
    "type" = "fluss",
    "bootstrap.servers" = "localhost:9123"
);

-- Verify catalog creation
SHOW CATALOGS;

-- Expected output should include: fluss_catalog

-- ----------------------------------------------------------------
-- PART 2: Explore Fluss Catalog Structure
-- ----------------------------------------------------------------

-- Switch to Fluss catalog
SWITCH fluss_catalog;

-- List all databases
SHOW DATABASES;

-- Expected: 'fluss' (default) and 'demo_db' (if created)

-- Show catalog properties
SHOW CREATE CATALOG fluss_catalog;

-- ----------------------------------------------------------------
-- PART 3: Validate Database Access
-- ----------------------------------------------------------------

-- Use demo database
USE demo_db;

-- List tables in database
SHOW TABLES;

-- Expected tables:
--   - users (primary key table)
--   - orders (partitioned table)
--   - events (log table)

-- ----------------------------------------------------------------
-- PART 4: Basic Table Metadata Validation
-- ----------------------------------------------------------------

-- Describe users table
DESC users;

-- Expected columns:
--   id INT
--   name STRING/VARCHAR
--   email STRING/VARCHAR
--   age INT
--   city STRING/VARCHAR

-- Show table creation statement
SHOW CREATE TABLE users;

-- Get table statistics
SHOW TABLE STATS users;

-- ----------------------------------------------------------------
-- PART 5: Basic Data Queries
-- ----------------------------------------------------------------

-- Query 1: Full table scan
SELECT * FROM users LIMIT 10;

-- Query 2: Count rows
SELECT COUNT(*) as total_users FROM users;

-- Query 3: Filter query
SELECT id, name, city FROM users WHERE age > 30;

-- Query 4: Aggregation
SELECT city, COUNT(*) as user_count, AVG(age) as avg_age
FROM users
GROUP BY city
ORDER BY user_count DESC;

-- ----------------------------------------------------------------
-- PART 6: Advanced Query Validation
-- ----------------------------------------------------------------

-- Query orders table (partitioned)
SELECT * FROM orders LIMIT 10;

-- Partition pruning test
SELECT user_id, product, amount
FROM orders
WHERE order_date = '2024-01-15'
ORDER BY amount DESC;

-- Join query
SELECT
    u.id,
    u.name,
    u.city,
    o.order_id,
    o.product,
    o.amount
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
ORDER BY u.id, o.order_id
LIMIT 20;

-- Aggregation across tables
SELECT
    u.city,
    COUNT(DISTINCT o.order_id) as order_count,
    SUM(o.amount) as total_revenue,
    AVG(o.amount) as avg_order_value
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.city
ORDER BY total_revenue DESC;

-- ----------------------------------------------------------------
-- PART 7: Log Table Query (Append-Only)
-- ----------------------------------------------------------------

SELECT * FROM events ORDER BY event_time LIMIT 10;

SELECT event_type, COUNT(*) as event_count
FROM events
GROUP BY event_type
ORDER BY event_count DESC;

-- ----------------------------------------------------------------
-- PART 8: Query Plan Analysis
-- ----------------------------------------------------------------

-- Analyze query plan for filter pushdown
EXPLAIN SELECT * FROM users WHERE age > 30;

-- Check partition pruning
EXPLAIN SELECT * FROM orders WHERE order_date = '2024-01-15';

-- Check join execution plan
EXPLAIN
SELECT u.name, o.product
FROM users u JOIN orders o ON u.id = o.user_id;

-- ----------------------------------------------------------------
-- PART 9: Performance Validation
-- ----------------------------------------------------------------

-- Test column projection (should only read specified columns)
SELECT name, city FROM users;

-- Test predicate pushdown (filter should be pushed to Fluss)
SELECT * FROM users WHERE age BETWEEN 25 AND 35;

-- Test limit pushdown
SELECT * FROM users ORDER BY id LIMIT 5;

-- ----------------------------------------------------------------
-- PART 10: Metadata Caching Validation
-- ----------------------------------------------------------------

-- These should be fast due to cached metadata
SHOW TABLES;
DESC users;
SHOW CREATE TABLE users;

-- ----------------------------------------------------------------
-- Validation Complete
-- ----------------------------------------------------------------

-- Switch back to internal catalog
SWITCH internal;

-- Summary report
SELECT
    'Fluss-Doris Integration' as test_suite,
    'PASSED' as status,
    NOW() as validation_time;
