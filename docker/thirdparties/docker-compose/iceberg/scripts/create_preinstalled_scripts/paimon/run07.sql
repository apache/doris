use paimon;

create database if not exists test_paimon_partition;

use test_paimon_partition;
-- ============================================
-- 1. Create Date Partition Table
-- ============================================
DROP TABLE IF EXISTS sales_by_date;
CREATE TABLE  test_paimon_partition.sales_by_date (
    id BIGINT,
    product_name STRING,
    price DECIMAL(10,2),
    quantity INT,
    sale_date DATE,
    created_at TIMESTAMP
)
PARTITIONED BY (sale_date)
TBLPROPERTIES (
    'primary-key' = 'id,sale_date',
    'file.format' = 'parquet',
    'bucket' = '2'
);

INSERT INTO test_paimon_partition.sales_by_date VALUES
    (1, 'iPhone 15', 999.99, 2, DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00'),
    (2, 'MacBook Pro', 2499.99, 1, DATE '2024-01-15', TIMESTAMP '2024-01-15 11:00:00'),
    (3, 'iPad Air', 599.99, 3, DATE '2024-01-16', TIMESTAMP '2024-01-16 09:15:00'),
    (4, 'Apple Watch', 399.99, 2, DATE '2024-01-16', TIMESTAMP '2024-01-16 14:20:00'),
    (5, 'AirPods Pro', 249.99, 5, DATE '2024-01-17', TIMESTAMP '2024-01-17 16:45:00');

-- ============================================
-- 2. Create Region Partition Table
-- ============================================
DROP TABLE IF EXISTS test_paimon_partition.sales_by_region;
CREATE TABLE sales_by_region (
    id BIGINT,
    customer_name STRING,
    product_name STRING,
    price DECIMAL(10,2),
    quantity INT,
    region STRING,
    created_at TIMESTAMP
)
PARTITIONED BY (region)
TBLPROPERTIES (
    'primary-key' = 'id,region',
    'bucket' = '2',
    'file.format' = 'parquet'
);

INSERT INTO test_paimon_partition.sales_by_region VALUES
    (1, 'Zhang Wei', 'iPhone 15', 999.99, 1, 'China-Beijing', TIMESTAMP '2024-01-15 10:30:00'),
    (2, 'John Smith', 'MacBook Pro', 2499.99, 1, 'USA-California', TIMESTAMP '2024-01-15 11:00:00'),
    (3, 'Tanaka Taro', 'iPad Air', 599.99, 2, 'Japan-Tokyo', TIMESTAMP '2024-01-16 09:15:00');

-- ============================================
-- 3. Create Date and Region Mixed Partition Table
-- ============================================
DROP TABLE IF EXISTS sales_by_date_region;
CREATE TABLE test_paimon_partition.sales_by_date_region (
    id BIGINT,
    customer_name STRING,
    product_name STRING,
    price DECIMAL(10,2),
    quantity INT,
    sale_date DATE,
    region STRING,
    created_at TIMESTAMP
)
PARTITIONED BY (sale_date, region)
TBLPROPERTIES (
    'primary-key' = 'id,sale_date,region',
    'bucket' = '2',
    'file.format' = 'parquet'
);

INSERT INTO test_paimon_partition.sales_by_date_region VALUES
    (1, 'Wang Qiang', 'iPhone 15', 999.99, 1, DATE '2024-01-15', 'China-Beijing', TIMESTAMP '2024-01-15 10:30:00'),
    (2, 'Alice Brown', 'MacBook Pro', 2499.99, 1, DATE '2024-01-15', 'USA-California', TIMESTAMP '2024-01-15 11:00:00'),
    (3, 'Yamada Taro', 'iPad Air', 599.99, 2, DATE '2024-01-15', 'Japan-Tokyo', TIMESTAMP '2024-01-15 09:15:00'),
    (4, 'Zhao Mei', 'Apple Watch', 399.99, 3, DATE '2024-01-16', 'China-Shanghai', TIMESTAMP '2024-01-16 14:20:00'),
    (5, 'Bob Johnson', 'AirPods Pro', 249.99, 2, DATE '2024-01-16', 'USA-New York', TIMESTAMP '2024-01-16 16:45:00'),
    (6, 'Suzuki Ichiro', 'iPhone 15', 999.99, 1, DATE '2024-01-16', 'Japan-Osaka', TIMESTAMP '2024-01-16 12:30:00');


-- ============================================
-- 4. Create Timestamp Partition Table (Hourly Partition)
-- ============================================
DROP TABLE IF EXISTS events_by_hour;
CREATE TABLE test_paimon_partition.events_by_hour (
    id BIGINT,
    event_type STRING,
    user_id STRING,
    event_data STRING,
    event_timestamp TIMESTAMP,
    hour_partition STRING
)
PARTITIONED BY (hour_partition)
TBLPROPERTIES (
    'primary-key' = 'id,hour_partition',
    'bucket' = '2',
    'file.format' = 'parquet'
);


INSERT INTO test_paimon_partition.events_by_hour VALUES
    (1, 'login', 'user001', 'successful login', TIMESTAMP '2024-01-15 10:30:00', '2024-01-15-10'),
    (2, 'purchase', 'user002', 'bought iPhone', TIMESTAMP '2024-01-15 10:45:00', '2024-01-15-10'),
    (3, 'logout', 'user001', 'session ended', TIMESTAMP '2024-01-15 11:15:00', '2024-01-15-11'),
    (4, 'login', 'user003', 'successful login', TIMESTAMP '2024-01-15 11:30:00', '2024-01-15-11'),
    (5, 'view_product', 'user002', 'viewed MacBook', TIMESTAMP '2024-01-15 14:20:00', '2024-01-15-14'),
    (6, 'purchase', 'user003', 'bought iPad', TIMESTAMP '2024-01-15 14:35:00', '2024-01-15-14');


-- ============================================
-- 5. Create Composite Time Partition Table (Year-Month-Day Hierarchical Partition)
-- ============================================
DROP TABLE IF EXISTS logs_by_date_hierarchy;
CREATE TABLE test_paimon_partition.logs_by_date_hierarchy (
    log_id BIGINT,
    log_level STRING,
    message STRING,
    service_name STRING,
    log_timestamp TIMESTAMP,
    year_val INT,
    month_val INT,
    day_val INT
)
PARTITIONED BY (year_val, month_val, day_val)
TBLPROPERTIES (
    'primary-key' = 'log_id,year_val,month_val,day_val',
    'bucket' = '2',
    'file.format' = 'parquet'
);
INSERT INTO test_paimon_partition.logs_by_date_hierarchy VALUES
    (1, 'INFO', 'Service started successfully', 'user-service', TIMESTAMP '2024-01-15 08:00:00', 2024, 1, 15),
    (2, 'WARN', 'High memory usage detected', 'order-service', TIMESTAMP '2024-01-15 10:30:00', 2024, 1, 15),
    (3, 'ERROR', 'Database connection failed', 'payment-service', TIMESTAMP '2024-01-16 09:15:00', 2024, 1, 16),
    (4, 'INFO', 'User login successful', 'auth-service', TIMESTAMP '2024-01-16 14:20:00', 2024, 1, 16),
    (5, 'DEBUG', 'Cache miss for user data', 'user-service', TIMESTAMP '2024-01-17 11:45:00', 2024, 1, 17),
    (6, 'ERROR', 'Payment processing failed', 'payment-service', TIMESTAMP '2024-02-01 13:30:00', 2024, 2, 1);

