use demo.test_db;
drop table if exists test_db.t_partitioned_table;
drop table if exists test_db.t_unpartitioned_table;
drop table if exists test_db.t_product_info;
drop table if exists test_db.t_sales_record;
CREATE TABLE IF NOT EXISTS t_partitioned_table (
    col1 int,
    col2 string,
    col3 int,
    col4 string,
    col5 string
) USING iceberg
PARTITIONED BY (col5);
CREATE TABLE IF NOT EXISTS t_unpartitioned_table (
    col1 INT,
    col2 VARCHAR(100),
    col3 DECIMAL(10, 2)
) USING iceberg;
CREATE TABLE IF NOT EXISTS t_product_info (
    product_id INT,
    product_name VARCHAR(100),
    price DECIMAL(10, 2)
) USING iceberg;
CREATE TABLE IF NOT EXISTS t_sales_record (
    sales_id INT,
    order_id INT,
    product_id INT,
    quantity_sold INT,
    sale_date DATE
) USING iceberg;
INSERT INTO t_partitioned_table (col1, col2, col3, col4, col5)
VALUES
(1, 'Alice', 25, 'Female', 'New York'),
(2, 'Bob', 30, 'Male', 'Los Angeles'),
(3, 'Charlie', 35, 'Male', 'Chicago'),
(4, 'David', 22, 'Male', 'Houston'),
(5, 'Eve', 28, 'Female', 'Phoenix');
INSERT INTO t_unpartitioned_table (col1, col2, col3)
VALUES
(1001, 'Product A', 20.00),
(1002, 'Product B', 30.00),
(1003, 'Product C', 40.00),
(1004, 'Product D', 50.00),
(1005, 'Product E', 60.00);
INSERT INTO t_product_info (product_id, product_name, price)
VALUES
(1001, 'Product A', 20.00),
(1002, 'Product B', 30.00),
(1003, 'Product C', 40.00),
(1004, 'Product D', 50.00),
(1005, 'Product E', 60.00);
INSERT INTO t_sales_record (sales_id, order_id, product_id, quantity_sold, sale_date)
VALUES
(7001, 101, 1001, 2, date '2024-01-01'),
(7002, 102, 1002, 3, date '2024-01-02'),
(7003, 103, 1003, 1, date '2024-01-03'),
(7004, 104, 1001, 1, date '2024-01-04'),
(7005, 105, 1004, 4, date '2024-01-05');
create view v_with_unpartitioned_table
as
select
    *
from
    t_unpartitioned_table;
create view v_with_partitioned_table
    as
    select * from t_partitioned_table order by col1;
create view v_with_partitioned_column
as
select
    col5
from
    t_partitioned_table;
create view v_with_joint_table
as
SELECT
    pi.product_name,
    sr.quantity_sold,
    sr.sale_date
FROM
    t_product_info pi
        JOIN
    t_sales_record sr
    ON pi.product_id = sr.product_id
WHERE
    sr.sale_date BETWEEN '2024-01-01' AND '2024-01-03'
  AND sr.quantity_sold > 1;
