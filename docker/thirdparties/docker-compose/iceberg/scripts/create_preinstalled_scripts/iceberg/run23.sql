
CREATE DATABASE IF NOT EXISTS demo.wap_test;


USE demo.wap_test;


DROP TABLE IF EXISTS orders_wap;

-- WAP-enabled orders table
CREATE TABLE orders_wap (
    order_id     INT,
    customer_id  INT,
    amount       DECIMAL(10, 2),
    order_date   STRING
)
USING iceberg;
ALTER TABLE wap_test.orders_wap SET TBLPROPERTIES ('write.wap.enabled'='true');

SET spark.wap.id = test_wap_001;



INSERT INTO orders_wap VALUES 
    (1, 103, 150.00, '2025-12-03'),
    (2, 104, 320.25, '2025-12-04');


DROP TABLE IF EXISTS orders_non_wap;
-- Non WAP-enabled orders table
CREATE TABLE orders_non_wap (
    order_id INT,
    customer_id INT,
    amount DECIMAL(10, 2),
    order_date STRING
)
USING iceberg;

INSERT INTO orders_non_wap VALUES
(1, 201, 10.00, '2025-12-01');
