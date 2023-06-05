CREATE DATABASE test;
  use test;
DROP TABLE IF EXISTS `mysql_order`;
CREATE TABLE test.mysql_order
(
    order_id INT,
    order_date DATETIME,
    customer_name VARCHAR(255),
    price DECIMAL(10, 5),
    product_id INT,
    order_status BOOLEAN
)
UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
);
