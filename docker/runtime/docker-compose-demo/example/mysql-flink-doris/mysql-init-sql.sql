CREATE DATABASE test;
  USE test;
DROP TABLE IF EXISTS `orders`;
CREATE TABLE orders (
                        order_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        order_date DATETIME NOT NULL,
                        customer_name VARCHAR(255) NOT NULL,
                        price DECIMAL(10, 5) NOT NULL,
                        product_id INTEGER NOT NULL,
                        order_status BOOLEAN NOT NULL -- Whether order has been placed
) AUTO_INCREMENT = 10001;

INSERT INTO orders
VALUES (default, '2020-07-05 10:08:22', 'Jark', 50.50, 102, false),
       (default, '2020-07-05 10:11:09', 'Sally', 15.00, 105, false),
       (default, '2020-07-05 12:00:30', 'Edward', 25.25, 106, false);
