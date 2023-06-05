CREATE TABLE mysql_source (
    order_id INT,
    order_date TIMESTAMP(0),
    customer_name STRING,
    price DECIMAL(10, 5),
    product_id INT,
    order_status BOOLEAN,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'hostname' = '${MySQL_CIP}',
      'port' = '3306',
      'username' = 'root',
      'password' = '123456',
      'database-name' = 'test',
      'table-name' = 'orders'
      );

SET execution.checkpointing.interval = 10s;

CREATE TABLE doris_sink (
    order_id INT,
    order_date TIMESTAMP(0),
    customer_name VARCHAR(255),
    price DECIMAL(10, 5),
    product_id INT,
    order_status BOOLEAN
)
WITH (
    'connector' = 'doris',
    'fenodes' = '${HOST_ADDR}:8030',
    'table.identifier' = 'test.mysql_order',
    'username' = 'root',
    'password' = '',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.enable-delete' = 'true',
    'sink.label-prefix' = 'doris_label'
    );

INSERT INTO doris_sink SELECT order_id,order_date,customer_name,price,product_id,order_status FROM mysql_source;
