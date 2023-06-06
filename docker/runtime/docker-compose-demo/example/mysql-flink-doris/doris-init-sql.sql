-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
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
