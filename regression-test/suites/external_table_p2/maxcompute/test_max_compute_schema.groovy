// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
/*
// Test DDL and Data:


use mc_datalake_schema;
set odps.namespace.schema = true;

USE schema `default`;
CREATE TABLE user_info (id INT, name STRING, age INT, city STRING);
INSERT INTO user_info VALUES
(1, 'Alice', 23, 'Beijing'),
(2, 'Bob', 30, 'Shanghai'),
(3, 'Carol', 27, 'XiAn'),
(4, 'David', 22, 'Guangzhou');

CREATE TABLE order_detail (id INT, order_id STRING, amount DOUBLE, buyer STRING) PARTITIONED BY (ds1 STRING);
INSERT INTO order_detail PARTITION (ds1='202510') VALUES
(1, 'ORD001', 500.0, 'Alice'),
(2, 'ORD002', 899.9, 'Bob');
INSERT INTO order_detail PARTITION (ds1='202511') VALUES
(3, 'ORD003', 120.5, 'Carol'),
(4, 'ORD004', 350.0, 'David'),
(5, 'ORD005', 780.0, 'Alice');
INSERT INTO order_detail PARTITION (ds1='202512') VALUES
(6, 'ORD006', 640.0, 'Bob'),
(7, 'ORD007', 220.0, 'Carol');



create schema analytics; 
USE schema analytics;
CREATE TABLE product_sales (id INT, product_name STRING, price DOUBLE, quantity INT, sale_time DATETIME);
INSERT INTO analytics.product_sales VALUES
(1, 'Keyboard', 199.9, 3, CAST('2025-10-15 10:00:00' AS DATETIME)),
(2, 'Mouse', 99.5, 5, CAST('2025-10-15 11:00:00' AS DATETIME)),
(3, 'Monitor', 899.0, 2, CAST('2025-10-15 12:00:00' AS DATETIME)),
(4, 'Laptop', 5699.0, 1, CAST('2025-10-15 13:00:00' AS DATETIME)),
(5, 'USB Cable', 25.0, 10, CAST('2025-10-15 14:00:00' AS DATETIME));
CREATE TABLE web_log (
  id INT,
  user_id INT,
  action STRING,
  url STRING,
  log_time DATETIME
) PARTITIONED BY (ds2 STRING, hour STRING);
INSERT INTO web_log PARTITION (ds2='20251015', hour='10') VALUES
(1, 1001, 'click', 'https://site.com/page1', CAST('2025-10-15 10:00:00' AS DATETIME)),
(2, 1002, 'view', 'https://site.com/page2', CAST('2025-10-15 10:05:00' AS DATETIME)),
(3, 1003, 'buy', 'https://site.com/page3', CAST('2025-10-15 10:10:00' AS DATETIME));
INSERT INTO web_log PARTITION (ds2='20251016', hour='09') VALUES
(4, 1004, 'logout', 'https://site.com/page4', CAST('2025-10-16 09:15:00' AS DATETIME)),
(5, 1005, 'login', 'https://site.com/page5', CAST('2025-10-16 09:30:00' AS DATETIME)),
(6, 1006, 'click', 'https://site.com/page6', CAST('2025-10-16 09:45:00' AS DATETIME));
INSERT INTO web_log PARTITION (ds2='20251017', hour='08') VALUES
(7, 1007, 'view', 'https://site.com/page7', CAST('2025-10-17 08:00:00' AS DATETIME)),
(8, 1008, 'buy', 'https://site.com/page8', CAST('2025-10-17 08:10:00' AS DATETIME));


create schema iot; 
USE schema iot;
CREATE TABLE employee_salary (
  id INT,
  emp_name STRING,
  department STRING,
  salary DECIMAL(10,2),
  hire_date DATE
);
INSERT INTO employee_salary VALUES
(1, 'Tom', 'Finance', 12000.00, CAST('2023-03-01' AS DATE)),
(2, 'Jerry', 'IT', 15000.50, CAST('2022-09-15' AS DATE)),
(3, 'Mike', 'HR', 9800.00, CAST('2024-06-20' AS DATE)),
(4, 'Lucy', 'IT', 13500.00, CAST('2022-11-05' AS DATE));
 */
suite("test_max_compute_schema", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ak = context.config.otherConfigs.get("ak")
        String sk = context.config.otherConfigs.get("sk");
        String mc_project = "mc_datalake_schema"
        String mc_catalog_name = "test_max_compute_schema"




        sql """drop catalog if exists ${mc_catalog_name};"""
        sql """
            create catalog if not exists ${mc_catalog_name} properties (
                "type" = "max_compute",
                "mc.default.project" = "${mc_project}",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
                "mc.enable.namespace.schema" = "true"
            );
        """

        sql """ switch ${mc_catalog_name};"""
        order_qt_show_db_1 """ show databases; """

        sql  """ use `default`; """
        order_qt_show_tb_1 """ show tables; """
        

        sql  """ use `analytics`; """
        order_qt_show_tb_2 """ show tables; """
        

        sql  """ use `iot`; """
        order_qt_show_tb_3 """ show tables; """

        order_qt_show_par """  show partitions from `default`.order_detail; """
        order_qt_show_par2 """  show partitions from analytics.web_log; """
        qt_desc  """ desc  iot.employee_salary; """

        sql """ alter catalog ${mc_catalog_name} set  PROPERTIES("mc.enable.namespace.schema" = "false"); """
        qt_show_db_2 """ show databases;"""
        sql """ use ${mc_project}; """
        order_qt_show_tb_4 """ show tables; """

        qt_mc_old_q1 """ SELECT * FROM user_info ORDER BY id;"""
        qt_mc_old_q2 """ SELECT * FROM order_detail ORDER BY id;"""

        sql """ alter catalog ${mc_catalog_name} set  PROPERTIES("mc.enable.namespace.schema" = "true"); """
       


        order_qt_show_db_1 """ show databases; """

        sql  """ use `default`; """
        order_qt_show_tb_1 """ show tables; """
        

        sql  """ use `analytics`; """
        order_qt_show_tb_2 """ show tables; """
        

        sql  """ use `iot`; """
        order_qt_show_tb_3 """ show tables; """

        order_qt_show_par """  show partitions from `default`.order_detail; """
        order_qt_show_par2 """  show partitions from analytics.web_log; """
        qt_desc  """ desc  iot.employee_salary; """


        explain {
            sql("""select * from `default`.order_detail """)
            contains "partition=3/3"
        }

        explain {
            sql("""select * from `default`.order_detail where ds1 = "11" """)
            contains "partition=0/3"
        }


        explain {
            sql("""select * from `default`.order_detail where ds1 = "202511" or ds1 = "202512" """)
            contains "partition=2/3"
        }


        explain {
            sql("""select * from  analytics.web_log; """)
            contains "partition=3/3"
        }


        explain {
            sql("""select * from  analytics.web_log where hour="09" ; """)
            contains "partition=1/3"
        }

        explain {
            sql("""select * from analytics.web_log where ds2="20251016" and hour="09" """)
            contains "partition=1/3"
        }


        explain {
            sql("""select * from analytics.web_log where ds2="20251016" and hour="99" """)
            contains "partition=0/3"
        }


        
        qt_mc_q1  """SELECT * FROM `default`.user_info ORDER BY id;"""
        qt_mc_q2  """SELECT id, name, age FROM `default`.user_info WHERE age > 25 ORDER BY id;"""
        qt_mc_q3  """SELECT name, city FROM `default`.user_info WHERE city IN ('Beijing','Shanghai') ORDER BY id;"""
        qt_mc_q4  """SELECT * FROM `default`.user_info WHERE name LIKE 'A%' ORDER BY id;"""
        qt_mc_q5  """SELECT * FROM `default`.order_detail WHERE amount > 400 ORDER BY id;"""
        qt_mc_q6  """SELECT order_id, buyer, amount FROM `default`.order_detail WHERE buyer='Bob' ORDER BY id;"""
        qt_mc_q7  """SELECT COUNT(*) AS cnt FROM `default`.order_detail WHERE ds1='202511';"""
        qt_mc_q8  """SELECT * FROM analytics.product_sales ORDER BY id;"""
        qt_mc_q9  """SELECT id, product_name, price FROM analytics.product_sales WHERE price BETWEEN 100 AND 900 ORDER BY id;"""
        qt_mc_q10  """SELECT product_name, quantity FROM analytics.product_sales WHERE quantity >= 3 ORDER BY id;"""
        qt_mc_q11  """SELECT * FROM analytics.product_sales WHERE sale_time >= '2025-10-15 11:00:00' ORDER BY id;"""
        qt_mc_q12  """SELECT * FROM analytics.web_log WHERE action='click' ORDER BY id;"""
        qt_mc_q13  """SELECT id, user_id, url FROM analytics.web_log WHERE ds2='20251016' ORDER BY id;"""
        qt_mc_q14  """SELECT * FROM analytics.web_log WHERE hour='09' ORDER BY id;"""
        qt_mc_q15  """SELECT COUNT(*) AS clicks FROM analytics.web_log WHERE action='buy';"""
        qt_mc_q16  """SELECT * FROM iot.employee_salary ORDER BY id;"""
        qt_mc_q17  """SELECT emp_name, department FROM iot.employee_salary WHERE salary > 10000 ORDER BY id;"""
        qt_mc_q18  """SELECT * FROM iot.employee_salary WHERE hire_date > '2023-01-01' ORDER BY id;"""
        qt_mc_q19  """SELECT emp_name, salary FROM iot.employee_salary WHERE department='IT' ORDER BY id;"""
        qt_mc_q20  """SELECT * FROM iot.employee_salary WHERE salary BETWEEN 9000 AND 16000 ORDER BY id;"""

        qt_mc_join_q21  """SELECT u.id, u.name, o.order_id, o.amount FROM `default`.user_info u JOIN `default`.order_detail o ON u.name=o.buyer WHERE o.ds1='202510' ORDER BY u.id;"""
        qt_mc_join_q22  """SELECT u.id, u.name, p.product_name, p.price FROM `default`.user_info u JOIN analytics.product_sales p ON u.id=p.id ORDER BY u.id;"""
        qt_mc_join_q23  """SELECT u.name, w.action, w.url FROM `default`.user_info u JOIN analytics.web_log w ON u.id=w.user_id-1000 WHERE w.ds2='20251015' AND w.hour='10' ORDER BY u.id;"""
        qt_mc_join_q24  """SELECT e.emp_name, p.product_name, p.sale_time FROM iot.employee_salary e JOIN analytics.product_sales p ON e.id=p.id ORDER BY e.id;"""
        qt_mc_join_q25  """SELECT u.name, e.department, e.salary FROM `default`.user_info u JOIN iot.employee_salary e ON u.id=e.id ORDER BY u.id;"""
        qt_mc_join_q26  """SELECT o.order_id, o.amount, p.product_name, p.price FROM `default`.order_detail o JOIN analytics.product_sales p ON o.id=p.id WHERE o.ds1='202511' ORDER BY o.id;"""
        qt_mc_join_q27  """SELECT o.order_id, w.url, w.action FROM `default`.order_detail o JOIN analytics.web_log w ON o.id=w.id WHERE o.ds1='202512' AND w.ds2='20251016' AND w.hour='09' ORDER BY o.id;"""
        qt_mc_join_q28  """SELECT o.order_id, e.emp_name, e.department FROM `default`.order_detail o JOIN iot.employee_salary e ON o.id=e.id WHERE o.ds1='202511' ORDER BY o.id;"""
        qt_mc_join_q29  """SELECT p.product_name, w.url, w.action FROM analytics.product_sales p JOIN analytics.web_log w ON p.id=w.id WHERE w.ds2='20251017' AND w.hour='08' ORDER BY p.id;"""
        qt_mc_join_q30  """SELECT u.name, p.product_name, e.salary FROM `default`.user_info u JOIN analytics.product_sales p ON u.id=p.id JOIN iot.employee_salary e ON u.id=e.id ORDER BY u.id;"""
        qt_mc_join_q31  """SELECT u.name, o.order_id, w.url FROM `default`.user_info u JOIN `default`.order_detail o ON u.id=o.id JOIN analytics.web_log w ON u.id=w.id WHERE o.ds1='202511' AND w.ds2='20251015' AND w.hour='10' ORDER BY u.id;"""
        qt_mc_join_q32  """SELECT e.emp_name, p.product_name, w.action FROM iot.employee_salary e JOIN analytics.product_sales p ON e.id=p.id JOIN analytics.web_log w ON e.id=w.id WHERE w.ds2='20251016' AND w.hour='09' ORDER BY e.id;"""
        qt_mc_join_q33  """SELECT u.name, e.emp_name, e.department FROM `default`.user_info u JOIN iot.employee_salary e ON u.id=e.id ORDER BY u.id;"""
        qt_mc_join_q34  """SELECT p.product_name, e.department, e.salary FROM analytics.product_sales p JOIN iot.employee_salary e ON p.id=e.id ORDER BY p.id;"""
        qt_mc_join_q35  """SELECT o.order_id, p.product_name, e.emp_name FROM `default`.order_detail o JOIN analytics.product_sales p ON o.id=p.id JOIN iot.employee_salary e ON o.id=e.id WHERE o.ds1='202510' ORDER BY o.id;"""
    }
}