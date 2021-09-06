<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Instructions for use

This series of sample codes mainly explain how to use Flink and Flink doris connector to read and write data to doris from the perspective of Flink framework and Flink doris connector, and give code examples based on actual usage scenarios.

## Flink Mysql CDC TO Doris


Realize the consumption of mysql binlog log data through flink cdc, and then import mysql data into doris table data in real time through flink doris connector sql

```java
org.apache.doris.demo.flink.FlinkConnectorMysqlCDCDemo
```

**Note:** Because the Flink doris connector jar package is not in the Maven central warehouse, you need to compile it separately and add it to the classpath of your project. Refer to the compilation and use of Flink doris connector: 

[Flink doris connector]: https://doris.apache.org/master/zh-CN/extending-doris/flink-doris-connector.html



1. First, enable mysql binlog

   For details on how to open binlog, please search by yourself or go to the official Mysql documentation to inquire 

2. Install Flink 

   Flink installation and use are not introduced here, but code examples are given in the development environment

3. Create mysql table

   ```sql
   CREATE TABLE `test` (
     `id` int NOT NULL AUTO_INCREMENT,
     `name` varchar(255) DEFAULT NULL,
     PRIMARY KEY (`id`)
   ) ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
   ```

4. Create doris table 

   ```sql
   CREATE TABLE `doris_test` (
     `id` int NULL COMMENT "",
     `name` varchar(100) NULL COMMENT ""
   ) ENGINE=OLAP
   DUPLICATE KEY(`id`)
   COMMENT "OLAP"
   DISTRIBUTED BY HASH(`id`) BUCKETS 1
   PROPERTIES (
   "replication_num" = "3",
   "in_memory" = "false",
   "storage_format" = "V2"
   );
   ```

   

5. Create Flink Mysql CDC

   ```sql
   tEnv.executeSql(
                   "CREATE TABLE orders (\n" +
                           "  id INT,\n" +
                           "  name STRING\n" +
                           ") WITH (\n" +
                           "  'connector' = 'mysql-cdc',\n" +
                           "  'hostname' = 'localhost',\n" +
                           "  'port' = '3306',\n" +
                           "  'username' = 'root',\n" +
                           "  'password' = 'zhangfeng',\n" +
                           "  'database-name' = 'demo',\n" +
                           "  'table-name' = 'test'\n" +
                           ")");
   ```

6. Create flink doris table

   ```sql
   tEnv.executeSql(
                   "CREATE TABLE doris_test_sink (" +
                           "id INT," +
                           "name STRING" +
                           ") " +
                           "WITH (\n" +
                           "  'connector' = 'doris',\n" +
                           "  'fenodes' = '10.220.146.10:8030',\n" +
                           "  'table.identifier' = 'test_2.doris_test',\n" +
                           "  'sink.batch.size' = '2',\n" +
                           "  'username' = 'root',\n" +
                           "  'password' = ''\n" +
                           ")");
   ```


6. Execute insert into 

   ```sql
   tEnv.executeSql("INSERT INTO doris_test_sink select id,name from orders");
   ```
