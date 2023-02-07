---
{
    "title": "MySql load",
    "language": "en"
}
---

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
<version since="dev"></version>

# Mysql load

MySql load is an import method of SQL interaction. Users import client side local or server level local data into Doris through SQL commands.

MySql load synchronously executes the import and returns the import result. Users can directly return information through SQL to determine whether the import is successful.

MySql load is mainly suitable for importing local files on the client side, or importing data from a data stream through a program.

## Basic Principles

The functions of MySql Load and Stream Load are similar. Both import local files into the Doris cluster, so the MySQL Load implementation reuses the basic import capabilities of StreamLoad:

 1. FE receives the MySQL Load request executed by the client side to complete the SQL parsing work

 2. FE disassembles the Load request and encapsulates it as a StreamLoad request.

 3. FE selects a BE node to send a StreamLoad request

 4. When sending the request, FE will read the local file data from the MySQL client side asynchronously and stream, and send it to the HTTP request of StreamLoad in real time.

 5. After the data transfer on the MySQL client side is completed, FE waits for the StreamLoad to complete, and displays the import success or failure information to the client side.


## Support data format

MySql Load currently supports data formats: CSV (text).

## Basic operations

### Create test table
```sql
 CREATE TABLE testdb.t1 (pk INT, v1 INT SUM) AGGREGATE KEY (pk) DISTRIBUTED BY hash (pk) PROPERTIES ('replication_num' = '1');
 ```
 ### import file from client node
 Suppose there is a CSV file named 'client_local.csv 'on the current path of the client side, which is imported into the test table'testdb.t1' using the MySQL LOAD syntax.

```sql
LOAD DATA LOCAL
INFILE 'client_local.csv '
INTO TABLE testdb.t1
PARTITION (partition_a, partition_b, partition_c, partition_d)
COLUMNS TERMINATED BY '\ t'
LINES TERMINATED BY '\ n'
IGNORE 1 LINES
(K1, k2, v2, v10, v11)
SET (c1 = k1, c2 = k2, c3 = v10, c4 = v11)
PROPERTIES ("strict_mode" = "true")
```
1. MySQL Load starts with the syntax'LOAD DATA ', and specifying'LOCAL' means reading client side files.
2. Fill in the local file path in'INFILE ', which can be a relative path or an absolute path. Currently only a single file is supported, and multiple files are not supported
3. The table name of'INTO TABLE 'can specify the database name, as shown in the case. It can also be omitted, and the database where the current user is located will be used.
4. 'PARTITION' syntax supports specified partition import
5. 'COLUMNS TERMINATED BY' specifies the column separator
6. 'LINES TERMINATED BY' specifies the line separator
7. 'IGNORE num LINES' The user skips the num header of the CSV.
8. Column mapping syntax, see the column mapping chapter of [Imported Data Transformation] (../import-scenes/load-data-convert.md) for specific parameters
9. 'PROPERTIES' import parameters, please refer to the [MySQL Load] (../../../sql-manual/sql-reference/Data-Management-Statements/Load/MYSQL-LOAD.md) command manual for specific parameters.

### import file from fe server node
Assuming that the '/root/server_local.csv' path on the FE node is a CSV file, use the MySQL client side to connect to the corresponding FE node, and then execute the following command to import the data into the test table.

```sql
LOAD DATA
INFILE '/root/server_local.csv'
INTO TABLE testdb.t1
PARTITION (partition_a, partition_b, partition_c, partition_d)
COLUMNS TERMINATED BY '\ t'
LINES TERMINATED BY '\ n'
IGNORE 1 LINES
(K1, k2, v2, v10, v11)
SET (c1 = k1, c2 = k2, c3 = v10, c4 = v11)
PROPERTIES ("strict_mode" = "true")
```
1. The only difference between the syntax of importing server level local files and importing client side syntax is whether the'LOCAL 'keyword is added after the'LOAD DATA' keyword.
2. FE is a multi-node deployment, and the function of importing server level files can only import FE nodes connected by the client side, and cannot import files local to other FE nodes.

### Return result
Since MySQL load is a synchronous import method, the imported results are returned to the user through SQL syntax.
If the import fails, a specific error message will be displayed. If the import is successful, the number of imported rows will be displayed.

```Text
Query OK, 1 row affected (0.17 sec)
Records: 1 Deleted: 0 Skipped: 0 Warnings: 0
```

## More Help

For more detailed syntax and best practices for using MySQL Load, see the [MySQL Load] (../../../sql-manual/sql-reference/Data-Management-Statements/Load/MYSQL-LOAD.md) command manual.