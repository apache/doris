---
{
    "title": "MySql Load",
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

# Mysql Load
<version since="dev">

This is an stand syntax of MySql [LOAD DATA](https://dev.mysql.com/doc/refman/8.0/en/load-data.html) for user to load local file.

MySql load synchronously executes the import and returns the import result. The return information will show whether the import is successful for user.

MySql load is mainly suitable for importing local files on the client side, or importing data from a data stream through a program.

</version>

## Basic Principles

The MySql Load are similar with Stream Load. Both import local files into the Doris cluster, so the MySQL Load will reuses StreamLoad:

 1. FE receives the MySQL Load request executed by the client and then analyse the SQL

 2. FE build the MySql Load request as a StreamLoad request.

 3. FE selects a BE node to send a StreamLoad request

 4. When sending the request, FE will read the local file data from the MySQL client side streamingly, and send it to the HTTP request of StreamLoad asynchronously.

 5. After the data transfer on the MySQL client side is completed, FE waits for the StreamLoad to complete, and displays the import success or failure information to the client side.


## Support data format

MySql Load currently only supports data formats: CSV (text).

## Basic operations

### client connection
```bash
mysql --local-infile  -h 127.0.0.1 -P 9030 -u root -D testdb
```

Notice that if you wants to use mysql load, you must connect doris server with `--local-infile` in client command.
If you're use jdbc to connect doris, you must add property named `allowLoadLocalInfile=true` in jdbc url.


### Create test table
```sql
 CREATE TABLE testdb.t1 (pk INT, v1 INT SUM) AGGREGATE KEY (pk) DISTRIBUTED BY hash (pk) PROPERTIES ('replication_num' = '1');
 ```
 ### import file from client node
 Suppose there is a CSV file named 'client_local.csv 'on the current path of the client side, which will be imported into the test table'testdb.t1' using the MySQL LOAD syntax.

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
1. MySQL Load starts with the syntax `LOAD DATA`, and specifying `LOCAL` means reading client side files.
2. The local fill path will be filled after `INFILE`, which can be a relative path or an absolute path. Currently only a single file is supported, and multiple files are not supported
3. The table name after `INTO TABLE` can specify the database name, as shown in the case. It can also be omitted, and the current database for the user will be used.
4. `PARTITION` syntax supports specified partition to import
5. `COLUMNS TERMINATED BY` specifies the column separator
6. `LINES TERMINATED BY` specifies the line separator
7. `IGNORE num LINES` skips the num header of the CSV.
8. Column mapping syntax, see the column mapping chapter of [Imported Data Transformation](../import-scenes/load-data-convert.md) for specific parameters
9. `PROPERTIES` is the configuration of import, please refer to the [MySQL Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/MYSQL-LOAD.md) command manual for specific properties.

### import file from fe server node
Assuming that the '/root/server_local.csv' path on the FE node is a CSV file, use the MySQL client side to connect to the corresponding FE node, and then execute the following command to import data into the test table.

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
2. FE will have multi-nodes, and importing server level files can only import FE nodes connected by the client side, and cannot import files local to other FE nodes.
3. Server side load was disabled by default. Enable it by setting `mysql_load_server_secure_path` with a secure path. All the load file should be under this path.

### Return result
Since MySQL load is a synchronous import method, the imported results are returned to the user through SQL syntax.
If the import fails, a specific error message will be displayed. If the import is successful, the number of imported rows will be displayed.

```Text
Query OK, 1 row affected (0.17 sec)
Records: 1 Deleted: 0 Skipped: 0 Warnings: 0
```

### Error result
If mysql load process goes wrong, it will show the error in the client as below:
```text
ERROR 1105 (HY000): errCode = 2, detailMessage = [DATA_QUALITY_ERROR]too many filtered rows with load id b612907c-ccf4-4ac2-82fe-107ece655f0f
```

If you meets this error, you can extract the `loadId` and use it in the `show load warnings` command to get more detail message.
```sql
show load warnings where label='b612907c-ccf4-4ac2-82fe-107ece655f0f';
```

The loadId was the label in this case.


### Configuration
1. `mysql_load_thread_pool`: the thread pool size for singe FE node, set 4 thread by default. The block queue size is 5 times of `mysql_load_thread_pool`. So FE can accept 4 + 4\*5 = 24 requests in one time. Increase this configuration if the parallelism are larger than 24.
2. `mysql_load_server_secure_path`: the secure path for load data from server. Empty path by default means that it's not allowed for server load. Recommend to create a `local_import_data` directory under `DORIS_HOME` to load data if you want enable it.
3. `mysql_load_in_memory_record` The failed mysql load record size. The record was keep in memory and only have 20 records by default. If you want to track more records,  you can rise the config but be careful about the fe memory. This record will expired after one day and there is a async thread to clean it in every day.


## Notice 

1. If you see this `LOAD DATA LOCAL INFILE file request rejected due to restrictions on access` message, you should connet mysql with `mysql  --local-infile=1` command to enable client to load local file.
2. The configuration for StreamLoad will also affect MySQL Load. Such as the configurate in be named `streaming_load_max_mb`, it's 10GB by default and it will control the max size for one load.

## More Help

1. For more detailed syntax and best practices for using MySQL Load, see the [MySQL Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/MYSQL-LOAD.md) command manual.
