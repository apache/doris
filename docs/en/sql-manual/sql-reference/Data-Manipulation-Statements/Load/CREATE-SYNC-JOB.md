---
{
    "title": "CREATE-SYNC-JOB",
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

## CREATE-SYNC-JOB

### Name

CREATE SYNC JOB

### Description

The data synchronization (Sync Job) function supports users to submit a resident data synchronization job, and incrementally synchronizes the CDC (Change Data Capture) of the user's data update operation in the Mysql database by reading the Binlog log from the specified remote address. Features.

Currently, the data synchronization job only supports connecting to Canal, obtaining the parsed Binlog data from the Canal Server and importing it into Doris.

Users can view the data synchronization job status through [SHOW SYNC JOB](../../../sql-manual/sql-reference/Show-Statements/SHOW-SYNC-JOB.html).

grammar:

```sql
CREATE SYNC [db.]job_name
 (
 channel_desc,
 channel_desc
 ...
 )
binlog_desc
````

1. `job_name`

   The synchronization job name is the unique identifier of the job in the current database. Only one job with the same `job_name` can be running.

2. `channel_desc`

   The data channel under the job is used to describe the mapping relationship between the mysql source table and the doris target table.

   grammar:

   ```sql
   FROM mysql_db.src_tbl INTO des_tbl
   [partitions]
   [columns_mapping]
   ````

   1. `mysql_db.src_tbl`

      Specify the database and source table on the mysql side.

   2. `des_tbl`

      Specify the target table on the doris side. Only unique tables are supported, and the batch delete function of the table needs to be enabled (see the 'batch delete function' of help alter table for how to enable it).

   3. `partitions`

      Specify in which partitions of the import destination table. If not specified, it will be automatically imported into the corresponding partition.

      Example:

      ````
      PARTITION(p1, p2, p3)
      ````

   4. `column_mapping`

      Specifies the mapping relationship between the columns of the mysql source table and the doris target table. If not specified, FE will default the columns of the source table and the target table to one-to-one correspondence in order.

      The form col_name = expr is not supported for columns.

      Example:

      ````
      Suppose the target table column is (k1, k2, v1),
      
      Change the order of columns k1 and k2
      COLUMNS(k2, k1, v1)
      
      Ignore the fourth column of the source data
      COLUMNS(k2, k1, v1, dummy_column)
      ````

3. `binlog_desc`

   Used to describe the remote data source, currently only one canal is supported.

   grammar:

   ```sql
   FROM BINLOG
   (
       "key1" = "value1",
       "key2" = "value2"
   )
   ````

   1. The properties corresponding to the Canal data source, prefixed with `canal.`

      1. canal.server.ip: address of canal server
      2. canal.server.port: the port of the canal server
      3. canal.destination: the identity of the instance
      4. canal.batchSize: The maximum batch size obtained, the default is 8192
      5. canal.username: username of instance
      6. canal.password: the password of the instance
      7. canal.debug: optional, when set to true, the batch and details of each row of data will be printed out

### Example

1. Simply create a data synchronization job named `job1` for `test_tbl` of `test_db`, connect to the local Canal server, corresponding to the Mysql source table `mysql_db1.tbl1`.

   ````SQL
   CREATE SYNC `test_db`.`job1`
   (
   FROM `mysql_db1`.`tbl1` INTO `test_tbl`
   )
   FROM BINLOG
   (
   "type" = "canal",
   "canal.server.ip" = "127.0.0.1",
   "canal.server.port" = "11111",
   "canal.destination" = "example",
   "canal.username" = "",
   "canal.password" = ""
   );
   ````

2. Create a data synchronization job named `job1` for multiple tables of `test_db`, corresponding to multiple Mysql source tables one-to-one, and explicitly specify the column mapping.

   ````SQL
   CREATE SYNC `test_db`.`job1`
   (
   FROM `mysql_db`.`t1` INTO `test1` COLUMNS(k1, k2, v1) PARTITIONS (p1, p2),
   FROM `mysql_db`.`t2` INTO `test2` COLUMNS(k3, k4, v2) PARTITION p1
   )
   FROM BINLOG
   (
   "type" = "canal",
   "canal.server.ip" = "xx.xxx.xxx.xx",
   "canal.server.port" = "12111",
   "canal.destination" = "example",
   "canal.username" = "username",
   "canal.password" = "password"
   );
   ````

### Keywords

    CREATE, SYNC, JOB

### Best Practice
