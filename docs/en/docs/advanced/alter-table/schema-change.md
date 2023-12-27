---
{
    "title": "Schema Change",
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

# Schema Change

Users can modify the schema of existing tables through the Schema Change operation. Doris currently supports the following modifications:

* Add and delete columns
* Modify column type
* Adjust column order
* Add and modify Bloom Filter
* Add and delete bitmap index

This document mainly describes how to create a Schema Change job, as well as some considerations and frequently asked questions about Schema Change.
## Glossary

* Base Table: When each table is created, it corresponds to a base table. The base table stores the complete data of this table. Rollups are usually created based on the data in the base table (and can also be created from other rollups).
* Index: Materialized index. Rollup or Base Table are both called materialized indexes.
* Transaction: Each import task is a transaction, and each transaction has a unique incrementing Transaction ID.
* Rollup: Roll-up tables based on base tables or other rollups.

## Basic Principles

The basic process of executing a Schema Change is to generate a copy of the index data of the new schema from the data of the original index. Among them, two parts of data conversion are required. One is the conversion of existing historical data, and the other is the conversion of newly arrived imported data during the execution of Schema Change.
```
+----------+
| Load Job |
+----+-----+
     |
     | Load job generates both origin and new index data
     |
     |      +------------------+ +---------------+
     |      | Origin Index     | | Origin Index  |
     +------> New Incoming Data| | History Data  |
     |      +------------------+ +------+--------+
     |                                  |
     |                                  | Convert history data
     |                                  |
     |      +------------------+ +------v--------+
     |      | New Index        | | New Index     |
     +------> New Incoming Data| | History Data  |
            +------------------+ +---------------+
```

Before starting the conversion of historical data, Doris will obtain a latest transaction ID. And wait for all import transactions before this Transaction ID to complete. This Transaction ID becomes a watershed. This means that Doris guarantees that all import tasks after the watershed will generate data for both the original Index and the new Index. In this way, when the historical data conversion is completed, the data in the new Index can be guaranteed to be complete.
## Create Job

The specific syntax for creating a Schema Change can be found in the help [ALTER TABLE COLUMN](../../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-TABLE-COLUMN.md) for the description of the Schema Change section .

The creation of Schema Change is an asynchronous process. After the job is submitted successfully, the user needs to view the job progress through the `SHOW ALTER TABLE COLUMN` command.
## View Job

`SHOW ALTER TABLE COLUMN` You can view the Schema Change jobs that are currently executing or completed. When multiple indexes are involved in a Schema Change job, the command displays multiple lines, each corresponding to an index. For example:

```sql
mysql> SHOW ALTER TABLE COLUMN\G;
*************************** 1. row ***************************
        JobId: 20021
    TableName: tbl1
   CreateTime: 2019-08-05 23:03:13
   FinishTime: 2019-08-05 23:03:42
    IndexName: tbl1
      IndexId: 20022
OriginIndexId: 20017
SchemaVersion: 2:792557838
TransactionId: 10023
        State: FINISHED
          Msg: 
     Progress: NULL
      Timeout: 86400
1 row in set (0.00 sec)
```

* JobId: A unique ID for each Schema Change job.
* TableName: The table name of the base table corresponding to Schema Change.
* CreateTime: Job creation time.
* FinishedTime: The end time of the job. If it is not finished, "N/A" is displayed.
* IndexName: The name of an Index involved in this modification.
* IndexId: The unique ID of the new Index.
* OriginIndexId: The unique ID of the old Index.
* SchemaVersion: Displayed in M: N format. M is the version of this Schema Change, and N is the corresponding hash value. With each Schema Change, the version is incremented.
* TransactionId: the watershed transaction ID of the conversion history data.
* State: The phase of the operation.
  * PENDING: The job is waiting in the queue to be scheduled.
  * WAITING_TXN: Wait for the import task before the watershed transaction ID to complete.
  * RUNNING: Historical data conversion.
  * FINISHED: The operation was successful.
  * CANCELLED: The job failed.
* Msg: If the job fails, a failure message is displayed here.
* Progress: operation progress. Progress is displayed only in the RUNNING state. Progress is displayed in M/N. Where N is the total number of copies involved in the Schema Change. M is the number of copies of historical data conversion completed.
* Timeout: Job timeout time. Unit of second.

## Cancel Job

In the case that the job status is not FINISHED or CANCELLED, you can cancel the Schema Change job with the following command:
`CANCEL ALTER TABLE COLUMN FROM tbl_name;`

## Best Practice

Schema Change can make multiple changes to multiple indexes in one job. For example:
Source Schema:

```
+-----------+-------+------+------+------+---------+-------+
| IndexName | Field | Type | Null | Key  | Default | Extra |
+-----------+-------+------+------+------+---------+-------+
| tbl1      | k1    | INT  | No   | true | N/A     |       |
|           | k2    | INT  | No   | true | N/A     |       |
|           | k3    | INT  | No   | true | N/A     |       |
|           |       |      |      |      |         |       |
| rollup2   | k2    | INT  | No   | true | N/A     |       |
|           |       |      |      |      |         |       |
| rollup1   | k1    | INT  | No   | true | N/A     |       |
|           | k2    | INT  | No   | true | N/A     |       |
+-----------+-------+------+------+------+---------+-------+
```

You can add a row k4 to both rollup1 and rollup2 by adding the following k5 to rollup2:
```
ALTER TABLE tbl1
ADD COLUMN k4 INT default "1" to rollup1,
ADD COLUMN k4 INT default "1" to rollup2,
ADD COLUMN k5 INT default "1" to rollup2;
```

When completion, the Schema becomes:

```
+-----------+-------+------+------+------+---------+-------+
| IndexName | Field | Type | Null | Key  | Default | Extra |
+-----------+-------+------+------+------+---------+-------+
| tbl1      | k1    | INT  | No   | true | N/A     |       |
|           | k2    | INT  | No   | true | N/A     |       |
|           | k3    | INT  | No   | true | N/A     |       |
|           | k4    | INT  | No   | true | 1       |       |
|           | k5    | INT  | No   | true | 1       |       |
|           |       |      |      |      |         |       |
| rollup2   | k2    | INT  | No   | true | N/A     |       |
|           | k4    | INT  | No   | true | 1       |       |
|           | k5    | INT  | No   | true | 1       |       |
|           |       |      |      |      |         |       |
| rollup1   | k1    | INT  | No   | true | N/A     |       |
|           | k2    | INT  | No   | true | N/A     |       |
|           | k4    | INT  | No   | true | 1       |       |
+-----------+-------+------+------+------+---------+-------+
```

As you can see, the base table tbl1 also automatically added k4, k5 columns. That is, columns added to any rollup are automatically added to the Base table.

At the same time, columns that already exist in the Base table are not allowed to be added to Rollup. If you need to do this, you can re-create a Rollup with the new columns and then delete the original Rollup.

### Modify Key column

Modifying the Key column of a table is done through the `key` keyword. Let's take a look at an example below.

**This usage is only for the key column of the duplicate key table**

Source Schema :

```text
+-----------+-------+-------------+------+------+---------+-------+
| IndexName | Field | Type        | Null | Key  | Default | Extra |
+-----------+-------+-------------+------+------+---------+-------+
| tbl1      | k1    | INT         | No   | true | N/A     |       |
|           | k2    | INT         | No   | true | N/A     |       |
|           | k3    | varchar(20) | No   | true | N/A     |       |
|           | k4    | INT         | No   | false| N/A     |       |
+-----------+-------+-------------+------+------+---------+-------+
```

The modification statement is as follows, we will change the degree of the k3 column to 50


```sql
alter table example_tbl modify column k3 varchar(50) key null comment 'to 50'
````

When done, the Schema becomes:

```text
+-----------+-------+-------------+------+------+---------+-------+
| IndexName | Field | Type        | Null | Key  | Default | Extra |
+-----------+-------+-------------+------+------+---------+-------+
| tbl1      | k1    | INT         | No   | true | N/A     |       |
|           | k2    | INT         | No   | true | N/A     |       |
|           | k3    | varchar(50) | No   | true | N/A     |       |
|           | k4    | INT         | No   | false| N/A     |       |
+-----------+-------+-------------+------+------+---------+-------+
```

Because the Schema Change job is an asynchronous operation, only one Schema change job can be performed on the same table at the same time. To check the operation status of the job, you can use the following command

```sql
SHOW ALTER TABLE COLUMN\G;
````

## Notice

* Only one Schema Change job can be running on a table at a time.

* Schema Change operation does not block import and query operations.

* The partition column and bucket column cannot be modified.

* If there is a value column aggregated by REPLACE in the schema, the Key column is not allowed to be deleted.

     If the Key column is deleted, Doris cannot determine the value of the REPLACE column.
    
     All non-Key columns of the Unique data model table are REPLACE aggregated.
    
* When adding a value column whose aggregation type is SUM or REPLACE, the default value of this column has no meaning to historical data.

     Because the historical data has lost the detailed information, the default value cannot actually reflect the aggregated value.
    
* When modifying the column type, fields other than Type need to be completed according to the information on the original column.

     If you modify the column `k1 INT SUM NULL DEFAULT" 1 "` as type BIGINT, you need to execute the following command:
    
    ```ALTER TABLE tbl1 MODIFY COLUMN `k1` BIGINT SUM NULL DEFAULT "1"; ```

   Note that in addition to the new column types, such as the aggregation mode, Nullable attributes, and default values must be completed according to the original information.
    
* Modifying column names, aggregation types, nullable attributes, default values, and column comments is not supported.

## FAQ

* the execution speed of Schema Change

    At present, the execution speed of Schema Change is estimated to be about 10MB / s according to the worst efficiency. To be conservative, users can set the timeout for jobs based on this rate.

* Submit job error `Table xxx is not stable. ...`

    Schema Change can only be started when the table data is complete and unbalanced. If some data shard copies of the table are incomplete, or if some copies are undergoing an equalization operation, the submission is rejected.
        
    Whether the data shard copy is complete can be checked with the following command:
        ```ADMIN SHOW REPLICA STATUS FROM tbl WHERE STATUS != "OK";```
    
    If a result is returned, there is a problem with the copy. These problems are usually fixed automatically by the system. You can also use the following commands to repair this table first:    
    ```ADMIN REPAIR TABLE tbl1;```
    
    You can check if there are running balancing tasks with the following command:
    
    ```SHOW PROC "/cluster_balance/pending_tablets";```
    
    You can wait for the balancing task to complete, or temporarily disable the balancing operation with the following command:
    
    ```ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");```
    
## Configurations

### FE Configurations

* `alter_table_timeout_second`: The default timeout for the job is 86400 seconds.

### BE Configurations

* `alter_tablet_worker_count`: Number of threads used to perform historical data conversion on the BE side. The default is 3. If you want to speed up the Schema Change job, you can increase this parameter appropriately and restart the BE. But too many conversion threads can cause increased IO pressure and affect other operations. This thread is shared with the Rollup job.


* `alter_index_worker_count`: Number of threads used to perform historical data build index on the BE size (note: only inverted index is supported now). The default is 3. If you want to speed up the Index Change job, you can increase this parameter appropriately and restart the BE. But too many threads can cause increased IO pressure and affect other operations.

## More Help

For more detailed syntax and best practices used by Schema Change, see [ALTER TABLE COLUMN](../../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-TABLE-COLUMN.md) command manual, you can also enter `HELP ALTER TABLE COLUMN` in the MySql client command line for more help information.

