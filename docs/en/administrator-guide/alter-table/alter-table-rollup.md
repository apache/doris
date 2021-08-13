---
{
    "title": "Rollup",
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

# Rollup

Users can speed up queries by creating rollup tables. For the concept and usage of Rollup, please refer to [Data
 Model, ROLLUP and Prefix Index](../../getting-started/data-model-rollup_EN.md) and 
 [Rollup and query](../../getting-started/hit-the-rollup_EN.md).

This document focuses on how to create a Rollup job, as well as some considerations and frequently asked questions about creating a Rollup.

## Glossary

* Base Table: When each table is created, it corresponds to a base table. The base table stores the complete data of this table. Rollups are usually created based on the data in the base table (and can also be created from other rollups).
* Index: Materialized index. Rollup or Base Table are both called materialized indexes.
* Transaction: Each import task is a transaction, and each transaction has a unique incrementing Transaction ID.

## Basic Principles

The basic process of creating a Rollup is to generate a new Rollup data containing the specified column from the data in the Base table. Among them, two parts of data conversion are needed. One is the conversion of existing historical data, and the other is the conversion of newly arrived imported data during Rollup execution.

```
+----------+
| Load Job |
+----+-----+
     |
     | Load job generates both base and rollup index data
     |
     |      +------------------+ +---------------+
     |      | Base Index       | | Base Index    |
     +------> New Incoming Data| | History Data  |
     |      +------------------+ +------+--------+
     |                                  |
     |                                  | Convert history data
     |                                  |
     |      +------------------+ +------v--------+
     |      | Rollup Index     | | Rollup Index  |
     +------> New Incoming Data| | History Data  |
            +------------------+ +---------------+
```

Before starting the conversion of historical data, Doris will obtain a latest transaction ID. And wait for all import transactions before this Transaction ID to complete. This Transaction ID becomes a watershed. This means that Doris guarantees that all import tasks after the watershed will generate data for the Rollup Index at the same time. In this way, after the historical data conversion is completed, the data of the Rollup and Base tables can be guaranteed to be flush.

## Create Job

The specific syntax for creating a Rollup can be found in the description of the Rollup section in the help `HELP ALTER TABLE`.

The creation of Rollup is an asynchronous process. After the job is submitted successfully, the user needs to use the `SHOW ALTER TABLE ROLLUP` command to view the progress of the job.

## View Job

`SHOW ALTER TABLE ROLLUP` You can view rollup jobs that are currently executing or completed. For example:

```
          JobId: 20037
      TableName: tbl1
     CreateTime: 2019-08-06 15:38:49
   FinishedTime: N/A
  BaseIndexName: tbl1
RollupIndexName: r1
       RollupId: 20038
  TransactionId: 10034
          State: PENDING
            Msg:
       Progress: N/A
        Timeout: 86400
```

* JobId: A unique ID for each Rollup job.
* TableName: The table name of the base table corresponding to Rollup.
* CreateTime: Job creation time.
* FinishedTime: The end time of the job. If it is not finished, "N / A" is displayed.
* BaseIndexName: The name of the source Index corresponding to Rollup.
* RollupIndexName: The name of the Rollup.
* RollupId: The unique ID of the Rollup.
* TransactionId: the watershed transaction ID of the conversion history data.
* State: The phase of the operation.
     * PENDING: The job is waiting in the queue to be scheduled.
     * WAITING_TXN: Wait for the import task before the watershed transaction ID to complete.
     * RUNNING: Historical data conversion.
     * FINISHED: The operation was successful.
     * CANCELLED: The job failed.
* Msg: If the job fails, a failure message is displayed here.
* Progress: operation progress. Progress is displayed only in the RUNNING state. Progress is displayed in M / N. Where N is the total number of copies of Rollup. M is the number of copies of historical data conversion completed.
* Timeout: Job timeout time. Unit of second.

## Cancel Job

In the case that the job status is not FINISHED or CANCELLED, you can cancel the Rollup job with the following command:

`CANCEL ALTER TABLE ROLLUP FROM tbl_name;`

## Notice

* A table can have only one Rollup job running at a time. And only one rollup can be created in a job.

* Rollup operations do not block import and query operations.

* If a DELETE operation has a Key column in a where condition that does not exist in a Rollup, the DELETE is not allowed.

    If a Key column does not exist in a Rollup, the DELETE operation cannot delete data from the Rollup, so the data consistency between the Rollup table and the Base table cannot be guaranteed.

* Rollup columns must exist in the Base table.

    Rollup columns are always a subset of the Base table columns. Columns that do not exist in the Base table cannot appear.

* If a rollup contains columns of the REPLACE aggregation type, the rollup must contain all the key columns.

    Assume the structure of the Base table is as follows:
    
    `` `(k1 INT, k2 INT, v1 INT REPLACE, v2 INT SUM)` ``
    
    If you need to create a Rollup that contains `v1` columns, you must include the` k1`, `k2` columns. Otherwise, the system cannot determine the value of `v1` listed in Rollup.
    
    Note that all Value columns in the Unique data model table are of the REPLACE aggregation type.
    
* Rollup of the DUPLICATE data model table, you can specify the DUPLICATE KEY of the rollup.

    The DUPLICATE KEY in the DUPLICATE data model table is actually sorted. Rollup can specify its own sort order, but the sort order must be a prefix of the Rollup column order. If not specified, the system will check if the Rollup contains all sort columns of the Base table, and if it does not, it will report an error. For example:
    
    Base table structure: `(k1 INT, k2 INT, k3 INT) DUPLICATE KEY (k1, k2)`
    
    Rollup can be: `(k2 INT, k1 INT) DUPLICATE KEY (k2)`

* Rollup does not need to include partitioned or bucket columns for the Base table.

## FAQ

* How many rollups can a table create

    There is theoretically no limit to the number of rollups a table can create, but too many rollups can affect import performance. Because when importing, data will be generated for all rollups at the same time. At the same time, Rollup will take up physical storage space. Usually the number of rollups for a table is less than 10.
    
* Rollup creation speed

    Rollup creation speed is currently estimated at about 10MB / s based on the worst efficiency. To be conservative, users can set the timeout for jobs based on this rate.

* Submitting job error `Table xxx is not stable. ...`

    Rollup can start only when the table data is complete and unbalanced. If some data shard copies of the table are incomplete, or if some copies are undergoing an equalization operation, the submission is rejected.
    
    Whether the data shard copy is complete can be checked with the following command:
    
    ```ADMIN SHOW REPLICA STATUS FROM tbl WHERE STATUS! =" OK ";```
    
    If a result is returned, there is a problem with the copy. These problems are usually fixed automatically by the system. You can also use the following commands to repair this table first:
    
    ```ADMIN REPAIR TABLE tbl1; ```
    
    You can check if there are running balancing tasks with the following command:
    
    ```SHOW PROC" / cluster_balance / pending_tablets ";```
    
    You can wait for the balancing task to complete, or temporarily disable the balancing operation with the following command:
    
    ```ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");```
    
## Configurations

### FE Configurations

* `alter_table_timeout_second`: The default timeout for the job is 86400 seconds.

### BE Configurations

* `alter_tablet_worker_count`: Number of threads used to perform historical data conversion on the BE side. The default is 3. If you want to speed up the rollup job, you can increase this parameter appropriately and restart the BE. But too many conversion threads can cause increased IO pressure and affect other operations. This thread is shared with the Schema Change job.
