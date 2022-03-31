---
{
    "title": "Independent Binlog Load",
    "language": "zh-CN"
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


# Independent Binlog Load
Independent Binlog Load enables Doris to synchronize update operation of MySQL, so as to MySQL CDC(Change Data Capture), without relying any other service.

## Scenarios

* INSERT / UPDATE / DELETE operation
* Filter Query
* temporarilly imcompatible with DDL statements

## Principle
```
+---------------------------------------------+
|                    Mysql                    |
+----------------------+----------------------+
                       | Binlog
+----------------------|----------------------+
| FE                   |                      |
| +--------------------|-------------------+  |
| | Sync Job           |                   |  |
| |    +---------------|--------------+    |  |
| |    |Debezium Client|              |    |  |
| |    |   +-----------v-----------+  |    |  |
| |    |   |       Receiver        |  |    |  |
| |    |   +-----------------------+  |    |  |
| |    |   +-----------------------+  |    |  |
| |    |   |       Consumer        |  |    |  |
| |    |   +-----------------------+  |    |  |
| |    +------------------------------+    |  |
| +----+---------------+--------------+----+  |
|      |               |              |       |
| +----v-----+   +-----v----+   +-----v----+  |
| | Channel1 |   | Channel2 |   | Channel3 |  |
| | [Table1] |   | [Table2] |   | [Table3] |  |
| +----+-----+   +-----+----+   +-----+----+  |
|      |               |              |       |
|   +--|-------+   +---|------+   +---|------+|
|  +---v------+|  +----v-----+|  +----v-----+||
| +----------+|+ +----------+|+ +----------+|+|
| |   Task   |+  |   Task   |+  |   Task   |+ |
| +----------+   +----------+   +----------+  |
+----------------------+----------------------+
     |                 |                  |
+----v-----------------v------------------v---+
|                 Coordinator                 |
|                     BE                      |
+----+-----------------+------------------+---+
     |                 |                  |
+----v---+         +---v----+        +----v---+
|   BE   |         |   BE   |        |   BE   |
+--------+         +--------+        +--------+

```

As shown in the figure above, the user first submits a SyncJob to the Fe.

Then, Fe will start to listen MySQL contained by the SyncJob, Every time a Batch is received, it will be distributed by the Consumer to different Channels according to the corresponding target table. Once a channel received data distributed by Consumer, it will submit a send task for sending data.

A Send task is a request from Channel to Be, which contains the data of the same Batch distributed to the current channel.

Channel controls the begin, commit and abort of transaction of single table. In a transaction, the consumer may distribute multiple Batches of data to a channel, so multiple send tasks may be generated. These tasks will not actually take effect until the transaction is committed successfully.

When certain conditions are met (for example, a certain period of time was passed, reach the maximun data size of commit), the Consumer will block and notify each channel to try commit the transaction.

## Usage Procedure

### Create MySQL table

Login MySQL, create source database and table。For example：

```
CREATE DATABASE demo;

USE demo;

CREATE TABLE `test_cdc` (
    `id` int NOT NULL AUTO_INCREMENT,
    `sex` TINYINT(1) DEFAULT NULL,
    `name` varchar(20) DEFAULT NULL,
    `address` varchar(255) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB
 ```

### Create Doris table

Login Doris, create target Doris table(only support Unique model):

```
CREATE TABLE doris_mysql_binlog_demo (   
    `id` int NOT NULL,   
    `sex` TINYINT(1),   
    `name` varchar(20),   
    `address` varchar(255)  
) ENGINE=OLAP 
UNIQUE KEY(`id`,sex) 
COMMENT "OLAP" 
DISTRIBUTED BY HASH(`sex`) BUCKETS 1 
PROPERTIES ( 
    "replication_allocation" = "tag.location.default: 3", 
    "in_memory" = "false", 
    "storage_format" = "V2" 
);
```

### Create SYNC JOB

You can get detailed syntax by `HELP CREATE SYNC JOB`.

Create an example sync job：

```
CREATE SYNC debezium_sync (
    FROM demo.test_cdc into doris_mysql_binlog_demo
)
FROM BINLOG (
    "type" = "debezium", 
    "mysql.server.ip" = "172.17.0.1", 
    "mysql.server.port" = "3306", 
    "mysql.username" = "root", 
    "mysql.password" = "root"
);
```

### Control Job State

Get detailed syntax by SQL:
- `HELP SHOW SYNC JOB;` to show job information.
- `HELP STOP SYNC JOB;` to stop a certain sync job.
- `HELP PAUSE SYNC JOB;` to pause a certain sync job.
- `HELP RESUME SYNC JOB;` to resume a certain sync job.

## Related Configure

### MySQL Side

- MySQL version 5.7 or above
- Open binlog and GTID，choose ROW format

Configure my.cnf with below:

```
[mysqld]
gtid_mode=ON
enforce-gtid-consistency=true
log-bin=replicas-mysql-bin  
binlog_format=row
```

### Doris FE端
* `enable_create_sync_job`

	Turn on the Binlog Load feature. The default value is false.
	
* `sync_commit_interval_second`

	Maximum interval time between commit transactions. If there is still data in the channel that has not been committed after this time, the consumer will notify the channel to commit the transaction.
	
* `min_sync_commit_size`

	The minimum number of events required to commit a transaction. If the number of events received by Fe is less than it, Fe will continue to wait for the next batch of data until the time exceeds `sync_commit_interval_second`. The default value is 10000 events. 
	
* `min_bytes_sync_commit`

	The minimum data size required to commit a transaction. If the data size received by Fe is smaller than it, it will continue to wait for the next batch of data until the time exceeds `sync_commit_interval_second`. The default value is 15MB. 
	
* `max_bytes_sync_commit`

	The maximum size of the data when the transaction is committed. If the data size received by Fe is larger than it, it will immediately commit the transaction and send the accumulated data. The default value is 64MB. 
	
* `max_sync_task_threads_num`

	The maximum number of threads in the SyncJobs' thread pool. There is only one thread pool in the whole Fe for synchronization, which is used to process the tasks created by all SyncJobs in the Fe.
