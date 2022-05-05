---
{
    "title": "Binlog Load",
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

# Binlog Load

The Binlog Load feature enables Doris to incrementally synchronize update operations in MySQL, so as to CDC(Change Data Capture) of data on Mysql.

## Scenarios
* Support insert / update / delete operations
* Filter query
* Temporarily incompatible with DDL statements

## Principle
In the design of phase one, Binlog Load needs to rely on canal as an intermediate medium, so that canal can be pretended to be a slave node to get and parse the binlog on the MySQL master node, and then Doris can get the parsed data on the canal. This process mainly involves mysql, canal and Doris. The overall data flow is as follows:

```
+---------------------------------------------+
|                    Mysql                    |
+----------------------+----------------------+
                       | Binlog
+----------------------v----------------------+
|                 Canal Server                |
+-------------------+-----^-------------------+
               Get  |     |  Ack
+-------------------|-----|-------------------+
| FE                |     |                   |
| +-----------------|-----|----------------+  |
| | Sync Job        |     |                |  |
| |    +------------v-----+-----------+    |  |
| |    | Canal Client                 |    |  |
| |    |   +-----------------------+  |    |  |
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

Then, Fe will start a Canal Client for each SyncJob to subscribe to and get data from the Canal Server.

The Receiver in the Canal Client will receives data by the GET request. Every time a Batch is received, it will be distributed by the Consumer to different Channels according to the corresponding target table. Once a channel received data distributed by Consumer, it will submit a send task for sending data.

A Send task is a request from Channel to Be, which contains the data of the same Batch distributed to the current channel.

Channel controls the begin, commit and abort of transaction of single table. In a transaction, the consumer may distribute multiple Batches of data to a channel, so multiple send tasks may be generated. These tasks will not actually take effect until the transaction is committed successfully.

When certain conditions are met (for example, a certain period of time was passed, reach the maximun data size of commit), the Consumer will block and notify each channel to try commit the transaction.

If and only if all channels are committed successfully, Canal Server will be notified by the ACK request and Canal Client continue to get and consume data.

If there are any Channel fails to commit, it will retrieve data from the location where the last consumption was successful and commit again (the Channel that has successfully commited before will not commmit again to ensure the idempotency of commit).

In the whole cycle of a SyncJob, Canal Client continuously received data from Canal Server and send it to Be through the above process to complete data synchronization.

## Configure MySQL Server

In the master-slave synchronization of MySQL Cluster mode, the binary log file (binlog) records all data changes on the master node. Data synchronization and backup among multiple nodes of the cluster should be carried out through binlog logs, so as to improve the availability of the cluster.

The architecture of master-slave synchronization is usually composed of a master node (responsible for writing) and one or more slave nodes (responsible for reading). All data changes on the master node will be copied to the slave node.

**Note that: Currently, you must use MySQL version 5.7 or above to support Binlog Load**

To enable the binlog of MySQL, you need to edit the my.cnf file and set it like:

```
[mysqld]
log-bin = mysql-bin # 开启 binlog
binlog-format=ROW # 选择 ROW 模式
```

### Principle Description

On MySQL, the binlog files usually name as mysql-bin.000001, mysql-bin.000002... And MySQL will automatically segment the binlog file when certain conditions are met:

1. MySQL is restarted
2. The user enters the `flush logs` command
3. The binlog file size exceeds 1G

To locate the latest consumption location of binlog, the binlog file name and position (offset) must be needed.

For instance, the binlog location of the current consumption so far will be saved on each slave node to prepare for disconnection, reconnection and continued consumption at any time.

```
---------------------                                ---------------------
|       Slave       |              read              |      Master       |
| FileName/Position | <<<--------------------------- |    Binlog Files   |
---------------------                                ---------------------
```

For the master node, it is only responsible for writing to the binlog. Multiple slave nodes can be connected to a master node at the same time to consume different parts of the binlog log without affecting each other.

Binlog log supports two main formats (in addition to mixed based mode):

* Statement-based format: 
	
	Binlog only records the SQL statements executed on the master node, and the slave node copies them to the local node for re-execution.
	
* Row-based format:

	Binlog will record the data change information of each row and all columns of the master node, and the slave node will copy and execute the change of each row to the local node.
	

The first format only writes the executed SQL statements. Although the log volume will be small, it has the following disadvantages:

1. The actual data of each row is not recorded
2. The UDF, random and time functions executed on the master node will have inconsistent results on the slave node
3. The execution order of limit statements may be inconsistent

Therefore, we need to choose the second format which parses each row of data from the binlog log.

In the row-based format, binlog will record the timestamp, server ID, offset and other information of each binlog event. For instance, the following transaction with two insert statements:

```
begin;
insert into canal_test.test_tbl values (3, 300);
insert into canal_test.test_tbl values (4, 400);
commit;
```

There will be four binlog events, including one begin event, two insert events and one commit event:

```
SET TIMESTAMP=1538238301/*!*/; 
BEGIN
/*!*/.
# at 211935643
# at 211935698
#180930 0:25:01 server id 1 end_log_pos 211935698 Table_map: 'canal_test'.'test_tbl' mapped to number 25 
#180930 0:25:01 server id 1 end_log_pos 211935744 Write_rows: table-id 25 flags: STMT_END_F
...
'/*!*/;
### INSERT INTO canal_test.test_tbl
### SET
### @1=1
### @2=100
# at 211935744
#180930 0:25:01 server id 1 end_log_pos 211935771 Xid = 2681726641
...
'/*!*/;
### INSERT INTO canal_test.test_tbl
### SET
### @1=2
### @2=200
# at 211935771
#180930 0:25:01 server id 1 end_log_pos 211939510 Xid = 2681726641 
COMMIT/*!*/;
```

As shown above, each insert event contains modified data. During delete/update, an event can also contain multiple rows of data, making the binlog more compact.

### Open GTID mode (Optional)

A global transaction ID (global transaction identifier) identifies a transaction that has been committed on the master node, which is unique and valid in global. After binlog is enabled, the gtid will be written to the binlog file.

To open the gtid mode of MySQL, you need to edit the my.cnf configuration file and set it like:

```
gtid-mode=on // Open gtid mode
enforce-gtid-consistency=1 // Enforce consistency between gtid and transaction
```

In gtid mode, the master server can easily track transactions, recover data and replicas without binlog file name and offset.

In gtid mode, due to the global validity of gtid, the slave node will no longer need to locate the binlog location on the master node by saving the file name and offset, but can be located by the data itself. During SyncJob, the slave node will skip the execution of any gtid transaction already executed before.

Gtid is expressed as a pair of coordinates, `source_ID` identifies the master node, `transaction_ID` indicates the order in which this transaction is executed on the master node (max 2<sup>63</sup>-1).

```
GTID = source_id:transaction_id
```

For example, the gtid of the 23rd transaction executed on the same master node is:

```
3E11FA47-71CA-11E1-9E33-C80AA9429562:23
```

## Configure Canal Server

Canal is a sub project of Alibaba Otter project. Its main purpose is to provide incremental data subscription and consumption based on MySQL database binlog analysis, which is originally used to solve the scenario of cross machine-room synchronization.

Canal version 1.1.5 and above is recommended. [download link](https://github.com/alibaba/canal/releases)

After downloading, please follow the steps below to complete the deployment.

1. Unzip the canal deployer
2. Create a new directory under the conf folder and rename it as the root directory of instance. The directory name is the destination mentioned later.
3. Modify the instance configuration file (you can copy from `conf/example/instance.properties`)

	```
	vim conf/{your destination}/instance.properties
	```
	```
	## canal instance serverId
	canal.instance.mysql.slaveId = 1234
	## mysql adress
	canal.instance.master.address = 127.0.0.1:3306 
	## mysql username/password
	canal.instance.dbUsername = canal
	canal.instance.dbPassword = canal
	```
4. start up canal server

	```
	sh bin/startup.sh
	```
	
5. Validation start up successfully

	```
	cat logs/{your destination}/{your destination}.log
	```
	```
	2013-02-05 22:50:45.636 [main] INFO  c.a.o.c.i.spring.support.PropertyPlaceholderConfigurer - Loading properties file from class path resource [canal.properties]
	2013-02-05 22:50:45.641 [main] INFO  c.a.o.c.i.spring.support.PropertyPlaceholderConfigurer - Loading properties file from class path resource [xxx/instance.properties]
	2013-02-05 22:50:45.803 [main] INFO  c.a.otter.canal.instance.spring.CanalInstanceWithSpring - start CannalInstance for 1-xxx 
	2013-02-05 22:50:45.810 [main] INFO  c.a.otter.canal.instance.spring.CanalInstanceWithSpring - start successful....
	```
	
### Canal End Description

By faking its own MySQL dump protocol, canal disguises itself as a slave node, get and parses the binlog of the master node.

Multiple instances can be started on the canal server. An instance can be regarded as a slave node. Each instance consists of the following parts:

```
-------------------------------------------------
|  Server                                       |
|  -------------------------------------------- |
|  |  Instance 1                              | |
|  |  -----------   -----------  -----------  | |
|  |  |  Parser |   |  Sink   |  | Store   |  | |
|  |  -----------   -----------  -----------  | |
|  |  -----------------------------------     | |
|  |  |             MetaManager         |     | |
|  |  -----------------------------------     | |
|  -------------------------------------------- |
-------------------------------------------------
```

* Parser: Access the data source, simulate the dump protocol, interact with the master, and analyze the protocol
* Sink: Linker between parser and store, for data filtering, processing and distribution
* Store: Data store
* Meta Manager: Metadata management module

Each instance has its own unique ID in the cluster, that is, server ID.

In the canal server, the instance is identified by a unique string named destination. The canal client needs destination to connect to the corresponding instance.

**Note that: canal client and canal instance should correspond to each other one by one**

Binlog load has forbidded multiple SyncJobs to connect to the same destination.

The data flow direction in instance is binlog -> Parser -> sink -> store.

Instance parses binlog logs through the parser module, and the parsed data is cached in the store. When the user submits a SyncJob to Fe, it will start a Canal Client to subscripe and get the data in the store in the corresponding instance.

The store is actually a ring queue. Users can configure its length and storage space by themselves.

![store](https://doris.apache.org/images/canal_store.png)

Store manages the data in the queue through three pointers:

1. Get pointer: the GET pointer points to the last location get by the Canal Client.
2. Ack pointer: the ACK pointer points to the location of the last successful consumption.
3. Put pointer: the PUT pointer points to the location where the sink module successfully wrote to the store at last.

```
canal client asynchronously get data in the store

       get 0        get 1       get 2                    put
         |            |           |         ......        |
         v            v           v                       v
--------------------------------------------------------------------- store ring queue
         ^            ^
         |            |
       ack 0        ack 1
```

When the Canal Client calls the Get command, the Canal Server will generate data batches and send them to the Canal Client, and move the Get pointer to the right. The Canal Client can get multiple batches until the Get pointer catches up with the Put pointer.

When the consumption of data is successful, the Canal Client will return Ack + Batch ID, notify that the consumption has been successful, and move the Ack pointer to the right. The store will delete the data of this batch from the ring queue, make room to get data from the upstream sink module, and then move the Put pointer to the right.

When the data consumption fails, the client will return a rollback notification of the consumption failure, and the store will reset the Get pointer to the left to the Ack pointer's position, so that the next data get by the Canal Client can start from the Ack pointer again.

Like the slave node in mysql, Canal Server also needs to save the latest consumption location of the client. All metadata in Canal Server (such as gtid and binlog location) is managed by the metamanager. At present, these metadata is persisted in the meta.dat file in the instance's root directory in JSON format by default.

## Basic Operation

### Configure Target Table Properties

User needs to first create the target table which is corresponding to the MySQL side.

Binlog Load can only support unique target tables from now, and the batch delete feature of the target table must be activated.

For the method of enabling Batch Delete, please refer to the batch delete function in [ALTER TABLE PROPERTY](../../../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-TABLE-PROPERTY.html).

Example:

```
-- create target table
CREATE TABLE `test1` (
  `a` int(11) NOT NULL COMMENT "",
  `b` int(11) NOT NULL COMMENT ""
) ENGINE=OLAP
UNIQUE KEY(`a`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`a`) BUCKETS 8;

-- enable batch delete
ALTER TABLE canal_test.test1 ENABLE FEATURE "BATCH_DELETE";
```

### Create SyncJob

The detailed syntax for creating a data synchronization job can be connected to Doris and [CREATE SYNC JOB](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/CREATE-SYNC-JOB.html) to view the syntax help. Here is a detailed introduction to the precautions when creating a job.

* job_name

	`job_Name` is the unique identifier of the SyncJob in the current database. With a specified job name, only one SyncJob can be running at the same time.
	
* channel_desc

	`column_Mapping` mainly refers to the mapping relationship between the columns of the MySQL source table and the Doris target table. 
	
	If it is not specified, the columns of the source table and the target table will consider correspond one by one in order. 
	
	However, we still recommend explicitly specifying the mapping relationship of columns, so that when the schema-change of the target table (such as adding a nullable column), data synchronization can still be carried out. 
	
	Otherwise, when the schema-change occur, because the column mapping relationship is no longer one-to-one, the SyncJob will report an error.
	
* binlog_desc

	`binlog_desc` defines some necessary information for docking the remote binlog address. 
	
	At present, the only supported docking type is the canal type. In canal type, all configuration items need to be prefixed with the canal prefix.
	
	1. canal.server.ip: the address of the canal server
	2. canal.server.port: the port of canal server
	3. canal.destination: the identifier of the instance
	4. canal.batchSize: the maximum batch size get from the canal server for each batch. Default 8192
	5. canal.username: the username of instance
	6. canal.password: the password of instance
	7. canal.debug: when set to true, the details message of each batch and each row will be printed, which may affect the performance.

### Show Job Status


Specific commands and examples for viewing job status can be viewed through the [SHOW SYNC JOB](../../../sql-manual/sql-reference/Show-Statements/SHOW-SYNC-JOB.html) command.

The parameters in the result set have the following meanings:

* State

	The current stage of the job. The transition between job states is shown in the following figure:
	
	```
	                   +-------------+
           create job  |  PENDING    |    resume job
           +-----------+             <-------------+
           |           +-------------+             |
      +----v-------+                       +-------+----+
      |  RUNNING   |     pause job         |  PAUSED    |
      |            +----------------------->            |
      +----+-------+     run error         +-------+----+
           |           +-------------+             |
           |           | CANCELLED   |             |
           +----------->             <-------------+
          stop job     +-------------+    stop job
          system error
	```
	
	After the SyncJob is submitted, the status is pending. 
	
	After the Fe scheduler starts the canal client, the status becomes running.
	
	User can control the status of the job by three commands: `stop/pause/resume`. After the operation, the job status is `cancelled/paused/running` respectively.
	
	There is only one final stage of the job, Cancelled. When the job status changes to Canceled, it cannot be resumed again. 
	
	When an error occurs during SyncJob is running, if the error is unrecoverable, the status will change to cancelled, otherwise it will change to paused.
	
* Channel

	The mapping relationship between all source tables and target tables of the job.
	
* Status

	The latest consumption location of the current binlog (if the gtid mode is on, the gtid will be displayed), and the delay time of the Doris side compared with the MySQL side.
	
* JobConfig

	The remote server information of the docking, such as the address of the Canal Server and the destination of the connected instance.
	
### Control Operation

Users can control the status of jobs through `stop/pause/resume` commands.

You can use [STOP SYNC JOB](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STOP-SYNC-JOB.html) ; [PAUSE SYNC JOB](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/PAUSE-SYNC-JOB.html); And [RESUME SYNC JOB](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/RESUME-SYNC-JOB.html); commands to view help and examples.

## Case Combat

[How to use Apache Doris Binlog Load and examples](https://doris.apache.org/zh-CN/article/articles/doris-binlog-load.html)

## Related Parameters

### Canal configuration

* `canal.ip`

	canal server's ip address
	
* `canal.port`

	canal server's port
	
* `canal.instance.memory.buffer.size`

	The queue length of the store ring queue, must be set to the power of 2, the default length is 16384. This value is equal to the maximum number of events that can be cached on the canal side and directly determines the maximum number of events that can be accommodated in a transaction on the Doris side. It is recommended to make it large enough to prevent the upper limit of the amount of data that can be accommodated in a transaction on the Doris side from being too small, resulting in too frequent transaction submission and data version accumulation.

* `canal.instance.memory.buffer.memunit`

	The default space occupied by an event at the canal end, default value is 1024 bytes. This value multiplied by `canal.instance.memory.buffer.size` is equal to the maximum space of the store. For example, if the queue length of the store is 16384, the space of the store is 16MB. However, the actual size of an event is not actually equal to this value, but is determined by the number of rows of data in the event and the length of each row of data. For example, the insert event of a table with only two columns is only 30 bytes, but the delete event may reach thousands of bytes. This is because the number of rows of delete event is usually more than that of insert event.


### Fe configuration

The following configuration belongs to the system level configuration of SyncJob. The configuration value can be modified in configuration file fe.conf.

* `enable_create_sync_job`

	Turn on the Binlog Load feature. The default value is false. This feature is turned off.
	
* `sync_commit_interval_second`

	Maximum interval time between commit transactions. If there is still data in the channel that has not been committed after this time, the consumer will notify the channel to commit the transaction.
	
* `min_sync_commit_size`

	The minimum number of events required to commit a transaction. If the number of events received by Fe is less than it, Fe will continue to wait for the next batch of data until the time exceeds `sync_commit_interval_second`. The default value is 10000 events. If you want to modify this configuration, please ensure that this value is less than the `canal.instance.memory.buffer.size` configuration on the canal side (16384 by default). Otherwise, Fe will try to get more events than the length of the store queue without ack, causing the store queue to block until timeout.
	
* `min_bytes_sync_commit`

	The minimum data size required to commit a transaction. If the data size received by Fe is smaller than it, it will continue to wait for the next batch of data until the time exceeds `sync_commit_interval_second`. The default value is 15MB. If you want to modify this configuration, please ensure that this value is less than the product `canal.instance.memory.buffer.size` and `canal.instance.memory.buffer.memunit` on the canal side (16MB by default). Otherwise, Fe will try to get data from canal larger than the store space without ack, causing the store queue to block until timeout.
	
* `max_bytes_sync_commit`

	The maximum size of the data when the transaction is committed. If the data size received by Fe is larger than it, it will immediately commit the transaction and send the accumulated data. The default value is 64MB. If you want to modify this configuration, please ensure that this value is greater than the product of `canal.instance.memory.buffer.size` and `canal.instance.memory.buffer.mmemunit` on the canal side (16MB by default) and `min_bytes_sync_commit`。
	
* `max_sync_task_threads_num`

	The maximum number of threads in the SyncJobs' thread pool. There is only one thread pool in the whole Fe for synchronization, which is used to process the tasks created by all SyncJobs in the Fe. 
	
## FAQ

1. Will modifying the table structure affect data synchronization?

	Yes. The SyncJob cannot prohibit `alter table` operation. 
	When the table's schema changes, if the column mapping cannot match, the job may be suspended incorrectly. It is recommended to reduce such problems by explicitly specifying the column mapping relationship in the data synchronization job, or by adding nullable columns or columns with default values.
	
2. Will the SyncJob continue to run after the database is deleted?

	No. In this case, the SyncJob will be checked by the Fe's scheduler thread and be stopped.
	
3. Can multiple SyncJobs be configured with the same `IP:Port + destination`?

	No. When creating a SyncJob, FE will check whether the `IP:Port + destination` is duplicate with the existing job to prevent multiple jobs from connecting to the same instance.
	
4. Why is the precision of floating-point type different between MySQL and Doris during data synchronization?	

	The precision of Doris floating-point type is different from that of MySQL. You can choose to use decimal type instead.

## More Help

For more detailed syntax and best practices used by Binlog Load, see [Binlog Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/CREATE-SYNC-JOB.html) command manual, you can also enter `HELP BINLOG` in the MySql client command line for more help information.
