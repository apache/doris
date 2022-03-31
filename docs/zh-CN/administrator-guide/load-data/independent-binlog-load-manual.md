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
Independent Binlog Load 以不依赖任何外部服务的方式，提供了一种使Doris增量同步MySQL数据库的CDC(Change Data Capture)功能。

## 适用场景

* INSERT/UPDATE/DELETE支持
* 过滤Query
* 暂不兼容DDL语句

## 基本原理
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
| |    | Canal Client  |              |    |  |
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

如上图，用户向FE提交一个数据同步作业。

FE的每个数据同步作业会监听指定的MySQL库，每获取到一个数据batch，都会由consumer根据对应表分发到不同的channel，每个channel都会为此数据batch产生一个发送数据的子任务Task。

在FE上，一个Task是channel向BE发送数据的子任务，里面包含分发到当前channel的同一个batch的数据。

channel控制着单个表事务的开始、提交、终止。一个事务周期内，一般会从consumer获取到多个batch的数据，因此会产生多个向BE发送数据的子任务Task，在提交事务成功前，这些Task不会实际生效。

满足一定条件时（比如超过一定时间、达到提交最大数据大小），consumer将会阻塞并通知各个channel提交事务。

## 使用流程

### 创建MySQL表

登录MySQL，创建源database和table。例如：

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

### 创建Doris表

仅支持Unique类型的表，例如：

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

### 创建SYNC JOB

可以通过`HELP CREATE SYNC JOB`查看语法帮助。

示例：

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

### 控制作业状态

具体使用方式可以通过SQL查询：
- 查看作业状态 `HELP SHOW SYNC JOB;`
- 停止作业 `HELP STOP SYNC JOB;`
- 暂停作业 `HELP PAUSE SYNC JOB;`
- 恢复作业 `HELP RESUME SYNC JOB;`

## 相关配置

### MySQL端

- 大于MySQL 5.7 版本
- 开启binlog，选择ROW模式，开启GTID

在my.cnf配置文件中增加配置：

```
[mysqld]
gtid_mode=ON
enforce-gtid-consistency=true
log-bin=replicas-mysql-bin  
binlog_format=row
```

### Doris FE端
* `enable_create_sync_job` 

    开启数据同步功能，默认为false。

* `sync_commit_interval_second`

	提交事务的最大时间间隔。若超过了这个时间channel中还有数据没有提交，consumer会通知channel提交事务。
	
* `min_sync_commit_size`

    提交事务需满足的最小event数量。若Fe接收到的event数量小于它，会继续等待下一批数据直到时间超过了`sync_commit_interval_second `为止，默认值是10000个events。
      
* `min_bytes_sync_commit`

	提交事务需满足的最小数据大小。若Fe接收到的数据大小小于它，会继续等待下一批数据直到时间超过了`sync_commit_interval_second `为止，默认值是15MB。
	
* `max_bytes_sync_commit`

	提交事务时的数据大小的最大值。若Fe接收到的数据大小大于它，会立即提交事务并发送已积累的数据，默认值是64MB。
	
* `max_sync_task_threads_num`

	数据同步作业线程池中的最大线程数量。此线程池整个FE中只有一个，用于处理FE中所有数据同步作业向BE发送数据的任务task，线程池的实现在`SyncTaskPool`类。
