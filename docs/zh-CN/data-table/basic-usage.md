---
{
    "title": "基础使用指南",
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

# 基础使用指南

Doris 采用 MySQL 协议进行通信，用户可通过 MySQL client 或者 MySQL JDBC连接到 Doris 集群。选择 MySQL client 版本时建议采用5.1 之后的版本，因为 5.1 之前不能支持长度超过 16 个字符的用户名。本文以 MySQL client 为例，通过一个完整的流程向用户展示 Doris 的基本使用方法。

## 创建用户

### Root用户登录与密码修改

Doris 内置 root 和 admin 用户，密码默认都为空。启动完 Doris 程序之后，可以通过 root 或 admin 用户连接到 Doris 集群。 使用下面命令即可登录 Doris，登录后进入到Doris对应的Mysql命令行操作界面：

```bash
[root@doris ~]# mysql  -h FE_HOST -P9030 -uroot
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 41
Server version: 5.1.0 Doris version 1.0.0-preview2-b48ee2734

Copyright (c) 2000, 2022, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> 
```

> `FE_HOST` 是任一FE节点的IP地址，`9030` 是fe.conf 中的 query_port 配置；

登录后，可以通过以下命令修改root密码：

```mysql
mysql> SET PASSWORD FOR 'root' = PASSWORD('your_password');
Query OK, 0 rows affected (0.00 sec)
```

> `your_password`是为`root`用户设置的新密码，可以随意设置，建议设置为强密码增加安全性，下次登录就用新密码登录。

### 创建新用户

我们可以通过下面的命令创建一个普通用户`test`：

```bash
mysql> CREATE USER 'test' IDENTIFIED BY 'test_passwd';
Query OK, 0 rows affected (0.00 sec)
```

后续登录时就可以通过下面链接命令登录：

```bash
[root@doris ~]# mysql -h FE_HOST -P9030 -utest -ptest_passwd
```

> 注意：新创建的普通用户默认没有任何权限，权限授予可以参考后面的权限授予。

## 数据表的创建与数据导入

### 创建数据库

初始可以通过 root 或 admin 用户创建数据库：

```sql
CREATE DATABASE example_db;
```

> 所有命令都可以使用 `HELP command;` 查看到详细的语法帮助，如：`HELP CREATE DATABASE;`。也可以查阅官网 [SHOW CREATE DATABASE](../sql-manual/sql-reference/Show-Statements/SHOW-CREATE-DATABASE.html) 命令手册。
>
> 如果不清楚命令的全名，可以使用 "help 命令某一字段" 进行模糊查询。如键入 'HELP CREATE'，可以匹配到 `CREATE DATABASE`, `CREATE TABLE`, `CREATE USER` 等命令。
>
> ```sql
> mysql> HELP CREATE;
> Many help items for your request exist.
> To make a more specific request, please type 'help <item>',
> where <item> is one of the following
> topics:
>    CREATE CLUSTER
>    CREATE DATABASE
>    CREATE ENCRYPTKEY
>    CREATE FILE
>    CREATE FUNCTION
>    CREATE INDEX
>    CREATE MATERIALIZED VIEW
>    CREATE REPOSITORY
>    CREATE RESOURCE
>    CREATE ROLE
>    CREATE SYNC JOB
>    CREATE TABLE
>    CREATE USER
>    CREATE VIEW
>    ROUTINE LOAD
>    SHOW CREATE FUNCTION
>    SHOW CREATE ROUTINE LOAD
> ```

数据库创建完成之后，可以通过 [SHOW DATABASES](../sql-manual/sql-reference/Show-Statements/SHOW-DATABASES.html#show-databases) 查看数据库信息。

```sql
mysql> SHOW DATABASES;
+--------------------+
| Database           |
+--------------------+
| example_db         |
| information_schema |
+--------------------+
2 rows in set (0.00 sec)
```

>information_schema数据库是为了兼容MySQL协议而存在，实际中信息可能不是很准确，所以关于具体数据库的信息建议通过直接查询相应数据库而获得。

### 账户授权

example_db 创建完成之后，可以通过 root/admin 账户使用[GRANT](../sql-manual/sql-reference/Account-Management-Statements/GRANT.html)命令将 example_db 读写权限授权给普通账户，如 test。授权之后采用 test 账户登录就可以操作 example_db 数据库了。

```sql
mysql> GRANT ALL ON example_db TO test;
Query OK, 0 rows affected (0.01 sec)
```

### 建表

使用 [CREATE TABLE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.html) 命令建立一个表(Table)。更多详细参数可以 `HELP CREATE TABLE;`		

首先，我们需要使用[USE](../sql-manual/sql-reference/Utility-Statements/USE.html)命令来切换数据库：

```sql
mysql> USE example_db;
Database changed
```

Doris支持[复合分区和单分区](data-partition.html#复合分区与单分区)两种建表方式。下面以聚合模型为例，分别演示如何创建两种分区的数据表。

#### 单分区

建立一个名字为 table1 的逻辑表。分桶列为 siteid，桶数为 10。

这个表的 schema 如下：

- siteid：类型是INT（4字节）, 默认值为10
- citycode：类型是SMALLINT（2字节）
- username：类型是VARCHAR, 最大长度为32, 默认值为空字符串
- pv：类型是BIGINT（8字节）, 默认值是0; 这是一个指标列, Doris内部会对指标列做聚合操作, 这个列的聚合方法是求和（SUM）

建表语句如下：

```sql
CREATE TABLE table1
(
    siteid INT DEFAULT '10',
    citycode SMALLINT,
    username VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, citycode, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");
```

#### 复合分区

建立一个名字为 table2 的逻辑表。

这个表的 schema 如下：

- event_day：类型是DATE，无默认值
- siteid：类型是INT（4字节）, 默认值为10
- citycode：类型是SMALLINT（2字节）
- username：类型是VARCHAR, 最大长度为32, 默认值为空字符串
- pv：类型是BIGINT（8字节）, 默认值是0; 这是一个指标列, Doris 内部会对指标列做聚合操作, 这个列的聚合方法是求和（SUM）

我们使用 event_day 列作为分区列，建立3个分区: p201706, p201707, p201708

- p201706：范围为 [最小值, 2017-07-01)
- p201707：范围为 [2017-07-01, 2017-08-01)
- p201708：范围为 [2017-08-01, 2017-09-01)

> 注意区间为左闭右开。

每个分区使用 siteid 进行哈希分桶，桶数为10

建表语句如下:

```sql
CREATE TABLE table2
(
    event_day DATE,
    siteid INT DEFAULT '10',
    citycode SMALLINT,
    username VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(event_day, siteid, citycode, username)
PARTITION BY RANGE(event_day)
(
    PARTITION p201706 VALUES LESS THAN ('2017-07-01'),
    PARTITION p201707 VALUES LESS THAN ('2017-08-01'),
    PARTITION p201708 VALUES LESS THAN ('2017-09-01')
)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");
```

数据表创建完成后，可以查看 example_db 中表的信息:

```sql
mysql> SHOW TABLES;
+----------------------+
| Tables_in_example_db |
+----------------------+
| table1               |
| table2               |
+----------------------+
2 rows in set (0.01 sec)

mysql> DESC table1;
+----------+-------------+------+-------+---------+-------+
| Field    | Type        | Null | Key   | Default | Extra |
+----------+-------------+------+-------+---------+-------+
| siteid   | int(11)     | Yes  | true  | 10      |       |
| citycode | smallint(6) | Yes  | true  | N/A     |       |
| username | varchar(32) | Yes  | true  |         |       |
| pv       | bigint(20)  | Yes  | false | 0       | SUM   |
+----------+-------------+------+-------+---------+-------+
4 rows in set (0.00 sec)

mysql> DESC table2;
+-----------+-------------+------+-------+---------+-------+
| Field     | Type        | Null | Key   | Default | Extra |
+-----------+-------------+------+-------+---------+-------+
| event_day | date        | Yes  | true  | N/A     |       |
| siteid    | int(11)     | Yes  | true  | 10      |       |
| citycode  | smallint(6) | Yes  | true  | N/A     |       |
| username  | varchar(32) | Yes  | true  |         |       |
| pv        | bigint(20)  | Yes  | false | 0       | SUM   |
+-----------+-------------+------+-------+---------+-------+
5 rows in set (0.00 sec)
```

>注意事项：

> 1. 上述表通过设置 replication_num 建的都是单副本的表，Doris建议用户采用默认的 3 副本设置，以保证高可用。
> 2. 可以对复合分区表动态的增删分区，详见 `HELP ALTER TABLE;` 中 Partition 相关部分。
> 3. 数据导入可以导入指定的 Partition，详见 `HELP LOAD;`。
> 4. 可以动态修改表的 Schema，详见`HELP ALTER TABLE;`。
> 5. 可以对 Table 增加上卷表（Rollup）以提高查询性能，这部分可以参见高级使用指南关于 Rollup 的描述。
> 6. 表的列的Null属性默认为true，会对查询性能有一定的影响。

### 导入数据

Doris 支持多种数据导入方式。具体可以参阅[数据导入](../data-operate/import/load-manual.html)文档。这里我们使用流式导入和 Broker 导入做示例。

#### 流式导入

流式导入通过 HTTP 协议向 Doris 传输数据，可以不依赖其他系统或组件直接导入本地数据。详细语法帮助可以参阅 `HELP STREAM LOAD;`。

示例1：以 "table1_20170707" 为 Label，使用本地文件 table1_data 导入 table1 表。

```bash
curl --location-trusted -u test:test_passwd -H "label:table1_20170707" -H "column_separator:," -T table1_data http://FE_HOST:8030/api/example_db/table1/_stream_load
```

> 1. FE_HOST 是任一 FE 所在节点 IP，8030 为 fe.conf 中的 http_port。
> 2. 可以使用任一 BE 的 IP，以及 be.conf 中的 webserver_port 进行导入。如：`BE_HOST:8040`

本地文件 `table1_data` 以 `,` 作为数据之间的分隔，具体内容如下：

```text
1,1,jim,2
2,1,grace,2
3,2,tom,2
4,3,bush,3
5,3,helen,3
```

示例2: 以 "table2_20170707" 为 Label，使用本地文件 table2_data 导入 table2 表。

```bash
curl --location-trusted -u test:test -H "label:table2_20170707" -H "column_separator:|" -T table2_data http://127.0.0.1:8030/api/example_db/table2/_stream_load
```

本地文件 `table2_data` 以 `|` 作为数据之间的分隔，具体内容如下：

```text
2017-07-03|1|1|jim|2
2017-07-05|2|1|grace|2
2017-07-12|3|2|tom|2
2017-07-15|4|3|bush|3
2017-07-12|5|3|helen|3
```

> 注意事项：
>
> 1. 采用流式导入建议文件大小限制在 10GB 以内，过大的文件会导致失败重试代价变大。
> 2. 每一批导入数据都需要取一个 Label，Label 最好是一个和一批数据有关的字符串，方便阅读和管理。Doris 基于 Label 保证在一个Database 内，同一批数据只可导入成功一次。失败任务的 Label 可以重用。
> 3. 流式导入是同步命令。命令返回成功则表示数据已经导入，返回失败表示这批数据没有导入。

#### Broker 导入

Broker 导入通过部署的 Broker 进程，读取外部存储上的数据进行导入。更多帮助请参阅 `HELP BROKER LOAD;`

示例：以 "table1_20170708" 为 Label，将 HDFS 上的文件导入 table1 表

```sql
LOAD LABEL table1_20170708
(
    DATA INFILE("hdfs://your.namenode.host:port/dir/table1_data")
    INTO TABLE table1
)
WITH BROKER hdfs 
(
    "username"="hdfs_user",
    "password"="hdfs_password"
)
PROPERTIES
(
    "timeout"="3600",
    "max_filter_ratio"="0.1"
);
```

Broker 导入是异步命令。以上命令执行成功只表示提交任务成功。导入是否成功需要通过 `SHOW LOAD;` 查看。如：

```sql
SHOW LOAD WHERE LABEL = "table1_20170708";
```

返回结果中，`State` 字段为 `FINISHED` 则表示导入成功。

关于 `SHOW LOAD` 的更多说明，可以参阅 `HELP SHOW LOAD;`

异步的导入任务在结束前可以取消：

```sql
CANCEL LOAD WHERE LABEL = "table1_20170708";
```

## 数据的查询

### 简单查询

查询示例:

```sql
mysql> SELECT * FROM table1 LIMIT 3;
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      2 |        1 | 'grace'  |    2 |
|      5 |        3 | 'helen'  |    3 |
|      3 |        2 | 'tom'    |    2 |
+--------+----------+----------+------+
3 rows in set (0.01 sec)

mysql> SELECT * FROM table1 ORDER BY citycode;
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      2 |        1 | 'grace'  |    2 |
|      1 |        1 | 'jim'    |    2 |
|      3 |        2 | 'tom'    |    2 |
|      4 |        3 | 'bush'   |    3 |
|      5 |        3 | 'helen'  |    3 |
+--------+----------+----------+------+
5 rows in set (0.01 sec)
```

### Join 查询

查询示例:

```sql
mysql> SELECT SUM(table1.pv) FROM table1 JOIN table2 WHERE table1.siteid = table2.siteid;
+--------------------+
| sum(`table1`.`pv`) |
+--------------------+
|                 12 |
+--------------------+
1 row in set (0.20 sec)
```

### 子查询

查询示例:

```sql
mysql> SELECT SUM(pv) FROM table2 WHERE siteid IN (SELECT siteid FROM table1 WHERE siteid > 2);
+-----------+
| sum(`pv`) |
+-----------+
|         8 |
+-----------+
1 row in set (0.13 sec)
```
