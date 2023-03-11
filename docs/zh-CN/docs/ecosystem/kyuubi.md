---

{
"title": "Kyuubi",
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

# 通过 Kyuubi 连接 Doris

## 介绍

[Apache Kyuubi](https://kyuubi.apache.org/) 是一个分布式和多租户网关，用于在 Lakehouse 上提供 Serverless
SQL，可连接包括Spark、Flink、Hive、JDBC等引擎，并对外提供Thrift、Trino等接口协议供灵活对接。
其中Apache Kyuubi实现了JDBC Engine并支持Doris方言，并可用于对接Doris作为数据源。
Apache Kyuubi可提供高可用、服务发现、租户隔离、统一认证、生命周期管理等一系列特性。

## 下载 Apache Kyuubi

## 配置方法

### 下载 Apache Kyuubi

从官网下载Apache Kyuubi 1.6.0或以上版本的安装包后解压。

Apache Kyuubi 下载地址： <https://kyuubi.apache.org/zh/releases.html>

### 配置Doris作为Kyuubi数据源

- 修改配置文件 `$KYUUBI_HOME/conf/kyuubi-defaults.conf`

```properties
kyuubi.engine.type=jdbc
kyuubi.engine.jdbc.type=doris
kyuubi.engine.jdbc.driver.class=com.mysql.cj.jdbc.Driver
kyuubi.engine.jdbc.connection.url=jdbc:mysql://xxx:xxx
kyuubi.engine.jdbc.connection.user=***
kyuubi.engine.jdbc.connection.password=***
```

| 配置项                                    | 说明                                            |
|----------------------------------------|-----------------------------------------------|
| kyuubi.engine.type                     | 引擎类型。请使用jdbc                                  |
| kyuubi.engine.jdbc.type                | JDBC 服务类型。这里请指定为doris                         |
| kyuubi.engine.jdbc.driver.class        | 连接 JDBC 服务使用的驱动类名。请使用com.mysql.cj.jdbc.Driver |
| kyuubi.engine.jdbc.connection.url      | JDBC 服务连接。这里请指定 Doris FE 上的 mysql server 连接地址 |
| kyuubi.engine.jdbc.connection.user     | JDBC 服务用户名                                    |
| kyuubi.engine.jdbc.connection.password | JDBC 服务密码                                     |

- 其他相关配置参考 [Apache Kyuubi配置说明](https://kyuubi.readthedocs.io/en/master/deployment/settings.html) 。

### 添加MySQL驱动

添加 Mysql JDB C驱动 `mysql-connector-j-8.X.X.jar` 到 `$KYUUBI_HOME/externals/engines/jdbc` 目录下。

### 启动 Kyuubi 服务

`$KYUUBI_HOME/bin/kyuubi start`
启动后，Kyuubi默认监听10009端口提供Thrift协议。

## 使用方法

以下例子展示通过Apache Kyuubi的beeline工具经Thrift协议查询Doris。

### 建立连接

```shell
$ $KYUUBI_HOME/bin/beeline -u "jdbc:hive2://xxxx:10009/"
```

### 执行查询

执行查询语句 `select * from demo.expamle_tbl;` 并得到结果。

```shell
0: jdbc:hive2://xxxx:10009/> select * from demo.example_tbl;

2023-03-07 09:29:14.771 INFO org.apache.kyuubi.operation.ExecuteStatement: Processing anonymous's query[bdc59dd0-ceea-4c02-8c3a-23424323f5db]: PENDING_STATE -> RUNNING_STATE, statement:
select * from demo.example_tbl
2023-03-07 09:29:14.786 INFO org.apache.kyuubi.operation.ExecuteStatement: Query[bdc59dd0-ceea-4c02-8c3a-23424323f5db] in FINISHED_STATE
2023-03-07 09:29:14.787 INFO org.apache.kyuubi.operation.ExecuteStatement: Processing anonymous's query[bdc59dd0-ceea-4c02-8c3a-23424323f5db]: RUNNING_STATE -> FINISHED_STATE, time taken: 0.015 seconds
+----------+-------------+-------+------+------+------------------------+-------+-----------------+-----------------+
| user_id  |    date     | city  | age  | sex  |    last_visit_date     | cost  | max_dwell_time  | min_dwell_time  |
+----------+-------------+-------+------+------+------------------------+-------+-----------------+-----------------+
| 10000    | 2017-10-01  | 北京   | 20   | 0    | 2017-10-01 07:00:00.0  | 70    | 10              | 2               |
| 10001    | 2017-10-01  | 北京   | 30   | 1    | 2017-10-01 17:05:45.0  | 4     | 22              | 22              |
| 10002    | 2017-10-02  | 上海   | 20   | 1    | 2017-10-02 12:59:12.0  | 400   | 5               | 5               |
| 10003    | 2017-10-02  | 广州   | 32   | 0    | 2017-10-02 11:20:00.0  | 60    | 11              | 11              |
| 10004    | 2017-10-01  | 深圳   | 35   | 0    | 2017-10-01 10:00:15.0  | 200   | 3               | 3               |
| 10004    | 2017-10-03  | 深圳   | 35   | 0    | 2017-10-03 10:20:22.0  | 22    | 6               | 6               |
+----------+-------------+-------+------+------+------------------------+-------+-----------------+-----------------+
6 rows selected (0.068 seconds)
```
