---

{
"title": "Kyuubi",
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

# Use Kyuubi with Doris

## Introduction

[Apache Kyuubi](https://kyuubi.apache.org/) is a distributed and multi-tenant gateway to provide serverless SQL on Data
Warehouses and Lakehouses.
Apache Kyuubi is providing varied protocols like Thrift, Trino, MySQL etc., to the engines including Spark, Flink, Hive,
JDBC, etc.
Doris could be connected as JDBC data source with Doris dialect supported in Apache Kyuubi.
Apache Kyuubi also provides a series of useful features including HA, service discovery,
unified authentication, engine lifecycle management, etc.

## Usage

### Download Apache Kyuubi

Download Apache Kyuubi from <https://kyuubi.apache.org/zh/releases.html>

Get Apache Kyuubi 1.6.0 or above and extract it to folder.

### Config Doris as Kyuubi data source

- Update Kyuubi configurations in `$KYUUBI_HOME/conf/kyuubi-defaults.conf`

```properties
kyuubi.engine.type=jdbc
kyuubi.engine.jdbc.type=doris
kyuubi.engine.jdbc.driver.class=com.mysql.cj.jdbc.Driver
kyuubi.engine.jdbc.connection.url=jdbc:mysql://xxx:xxx
kyuubi.engine.jdbc.connection.user=***
kyuubi.engine.jdbc.connection.password=***
```

| Configuration                          | Description                                                   |
|----------------------------------------|---------------------------------------------------------------|
| kyuubi.engine.type                     | Engine Type, specify to `jdbc`                                |
| kyuubi.engine.jdbc.type                | JDBC service type, specify to `doris`                         |
| kyuubi.engine.jdbc.driver.class        | JDBC driver class name, specify to `com.mysql.cj.jdbc.Driver` |
| kyuubi.engine.jdbc.connection.url      | JDBC url to Doris FE                                          |
| kyuubi.engine.jdbc.connection.user     | JDBC username                                                 |
| kyuubi.engine.jdbc.connection.password | JDBC password                                                 |

- For other configuration in Apache Kyuubi, please refer
  to [Apache Kyuubi Configuration Docs](https://kyuubi.readthedocs.io/en/master/deployment/settings.html) .

### Add MySQL JDBC Driver

Copy the Mysql JDBC Driver `mysql-connector-j-8.X.X.jar` to `$KYUUBI_HOME/externals/engines/jdbc`.

### Start Kyuubi Server

Run `$KYUUBI_HOME/bin/kyuubi start`.
After started, port 10009 by default is listened by Kyuubi Server with Thrift protocol.

## Example

The following example shows basic example of querying Doris with Kyuubi with beeline CLI in Thrift protocol.

### Connect to Kyuubi with Beeline

```shell
$ $KYUUBI_HOME/bin/beeline -u "jdbc:hive2://xxxx:10009/"
```

### Execute Query to Kyuubi

Execute query statement `select * from demo.expamle_tbl;` with query results returned.

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
