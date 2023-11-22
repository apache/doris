---
{
    "title": "CATALOGS",
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

## `catalogs`

### Name


catalogs


### description

表函数，生成 catalogs 临时表，可以查看当前doris中的创建的 catalogs 信息。

该函数用于 from 子句中。

#### syntax

`catalogs()`

catalogs()表结构：
```
mysql> desc function catalogs();
+-------------+--------+------+-------+---------+-------+
| Field       | Type   | Null | Key   | Default | Extra |
+-------------+--------+------+-------+---------+-------+
| CatalogId   | BIGINT | No   | false | NULL    | NONE  |
| CatalogName | TEXT   | No   | false | NULL    | NONE  |
| CatalogType | TEXT   | No   | false | NULL    | NONE  |
| Property    | TEXT   | No   | false | NULL    | NONE  |
| Value       | TEXT   | No   | false | NULL    | NONE  |
+-------------+--------+------+-------+---------+-------+
5 rows in set (0.04 sec)
```

`catalogs()` tvf展示的信息是综合了 `show catalogs` 与 `show catalog xxx` 语句的结果。

可以利用tvf生成的表去做过滤、join等操作。



### example

```
mysql> select * from catalogs();
+-----------+-------------+-------------+--------------------------------------------+---------------------------------------------------------------------------+
| CatalogId | CatalogName | CatalogType | Property                                   | Value                                                                     |
+-----------+-------------+-------------+--------------------------------------------+---------------------------------------------------------------------------+
|     16725 | hive        | hms         | dfs.client.failover.proxy.provider.HANN    | org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider |
|     16725 | hive        | hms         | dfs.ha.namenodes.HANN                      | nn1,nn2                                                                   |
|     16725 | hive        | hms         | create_time                                | 2023-07-13 16:24:38.968                                                   |
|     16725 | hive        | hms         | ipc.client.fallback-to-simple-auth-allowed | true                                                                      |
|     16725 | hive        | hms         | dfs.namenode.rpc-address.HANN.nn1          | nn1_host:rpc_port                                                         |
|     16725 | hive        | hms         | hive.metastore.uris                        | thrift://127.0.0.1:7004                                                   |
|     16725 | hive        | hms         | dfs.namenode.rpc-address.HANN.nn2          | nn2_host:rpc_port                                                         |
|     16725 | hive        | hms         | type                                       | hms                                                                       |
|     16725 | hive        | hms         | dfs.nameservices                           | HANN                                                                      |
|         0 | internal    | internal    | NULL                                       | NULL                                                                      |
|     16726 | es          | es          | create_time                                | 2023-07-13 16:24:44.922                                                   |
|     16726 | es          | es          | type                                       | es                                                                        |
|     16726 | es          | es          | hosts                                      | http://127.0.0.1:9200                                                     |
+-----------+-------------+-------------+--------------------------------------------+---------------------------------------------------------------------------+
13 rows in set (0.01 sec)
```

### keywords

    catalogs
