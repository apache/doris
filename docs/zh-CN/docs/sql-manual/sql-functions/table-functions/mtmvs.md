---
{
    "title": "MTMVS",
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

## `mtmvs`

### Name


mtmvs


### description

表函数，生成多表物化视图临时表，可以查看某个db中创建的多表物化视图信息。

该函数用于 from 子句中。

#### syntax

`catalogs("database"="")`

mtmvs()表结构：
```
mysql> desc function mtmvs("database"="db1");
+--------------------+--------+------+-------+---------+-------+
| Field              | Type   | Null | Key   | Default | Extra |
+--------------------+--------+------+-------+---------+-------+
| Id                 | BIGINT | No   | false | NULL    | NONE  |
| Name               | TEXT   | No   | false | NULL    | NONE  |
| JobName            | TEXT   | No   | false | NULL    | NONE  |
| State              | TEXT   | No   | false | NULL    | NONE  |
| SchemaChangeDetail | TEXT   | No   | false | NULL    | NONE  |
| RefreshState       | TEXT   | No   | false | NULL    | NONE  |
| RefreshInfo        | TEXT   | No   | false | NULL    | NONE  |
| QuerySql           | TEXT   | No   | false | NULL    | NONE  |
| EnvInfo            | TEXT   | No   | false | NULL    | NONE  |
| MvProperties       | TEXT   | No   | false | NULL    | NONE  |
+--------------------+--------+------+-------+---------+-------+
10 rows in set (0.00 sec)
```

* Id：物化视图id.
* Name：物化视图Name.
* JobName：物化视图对应的job名称.
* State：物化视图状态.
* SchemaChangeDetail：物化视图State变为SchemaChange的原因.
* RefreshState：物化视图刷新状态.
* RefreshInfo：物化视图定义的刷新策略信息.
* QuerySql：物化视图定义的查询语句.
* EnvInfo：物化视图创建时的环境信息.
* MvPropertiesMvProperties：物化视属性.

### example

1. 查看db1下的所有物化视图

```
mysql> select * from mtmvs("database"="db1");
```

2. 查看db1下的物化视图名称为mv1的物化视图

```
mysql> select * from mtmvs("database"="db1") where Name = "mv1";
```

3. 查看db1下的物化视图名称为mv1的状态

```
mysql> select State from mtmvs("database"="db1") where Name = "mv1";
```

### keywords

    mtmvs
