---
{
    "title": "Min Load Replica Num",
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

# Min Load Replica Num

默认情况下，数据导入要求至少有超过半数的副本写入成功，导入才算成功。然而，这种方式不够灵活，在某些场景会带来不便。

举个例子，对于两副本情况，按上面的多数派原则，要想导入数据，则需要这两个副本都写入成功。这意味着，在导入数据过程中，不允许任意一个副本不可用。这极大影响了集群的可用性。

为了解决以上问题，Doris允许用户设置最小写入副本数(Min Load Replica Num)。对导入数据任务，当它成功写入的副本数大于或等于最小写入副本数时，导入即成功。

## 用法

### 单个表的最小写入副本数

可以对单个olap表，设置最小写入副本数，并用表属性`min_load_replica_num`来表示。该属性的有效值要求大于0且不超过表的副本数。其默认值为-1，表示不启用该属性。

可以在创建表时设置表的`min_load_replica_num`。

```sql
CREATE TABLE test_table1
(
    k1 INT,
    k2 INT
)
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 5
PROPERTIES
(
    'replication_num' = '2',
    'min_load_replica_num' = '1'
);
```

对一个已存在的表，可以使用语句`ALTER TABLE`来修改它的`min_load_replica_num`。

```sql
ALTER TABLE test_table1
SET ( 'min_load_replica_num' = '1');
```

可以使用语句`SHOW CREATE TABLE`来查看表的属性`min_load_replica_num`。

```SQL
SHOW CREATE TABLE test_table1;
```

输出结果的PROPERTIES中将包含`min_load_replica_num`。例如：

```text
Create Table: CREATE TABLE `test_table1` (
  `k1` int(11) NULL,
  `k2` int(11) NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`k1`) BUCKETS 5
PROPERTIES (
"replication_allocation" = "tag.location.default: 2",
"min_load_replica_num" = "1",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);
```

### 全局最小写入副本数

可以对所有olap表，设置全局最小写入副本数，并用FE的配置项`min_load_replica_num`来表示。该配置项的有效值要求大于0。其默认值为-1，表示不开启全局最小写入副本数。

对一个表，如果表属性`min_load_replica_num`有效（即大于0），那么该表将会忽略全局配置`min_load_replica_num`。否则，如果全局配置`min_load_replica_num`有效（即大于0），那么该表的最小写入副本数将等于`min(FE.conf.min_load_replica_num，table.replication_num/2 + 1)`。

对于FE配置项的查看和修改，可以参考[这里](../../../admin-manual/config/fe-config.md)。

### 其余情况

如果没有开启表属性`min_load_replica_num`（即小于或者等于0），也没有设置全局配置`min_load_replica_num`(即小于或等于0)，那么数据的导入仍需多数派副本写入成功才算成功。此时，表的最小写入副本数等于`table.replicatition_num/2 + 1`。

