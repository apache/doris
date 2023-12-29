---
{
    "title": "自增列",
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

# 自增列

<version since="2.1">

</version>

自增列功能支持了在导入过程中对用户没有在自增列上指定值的数据行分配一个表内唯一的值。

## 功能说明

对于含有自增列的表，用户在在导入数据时:
- 如果导入的目标列中不包含自增列，则自增列将会被Doris自动生成的值填充。
- 如果导入的目标列中包含自增列，则导入数据中该列中的null值将会被Doris自动生成的值替换，非null值则保持不变。

### 唯一性

Doris保证了自增列上生成的值具有**表内唯一性**。但需要注意的是，**自增列的唯一性仅保证由Doris自动填充的值具有唯一性，而不考虑由用户提供的值**，如果用户同时对该表通过显示指定自增列的方式插入了用户提供的值，则不能保证这个唯一性。

### 单调性

Doris保证了在自增列上填充的值**在一个分桶内**是严格单调递增的。但需要注意的是，Doris**不能保证**在物理时间上后一次导入的数据在自增列上填充的值比前一次更大，这是因为处于性能考虑，每个BE上都会缓存一部分预先分配的自增列的值，每个BE上缓存的值互不相交。因此，不能根据自增列分配出的值的大小来判断导入时间上的先后顺序。同时，由于BE上缓存的存在，Doris仅能保证自增列上自动填充的值在一定程度上是稠密的，但**不能保证**在一次导入中自动填充的自增列的值是完全连续的。因此可能会出现一次导入中自增列自动填充的值具有一定的跳跃性的现象。

## 语法

要使用自增列，需要在建表[CREATE-TABLE](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE)时为对应的列添加`AUTO_INCREMENT`属性。

### 示例

1. 创建一个Dupliciate模型表，其中一个key列是自增列

  ```sql
  CREATE TABLE `tbl` (
        `id` BIGINT NOT NULL AUTO_INCREMENT,
        `value` BIGINT NOT NULL
  ) ENGINE=OLAP
  DUPLICATE KEY(`id`)
  DISTRIBUTED BY HASH(`id`) BUCKETS 10
  PROPERTIES (
  "replication_allocation" = "tag.location.default: 3"
  );
  ```

2. 创建一个Dupliciate模型表，其中一个value列是自增列

  ```sql
  CREATE TABLE `tbl` (
        `uid` BIGINT NOT NULL,
        `name` BIGINT NOT NULL,
        `id` BIGINT NOT NULL AUTO_INCREMENT,
        `value` BIGINT NOT NULL
  ) ENGINE=OLAP
  DUPLICATE KEY(`uid`, `name`)
  DISTRIBUTED BY HASH(`uid`) BUCKETS 10
  PROPERTIES (
  "replication_allocation" = "tag.location.default: 3"
  );
  ```

3. 创建一个Unique模型表，其中一个key列是自增列

  ```sql
  CREATE TABLE `tbl` (
        `id` BIGINT NOT NULL AUTO_INCREMENT,
        `name` varchar(65533) NOT NULL,
        `value` int(11) NOT NULL
  ) ENGINE=OLAP
  UNIQUE KEY(`id`)
  DISTRIBUTED BY HASH(`id`) BUCKETS 10
  PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "enable_unique_key_merge_on_write" = "true"
  );
  ```

4. 创建一个Unique模型表，其中一个value列是自增列

  ```sql
  CREATE TABLE `tbl` (
        `text` varchar(65533) NOT NULL,
        `id` BIGINT NOT NULL AUTO_INCREMENT,
  ) ENGINE=OLAP
  UNIQUE KEY(`text`)
  DISTRIBUTED BY HASH(`text`) BUCKETS 10
  PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "enable_unique_key_merge_on_write" = "true"
  );
  ```

### 约束和限制

1. 仅Duplicate模型表和Unique模型表可以包含自增列。
2. 一张表最多只能包含一个自增列。
3. 自增列的类型必须是BIGINT类型，且必须为NOT NULL。

## 使用方式

### 普通导入

以下表为例：

```sql
CREATE TABLE `tbl` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `name` varchar(65533) NOT NULL,
    `value` int(11) NOT NULL
) ENGINE=OLAP
UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"enable_unique_key_merge_on_write" = "true"
);
```

使用insert into语句导入并且不指定自增列`id`时，`id`列会被自动填充生成的值。
```sql
mysql> insert into tbl(name, value) values("Bob", 10), ("Alice", 20), ("Jack", 30);
Query OK, 3 rows affected (0.09 sec)
{'label':'label_183babcb84ad4023_a2d6266ab73fb5aa', 'status':'VISIBLE', 'txnId':'7'}

mysql> select * from tbl order by id;
+------+-------+-------+
| id   | name  | value |
+------+-------+-------+
|    0 | Bob   |    10 |
|    1 | Alice |    20 |
|    2 | Jack  |    30 |
+------+-------+-------+
3 rows in set (0.05 sec)
```

类似地，使用stream load导入文件test.csv且不指定自增列`id`，`id`列会被自动填充生成的值。

test.csv:
```
Tom, 40
John, 50
```

```
curl --location-trusted -u user:passwd -H "columns:name,value" -H "column_separator:," -T ./test1.csv http://{host}:{port}/api/{db}/tbl/_stream_load
```

```sql
mysql> select * from tbl order by id;
+------+-------+-------+
| id   | name  | value |
+------+-------+-------+
|    0 | Bob   |    10 |
|    1 | Alice |    20 |
|    2 | Jack  |    30 |
|    3 | Tom   |    40 |
|    4 | John  |    50 |
+------+-------+-------+
5 rows in set (0.04 sec)
```

使用insert into导入时指定自增列`id`，则该列数据中的null值会被生成的值替换。

```sql
mysql> insert into tbl(id, name, value) values(null, "Doris", 60), (null, "Nereids", 70);
Query OK, 2 rows affected (0.07 sec)
{'label':'label_9cb0c01db1a0402c_a2b8b44c11ce4703', 'status':'VISIBLE', 'txnId':'10'}

mysql> select * from tbl order by id;
+------+---------+-------+
| id   | name    | value |
+------+---------+-------+
|    0 | Bob     |    10 |
|    1 | Alice   |    20 |
|    2 | Jack    |    30 |
|    3 | Tom     |    40 |
|    4 | John    |    50 |
|    5 | Doris   |    60 |
|    6 | Nereids |    70 |
+------+---------+-------+
7 rows in set (0.04 sec)
```


### 部分列更新

在对一张包含自增列的merge-on-write Unique表进行部分列更新时，如果自增列是key列，由于部分列更新时用户必须显示指定key列，部分列更新的目标列必须包含自增列。此时的导入行为和普通的部分列更新相同。
```sql
mysql> CREATE TABLE `tbl2` (
    ->     `id` BIGINT NOT NULL AUTO_INCREMENT,
    ->     `name` varchar(65533) NOT NULL,
    ->     `value` int(11) NOT NULL DEFAULT "0"
    -> ) ENGINE=OLAP
    -> UNIQUE KEY(`id`)
    -> DISTRIBUTED BY HASH(`id`) BUCKETS 10
    -> PROPERTIES (
    -> "replication_allocation" = "tag.location.default: 1",
    -> "enable_unique_key_merge_on_write" = "true"
    -> );
Query OK, 0 rows affected (0.03 sec)

mysql> insert into tbl2(id, name, value) values(1, "Bob", 10), (2, "Alice", 20), (3, "Jack", 30);
Query OK, 3 rows affected (0.14 sec)
{'label':'label_5538549c866240b6_bce75ef323ac22a0', 'status':'VISIBLE', 'txnId':'1004'}

mysql> select * from tbl2 order by id;
+------+-------+-------+
| id   | name  | value |
+------+-------+-------+
|    1 | Bob   |    10 |
|    2 | Alice |    20 |
|    3 | Jack  |    30 |
+------+-------+-------+
3 rows in set (0.08 sec)

mysql> set enable_unique_key_partial_update=true;
Query OK, 0 rows affected (0.01 sec)

mysql> set enable_insert_strict=false;
Query OK, 0 rows affected (0.00 sec)

mysql> insert into tbl2(id, name) values(1, "modified"), (4, "added");
Query OK, 2 rows affected (0.06 sec)
{'label':'label_3e68324cfd87457d_a6166cc0a878cfdc', 'status':'VISIBLE', 'txnId':'1005'}

mysql> select * from tbl2 order by id;
+------+----------+-------+
| id   | name     | value |
+------+----------+-------+
|    1 | modified |    10 |
|    2 | Alice    |    20 |
|    3 | Jack     |    30 |
|    4 | added    |     0 |
+------+----------+-------+
4 rows in set (0.04 sec)
```

当自增列是非key列时，如果用户没有指定自增列的值，其值会从表中原有的数据行中进行补齐。如果用户指定了自增列，则该列数据中的null值会被替换为生成出的值，非null值则保持不表，然后以部分列更新的语义插入该表。

```sql
mysql> CREATE TABLE `tbl3` (
    ->     `id` BIGINT NOT NULL,
    ->     `name` varchar(100) NOT NULL,
    ->     `score` BIGINT NOT NULL,
    ->     `aid` BIGINT NOT NULL AUTO_INCREMENT
    -> ) ENGINE=OLAP
    -> UNIQUE KEY(`id`)
    -> DISTRIBUTED BY HASH(`id`) BUCKETS 1
    -> PROPERTIES (
    -> "replication_allocation" = "tag.location.default: 1",
    -> "enable_unique_key_merge_on_write" = "true"
    -> );
Query OK, 0 rows affected (0.16 sec)

mysql> insert into tbl3(id, name, score) values(1, "Doris", 100), (2, "Nereids", 200), (3, "Bob", 300);
Query OK, 3 rows affected (0.28 sec)
{'label':'label_c52b2c246e244dda_9b91ee5e27a31f9b', 'status':'VISIBLE', 'txnId':'2003'}

mysql> select * from tbl3 order by id;
+------+---------+-------+------+
| id   | name    | score | aid  |
+------+---------+-------+------+
|    1 | Doris   |   100 |    0 |
|    2 | Nereids |   200 |    1 |
|    3 | Bob     |   300 |    2 |
+------+---------+-------+------+
3 rows in set (0.13 sec)

mysql> set enable_unique_key_partial_update=true;
Query OK, 0 rows affected (0.00 sec)

mysql> set enable_insert_strict=false;
Query OK, 0 rows affected (0.00 sec)

mysql> insert into tbl3(id, score) values(1, 999), (2, 888);
Query OK, 2 rows affected (0.07 sec)
{'label':'label_dfec927d7a4343ca_9f9ade581391de97', 'status':'VISIBLE', 'txnId':'2004'}

mysql> select * from tbl3 order by id;
+------+---------+-------+------+
| id   | name    | score | aid  |
+------+---------+-------+------+
|    1 | Doris   |   999 |    0 |
|    2 | Nereids |   888 |    1 |
|    3 | Bob     |   300 |    2 |
+------+---------+-------+------+
3 rows in set (0.06 sec)

mysql> insert into tbl3(id, aid) values(1, 1000), (3, 500);
Query OK, 2 rows affected (0.07 sec)
{'label':'label_b26012959f714f60_abe23c87a06aa0bf', 'status':'VISIBLE', 'txnId':'2005'}

mysql> select * from tbl3 order by id;
+------+---------+-------+------+
| id   | name    | score | aid  |
+------+---------+-------+------+
|    1 | Doris   |   999 | 1000 |
|    2 | Nereids |   888 |    1 |
|    3 | Bob     |   300 |  500 |
+------+---------+-------+------+
3 rows in set (0.06 sec)
```

## 使用场景

### 字典编码

在用户画像场景中使用 bitmap 做人群分析时需要构建用户字典，每个用户对应一个唯一的整数字典值，聚集的字典值可以获得更好的 bitmap 性能。

以离线uv，pv分析场景为例，假设有如下用户行为表存放明细数据：

```sql
CREATE TABLE `dwd_tbl` (
    `user_id` varchar(50) NOT NULL,
    `dim1` varchar(50) NOT NULL,
    `dim2` varchar(50) NOT NULL,
    `dim3` varchar(50) NOT NULL,
    `dim4` varchar(50) NOT NULL,
    `dim5` varchar(50) NOT NULL,
    `visit_time` DATE NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`user_id`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 32
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

利用自增列创建如下字典表

```sql
CREATE TABLE `dict_tbl` (
    `user_id` varchar(50) NOT NULL,
    `aid` BIGINT NOT NULL AUTO_INCREMENT
) ENGINE=OLAP
PRIMARY KEY(`user_id`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 32
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

将存量数据中的`user_id`导入字典表，建立`user_id`到整数值的编码映射

```sql
insert into dit_tbl(user_id)
select user_id from dwd_tbl group by dwd_tbl;
```

或者使用如下方式仅将增量数据中的`user_id`导入到字典表

```sql
insert into dit_tbl(user_id)
select dwd_tbl.user_id from dwd_tbl left join dict_tbl
on dwd_tbl.user_id = dict_tbl.user_id where dwd_tbl.visit_time > '2023-12-10' and dict_tbl.user_id is NULL
```

假设`dim1`, `dim3`, `dim4`是我们关系的统计维度，建立下表存放聚合结果

```sql
CREATE TABLE `dws_tbl` (
    `dim1` varchar(50) NOT NULL,
    `dim3` varchar(50) NOT NULL,
    `dim4` varchar(50) NOT NULL,
    `visit_time` DATE NOT NULL,
    `user_id_bitmap` BITMAP NOT NULL,
    `pv` BIGINT NOT NULL 
) ENGINE=OLAP
PRIMARY KEY(`dim1`,`dim3`,`dim4`,`visit_time`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 32
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

将数据聚合运算后存放至聚合结果表

```sql
insert into dws_tbl
select dwd_tbl.dim1, dwd_tbl.dim3, dwd_tbl.dim5, dwd_tbl.visit_time, BITMAP_UNION(TO_BITMAP(dict_tbl.aid)), COUNT(1)
from dwd_tbl INNER JOIN dict_tbl on dwd_tbl.user_id = dict_tbl.user_id
```

用如下语句进行 uv, pv 查询

```sql
select dim1, dim3, dim5, visit_time, BITMAP_UNION(TO_BITMAP(dict_tbl.aid)) as uv, SUM(pv) as pv
from dws_tbl where visit_time >= '2023-11-01' and visit_time <= '2023-11-30' group by dim1, dim3, dim5, visit_time;
```

### 高效分页

在页面展示数据时，往往需要做分页展示，传统的分页使用 SQL 重的 limit offset 语法实现，查询涉及的数据量很大时，这种方法比较低效。可以通过自增列给每行数据一个唯一值，就可以使用 where unique_value > xx limit 100 的方法来实现分页。



