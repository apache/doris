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

对于含有自增列的表，用户在在导入数据时，如果导入的目标列中不包含自增列，则自增列的值将全部由生成的值填充。如果导入的目标列中包含自增列，则导入数据中该列的null值将会被生成的值替换，非null值则保持不变。

### 唯一性

Doris保证了自增列上生成的值具有**表内唯一性**。但需要注意的是，**自增列的唯一性仅保证由Doris自动填充的值具有唯一性，而不考虑由用户提供的值**，如果用户同时对该表通过显示指定自增列的方式插入了用户提供的值，则不能保证这个唯一性。

### 单调性

Doris保证了在自增列上填充的值**在一个分桶内**是严格单调递增的。但需要注意的是，Doris**不能保证**在物理时间上后一次导入的数据在自增列上填充的值比前一次更大，这是因为处于性能考虑，每个BE上都会缓存一部分自增列的值。因此，不能根据自增列分配出的值的大小来判断导入时间上的先后顺序。同时，由于BE上缓存的存在，Doris能保证自增列上自动填充的值在一定程度上是稠密的，但**不能保证**在一次导入中自动填充的自增列的值是完全连续的。因此可能会出现一次导入中自增列自动填充的值具有一定的跳跃性的现象。


## 语法

要使用自增列，需要在建表[CREATE-TABLE](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE)时为对应的列添加`AUTO_INCREMENT`属性。

### 示例

1. 创建一个Dupliciate表，其中一个key列是自增列

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

2. 创建一个Dupliciate表，其中一个value列是自增列

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

3. 创建一个Unique表，其中一个key列是自增列

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

4. 创建一个Unique表，其中一个value列是自增列

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

使用insert into导入时指定自增列`id`，则其中的null值会被生成的值替换。

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

在对一张包含自增列的merge-on-write Unique表进行部分列更新时，如果自增列是key列，由于部分列更新时用户必须显示指定key列，即部分列更新的目标列必须包含自增列。此时的导入行为和普通的部分列更新相同。
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

当自增列是非key列时，如果用户没有指定自增列的值，则其值会从表中原有的数据行中进行补齐。如果用户指定了自增列，则其中的null值会被替换为生成出的值，非null值则保持不表。




## 场景示例


## 注意事项

### 唯一性说明
