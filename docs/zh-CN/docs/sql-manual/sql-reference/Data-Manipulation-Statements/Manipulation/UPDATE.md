---
{
    "title": "UPDATE",
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

## UPDATE

### Name

UPDATE

### Description

该语句是为进行对数据进行更新的操作，UPDATE 语句目前仅支持 UNIQUE KEY 模型。

UPDATE操作目前只支持更新Value列，Key列的更新可参考[使用FlinkCDC更新Key列](../../../../ecosystem/flink-doris-connector.md#使用flinkcdc更新key列)。

#### Syntax

```sql
UPDATE target_table [table_alias]
    SET assignment_list
    WHERE condition

assignment_list:
    assignment [, assignment] ...

assignment:
    col_name = value

value:
    {expr | DEFAULT}
```

<version since="dev">

```sql
UPDATE target_table [table_alias]
    SET assignment_list
    [ FROM additional_tables]
    WHERE condition
```

</version>

#### Required Parameters

+ target_table: 待更新数据的目标表。可以是 'db_name.table_name' 形式
+ assignment_list: 待更新的目标列，形如 'col_name = value, col_name = value' 格式
+ WHERE condition: 期望更新的条件，一个返回 true 或者 false 的表达式即可

#### Optional Parameters

<version since="dev">

+ table_alias: 表的别名
+ FROM additional_tables: 指定一个或多个表，用于选中更新的行，或者获取更新的值。注意，如需要在此列表中再次使用目标表，需要为其显式指定别名。

</version>

#### Note

当前 UPDATE 语句仅支持在 UNIQUE KEY 模型上的行更新。

### Example

`test` 表是一个 unique 模型的表，包含: k1, k2, v1, v2  四个列。其中 k1, k2 是 key，v1, v2 是value，聚合方式是 Replace。

1. 将 'test' 表中满足条件 k1 =1 , k2 =2 的 v1 列更新为 1

```sql
UPDATE test SET v1 = 1 WHERE k1=1 and k2=2;
```

2. 将 'test' 表中 k1=1 的列的 v1 列自增1

```sql
UPDATE test SET v1 = v1+1 WHERE k1=1;
```

<version since="dev">

3. 使用`t2`和`t3`表连接的结果，更新`t1`

```sql
-- 创建t1, t2, t3三张表
CREATE TABLE t1
  (id INT, c1 BIGINT, c2 STRING, c3 DOUBLE, c4 DATE)
UNIQUE KEY (id)
DISTRIBUTED BY HASH (id)
PROPERTIES('replication_num'='1', "function_column.sequence_col" = "c4");

CREATE TABLE t2
  (id INT, c1 BIGINT, c2 STRING, c3 DOUBLE, c4 DATE)
DISTRIBUTED BY HASH (id)
PROPERTIES('replication_num'='1');

CREATE TABLE t3
  (id INT)
DISTRIBUTED BY HASH (id)
PROPERTIES('replication_num'='1');

-- 插入数据
INSERT INTO t1 VALUES
  (1, 1, '1', 1.0, '2000-01-01'),
  (2, 2, '2', 2.0, '2000-01-02'),
  (3, 3, '3', 3.0, '2000-01-03');

INSERT INTO t2 VALUES
  (1, 10, '10', 10.0, '2000-01-10'),
  (2, 20, '20', 20.0, '2000-01-20'),
  (3, 30, '30', 30.0, '2000-01-30'),
  (4, 4, '4', 4.0, '2000-01-04'),
  (5, 5, '5', 5.0, '2000-01-05');

INSERT INTO t3 VALUES
  (1),
  (4),
  (5);

-- 更新 t1
UPDATE t1
  SET t1.c1 = t2.c1, t1.c3 = t2.c3 * 100
  FROM t2 INNER JOIN t3 ON t2.id = t3.id
  WHERE t1.id = t2.id;
```

预期结果为，更新了`t1`表`id`为`1`的列

```
+----+----+----+--------+------------+
| id | c1 | c2 | c3     | c4         |
+----+----+----+--------+------------+
| 1  | 10 | 1  | 1000.0 | 2000-01-01 |
| 2  | 2  | 2  |    2.0 | 2000-01-02 |
| 3  | 3  | 3  |    3.0 | 2000-01-03 |
+----+----+----+--------+------------+
```

</version>

### Keywords

    UPDATE
