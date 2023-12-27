---
{
    "title": "Sequence 列",
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

# Sequence 列

Uniq模型主要针对需要唯一主键的场景，可以保证主键唯一性约束，但是由于使用REPLACE聚合方式，在同一批次中导入的相同主键数据，替换顺序不做保证，详细介绍可以参考[数据模型](../../data-table/data-model.md)。替换顺序无法保证，则最终导入到表中的具体数据，就存在不确定性。

为了解决这个问题，Doris支持了sequence列，通过用户在导入时指定sequence列，相同key列下，REPLACE聚合类型的列将按照sequence列的值进行替换，较大值可以替换较小值，反之则无法替换。该方法将顺序的确定交给了用户，由用户控制替换顺序。

sequence列目前只支持Uniq模型。

## 适用场景

Sequence列只能在Uniq数据模型下使用。

## 基本原理

通过增加一个隐藏列`__DORIS_SEQUENCE_COL__`实现，该列的类型由用户在建表时指定，在导入时确定该列具体值，并依据该值对REPLACE列进行替换。

### 建表

创建Uniq表时，将按照用户指定类型自动添加一个隐藏列`__DORIS_SEQUENCE_COL__`。

### 导入

导入时，fe在解析的过程中将隐藏列的值设置成 `order by` 表达式的值(broker load和routine load)，或者`function_column.sequence_col`表达式的值(stream load)，value列将按照该值进行替换。隐藏列`__DORIS_SEQUENCE_COL__`的值既可以设置为数据源中一列，也可以是表结构中的一列。

### 读取

请求包含value列时需要额外读取`__DORIS_SEQUENCE_COL__`列，该列用于在相同key列下，REPLACE聚合函数替换顺序的依据，较大值可以替换较小值，反之则不能替换。

### Cumulative Compaction

Cumulative Compaction 时和读取过程原理相同。

### Base Compaction

Base Compaction 时和读取过程原理相同。

## 使用语法

Sequence列建表时有两种方式，一种是建表时设置`sequence_col`属性，一种是建表时设置`sequence_type`属性。

### 设置`sequence_col`（推荐）

创建Uniq表时，指定sequence列到表中其他column的映射

```text
PROPERTIES (
    "function_column.sequence_col" = 'column_name'
);
```
sequence_col用来指定sequence列到表中某一列的映射，该列可以为整型和时间类型（DATE、DATETIME），创建后不能更改该列的类型。

导入方式和没有sequence列时一样，使用相对比较简单，推荐使用。

### 设置`sequence_type`

创建Uniq表时，指定sequence列类型

```text
PROPERTIES (
    "function_column.sequence_type" = 'Date'
);
```

sequence_type用来指定sequence列的类型，可以为整型和时间类型（DATE、DATETIME）。

导入时需要指定sequence列到其他列的映射。

**Stream Load**

stream load 的写法是在header中的`function_column.sequence_col`字段添加隐藏列对应的source_sequence的映射， 示例

```bash
curl --location-trusted -u root -H "columns: k1,k2,source_sequence,v1,v2" -H "function_column.sequence_col: source_sequence" -T testData http://host:port/api/testDb/testTbl/_stream_load
```

**Broker Load**

在`ORDER BY` 处设置隐藏列映射的source_sequence字段

```sql
LOAD LABEL db1.label1
(
    DATA INFILE("hdfs://host:port/user/data/*/test.txt")
    INTO TABLE `tbl1`
    COLUMNS TERMINATED BY ","
    (k1,k2,source_sequence,v1,v2)
    ORDER BY source_sequence
)
WITH BROKER 'broker'
(
    "username"="user",
    "password"="pass"
)
PROPERTIES
(
    "timeout" = "3600"
);
```

**Routine Load**

映射方式同上，示例如下

```sql
   CREATE ROUTINE LOAD example_db.test1 ON example_tbl 
    [WITH MERGE|APPEND|DELETE]
    COLUMNS(k1, k2, source_sequence, v1, v2),
    WHERE k1 > 100 and k2 like "%doris%"
    [ORDER BY source_sequence]
    PROPERTIES
    (
        "desired_concurrent_number"="3",
        "max_batch_interval" = "20",
        "max_batch_rows" = "300000",
        "max_batch_size" = "209715200",
        "strict_mode" = "false"
    )
    FROM KAFKA
    (
        "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
        "kafka_topic" = "my_topic",
        "kafka_partitions" = "0,1,2,3",
        "kafka_offsets" = "101,0,0,200"
    );
```

## 启用sequence column支持

在新建表时如果设置了`function_column.sequence_col`或者`function_column.sequence_type` ，则新建表将支持sequence column。 对于一个不支持sequence column的表，如果想要使用该功能，可以使用如下语句： 
```sql
ALTER TABLE example_db.my_table ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES ("function_column.sequence_type" = "Date")
```
如果不确定一个表是否支持sequence column，可以通过设置一个session variable来显示隐藏列 `SET show_hidden_columns=true` ，之后使用`desc tablename`，如果输出中有`__DORIS_SEQUENCE_COL__` 列则支持，如果没有则不支持。

## 使用示例

下面以Stream Load为例来展示使用方式：

1. 创建支持sequence column的表

创建Uniq模型的test_table数据表，并指定sequence列映射到表中的modify_date列。

```sql
CREATE TABLE test.test_table
(
    user_id bigint,
    date date,
    group_id bigint,
    modify_date date,
    keyword VARCHAR(128)
)
UNIQUE KEY(user_id, date, group_id)
DISTRIBUTED BY HASH (user_id) BUCKETS 32
PROPERTIES(
    "function_column.sequence_col" = 'modify_date',
    "replication_num" = "1",
    "in_memory" = "false"
);
```

表结构如下：

```sql
MySQL > desc test_table;
+-------------+--------------+------+-------+---------+---------+
| Field       | Type         | Null | Key   | Default | Extra   |
+-------------+--------------+------+-------+---------+---------+
| user_id     | BIGINT       | No   | true  | NULL    |         |
| date        | DATE         | No   | true  | NULL    |         |
| group_id    | BIGINT       | No   | true  | NULL    |         |
| modify_date | DATE         | No   | false | NULL    | REPLACE |
| keyword     | VARCHAR(128) | No   | false | NULL    | REPLACE |
+-------------+--------------+------+-------+---------+---------+
```

2. 正常导入数据：

导入如下数据

```text
1       2020-02-22      1       2020-02-21      a
1       2020-02-22      1       2020-02-22      b
1       2020-02-22      1       2020-03-05      c
1       2020-02-22      1       2020-02-26      d
1       2020-02-22      1       2020-02-23      e
1       2020-02-22      1       2020-02-24      b
```

此处以stream load为例

```bash
curl --location-trusted -u root: -T testData http://host:port/api/test/test_table/_stream_load
```

结果为

```sql
MySQL > select * from test_table;
+---------+------------+----------+-------------+---------+
| user_id | date       | group_id | modify_date | keyword |
+---------+------------+----------+-------------+---------+
|       1 | 2020-02-22 |        1 | 2020-03-05  | c       |
+---------+------------+----------+-------------+---------+
```

在这次导入中，因sequence column的值（也就是modify_date中的值）中'2020-03-05'为最大值，所以keyword列中最终保留了c。

3. 替换顺序的保证

上述步骤完成后，接着导入如下数据

```text
1       2020-02-22      1       2020-02-22      a
1       2020-02-22      1       2020-02-23      b
```

查询数据

```sql
MySQL [test]> select * from test_table;
+---------+------------+----------+-------------+---------+
| user_id | date       | group_id | modify_date | keyword |
+---------+------------+----------+-------------+---------+
|       1 | 2020-02-22 |        1 | 2020-03-05  | c       |
+---------+------------+----------+-------------+---------+
```
在这次导入的数据中，会比较所有已导入数据的sequence column（也就是modify_date)，其中'2020-03-05'为最大值，所以keyword列中最终保留了c。

再尝试导入如下数据

```text
1       2020-02-22      1       2020-02-22      a
1       2020-02-22      1       2020-03-23      w
```

查询数据

```sql
MySQL [test]> select * from test_table;
+---------+------------+----------+-------------+---------+
| user_id | date       | group_id | modify_date | keyword |
+---------+------------+----------+-------------+---------+
|       1 | 2020-02-22 |        1 | 2020-03-23  | w       |
+---------+------------+----------+-------------+---------+
```

此时就可以替换表中原有的数据。综上，在导入过程中，会比较所有批次的sequence列值，选择值最大的记录导入Doris表中。

## 注意
1. 为防止误用，在StreamLoad/BrokerLoad等导入任务以及行更新insert语句中，用户必须显示指定sequence列(除非sequence列的默认值为CURRENT_TIMESTAMP)，不然会收到以下报错信息：
```
Table test_tbl has sequence column, need to specify the sequence column
```
2. 自2.0版本起，Doris对Unique Key表的Merge-on-Write实现支持了部分列更新能力，在部分列更新导入中，用户每次可以只更新一部分列，因此并不是必须要包含sequence列。若用户提交的导入任务中，包含sequence列，则行为无影响；若用户提交的导入任务不包含sequence列，Doris会使用匹配的历史数据中的sequence列作为更新后该行的sequence列的值。如果历史数据中不存在相同key的列，则会自动用null或默认值填充。 
