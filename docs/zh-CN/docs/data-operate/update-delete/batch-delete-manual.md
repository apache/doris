---
{
    "title": "批量删除",
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

# 批量删除

目前Doris 支持 [Broker Load](../import/import-way/broker-load-manual.md)，[Routine Load](../import/import-way/routine-load-manual)， [Stream Load](../import/import-way/stream-load-manual) 等多种导入方式，对于数据的删除目前只能通过delete语句进行删除，使用delete 语句的方式删除时，每执行一次delete 都会生成一个新的数据版本，如果频繁删除会严重影响查询性能，并且在使用delete方式删除时，是通过生成一个空的rowset来记录删除条件实现，每次读取都要对删除条件进行过滤，同样在条件较多时会对性能造成影响。对比其他的系统，greenplum 的实现方式更像是传统数据库产品，snowflake 通过merge 语法实现。

对于类似于cdc数据导入的场景，数据中insert和delete一般是穿插出现的，面对这种场景我们目前的导入方式也无法满足，即使我们能够分离出insert和delete虽然可以解决导入的问题，但是仍然解决不了删除的问题。使用批量删除功能可以解决这些个别场景的需求。数据导入有三种合并方式：

1. APPEND: 数据全部追加到现有数据中；
2. DELETE: 删除所有与导入数据key 列值相同的行(当表存在[`sequence`](sequence-column-manual.md)列时，需要同时满足主键相同以及sequence列的大小逻辑才能正确删除，详见下边用例4)；
3. MERGE: 根据 DELETE ON 的决定 APPEND 还是 DELETE。

## 基本原理

通过增加一个隐藏列`__DORIS_DELETE_SIGN__`实现，因为我们只是在unique 模型上做批量删除，因此只需要增加一个类型为bool 聚合函数为replace 的隐藏列即可。在be 各种聚合写入流程都和正常列一样，读取方案有两个：

在fe遇到 * 等扩展时去掉`__DORIS_DELETE_SIGN__`，并且默认加上 `__DORIS_DELETE_SIGN__ != true` 的条件， be 读取时都会加上一列进行判断，通过条件确定是否删除。

### 导入

导入时在fe 解析时将隐藏列的值设置成 `DELETE ON` 表达式的值，其他的聚合行为和replace的聚合列相同。

### 读取

读取时在所有存在隐藏列的olapScanNode上增加`__DORIS_DELETE_SIGN__ != true` 的条件，be 不感知这一过程，正常执行。

### Cumulative Compaction

Cumulative Compaction 时将隐藏列看作正常的列处理，Compaction逻辑没有变化。

### Base Compaction

Base Compaction 时要将标记为删除的行的删掉，以减少数据占用的空间。

## 启用批量删除支持

启用批量删除支持有一下两种形式：

1. 通过在fe 配置文件中增加`enable_batch_delete_by_default=true` 重启fe 后新建表的都支持批量删除，此选项默认为true；
2. 对于没有更改上述fe配置或对于已存在的不支持批量删除功能的表，可以使用如下语句： `ALTER TABLE tablename ENABLE FEATURE "BATCH_DELETE"` 来启用批量删除。本操作本质上是一个schema change 操作，操作立即返回，可以通过`show alter table column` 来确认操作是否完成。

那么如何确定一个表是否支持批量删除，可以通过设置一个session variable 来显示隐藏列 `SET show_hidden_columns=true` ，之后使用`desc tablename`，如果输出中有`__DORIS_DELETE_SIGN__` 列则支持，如果没有则不支持。

## 语法说明

导入的语法设计方面主要是增加一个指定删除标记列的字段的column映射，并且需要在导入的数据中增加一列，各种导入方式设置的语法如下

### Stream Load

`Stream Load` 的写法在header 中的 columns 字段增加一个设置删除标记列的字段， 示例 `-H "columns: k1, k2, label_c3" -H "merge_type: [MERGE|APPEND|DELETE]" -H "delete: label_c3=1"`。

### Broker Load

`Broker Load` 的写法在 `PROPERTIES` 处设置删除标记列的字段，语法如下：

```sql
LOAD LABEL db1.label1
(
    [MERGE|APPEND|DELETE] DATA INFILE("hdfs://abc.com:8888/user/palo/test/ml/file1")
    INTO TABLE tbl1
    COLUMNS TERMINATED BY ","
    (tmp_c1,tmp_c2, label_c3)
    SET
    (
        id=tmp_c2,
        name=tmp_c1,
    )
    [DELETE ON label_c3=true]
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

### Routine Load

`Routine Load`的写法在  `columns`字段增加映射，映射方式同上，语法如下：

```sql
CREATE ROUTINE LOAD example_db.test1 ON example_tbl 
 [WITH MERGE|APPEND|DELETE]
 COLUMNS(k1, k2, k3, v1, v2, label),
 WHERE k1 > 100 and k2 like "%doris%"
 [DELETE ON label=true]
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

## 注意事项

1. 由于除`Stream Load` 外的导入操作在doris 内部有可能乱序执行，因此在使用`MERGE` 方式导入时如果不是`Stream Load`，需要与 load sequence 一起使用，具体的 语法可以参照[`sequence`](sequence-column-manual.md)列 相关的文档；
2. `DELETE ON` 条件只能与 MERGE 一起使用。
3. 如果在执行导入作业前按上文所述开启了`SET show_hidden_columns = true`的session variable来查看表是否支持批量删除, 按示例完成DELETE/MERGE的导入作业后, 如果在同一个session中执行`select count(*) from xxx`等语句时, 需要执行`SET show_hidden_columns = false`或者开启新的session, 避免查询结果中包含那些被批量删除的记录, 导致结果与预期不符.

## 使用示例

### 查看是否启用批量删除支持

```sql
mysql> SET show_hidden_columns=true;
Query OK, 0 rows affected (0.00 sec)

mysql> DESC test;
+-----------------------+--------------+------+-------+---------+---------+
| Field                 | Type         | Null | Key   | Default | Extra   |
+-----------------------+--------------+------+-------+---------+---------+
| name                  | VARCHAR(100) | No   | true  | NULL    |         |
| gender                | VARCHAR(10)  | Yes  | false | NULL    | REPLACE |
| age                   | INT          | Yes  | false | NULL    | REPLACE |
| __DORIS_DELETE_SIGN__ | TINYINT      | No   | false | 0       | REPLACE |
+-----------------------+--------------+------+-------+---------+---------+
4 rows in set (0.00 sec)
```

### Stream Load使用示例

1. 正常导入数据：

```bash
curl --location-trusted -u root: -H "column_separator:," -H "columns: siteid, citycode, username, pv" -H "merge_type: APPEND"  -T ~/table1_data http://127.0.0.1:8130/api/test/table1/_stream_load
```

其中的APPEND 条件可以省略，与下面的语句效果相同：

```bash
curl --location-trusted -u root: -H "column_separator:," -H "columns: siteid, citycode, username, pv" -T ~/table1_data http://127.0.0.1:8130/api/test/table1/_stream_load
```

2. 将与导入数据key 相同的数据全部删除

```bash
curl --location-trusted -u root: -H "column_separator:," -H "columns: siteid, citycode, username, pv" -H "merge_type: DELETE"  -T ~/table1_data http://127.0.0.1:8130/api/test/table1/_stream_load
```

假设导入表中原有数据为:

```text
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      3 |        2 | tom      |    2 |
|      4 |        3 | bush     |    3 |
|      5 |        3 | helen    |    3 |
+--------+----------+----------+------+
```

导入数据为：

```text
3,2,tom,0
```

导入后数据变成:

```text
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      4 |        3 | bush     |    3 |
|      5 |        3 | helen    |    3 |
+--------+----------+----------+------+
```

3. 将导入数据中与`site_id=1` 的行的key列相同的行

```bash
curl --location-trusted -u root: -H "column_separator:," -H "columns: siteid, citycode, username, pv" -H "merge_type: MERGE" -H "delete: siteid=1"  -T ~/table1_data http://127.0.0.1:8130/api/test/table1/_stream_load
```

假设导入前数据为：

```text
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      4 |        3 | bush     |    3 |
|      5 |        3 | helen    |    3 |
|      1 |        1 | jim      |    2 |
+--------+----------+----------+------+
```

导入数据为：

```text
2,1,grace,2
3,2,tom,2
1,1,jim,2
```

导入后为：

```text
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      4 |        3 | bush     |    3 |
|      2 |        1 | grace    |    2 |
|      3 |        2 | tom      |    2 |
|      5 |        3 | helen    |    3 |
+--------+----------+----------+------+
```

4. 当存在sequence列时，将与导入数据key 相同的数据全部删除

```bash
curl --location-trusted -u root: -H "column_separator:," -H "columns: name, gender, age" -H "function_column.sequence_col: age" -H "merge_type: DELETE"  -T ~/table1_data http://127.0.0.1:8130/api/test/table1/_stream_load
```

当unique表设置了sequence列时，在相同key列下，sequence列的值会作为REPLACE聚合函数替换顺序的依据，较大值可以替换较小值。
当对这种表基于`__DORIS_DELETE_SIGN__`进行删除标记时，需要保证key相同和sequence列值要大于等于当前值。

假设有表，结构如下
```sql
mysql> SET show_hidden_columns=true;
Query OK, 0 rows affected (0.00 sec)

mysql> DESC table1;
+------------------------+--------------+------+-------+---------+---------+
| Field                  | Type         | Null | Key   | Default | Extra   |
+------------------------+--------------+------+-------+---------+---------+
| name                   | VARCHAR(100) | No   | true  | NULL    |         |
| gender                 | VARCHAR(10)  | Yes  | false | NULL    | REPLACE |
| age                    | INT          | Yes  | false | NULL    | REPLACE |
| __DORIS_DELETE_SIGN__  | TINYINT      | No   | false | 0       | REPLACE |
| __DORIS_SEQUENCE_COL__ | INT          | Yes  | false | NULL    | REPLACE |
+------------------------+--------------+------+-------+---------+---------+
4 rows in set (0.00 sec)
```

假设导入表中原有数据为:

```text
+-------+--------+------+
| name  | gender | age  |
+-------+--------+------+
| li    | male   |   10 |
| wang  | male   |   14 |
| zhang | male   |   12 |
+-------+--------+------+
```

当导入数据为：
```text
li,male,10
```

导入后数据后会变成:
```text
+-------+--------+------+
| name  | gender | age  |
+-------+--------+------+
| wang  | male   |   14 |
| zhang | male   |   12 |
+-------+--------+------+
```
会发现数据
```text
li,male,10
```
被删除成功。

但是假如导入数据为：
```text
li,male,9
```

导入后数据会变成:
```text
+-------+--------+------+
| name  | gender | age  |
+-------+--------+------+
| li    | male   |   10 |
| wang  | male   |   14 |
| zhang | male   |   12 |
+-------+--------+------+
```

会看到数据
```text
li,male,10
```
并没有被删除，这是因为在底层的依赖关系上，会先判断key相同的情况，对外展示sequence列的值大的行数据，然后在看该行的`__DORIS_DELETE_SIGN__`值是否为1，如果为1则不会对外展示，如果为0，则仍会读出来。

**当导入数据中同时存在数据写入和删除时（例如Flink CDC场景中），使用seq列可以有效的保证当数据乱序到达时的一致性，避免后到达的一个旧版本的删除操作，误删掉了先到达的新版本的数据。**
