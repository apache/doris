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
目前Doris 支持broker load， routine load， stream load 等多种导入方式，对于数据的删除目前只能通过delete 语句进行删除，使用delete 语句的方式删除时，每执行一次delete 都会生成一个新的数据版本，如果频繁删除会严重影响查询性能，并且在使用delete 方式删除时，是通过生成一个空的rowset来记录删除条件实现，每次读取都要对删除条件进行过滤，同样在条件较多时会对性能造成影响。对比其他的系统，greenplum 的实现方式更像是传统数据库产品，snowflake 通过merge 语法实现。

对于类似于cdc 数据的导入的场景，数据数据中insert 和delete 一般是穿插出现的，面对这种场景我们目前的导入方式也无法满足，即使我们能够分离出insert 和delete 虽然可以解决导入的问题，但是仍然解决不了删除的问题。使用批量删除功能可以解决这些个场景的需求。
数据导入有三种合并方式：
1. APPEND: 数据全部追加到现有数据中
2. DELETE: 删除所有与导入数据key 列值相同的行
3. MERGE: 根据 DELETE ON 的决定 APPEND 还是 DELETE

## 原理
通过增加一个隐藏列`__DORIS_DELETE_SIGN__`实现，因为我们只是在unique 模型上做批量删除，因此只需要增加一个 类型为bool 聚合函数为replace 的隐藏列即可。在be 各种聚合写入流程都和正常列一样，读取方案有两个：

在fe遇到 * 等扩展时去去掉`__DORIS_DELETE_SIGN__`，并且默认加上 `__DORIS_DELETE_SIGN__ != true` 的条件
be 读取时都会加上一列进行判断，通过条件确定是否删除。

### 导入

导入时在fe 解析时将隐藏列的值设置成 `DELETE ON` 表达式的值，其他的聚合行为和replace的聚合列相同

### 读取

读取时在所有存在隐藏列的olapScanNode上增加`__DORIS_DELETE_SIGN__ != true` 的条件，be 不感知这以过程，正常执行

### Cumulative Compaction

Cumulative Compaction 时将隐藏列看作正常的列处理，Compaction逻辑没有变化

### Base Compaction

Base Compaction 时要将标记为删除的行的删掉，以减少数据占用的空间

### 语法
导入的语法设计方面主要是增加一个指定删除标记列的字段的column 映射，并且需要在导入数据中增加这一列，各个导入方式设置的方法如下

#### stream load

stream load 的写法在在header 中的 columns  字段增加一个设置删除标记列的字段， 示例
` -H "columns: k1, k2, label_c3" -H "merge_type: [MERGE|APPEND|DELETE]" -H "delete: label_c3=1"`

#### broker load

在`PROPERTIES ` 处设置删除标记列的字段

```
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
    [DELETE ON label=true]

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

#### routine load

routine load 在`columns` 字段增加映射 映射方式同上，示例如下

```
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

## 启用批量删除支持
启用批量删除支持 有两种形式：
1. 通过在fe 配置文件中增加`enable_batch_delete_by_default=true` 重启fe 后新建表的都支持批量删除，此选项默认为false

2. 对于没有更改上述fe 配置或对于以存在的不支持批量删除功能的表，可以使用如下语句：
`ALTER TABLE tablename ENABLE FEATURE "BATCH_DELETE"` 来启用批量删除。本操作本质上是一个schema change 操作，操作立即返回，可以通过`show alter table column` 来确认操作是否完成。

如果确定一个表是否支持批量删除，可以通过 设置一个session variable 来显示隐藏列 `SET show_hidden_columns=true` ，之后使用`desc tablename`，如果输出中有`__DORIS_DELETE_SIGN__` 列则支持，如果没有则不支持

## 注意
1. 由于除stream load 外的导入操作在doris 内部有可能乱序执行，因此在使用`MERGE` 方式导入时如果不是stream load，需要与 load sequence 一起使用，具体的 语法可以参照sequence列 相关的文档 
2. `DELETE ON` 条件只能与 MERGE 一起使用

## 使用示例
下面以stream load 为例 展示下使用方式
1. 正常导入数据：
```
curl --location-trusted -u root: -H "column_separator:," -H "columns: siteid, citycode, username, pv" -H "merge_type: APPEND"  -T ~/table1_data http://127.0.0.1:8130/api/test/table1/_stream_load
```
其中的APPEND 条件可以省略，与下面的语句效果相同：
```
curl --location-trusted -u root: -H "column_separator:," -H "columns: siteid, citycode, username, pv" -T ~/table1_data http://127.0.0.1:8130/api/test/table1/_stream_load
```
2. 将与导入数据key 相同的数据全部删除
```
curl --location-trusted -u root: -H "column_separator:," -H "columns: siteid, citycode, username, pv" -H "merge_type: DELETE"  -T ~/table1_data http://127.0.0.1:8130/api/test/table1/_stream_load
```
假设导入表中原有数据为:
```
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      3 |        2 | tom      |    2 |
|      4 |        3 | bush     |    3 |
|      5 |        3 | helen    |    3 |
+--------+----------+----------+------+
```
导入数据为：
```
3,2,tom,0
``` 
导入后数据变成:
```
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      4 |        3 | bush     |    3 |
|      5 |        3 | helen    |    3 |
+--------+----------+----------+------+
```
3. 将导入数据中与`site_id=1` 的行的key列相同的行
```
curl --location-trusted -u root: -H "column_separator:," -H "columns: siteid, citycode, username, pv" -H "merge_type: MERGE" -H "delete: siteid=1"  -T ~/table1_data http://127.0.0.1:8130/api/test/table1/_stream_load
```
假设导入前数据为：
```
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      4 |        3 | bush     |    3 |
|      5 |        3 | helen    |    3 |
|      1 |        1 | jim      |    2 |
+--------+----------+----------+------+
```
 导入数据为：
```
2,1,grace,2
3,2,tom,2
1,1,jim,2
```
导入后为：
```
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      4 |        3 | bush     |    3 |
|      2 |        1 | grace    |    2 |
|      3 |        2 | tom      |    2 |
|      5 |        3 | helen    |    3 |
+--------+----------+----------+------+
```