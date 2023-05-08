---
{
    "title": "动态 Schema 表",
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

<version since="2.0.0"></version>

动态 Schema 表是一种特殊的表，其 Schema 随着导入自动进行扩展。目前该功能，主要用于半结构数据，例如 JSON 等的导入、自动列生成。因为 JSON 是类型自描述的，所以我们可以从原始文档中提取 Schema 信息，推断最终类型信息。这种特殊的表可以减少人工 Schema Change 的操作，并轻松导入半结构数据并自动扩展其 Schema。

## 名词解释
- Schema Change, 改变表的结构， 例如增加列、减少列， 修改列类型
- 静态列， 在建表时指定的列， 例如分区列、主键列
- 动态列， 随着导入自动识别并增加的列

## 建表

```sql
CREATE DATABASE test_dynamic_table;

-- 建表， 并指定静态列类型， 导入遇到对应列会自动转换成静态列的类型
-- 选择随机分桶方式
CREATE TABLE IF NOT EXISTS test_dynamic_table (
                qid bigint,
                `answers.date` array<datetime>,
                `title` string,
		        ...   -- ...标识该表是动态表， 是动态表的语法
        )
DUPLICATE KEY(`qid`)
DISTRIBUTED BY RANDOM BUCKETS 5 
properties("replication_num" = "1");

-- 可以看到三列Column在表中默认添加， 类型是指定类型
mysql> DESC test_dynamic_table;
+--------------+-----------------+------+-------+---------+-------+
| Field        | Type            | Null | Key   | Default | Extra |
+--------------+-----------------+------+-------+---------+-------+
| qid          | BIGINT          | Yes  | true  | NULL    |       |
| answers.date | ARRAY<DATETIME> | Yes  | false | NULL    | NONE  |
| user         | TEXT            | Yes  | false | NULL    | NONE  |
+--------------+-----------------+------+-------+---------+-------+
3 rows in set (0.00 sec)
```

## 导入数据

``` sql
-- example1.json
'{
    "title": "Display Progress Bar at the Time of Processing",
    "qid": "1000000",
    "answers": [
        {"date": "2009-06-16T09:55:57.320", "user": "Micha\u0142 Niklas (22595)"},
        {"date": "2009-06-17T12:34:22.643", "user": "Jack Njiri (77153)"}
    ],
    "tag": ["vb6", "progress-bar"],
    "user": "Jash",
    "creationdate": "2009-06-16T07:28:42.770"
}'

curl -X PUT -T example1.json --location-trusted -u root: -H "read_json_by_line:false" -H "format:json"   http://127.0.0.1:8147/api/regression_test_dynamic_table/test_dynamic_table/_stream_load

-- 新增 title，answers.user， tag， title， creationdate 五列
-- 且 qid，answers.date，user三列类型与建表时保持一致
-- 新增数组类型默认Default值为空数组[]
mysql> DESC test_dynamic_table;                                                                                 
+--------------+-----------------+------+-------+---------+-------+
| Field        | Type            | Null | Key   | Default | Extra |
+--------------+-----------------+------+-------+---------+-------+
| qid          | BIGINT          | Yes  | true  | NULL    |       |
| answers.date | ARRAY<DATETIME> | Yes  | false | NULL    | NONE  |
| title        | TEXT            | Yes  | false | NULL    | NONE  |
| answers.user | ARRAY<TEXT>     | No   | false | []      | NONE  |
| tag          | ARRAY<TEXT>     | No   | false | []      | NONE  |
| user         | TEXT            | Yes  | false | NULL    | NONE  |
| creationdate | TEXT            | Yes  | false | NULL    | NONE  |
| date         | TEXT            | Yes  | false | NULL    | NONE  |
+--------------+-----------------+------+-------+---------+-------+

-- 批量导入数据

-- 指定 -H "read_json_by_line:true"， 逐行解析JSON
curl -X PUT -T example_batch.json --location-trusted -u root: -H "read_json_by_line:true" -H "format:json"   http://127.0.0.1:8147/api/regression_test_dynamic_table/test_dynamic_table/_stream_load

-- 指定 -H "strip_outer_array:true"， 整个文件当做一个JSON array解析， array中的每个元素是一行， 解析效率更高效
curl -X PUT -T example_batch_array.json --location-trusted -u root: -H "strip_outer_array:true" -H "format:json"   http://127.0.0.1:8147/api/regression_test_dynamic_table/test_dynamic_table/_stream_load
```
对于 Dynamic Table， 你也可以使用 S3 Load 或者 Routine Load, 使用方式类似

## 对动态列增加索引
```sql
-- 将在 titile 列上新建倒排索引， 并按照english分词
CREATE INDEX title_idx ON test_dynamic_table (`title`) using inverted PROPERTIES("parser"="english")
```

## 类型冲突
在第一批导入会自动推断出统一的类型， 并以此作为最终的 Column 类型，所以建议保持 Column 类型的一致, 例如
```
{"id" : 123}
{"id" : "123"}
-- 类型会被最终推断为Text类型， 如果在后续导入{"id" : 123}则类型会被自动转成String类型

对于无法统一的类型， 例如
{"id" : [123]}
{"id" : 123}
-- 导入将会报错
```
