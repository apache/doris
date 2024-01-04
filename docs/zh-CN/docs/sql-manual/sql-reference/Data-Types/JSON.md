---
{
    "title": "JSON",
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

## JSON

<version since="1.2.0">

</version>

注意：在1.2.x版本中，JSON类型的名字是JSONB，为了尽量跟MySQL兼容，从2.0.0版本开始改名为JSON，老的表仍然可以使用。

### description
    JSON类型
        二进制JSON类型，采用二进制JSON格式存储，通过json函数访问JSON内部字段。默认支持1048576 字节（1M），可调大到 2147483643 字节（2G），可通过be配置`jsonb_type_length_soft_limit_bytes`调整

### note
    与普通STRING类型存储的JSON字符串相比，JSON类型有两点优势
    1. 数据写入时进行JSON格式校验
    2. 二进制存储格式更加高效，通过json_extract等函数可以高效访问JSON内部字段，比get_json_xx函数快几倍

### example
    用一个从建表、导数据、查询全周期的例子说明JSON数据类型的功能和用法。

#### 创建库表

```
CREATE DATABASE testdb;

USE testdb;

CREATE TABLE test_json (
  id INT,
  j JSON
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES("replication_num" = "1");
```

#### 导入数据

##### stream load 导入test_json.csv测试数据

- 测试数据有2列，第一列id，第二列是json
- 测试数据有25行，其中前18行的json是合法的，后7行的json是非法的

```
1	\N
2	null
3	true
4	false
5	100
6	10000
7	1000000000
8	1152921504606846976
9	6.18
10	"abcd"
11	{}
12	{"k1":"v31", "k2": 300}
13	[]
14	[123, 456]
15	["abc", "def"]
16	[null, true, false, 100, 6.18, "abc"]
17	[{"k1":"v41", "k2": 400}, 1, "a", 3.14]
18	{"k1":"v31", "k2": 300, "a1": [{"k1":"v41", "k2": 400}, 1, "a", 3.14]}
19	''
20	'abc'
21	abc
22	100x
23	6.a8
24	{x
25	[123, abc]
```

- 由于有28%的非法数据，默认会失败报错 "too many filtered rows"
```
curl --location-trusted -u root: -T test_json.csv http://127.0.0.1:8840/api/testdb/test_json/_stream_load
{
    "TxnId": 12019,
    "Label": "744d9821-9c9f-43dc-bf3b-7ab048f14e32",
    "TwoPhaseCommit": "false",
    "Status": "Fail",
    "Message": "too many filtered rows",
    "NumberTotalRows": 25,
    "NumberLoadedRows": 18,
    "NumberFilteredRows": 7,
    "NumberUnselectedRows": 0,
    "LoadBytes": 380,
    "LoadTimeMs": 48,
    "BeginTxnTimeMs": 0,
    "StreamLoadPutTimeMs": 1,
    "ReadDataTimeMs": 0,
    "WriteDataTimeMs": 45,
    "CommitAndPublishTimeMs": 0,
    "ErrorURL": "http://172.21.0.5:8840/api/_load_error_log?file=__shard_2/error_log_insert_stmt_95435c4bf5f156df-426735082a9296af_95435c4bf5f156df_426735082a9296af"
}
```

- 设置容错率参数 'max_filter_ratio: 0.3'
```
curl --location-trusted -u root: -H 'max_filter_ratio: 0.3' -T test_json.csv http://127.0.0.1:8840/api/testdb/test_json/_stream_load
{
    "TxnId": 12017,
    "Label": "f37a50c1-43e9-4f4e-a159-a3db6abe2579",
    "TwoPhaseCommit": "false",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 25,
    "NumberLoadedRows": 18,
    "NumberFilteredRows": 7,
    "NumberUnselectedRows": 0,
    "LoadBytes": 380,
    "LoadTimeMs": 68,
    "BeginTxnTimeMs": 0,
    "StreamLoadPutTimeMs": 2,
    "ReadDataTimeMs": 0,
    "WriteDataTimeMs": 45,
    "CommitAndPublishTimeMs": 19,
    "ErrorURL": "http://172.21.0.5:8840/api/_load_error_log?file=__shard_0/error_log_insert_stmt_a1463f98a7b15caf-c79399b920f5bfa3_a1463f98a7b15caf_c79399b920f5bfa3"
}
```

- 查看stream load导入的数据，JSON类型的列j会自动转成JSON string展示

```
mysql> SELECT * FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+
| id   | j                                                             |
+------+---------------------------------------------------------------+
|    1 |                                                          NULL |
|    2 |                                                          null |
|    3 |                                                          true |
|    4 |                                                         false |
|    5 |                                                           100 |
|    6 |                                                         10000 |
|    7 |                                                    1000000000 |
|    8 |                                           1152921504606846976 |
|    9 |                                                          6.18 |
|   10 |                                                        "abcd" |
|   11 |                                                            {} |
|   12 |                                         {"k1":"v31","k2":300} |
|   13 |                                                            [] |
|   14 |                                                     [123,456] |
|   15 |                                                 ["abc","def"] |
|   16 |                              [null,true,false,100,6.18,"abc"] |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |
+------+---------------------------------------------------------------+
18 rows in set (0.03 sec)

```

##### insert into 插入数据

- insert 1条数据，总数据从18条增加到19条
```
mysql> INSERT INTO test_json VALUES(26, '{"k1":"v1", "k2": 200}');
Query OK, 1 row affected (0.09 sec)
{'label':'insert_4ece6769d1b42fd_ac9f25b3b8f3dc02', 'status':'VISIBLE', 'txnId':'12016'}

mysql> SELECT * FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+
| id   | j                                                             |
+------+---------------------------------------------------------------+
|    1 |                                                          NULL |
|    2 |                                                          null |
|    3 |                                                          true |
|    4 |                                                         false |
|    5 |                                                           100 |
|    6 |                                                         10000 |
|    7 |                                                    1000000000 |
|    8 |                                           1152921504606846976 |
|    9 |                                                          6.18 |
|   10 |                                                        "abcd" |
|   11 |                                                            {} |
|   12 |                                         {"k1":"v31","k2":300} |
|   13 |                                                            [] |
|   14 |                                                     [123,456] |
|   15 |                                                 ["abc","def"] |
|   16 |                              [null,true,false,100,6.18,"abc"] |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |
|   26 |                                          {"k1":"v1","k2":200} |
+------+---------------------------------------------------------------+
19 rows in set (0.03 sec)

```

#### 查询

##### 用json_extract取json内的某个字段

1. 获取整个json，$ 在json path中代表root，即整个json
```
+------+---------------------------------------------------------------+---------------------------------------------------------------+
| id   | j                                                             | json_extract(`j`, '$')                                       |
+------+---------------------------------------------------------------+---------------------------------------------------------------+
|    1 |                                                          NULL |                                                          NULL |
|    2 |                                                          null |                                                          null |
|    3 |                                                          true |                                                          true |
|    4 |                                                         false |                                                         false |
|    5 |                                                           100 |                                                           100 |
|    6 |                                                         10000 |                                                         10000 |
|    7 |                                                    1000000000 |                                                    1000000000 |
|    8 |                                           1152921504606846976 |                                           1152921504606846976 |
|    9 |                                                          6.18 |                                                          6.18 |
|   10 |                                                        "abcd" |                                                        "abcd" |
|   11 |                                                            {} |                                                            {} |
|   12 |                                         {"k1":"v31","k2":300} |                                         {"k1":"v31","k2":300} |
|   13 |                                                            [] |                                                            [] |
|   14 |                                                     [123,456] |                                                     [123,456] |
|   15 |                                                 ["abc","def"] |                                                 ["abc","def"] |
|   16 |                              [null,true,false,100,6.18,"abc"] |                              [null,true,false,100,6.18,"abc"] |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                            [{"k1":"v41","k2":400},1,"a",3.14] |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |
|   26 |                                          {"k1":"v1","k2":200} |                                          {"k1":"v1","k2":200} |
+------+---------------------------------------------------------------+---------------------------------------------------------------+
19 rows in set (0.03 sec)
```

1. 获取k1字段，没有k1字段的行返回NULL
```
mysql> SELECT id, j, json_extract(j, '$.k1') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+----------------------------+
| id   | j                                                             | json_extract(`j`, '$.k1') |
+------+---------------------------------------------------------------+----------------------------+
|    1 |                                                          NULL |                       NULL |
|    2 |                                                          null |                       NULL |
|    3 |                                                          true |                       NULL |
|    4 |                                                         false |                       NULL |
|    5 |                                                           100 |                       NULL |
|    6 |                                                         10000 |                       NULL |
|    7 |                                                    1000000000 |                       NULL |
|    8 |                                           1152921504606846976 |                       NULL |
|    9 |                                                          6.18 |                       NULL |
|   10 |                                                        "abcd" |                       NULL |
|   11 |                                                            {} |                       NULL |
|   12 |                                         {"k1":"v31","k2":300} |                      "v31" |
|   13 |                                                            [] |                       NULL |
|   14 |                                                     [123,456] |                       NULL |
|   15 |                                                 ["abc","def"] |                       NULL |
|   16 |                              [null,true,false,100,6.18,"abc"] |                       NULL |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                       NULL |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |                      "v31" |
|   26 |                                          {"k1":"v1","k2":200} |                       "v1" |
+------+---------------------------------------------------------------+----------------------------+
19 rows in set (0.03 sec)
```

1. 获取顶层数组的第0个元素
```
mysql> SELECT id, j, json_extract(j, '$[0]') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+----------------------------+
| id   | j                                                             | json_extract(`j`, '$[0]') |
+------+---------------------------------------------------------------+----------------------------+
|    1 |                                                          NULL |                       NULL |
|    2 |                                                          null |                       NULL |
|    3 |                                                          true |                       NULL |
|    4 |                                                         false |                       NULL |
|    5 |                                                           100 |                       NULL |
|    6 |                                                         10000 |                       NULL |
|    7 |                                                    1000000000 |                       NULL |
|    8 |                                           1152921504606846976 |                       NULL |
|    9 |                                                          6.18 |                       NULL |
|   10 |                                                        "abcd" |                       NULL |
|   11 |                                                            {} |                       NULL |
|   12 |                                         {"k1":"v31","k2":300} |                       NULL |
|   13 |                                                            [] |                       NULL |
|   14 |                                                     [123,456] |                        123 |
|   15 |                                                 ["abc","def"] |                      "abc" |
|   16 |                              [null,true,false,100,6.18,"abc"] |                       null |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |      {"k1":"v41","k2":400} |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |                       NULL |
|   26 |                                          {"k1":"v1","k2":200} |                       NULL |
+------+---------------------------------------------------------------+----------------------------+
19 rows in set (0.03 sec)
```

1. 获取整个json array
```
mysql> SELECT id, j, json_extract(j, '$.a1') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+------------------------------------+
| id   | j                                                             | json_extract(`j`, '$.a1')         |
+------+---------------------------------------------------------------+------------------------------------+
|    1 |                                                          NULL |                               NULL |
|    2 |                                                          null |                               NULL |
|    3 |                                                          true |                               NULL |
|    4 |                                                         false |                               NULL |
|    5 |                                                           100 |                               NULL |
|    6 |                                                         10000 |                               NULL |
|    7 |                                                    1000000000 |                               NULL |
|    8 |                                           1152921504606846976 |                               NULL |
|    9 |                                                          6.18 |                               NULL |
|   10 |                                                        "abcd" |                               NULL |
|   11 |                                                            {} |                               NULL |
|   12 |                                         {"k1":"v31","k2":300} |                               NULL |
|   13 |                                                            [] |                               NULL |
|   14 |                                                     [123,456] |                               NULL |
|   15 |                                                 ["abc","def"] |                               NULL |
|   16 |                              [null,true,false,100,6.18,"abc"] |                               NULL |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                               NULL |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} | [{"k1":"v41","k2":400},1,"a",3.14] |
|   26 |                                          {"k1":"v1","k2":200} |                               NULL |
+------+---------------------------------------------------------------+------------------------------------+
19 rows in set (0.02 sec)
```

1. 获取json array中嵌套object的字段
```
mysql> SELECT id, j, json_extract(j, '$.a1[0]'), json_extract(j, '$.a1[0].k1') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+-------------------------------+----------------------------------+
| id   | j                                                             | json_extract(`j`, '$.a1[0]') | json_extract(`j`, '$.a1[0].k1') |
+------+---------------------------------------------------------------+-------------------------------+----------------------------------+
|    1 |                                                          NULL |                          NULL |                             NULL |
|    2 |                                                          null |                          NULL |                             NULL |
|    3 |                                                          true |                          NULL |                             NULL |
|    4 |                                                         false |                          NULL |                             NULL |
|    5 |                                                           100 |                          NULL |                             NULL |
|    6 |                                                         10000 |                          NULL |                             NULL |
|    7 |                                                    1000000000 |                          NULL |                             NULL |
|    8 |                                           1152921504606846976 |                          NULL |                             NULL |
|    9 |                                                          6.18 |                          NULL |                             NULL |
|   10 |                                                        "abcd" |                          NULL |                             NULL |
|   11 |                                                            {} |                          NULL |                             NULL |
|   12 |                                         {"k1":"v31","k2":300} |                          NULL |                             NULL |
|   13 |                                                            [] |                          NULL |                             NULL |
|   14 |                                                     [123,456] |                          NULL |                             NULL |
|   15 |                                                 ["abc","def"] |                          NULL |                             NULL |
|   16 |                              [null,true,false,100,6.18,"abc"] |                          NULL |                             NULL |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                          NULL |                             NULL |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |         {"k1":"v41","k2":400} |                            "v41" |
|   26 |                                          {"k1":"v1","k2":200} |                          NULL |                             NULL |
+------+---------------------------------------------------------------+-------------------------------+----------------------------------+
19 rows in set (0.02 sec)

```

1. 获取具体类型的
- json_extract_string 获取string类型字段，非string类型转成string
```
mysql> SELECT id, j, json_extract_string(j, '$') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+---------------------------------------------------------------+
| id   | j                                                             | json_extract_string(`j`, '$')                                |
+------+---------------------------------------------------------------+---------------------------------------------------------------+
|    1 | NULL                                                          | NULL                                                          |
|    2 | null                                                          | null                                                          |
|    3 | true                                                          | true                                                          |
|    4 | false                                                         | false                                                         |
|    5 | 100                                                           | 100                                                           |
|    6 | 10000                                                         | 10000                                                         |
|    7 | 1000000000                                                    | 1000000000                                                    |
|    8 | 1152921504606846976                                           | 1152921504606846976                                           |
|    9 | 6.18                                                          | 6.18                                                          |
|   10 | "abcd"                                                        | abcd                                                          |
|   11 | {}                                                            | {}                                                            |
|   12 | {"k1":"v31","k2":300}                                         | {"k1":"v31","k2":300}                                         |
|   13 | []                                                            | []                                                            |
|   14 | [123,456]                                                     | [123,456]                                                     |
|   15 | ["abc","def"]                                                 | ["abc","def"]                                                 |
|   16 | [null,true,false,100,6.18,"abc"]                              | [null,true,false,100,6.18,"abc"]                              |
|   17 | [{"k1":"v41","k2":400},1,"a",3.14]                            | [{"k1":"v41","k2":400},1,"a",3.14]                            |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |
|   26 | {"k1":"v1","k2":200}                                          | {"k1":"v1","k2":200}                                          |
+------+---------------------------------------------------------------+---------------------------------------------------------------+
19 rows in set (0.02 sec)

mysql> SELECT id, j, json_extract_string(j, '$.k1') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+-----------------------------------+
| id   | j                                                             | json_extract_string(`j`, '$.k1') |
+------+---------------------------------------------------------------+-----------------------------------+
|    1 |                                                          NULL | NULL                              |
|    2 |                                                          null | NULL                              |
|    3 |                                                          true | NULL                              |
|    4 |                                                         false | NULL                              |
|    5 |                                                           100 | NULL                              |
|    6 |                                                         10000 | NULL                              |
|    7 |                                                    1000000000 | NULL                              |
|    8 |                                           1152921504606846976 | NULL                              |
|    9 |                                                          6.18 | NULL                              |
|   10 |                                                        "abcd" | NULL                              |
|   11 |                                                            {} | NULL                              |
|   12 |                                         {"k1":"v31","k2":300} | v31                               |
|   13 |                                                            [] | NULL                              |
|   14 |                                                     [123,456] | NULL                              |
|   15 |                                                 ["abc","def"] | NULL                              |
|   16 |                              [null,true,false,100,6.18,"abc"] | NULL                              |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] | NULL                              |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} | v31                               |
|   26 |                                          {"k1":"v1","k2":200} | v1                                |
+------+---------------------------------------------------------------+-----------------------------------+
19 rows in set (0.03 sec)

```

- json_extract_int 获取int类型字段，非int类型返回NULL
```
mysql> SELECT id, j, json_extract_int(j, '$') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+-----------------------------+
| id   | j                                                             | json_extract_int(`j`, '$') |
+------+---------------------------------------------------------------+-----------------------------+
|    1 |                                                          NULL |                        NULL |
|    2 |                                                          null |                        NULL |
|    3 |                                                          true |                        NULL |
|    4 |                                                         false |                        NULL |
|    5 |                                                           100 |                         100 |
|    6 |                                                         10000 |                       10000 |
|    7 |                                                    1000000000 |                  1000000000 |
|    8 |                                           1152921504606846976 |                        NULL |
|    9 |                                                          6.18 |                        NULL |
|   10 |                                                        "abcd" |                        NULL |
|   11 |                                                            {} |                        NULL |
|   12 |                                         {"k1":"v31","k2":300} |                        NULL |
|   13 |                                                            [] |                        NULL |
|   14 |                                                     [123,456] |                        NULL |
|   15 |                                                 ["abc","def"] |                        NULL |
|   16 |                              [null,true,false,100,6.18,"abc"] |                        NULL |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                        NULL |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |                        NULL |
|   26 |                                          {"k1":"v1","k2":200} |                        NULL |
+------+---------------------------------------------------------------+-----------------------------+
19 rows in set (0.02 sec)

mysql> SELECT id, j, json_extract_int(j, '$.k2') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+--------------------------------+
| id   | j                                                             | json_extract_int(`j`, '$.k2') |
+------+---------------------------------------------------------------+--------------------------------+
|    1 |                                                          NULL |                           NULL |
|    2 |                                                          null |                           NULL |
|    3 |                                                          true |                           NULL |
|    4 |                                                         false |                           NULL |
|    5 |                                                           100 |                           NULL |
|    6 |                                                         10000 |                           NULL |
|    7 |                                                    1000000000 |                           NULL |
|    8 |                                           1152921504606846976 |                           NULL |
|    9 |                                                          6.18 |                           NULL |
|   10 |                                                        "abcd" |                           NULL |
|   11 |                                                            {} |                           NULL |
|   12 |                                         {"k1":"v31","k2":300} |                            300 |
|   13 |                                                            [] |                           NULL |
|   14 |                                                     [123,456] |                           NULL |
|   15 |                                                 ["abc","def"] |                           NULL |
|   16 |                              [null,true,false,100,6.18,"abc"] |                           NULL |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                           NULL |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |                            300 |
|   26 |                                          {"k1":"v1","k2":200} |                            200 |
+------+---------------------------------------------------------------+--------------------------------+
19 rows in set (0.03 sec)
```

- json_extract_bigint 获取bigint类型字段，非bigint类型返回NULL
```
mysql> SELECT id, j, json_extract_bigint(j, '$') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+--------------------------------+
| id   | j                                                             | json_extract_bigint(`j`, '$') |
+------+---------------------------------------------------------------+--------------------------------+
|    1 |                                                          NULL |                           NULL |
|    2 |                                                          null |                           NULL |
|    3 |                                                          true |                           NULL |
|    4 |                                                         false |                           NULL |
|    5 |                                                           100 |                            100 |
|    6 |                                                         10000 |                          10000 |
|    7 |                                                    1000000000 |                     1000000000 |
|    8 |                                           1152921504606846976 |            1152921504606846976 |
|    9 |                                                          6.18 |                           NULL |
|   10 |                                                        "abcd" |                           NULL |
|   11 |                                                            {} |                           NULL |
|   12 |                                         {"k1":"v31","k2":300} |                           NULL |
|   13 |                                                            [] |                           NULL |
|   14 |                                                     [123,456] |                           NULL |
|   15 |                                                 ["abc","def"] |                           NULL |
|   16 |                              [null,true,false,100,6.18,"abc"] |                           NULL |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                           NULL |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |                           NULL |
|   26 |                                          {"k1":"v1","k2":200} |                           NULL |
+------+---------------------------------------------------------------+--------------------------------+
19 rows in set (0.03 sec)

mysql> SELECT id, j, json_extract_bigint(j, '$.k2') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+-----------------------------------+
| id   | j                                                             | json_extract_bigint(`j`, '$.k2') |
+------+---------------------------------------------------------------+-----------------------------------+
|    1 |                                                          NULL |                              NULL |
|    2 |                                                          null |                              NULL |
|    3 |                                                          true |                              NULL |
|    4 |                                                         false |                              NULL |
|    5 |                                                           100 |                              NULL |
|    6 |                                                         10000 |                              NULL |
|    7 |                                                    1000000000 |                              NULL |
|    8 |                                           1152921504606846976 |                              NULL |
|    9 |                                                          6.18 |                              NULL |
|   10 |                                                        "abcd" |                              NULL |
|   11 |                                                            {} |                              NULL |
|   12 |                                         {"k1":"v31","k2":300} |                               300 |
|   13 |                                                            [] |                              NULL |
|   14 |                                                     [123,456] |                              NULL |
|   15 |                                                 ["abc","def"] |                              NULL |
|   16 |                              [null,true,false,100,6.18,"abc"] |                              NULL |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                              NULL |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |                               300 |
|   26 |                                          {"k1":"v1","k2":200} |                               200 |
+------+---------------------------------------------------------------+-----------------------------------+
19 rows in set (0.02 sec)

```

- json_extract_double 获取double类型字段，非double类型返回NULL
```
mysql> SELECT id, j, json_extract_double(j, '$') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+--------------------------------+
| id   | j                                                             | json_extract_double(`j`, '$') |
+------+---------------------------------------------------------------+--------------------------------+
|    1 |                                                          NULL |                           NULL |
|    2 |                                                          null |                           NULL |
|    3 |                                                          true |                           NULL |
|    4 |                                                         false |                           NULL |
|    5 |                                                           100 |                            100 |
|    6 |                                                         10000 |                          10000 |
|    7 |                                                    1000000000 |                     1000000000 |
|    8 |                                           1152921504606846976 |          1.152921504606847e+18 |
|    9 |                                                          6.18 |                           6.18 |
|   10 |                                                        "abcd" |                           NULL |
|   11 |                                                            {} |                           NULL |
|   12 |                                         {"k1":"v31","k2":300} |                           NULL |
|   13 |                                                            [] |                           NULL |
|   14 |                                                     [123,456] |                           NULL |
|   15 |                                                 ["abc","def"] |                           NULL |
|   16 |                              [null,true,false,100,6.18,"abc"] |                           NULL |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                           NULL |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |                           NULL |
|   26 |                                          {"k1":"v1","k2":200} |                           NULL |
+------+---------------------------------------------------------------+--------------------------------+
19 rows in set (0.02 sec)

mysql> SELECT id, j, json_extract_double(j, '$.k2') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+-----------------------------------+
| id   | j                                                             | json_extract_double(`j`, '$.k2') |
+------+---------------------------------------------------------------+-----------------------------------+
|    1 |                                                          NULL |                              NULL |
|    2 |                                                          null |                              NULL |
|    3 |                                                          true |                              NULL |
|    4 |                                                         false |                              NULL |
|    5 |                                                           100 |                              NULL |
|    6 |                                                         10000 |                              NULL |
|    7 |                                                    1000000000 |                              NULL |
|    8 |                                           1152921504606846976 |                              NULL |
|    9 |                                                          6.18 |                              NULL |
|   10 |                                                        "abcd" |                              NULL |
|   11 |                                                            {} |                              NULL |
|   12 |                                         {"k1":"v31","k2":300} |                               300 |
|   13 |                                                            [] |                              NULL |
|   14 |                                                     [123,456] |                              NULL |
|   15 |                                                 ["abc","def"] |                              NULL |
|   16 |                              [null,true,false,100,6.18,"abc"] |                              NULL |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                              NULL |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |                               300 |
|   26 |                                          {"k1":"v1","k2":200} |                               200 |
+------+---------------------------------------------------------------+-----------------------------------+
19 rows in set (0.03 sec)
```

- json_extract_bool 获取bool类型字段，非bool类型返回NULL
```
mysql> SELECT id, j, json_extract_bool(j, '$') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+------------------------------+
| id   | j                                                             | json_extract_bool(`j`, '$') |
+------+---------------------------------------------------------------+------------------------------+
|    1 |                                                          NULL |                         NULL |
|    2 |                                                          null |                         NULL |
|    3 |                                                          true |                            1 |
|    4 |                                                         false |                            0 |
|    5 |                                                           100 |                         NULL |
|    6 |                                                         10000 |                         NULL |
|    7 |                                                    1000000000 |                         NULL |
|    8 |                                           1152921504606846976 |                         NULL |
|    9 |                                                          6.18 |                         NULL |
|   10 |                                                        "abcd" |                         NULL |
|   11 |                                                            {} |                         NULL |
|   12 |                                         {"k1":"v31","k2":300} |                         NULL |
|   13 |                                                            [] |                         NULL |
|   14 |                                                     [123,456] |                         NULL |
|   15 |                                                 ["abc","def"] |                         NULL |
|   16 |                              [null,true,false,100,6.18,"abc"] |                         NULL |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                         NULL |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |                         NULL |
|   26 |                                          {"k1":"v1","k2":200} |                         NULL |
+------+---------------------------------------------------------------+------------------------------+
19 rows in set (0.01 sec)

mysql> SELECT id, j, json_extract_bool(j, '$[1]') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+---------------------------------+
| id   | j                                                             | json_extract_bool(`j`, '$[1]') |
+------+---------------------------------------------------------------+---------------------------------+
|    1 |                                                          NULL |                            NULL |
|    2 |                                                          null |                            NULL |
|    3 |                                                          true |                            NULL |
|    4 |                                                         false |                            NULL |
|    5 |                                                           100 |                            NULL |
|    6 |                                                         10000 |                            NULL |
|    7 |                                                    1000000000 |                            NULL |
|    8 |                                           1152921504606846976 |                            NULL |
|    9 |                                                          6.18 |                            NULL |
|   10 |                                                        "abcd" |                            NULL |
|   11 |                                                            {} |                            NULL |
|   12 |                                         {"k1":"v31","k2":300} |                            NULL |
|   13 |                                                            [] |                            NULL |
|   14 |                                                     [123,456] |                            NULL |
|   15 |                                                 ["abc","def"] |                            NULL |
|   16 |                              [null,true,false,100,6.18,"abc"] |                               1 |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                            NULL |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |                            NULL |
|   26 |                                          {"k1":"v1","k2":200} |                            NULL |
+------+---------------------------------------------------------------+---------------------------------+
19 rows in set (0.01 sec)
```

- json_extract_isnull 获取json null类型字段，null返回1，非null返回0
- 需要注意的是json null和SQL NULL不一样，SQL NULL表示某个字段的值不存在，而json null表示值存在但是是一个特殊值null
```
mysql> SELECT id, j, json_extract_isnull(j, '$') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+--------------------------------+
| id   | j                                                             | json_extract_isnull(`j`, '$') |
+------+---------------------------------------------------------------+--------------------------------+
|    1 |                                                          NULL |                           NULL |
|    2 |                                                          null |                              1 |
|    3 |                                                          true |                              0 |
|    4 |                                                         false |                              0 |
|    5 |                                                           100 |                              0 |
|    6 |                                                         10000 |                              0 |
|    7 |                                                    1000000000 |                              0 |
|    8 |                                           1152921504606846976 |                              0 |
|    9 |                                                          6.18 |                              0 |
|   10 |                                                        "abcd" |                              0 |
|   11 |                                                            {} |                              0 |
|   12 |                                         {"k1":"v31","k2":300} |                              0 |
|   13 |                                                            [] |                              0 |
|   14 |                                                     [123,456] |                              0 |
|   15 |                                                 ["abc","def"] |                              0 |
|   16 |                              [null,true,false,100,6.18,"abc"] |                              0 |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                              0 |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |                              0 |
|   26 |                                          {"k1":"v1","k2":200} |                              0 |
+------+---------------------------------------------------------------+--------------------------------+
19 rows in set (0.03 sec)

```

##### 用json_exists_path检查json内的某个字段是否存在

```
mysql> SELECT id, j, json_exists_path(j, '$') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+-----------------------------+
| id   | j                                                             | json_exists_path(`j`, '$') |
+------+---------------------------------------------------------------+-----------------------------+
|    1 |                                                          NULL |                        NULL |
|    2 |                                                          null |                           1 |
|    3 |                                                          true |                           1 |
|    4 |                                                         false |                           1 |
|    5 |                                                           100 |                           1 |
|    6 |                                                         10000 |                           1 |
|    7 |                                                    1000000000 |                           1 |
|    8 |                                           1152921504606846976 |                           1 |
|    9 |                                                          6.18 |                           1 |
|   10 |                                                        "abcd" |                           1 |
|   11 |                                                            {} |                           1 |
|   12 |                                         {"k1":"v31","k2":300} |                           1 |
|   13 |                                                            [] |                           1 |
|   14 |                                                     [123,456] |                           1 |
|   15 |                                                 ["abc","def"] |                           1 |
|   16 |                              [null,true,false,100,6.18,"abc"] |                           1 |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                           1 |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |                           1 |
|   26 |                                          {"k1":"v1","k2":200} |                           1 |
+------+---------------------------------------------------------------+-----------------------------+
19 rows in set (0.02 sec)

mysql> SELECT id, j, json_exists_path(j, '$.k1') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+--------------------------------+
| id   | j                                                             | json_exists_path(`j`, '$.k1') |
+------+---------------------------------------------------------------+--------------------------------+
|    1 |                                                          NULL |                           NULL |
|    2 |                                                          null |                              0 |
|    3 |                                                          true |                              0 |
|    4 |                                                         false |                              0 |
|    5 |                                                           100 |                              0 |
|    6 |                                                         10000 |                              0 |
|    7 |                                                    1000000000 |                              0 |
|    8 |                                           1152921504606846976 |                              0 |
|    9 |                                                          6.18 |                              0 |
|   10 |                                                        "abcd" |                              0 |
|   11 |                                                            {} |                              0 |
|   12 |                                         {"k1":"v31","k2":300} |                              1 |
|   13 |                                                            [] |                              0 |
|   14 |                                                     [123,456] |                              0 |
|   15 |                                                 ["abc","def"] |                              0 |
|   16 |                              [null,true,false,100,6.18,"abc"] |                              0 |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                              0 |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |                              1 |
|   26 |                                          {"k1":"v1","k2":200} |                              1 |
+------+---------------------------------------------------------------+--------------------------------+
19 rows in set (0.03 sec)

mysql> SELECT id, j, json_exists_path(j, '$[2]') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+--------------------------------+
| id   | j                                                             | json_exists_path(`j`, '$[2]') |
+------+---------------------------------------------------------------+--------------------------------+
|    1 |                                                          NULL |                           NULL |
|    2 |                                                          null |                              0 |
|    3 |                                                          true |                              0 |
|    4 |                                                         false |                              0 |
|    5 |                                                           100 |                              0 |
|    6 |                                                         10000 |                              0 |
|    7 |                                                    1000000000 |                              0 |
|    8 |                                           1152921504606846976 |                              0 |
|    9 |                                                          6.18 |                              0 |
|   10 |                                                        "abcd" |                              0 |
|   11 |                                                            {} |                              0 |
|   12 |                                         {"k1":"v31","k2":300} |                              0 |
|   13 |                                                            [] |                              0 |
|   14 |                                                     [123,456] |                              0 |
|   15 |                                                 ["abc","def"] |                              0 |
|   16 |                              [null,true,false,100,6.18,"abc"] |                              1 |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] |                              1 |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} |                              0 |
|   26 |                                          {"k1":"v1","k2":200} |                              0 |
+------+---------------------------------------------------------------+--------------------------------+
19 rows in set (0.02 sec)


```

##### 用json_type获取json内的某个字段的类型

- 返回json path对应的json字段类型，如果不存在返回NULL
```
mysql> SELECT id, j, json_type(j, '$') FROM test_json ORDER BY id;
+------+---------------------------------------------------------------+----------------------+
| id   | j                                                             | json_type(`j`, '$') |
+------+---------------------------------------------------------------+----------------------+
|    1 |                                                          NULL | NULL                 |
|    2 |                                                          null | null                 |
|    3 |                                                          true | bool                 |
|    4 |                                                         false | bool                 |
|    5 |                                                           100 | int                  |
|    6 |                                                         10000 | int                  |
|    7 |                                                    1000000000 | int                  |
|    8 |                                           1152921504606846976 | bigint               |
|    9 |                                                          6.18 | double               |
|   10 |                                                        "abcd" | string               |
|   11 |                                                            {} | object               |
|   12 |                                         {"k1":"v31","k2":300} | object               |
|   13 |                                                            [] | array                |
|   14 |                                                     [123,456] | array                |
|   15 |                                                 ["abc","def"] | array                |
|   16 |                              [null,true,false,100,6.18,"abc"] | array                |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] | array                |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} | object               |
|   26 |                                          {"k1":"v1","k2":200} | object               |
+------+---------------------------------------------------------------+----------------------+
19 rows in set (0.02 sec)

mysql> select id, j, json_type(j, '$.k1') from test_json order by id;
+------+---------------------------------------------------------------+-------------------------+
| id   | j                                                             | json_type(`j`, '$.k1') |
+------+---------------------------------------------------------------+-------------------------+
|    1 |                                                          NULL | NULL                    |
|    2 |                                                          null | NULL                    |
|    3 |                                                          true | NULL                    |
|    4 |                                                         false | NULL                    |
|    5 |                                                           100 | NULL                    |
|    6 |                                                         10000 | NULL                    |
|    7 |                                                    1000000000 | NULL                    |
|    8 |                                           1152921504606846976 | NULL                    |
|    9 |                                                          6.18 | NULL                    |
|   10 |                                                        "abcd" | NULL                    |
|   11 |                                                            {} | NULL                    |
|   12 |                                         {"k1":"v31","k2":300} | string                  |
|   13 |                                                            [] | NULL                    |
|   14 |                                                     [123,456] | NULL                    |
|   15 |                                                 ["abc","def"] | NULL                    |
|   16 |                              [null,true,false,100,6.18,"abc"] | NULL                    |
|   17 |                            [{"k1":"v41","k2":400},1,"a",3.14] | NULL                    |
|   18 | {"k1":"v31","k2":300,"a1":[{"k1":"v41","k2":400},1,"a",3.14]} | string                  |
|   26 |                                          {"k1":"v1","k2":200} | string                  |
+------+---------------------------------------------------------------+-------------------------+
19 rows in set (0.03 sec)

```

### keywords
JSON, json_parse, json_parse_error_to_null, json_parse_error_to_value, json_extract, json_extract_isnull, json_extract_bool, json_extract_int, json_extract_bigint, json_extract_double, json_extract_string, json_exists_path, json_type
