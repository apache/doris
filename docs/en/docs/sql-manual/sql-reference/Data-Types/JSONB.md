---
{
    "title": "JSONB",
    "language": "en"
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

## DATEV2
### description
    JSONB (JSON Binary) datatype.
        Use binary JSON format for storage and jsonb function to extract field.

### note
    There are some advantanges for JSONB over plain JSON STRING.
    1. JSON syntax will be validated on write to ensure data quality
    2. JSONB format is more efficient. Using jsonb_extract functions on JSONB format is 2-4 times faster than get_json_xx on JSON STRING format.

### example
A tutorial for JSONB datatype including create table, load data and query.

#### create database and table

```
CREATE DATABASE testdb;

USE testdb;

CREATE TABLE test_jsonb (
  id INT,
  j JSONB
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES("replication_num" = "1");
```

#### Load data

##### stream load test_jsonb.csv test data

- there are 2 columns, the 1st column is id and the 2nd column is json string
- there are 25 rows, the first 18 rows are valid json and the last 7 rows are invalid


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

- due to the 28% of rows is invalid，stream load with default configuration will fail with error message "too many filtered rows"

```
curl --location-trusted -u root: -T test_jsonb.csv http://127.0.0.1:8840/api/testdb/test_jsonb/_stream_load
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

- stream load will success after set header configuration 'max_filter_ratio: 0.3'
```
curl --location-trusted -u root: -H 'max_filter_ratio: 0.3' -T test_jsonb.csv http://127.0.0.1:8840/api/testdb/test_jsonb/_stream_load
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

- use SELECT to view the data loaded by stream load. The column with JSONB type will be displayed as plain JSON string.

```
mysql> SELECT * FROM test_jsonb ORDER BY id;
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

##### write data using insert into

- total rows increae from 18 to 19 after insert 1 row
```
mysql> INSERT INTO test_jsonb VALUES(26, '{"k1":"v1", "k2": 200}');
Query OK, 1 row affected (0.09 sec)
{'label':'insert_4ece6769d1b42fd_ac9f25b3b8f3dc02', 'status':'VISIBLE', 'txnId':'12016'}

mysql> SELECT * FROM test_jsonb ORDER BY id;
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

#### Query

##### extract some filed from json by jsonb_extract functions

1. extract the whole json, '$' stands for root in json path
```
+------+---------------------------------------------------------------+---------------------------------------------------------------+
| id   | j                                                             | jsonb_extract(`j`, '$')                                       |
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

1. extract k1 field, return NULL if it does not exist
```
mysql> SELECT id, j, jsonb_extract(j, '$.k1') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+----------------------------+
| id   | j                                                             | jsonb_extract(`j`, '$.k1') |
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

1. extract element 0 of the top level array
```
mysql> SELECT id, j, jsonb_extract(j, '$[0]') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+----------------------------+
| id   | j                                                             | jsonb_extract(`j`, '$[0]') |
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

1. extract a whole json array of name a1
```
mysql> SELECT id, j, jsonb_extract(j, '$.a1') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+------------------------------------+
| id   | j                                                             | jsonb_extract(`j`, '$.a1')         |
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

1. extract nested field from an object in an array
```
mysql> SELECT id, j, jsonb_extract(j, '$.a1[0]'), jsonb_extract(j, '$.a1[0].k1') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+-------------------------------+----------------------------------+
| id   | j                                                             | jsonb_extract(`j`, '$.a1[0]') | jsonb_extract(`j`, '$.a1[0].k1') |
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

1. extract field with specific datatype
- jsonb_extract_string will extract field with string type，convert to string if the field is not string
```
mysql> SELECT id, j, jsonb_extract_string(j, '$') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+---------------------------------------------------------------+
| id   | j                                                             | jsonb_extract_string(`j`, '$')                                |
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

mysql> SELECT id, j, jsonb_extract_string(j, '$.k1') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+-----------------------------------+
| id   | j                                                             | jsonb_extract_string(`j`, '$.k1') |
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

- jsonb_extract_int will extract field with int type，return NULL if the field is not int
```
mysql> SELECT id, j, jsonb_extract_int(j, '$') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+-----------------------------+
| id   | j                                                             | jsonb_extract_int(`j`, '$') |
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

mysql> SELECT id, j, jsonb_extract_int(j, '$.k2') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+--------------------------------+
| id   | j                                                             | jsonb_extract_int(`j`, '$.k2') |
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

- jsonb_extract_bigint will extract field with bigint type，return NULL if the field is not bigint
```
mysql> SELECT id, j, jsonb_extract_bigint(j, '$') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+--------------------------------+
| id   | j                                                             | jsonb_extract_bigint(`j`, '$') |
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

mysql> SELECT id, j, jsonb_extract_bigint(j, '$.k2') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+-----------------------------------+
| id   | j                                                             | jsonb_extract_bigint(`j`, '$.k2') |
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

- jsonb_extract_double will extract field with double type，return NULL if the field is not double
```
mysql> SELECT id, j, jsonb_extract_double(j, '$') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+--------------------------------+
| id   | j                                                             | jsonb_extract_double(`j`, '$') |
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

mysql> SELECT id, j, jsonb_extract_double(j, '$.k2') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+-----------------------------------+
| id   | j                                                             | jsonb_extract_double(`j`, '$.k2') |
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

- jsonb_extract_bool will extract field with boolean type，return NULL if the field is not boolean
```
mysql> SELECT id, j, jsonb_extract_bool(j, '$') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+------------------------------+
| id   | j                                                             | jsonb_extract_bool(`j`, '$') |
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

mysql> SELECT id, j, jsonb_extract_bool(j, '$[1]') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+---------------------------------+
| id   | j                                                             | jsonb_extract_bool(`j`, '$[1]') |
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

- jsonb_extract_isnull will extract field with json null type，return 1 if the field is json null , else 0
- json null is different from SQL NULL. SQL NULL stands for no value for a field, but json null stands for an field with special value null.
```
mysql> SELECT id, j, jsonb_extract_isnull(j, '$') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+--------------------------------+
| id   | j                                                             | jsonb_extract_isnull(`j`, '$') |
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

##### check if a field is existed in json by jsonb_exists_path

```
mysql> SELECT id, j, jsonb_exists_path(j, '$') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+-----------------------------+
| id   | j                                                             | jsonb_exists_path(`j`, '$') |
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

mysql> SELECT id, j, jsonb_exists_path(j, '$.k1') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+--------------------------------+
| id   | j                                                             | jsonb_exists_path(`j`, '$.k1') |
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

mysql> SELECT id, j, jsonb_exists_path(j, '$[2]') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+--------------------------------+
| id   | j                                                             | jsonb_exists_path(`j`, '$[2]') |
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

##### get the datatype of a field in json by jsonb_type

- return the data type of the field specified by json path, NULL if not existed.
```
mysql> SELECT id, j, jsonb_type(j, '$') FROM test_jsonb ORDER BY id;
+------+---------------------------------------------------------------+----------------------+
| id   | j                                                             | jsonb_type(`j`, '$') |
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

mysql> select id, j, jsonb_type(j, '$.k1') from test_jsonb order by id;
+------+---------------------------------------------------------------+-------------------------+
| id   | j                                                             | jsonb_type(`j`, '$.k1') |
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
JSONB, JSON, jsonb_parse, jsonb_parse_error_to_null, jsonb_parse_error_to_value, jsonb_extract, jsonb_extract_isnull, jsonb_extract_bool, jsonb_extract_int, jsonb_extract_bigint, jsonb_extract_double, jsonb_extract_string, jsonb_exists_path, jsonb_type

