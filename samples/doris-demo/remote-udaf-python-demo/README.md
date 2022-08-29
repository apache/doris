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

# Remote UDAF Function Service In Python Demo

## Compile 
1. `pip install grpcio-tools`
2. `python -m grpc_tools.protoc -Iproto --python_out=. --grpc_python_out=. proto/function_service.proto &&  python -m grpc_tools.protoc -Iproto --python_out=. proto/types.proto`

# Run

`python function_server_demo.py 9000`
`9000` is the port that the server will listen on

# Demo


```
//create one table such as table2
CREATE TABLE `table2` (
  `event_day` date NULL,
  `siteid` int(11) NULL DEFAULT "10",
  `citycode` smallint(6) NULL,
  `visitinfo` varchar(1024) NULL DEFAULT "",
  `pv` varchar(1024) REPLACE NULL DEFAULT "0"
) ENGINE=OLAP
AGGREGATE KEY(`event_day`, `siteid`, `citycode`, `visitinfo`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`siteid`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
)
//import some data 
MySQL [test_db]> select * from table2;
+------------+--------+----------+------------------------------------+------+
| event_day  | siteid | citycode | visitinfo                           | pv   |
+------------+--------+----------+------------------------------------+------+
| 2017-07-03 |      8 |       12 | {"ip":"192.168.0.5","source":"pc"} | 81   |
| 2017-07-03 |     37 |       12 | {"ip":"192.168.0.3","source":"pc"} | 81   |
| 2017-07-03 |     67 |       16 | {"ip":"192.168.0.2","source":"pc"} | 79   |
| 2017-07-03 |    101 |       11 | {"ip":"192.168.0.5","source":"pc"} | 65   |
| 2017-07-03 |     32 |       15 | {"ip":"192.168.0.1","source":"pc"} | 188  |
| 2017-07-03 |    103 |       12 | {"ip":"192.168.0.5","source":"pc"} | 123  |
| 2017-07-03 |    104 |       16 | {"ip":"192.168.0.5","source":"pc"} | 79   |
| 2017-07-03 |      3 |       12 | {"ip":"192.168.0.3","source":"pc"} | 123  |
| 2017-07-03 |      3 |       15 | {"ip":"192.168.0.2","source":"pc"} | 188  |
| 2017-07-03 |     13 |       11 | {"ip":"192.168.0.1","source":"pc"} | 65   |
| 2017-07-03 |     53 |       12 | {"ip":"192.168.0.2","source":"pc"} | 123  |
| 2017-07-03 |      1 |       11 | {"ip":"192.168.0.1","source":"pc"} | 65   |
| 2017-07-03 |      7 |       16 | {"ip":"192.168.0.4","source":"pc"} | 79   |
| 2017-07-03 |    102 |       15 | {"ip":"192.168.0.5","source":"pc"} | 188  |
| 2017-07-03 |    105 |       12 | {"ip":"192.168.0.5","source":"pc"} | 81   |
+------------+--------+----------+------------------------------------+------+
```

### 1. find most visit top 3 ip 
```
MySQL [test_db]> CREATE AGGREGATE FUNCTION  rpc_count_visit_info(varchar(1024)) RETURNS varchar(1024) PROPERTIES (
    "TYPE"="RPC",
    "OBJECT_FILE"="127.0.0.1:9000",
    "update_fn"="rpc_count_visit_info_update",
    "merge_fn"="rpc_count_visit_info_merge",
    "finalize_fn"="rpc_count_visit_info_finalize"
);
MySQL [test_db]> select rpc_count_visit_info(visitinfo) from table2;
+--------------------------------------------+
| rpc_count_visit_info(`visitinfo`)           |
+--------------------------------------------+
| 192.168.0.5:6 192.168.0.2:3 192.168.0.1:3  |
+--------------------------------------------+
1 row in set (0.036 sec)
MySQL [test_db]> select citycode, rpc_count_visit_info(visitinfo) from table2 group by citycode;
+----------+--------------------------------------------+
| citycode | rpc_count_visit_info(`visitinfo`)           |
+----------+--------------------------------------------+
|       15 | 192.168.0.2:1 192.168.0.1:1 192.168.0.5:1  |
|       11 | 192.168.0.1:2 192.168.0.5:1                |
|       12 | 192.168.0.5:3 192.168.0.3:2 192.168.0.2:1  |
|       16 | 192.168.0.2:1 192.168.0.4:1 192.168.0.5:1  |
+----------+--------------------------------------------+
4 rows in set (0.050 sec)
```
### 2. sum pv 
```
CREATE AGGREGATE FUNCTION  rpc_sum(bigint) RETURNS bigint PROPERTIES (
    "TYPE"="RPC",
    "OBJECT_FILE"="127.0.0.1:9700",
    "update_fn"="rpc_sum_update",
    "merge_fn"="rpc_sum_merge",
    "finalize_fn"="rpc_sum_finalize"
);
MySQL [test_db]> select citycode, rpc_sum(pv) from table2 group by citycode;
+----------+---------------+
| citycode | rpc_sum(`pv`) |
+----------+---------------+
|       15 |           564 |
|       11 |           195 |
|       12 |           612 |
|       16 |           237 |
+----------+---------------+
4 rows in set (0.067 sec)
MySQL [test_db]> select rpc_sum(pv) from table2;
+---------------+
| rpc_sum(`pv`) |
+---------------+
|          1608 |
+---------------+
1 row in set (0.030 sec)
```

### 3. avg pv

```
CREATE AGGREGATE FUNCTION  rpc_avg(int) RETURNS double PROPERTIES (
    "TYPE"="RPC",
    "OBJECT_FILE"="127.0.0.1:9000",
    "update_fn"="rpc_avg_update",
    "merge_fn"="rpc_avg_merge",
    "finalize_fn"="rpc_avg_finalize"
);
MySQL [test_db]> select citycode, rpc_avg(pv) from table2 group by citycode;
+----------+---------------+
| citycode | rpc_avg(`pv`) |
+----------+---------------+
|       15 |           188 |
|       11 |            65 |
|       12 |           102 |
|       16 |            79 |
+----------+---------------+
4 rows in set (0.039 sec)
MySQL [test_db]> select rpc_avg(pv) from table2;
+---------------+
| rpc_avg(`pv`) |
+---------------+
|         107.2 |
+---------------+
1 row in set (0.028 sec)
```