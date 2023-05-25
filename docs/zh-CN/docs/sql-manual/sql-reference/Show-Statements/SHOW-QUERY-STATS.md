---
{
    "title": "SHOW-QUERY-STATS",
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

## SHOW-QUERY-STATS


### Name
<version since="dev">
SHOW QUERY STATS
</version>

### Description

该语句用于展示数据库中历史查询命中的库表列的情况

```sql
SHOW QUERY STATS [[FOR db_name]|[FROM table_name]] [ALL] [VERBOSE]];
```

说明：

1. 支持查询数据库和表的历史查询命中情况，重启fe后数据会重置，每个fe单独统计
2. 通过 FOR DATABASE 和FROM TABLE 可以指定查询数据库或者表的命中情况，后面分别接数据库名或者表名
3. ALL 可以指定是否展示所有index的查询命中情况，VERBOSE 可以展示更详细的命中情况， 这两个参数可以单独使用，
   也可以一起使用，但是必须放在最后 而且只能用在表的查询上
4. 如果没有 use 任何数据库那么直接执行`SHOW QUERY STATS` 将展示所有数据库的命中情况
5. 命中结果中可能有两列：
   QueryCount：该列被查询次数
   FilterCount: 该列作为where 条件被查询的次数
### Example

1. 展示表`baseall` 的查询命中情况

   ```sql
    MySQL [test_query_db]> show query stats from baseall;
    +-------+------------+-------------+
    | Field | QueryCount | FilterCount |
    +-------+------------+-------------+
    | k0    | 0          | 0           |
    | k1    | 0          | 0           |
    | k2    | 0          | 0           |
    | k3    | 0          | 0           |
    | k4    | 0          | 0           |
    | k5    | 0          | 0           |
    | k6    | 0          | 0           |
    | k10   | 0          | 0           |
    | k11   | 0          | 0           |
    | k7    | 0          | 0           |
    | k8    | 0          | 0           |
    | k9    | 0          | 0           |
    | k12   | 0          | 0           |
    | k13   | 0          | 0           |
    +-------+------------+-------------+
    14 rows in set (0.002 sec)
    
    MySQL [test_query_db]> select k0, k1,k2, sum(k3) from baseall  where k9 > 1 group by k0,k1,k2;
    +------+------+--------+-------------+
    | k0   | k1   | k2     | sum(`k3`)   |
    +------+------+--------+-------------+
    |    0 |    6 |  32767 |        3021 |
    |    1 |   12 |  32767 | -2147483647 |
    |    0 |    3 |   1989 |        1002 |
    |    0 |    7 | -32767 |        1002 |
    |    1 |    8 |    255 |  2147483647 |
    |    1 |    9 |   1991 | -2147483647 |
    |    1 |   11 |   1989 |       25699 |
    |    1 |   13 | -32767 |  2147483647 |
    |    1 |   14 |    255 |         103 |
    |    0 |    1 |   1989 |        1001 |
    |    0 |    2 |   1986 |        1001 |
    |    1 |   15 |   1992 |        3021 |
    +------+------+--------+-------------+
    12 rows in set (0.050 sec)
    
    MySQL [test_query_db]> show query stats from baseall;
    +-------+------------+-------------+
    | Field | QueryCount | FilterCount |
    +-------+------------+-------------+
    | k0    | 1          | 0           |
    | k1    | 1          | 0           |
    | k2    | 1          | 0           |
    | k3    | 1          | 0           |
    | k4    | 0          | 0           |
    | k5    | 0          | 0           |
    | k6    | 0          | 0           |
    | k10   | 0          | 0           |
    | k11   | 0          | 0           |
    | k7    | 0          | 0           |
    | k8    | 0          | 0           |
    | k9    | 1          | 1           |
    | k12   | 0          | 0           |
    | k13   | 0          | 0           |
    +-------+------------+-------------+
    14 rows in set (0.001 sec)
   ```

2. 展示表的所物化视图的的命中的汇总情况

   ```sql
   MySQL [test_query_db]> show query stats from baseall all;
    +-----------+------------+
    | IndexName | QueryCount |
    +-----------+------------+
    | baseall   | 1          |
    +-----------+------------+
    1 row in set (0.005 sec)
   ```

3. 展示表的所物化视图的的命中的详细情况

   ```sql
    MySQL [test_query_db]> show query stats from baseall all verbose;
    +-----------+-------+------------+-------------+
    | IndexName | Field | QueryCount | FilterCount |
    +-----------+-------+------------+-------------+
    | baseall   | k0    | 1          | 0           |
    |           | k1    | 1          | 0           |
    |           | k2    | 1          | 0           |
    |           | k3    | 1          | 0           |
    |           | k4    | 0          | 0           |
    |           | k5    | 0          | 0           |
    |           | k6    | 0          | 0           |
    |           | k10   | 0          | 0           |
    |           | k11   | 0          | 0           |
    |           | k7    | 0          | 0           |
    |           | k8    | 0          | 0           |
    |           | k9    | 1          | 1           |
    |           | k12   | 0          | 0           |
    |           | k13   | 0          | 0           |
    +-----------+-------+------------+-------------+
    14 rows in set (0.017 sec)
   ```

4. 展示数据库的命中情况

   ```sql
    MySQL [test_query_db]> show query stats for test_query_db;
    +----------------------------+------------+
    | TableName                  | QueryCount |
    +----------------------------+------------+
    | compaction_tbl             | 0          |
    | bigtable                   | 0          |
    | empty                      | 0          |
    | tempbaseall                | 0          |
    | test                       | 0          |
    | test_data_type             | 0          |
    | test_string_function_field | 0          |
    | baseall                    | 1          |
    | nullable                   | 0          |
    +----------------------------+------------+
    9 rows in set (0.005 sec)
   ```

5. 展示所有数据库的命中情况，这时不能use 任何数据库

   ```sql
    MySQL [(none)]> show query stats;
    +-----------------+------------+
    | Database        | QueryCount |
    +-----------------+------------+
    | test_query_db   | 1          |
    +-----------------+------------+
    1 rows in set (0.005 sec)
   ```
   SHOW QUERY STATS;
   ```

### Keywords

     SHOW， QUERY, STATS;

### Best Practice
