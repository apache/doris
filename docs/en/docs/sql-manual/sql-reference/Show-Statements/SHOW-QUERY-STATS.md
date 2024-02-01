---
{
    "title": "SHOW-QUERY-STATS",
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

## SHOW-QUERY-STATS

### Name

<version since="dev">
SHOW QUERY STATS
</version>

### Description

This statement is used to show the query hit statistics of the database and table

grammar:

```sql
SHOW QUERY STATS [[FOR db_name]|[FROM table_name]] [ALL] [VERBOSE]];
```

Remarks：

1. Support query database and table history query hit statistics, restart fe after data will be reset, each fe separately statistics
2. Use FOR DATABASE and FROM TABLE to specify the query database or table hit statistics, respectively followed by the database name or table name
3. ALL can specify whether to display all index query hit statistics, VERBOSE can display more detailed hit statistics, these two parameters can be used separately, but must be placed at the end and can only be used on table queries
4. If no database is used, execute `SHOW QUERY STATS` directly to display the hit statistics of all databases
5. The result may have two columns:
   QueryCount: The number of times the column was queried
   FilterCount: The number of times the column was queried as a where condition
### Example

1. Show the query hit statistics for `baseall`

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

2. Show the query hit statistics summary for all the mv in a table

   ```sql
   MySQL [test_query_db]> show query stats from baseall all;
    +-----------+------------+
    | IndexName | QueryCount |
    +-----------+------------+
    | baseall   | 1          |
    +-----------+------------+
    1 row in set (0.005 sec)
   ```

3. Show the query hit statistics detail info for all the mv in a table

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

4. Show the query hit for a database

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

5. Show query hit statistics for all the databases

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
