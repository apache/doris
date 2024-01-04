---
{
    "title": "SHOW-DATA",
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

## SHOW-DATA

### Name

SHOW DATA

### Description

该语句用于展示数据量、副本数量以及统计行数。

语法：

```sql
SHOW DATA [FROM [db_name.]table_name] [ORDER BY ...];
```

说明：

1. 如果不指定 FROM 子句，则展示当前 db 下细分到各个 table 的数据量和副本数量。其中数据量为所有副本的总数据量。而副本数量为表的所有分区以及所有物化视图的副本数量。

2. 如果指定 FROM 子句，则展示 table 下细分到各个物化视图的数据量、副本数量和统计行数。其中数据量为所有副本的总数据量。副本数量为对应物化视图的所有分区的副本数量。统计行数为对应物化视图的所有分区统计行数。

3. 统计行数时，以多个副本中，行数最大的那个副本为准。

4. 结果集中的 `Total` 行表示汇总行。`Quota` 行表示当前数据库设置的配额。`Left` 行表示剩余配额。

5. 如果想查看各个 Partition 的大小，请参阅 `help show partitions`。

6. 可以使用 ORDER BY 对任意列组合进行排序。

### Example

1. 展示默认 db 的各个 table 的数据量，副本数量，汇总数据量和汇总副本数量。

    ```sql
    SHOW DATA;
    ```

    ```
    +-----------+-------------+--------------+
    | TableName | Size        | ReplicaCount |
    +-----------+-------------+--------------+
    | tbl1      | 900.000 B   | 6            |
    | tbl2      | 500.000 B   | 3            |
    | Total     | 1.400 KB    | 9            |
    | Quota     | 1024.000 GB | 1073741824   |
    | Left      | 1021.921 GB | 1073741815   |
    +-----------+-------------+--------------+
    ```

2. 展示指定 db 的下指定表的细分数据量、副本数量和统计行数

    ```sql
    SHOW DATA FROM example_db.test;
    ```

    ```
    +-----------+-----------+-----------+--------------+----------+
    | TableName | IndexName | Size      | ReplicaCount | RowCount |
    +-----------+-----------+-----------+--------------+----------+
    | test      | r1        | 10.000MB  | 30           | 10000    |
    |           | r2        | 20.000MB  | 30           | 20000    |
    |           | test2     | 50.000MB  | 30           | 50000    |
    |           | Total     | 80.000    | 90           |          |
    +-----------+-----------+-----------+--------------+----------+
    ```

3. 可以按照数据量、副本数量、统计行数等进行组合排序

    ```sql
    SHOW DATA ORDER BY ReplicaCount desc,Size asc;
    ```

    ```
    +-----------+-------------+--------------+
    | TableName | Size        | ReplicaCount |
    +-----------+-------------+--------------+
    | table_c   | 3.102 KB    | 40           |
    | table_d   | .000        | 20           |
    | table_b   | 324.000 B   | 20           |
    | table_a   | 1.266 KB    | 10           |
    | Total     | 4.684 KB    | 90           |
    | Quota     | 1024.000 GB | 1073741824   |
    | Left      | 1024.000 GB | 1073741734   |
    +-----------+-------------+--------------+
    ```

### Keywords

    SHOW, DATA

### Best Practice

