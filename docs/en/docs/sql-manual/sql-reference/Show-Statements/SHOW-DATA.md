---
{
    "title": "SHOW-DATA",
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

## SHOW-DATA

### Name

SHOW DATA

### Description

This statement is used to display the amount of data, the number of replicas, and the number of statistical rows.

grammar:

```sql
SHOW DATA [FROM [db_name.]table_name] [ORDER BY ...];
````

illustrate:

1. If the FROM clause is not specified, the data volume and number of replicas subdivided into each table under the current db will be displayed. The data volume is the total data volume of all replicas. The number of replicas is the number of replicas for all partitions of the table and all materialized views.

2. If the FROM clause is specified, the data volume, number of copies and number of statistical rows subdivided into each materialized view under the table will be displayed. The data volume is the total data volume of all replicas. The number of replicas is the number of replicas for all partitions of the corresponding materialized view. The number of statistical rows is the number of statistical rows for all partitions of the corresponding materialized view.

3. When counting the number of rows, the one with the largest number of rows among the multiple copies shall prevail.

4. The `Total` row in the result set represents the total row. The `Quota` line represents the quota set by the current database. The `Left` line indicates the remaining quota.

5. If you want to see the size of each Partition, see `help show partitions`.

6. You can use ORDER BY to sort on any combination of columns.

### Example

1. Display the data size and RecycleBin size of each database by default.

    ```
    SHOW DATA;
    ```

    ```
    +-------+-----------------------------------+--------+------------+-------------+-------------------+
    | DbId  | DbName                            | Size   | RemoteSize | RecycleSize | RecycleRemoteSize |
    +-------+-----------------------------------+--------+------------+-------------+-------------------+
    | 21009 | db1                               | 0      | 0          | 0           | 0                 |
    | 22011 | regression_test_inverted_index_p0 | 72764  | 0          | 0           | 0                 |
    | 0     | information_schema                | 0      | 0          | 0           | 0                 |
    | 22010 | regression_test                   | 0      | 0          | 0           | 0                 |
    | 1     | mysql                             | 0      | 0          | 0           | 0                 |
    | 22017 | regression_test_show_p0           | 0      | 0          | 0           | 0                 |
    | 10002 | __internal_schema                 | 46182  | 0          | 0           | 0                 |
    | Total | NULL                              | 118946 | 0          | 0           | 0                 |
    +-------+-----------------------------------+--------+------------+-------------+-------------------+
    ```

2. Display the data volume, replica number, aggregate data volume and aggregate replica number of each table in a database.

   ```sql
   USE db1;
   SHOW DATA;
   ````

   ````
   +-----------+-------------+--------------+
   | TableName | Size        | ReplicaCount |
   +-----------+-------------+--------------+
   | tbl1      | 900.000 B   | 6            |
   | tbl2      | 500.000 B   | 3            |
   | Total     | 1.400 KB    | 9            |
   | Quota     | 1024.000 GB | 1073741824   |
   | Left      | 1021.921 GB | 1073741815   |
   +-----------+-------------+--------------+
   ````

3. Display the subdivided data volume, the number of replicas and the number of statistical rows of the specified table under the specified db

   ```sql
   SHOW DATA FROM example_db.test;
   ````

   ````
   +-----------+-----------+-----------+--------------+----------+
   | TableName | IndexName | Size      | ReplicaCount | RowCount |
   +-----------+-----------+-----------+--------------+----------+
   | test      | r1        | 10.000MB  | 30           | 10000    |
   |           | r2        | 20.000MB  | 30           | 20000    |
   |           | test2     | 50.000MB  | 30           | 50000    |
   |           | Total     | 80.000    | 90           |          |
   +-----------+-----------+-----------+--------------+----------+
   ````

4. It can be combined and sorted according to the amount of data, the number of copies, the number of statistical rows, etc.

   ```sql
   SHOW DATA ORDER BY ReplicaCount desc,Size asc;
   ````

   ````
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
   ````

### Keywords

    SHOW, DATA

### Best Practice

