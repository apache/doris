---
{
    "title": "SHOW DATA",
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

# SHOW DATA

## Description

This statement is used to show the amount of data, the number of replica and num of rows.

Syntax:

```
SHOW DATA [FROM db_name[.table_name]] [ORDER BY ...];
```

Explain:

1. If the FROM clause is not specified, the amount of data and the number of copies subdivided into each table under the current db are displayed. The data volume is the total data volume of all replicas. The number of replicas is of all partitions of the table and all materialized views.

2. If the FROM clause is specified, the amount of data, the number of replicas, and the number of statistical rows subdivided into individual materialized views under table are displayed. The data volume is the total data volume of all replicas. The number of replicas is corresponding to all partitions of the materialized view. The number of statistical rows is corresponding to all partitions of the materialized view.

3. When counting the number of rows, the replica with the largest number of rows among multiple replicas shall prevail.

4. The `Total` row in the result set represents the summary row. The `Quota` row indicates the current quota of the database. The `Left` line indicates the remaining quota.

5. If you want to check the size of each Partition, please refer to `help show partitions`.

6. Arbitrary column combinations can be sorted using ORDER BY.

## example

1. Display the data volume, replica size, aggregate data volume and aggregate replica count of each table of default DB.

    ```
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

2. Display the subdivision data volume, replica count and number of rows of the specified table below the specified DB.

    ```
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
3. Can be combined and sorted according to the data volume, replica count,and number of rows,etc.

    ```
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
        
## keyword

    SHOW,DATA
