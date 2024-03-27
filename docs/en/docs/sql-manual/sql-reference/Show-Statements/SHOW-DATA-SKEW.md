---
{
"title": "SHOW DATA SKEW",
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

## SHOW-DATA-SKEW

### Name

SHOW DATA SKEW

### Description

    This statement is used to view the data skew of a table or the specified partitions.

    grammar:

        SHOW DATA SKEW FROM [db_name.]tbl_name [PARTITION (partition_name, ...)];

    Description:

        1. The result will show row count and data volume of each bucket under the specified partition, and the proportion of the data volume of each bucket in the total data volume.
        2. For non-partitioned tables, the partition name in result is the same as the table name.

### Example
1. For partitioned tables

* CREATE-TABLE
    ```sql
    CREATE TABLE test_show_data_skew
    (
      id int, 
      name string, 
      pdate date
    ) 
    PARTITION BY RANGE(pdate) 
    (
      FROM ("2023-04-16") TO ("2023-04-20") INTERVAL 1 DAY
    ) 
    DISTRIBUTED BY HASH(id) BUCKETS 5
    PROPERTIES (
      "replication_num" = "1"
    );
    ```
* View the data skew of the table
   ```sql
    mysql> SHOW DATA SKEW FROM test_show_data_skew;
    +---------------+-----------+-------------+-------------+-------+---------+
    | PartitionName | BucketIdx | AvgRowCount | AvgDataSize | Graph | Percent |
    +---------------+-----------+-------------+-------------+-------+---------+
    | p_20230416    | 0         | 0           | 0           |       | 0.00%   |
    | p_20230416    | 1         | 0           | 0           |       | 0.00%   |
    | p_20230416    | 2         | 0           | 0           |       | 0.00%   |
    | p_20230416    | 3         | 0           | 0           |       | 0.00%   |
    | p_20230416    | 4         | 0           | 0           |       | 0.00%   |
    | p_20230417    | 0         | 0           | 0           |       | 0.00%   |
    | p_20230417    | 1         | 0           | 0           |       | 0.00%   |
    | p_20230417    | 2         | 0           | 0           |       | 0.00%   |
    | p_20230417    | 3         | 0           | 0           |       | 0.00%   |
    | p_20230417    | 4         | 0           | 0           |       | 0.00%   |
    | p_20230418    | 0         | 0           | 0           |       | 0.00%   |
    | p_20230418    | 1         | 0           | 0           |       | 0.00%   |
    | p_20230418    | 2         | 0           | 0           |       | 0.00%   |
    | p_20230418    | 3         | 0           | 0           |       | 0.00%   |
    | p_20230418    | 4         | 0           | 0           |       | 0.00%   |
    | p_20230419    | 0         | 0           | 0           |       | 0.00%   |
    | p_20230419    | 1         | 0           | 0           |       | 0.00%   |
    | p_20230419    | 2         | 0           | 0           |       | 0.00%   |
    | p_20230419    | 3         | 0           | 0           |       | 0.00%   |
    | p_20230419    | 4         | 0           | 0           |       | 0.00%   |
    +---------------+-----------+-------------+-------------+-------+---------+
    ```
* View the data skew of the specified partitions.
    ```sql
    mysql> SHOW DATA SKEW FROM test_show_data_skew PARTITION(p_20230416, p_20230418);
    +---------------+-----------+-------------+-------------+-------+---------+
    | PartitionName | BucketIdx | AvgRowCount | AvgDataSize | Graph | Percent |
    +---------------+-----------+-------------+-------------+-------+---------+
    | p_20230416    | 0         | 0           | 0           |       | 0.00%   |
    | p_20230416    | 1         | 0           | 0           |       | 0.00%   |
    | p_20230416    | 2         | 0           | 0           |       | 0.00%   |
    | p_20230416    | 3         | 0           | 0           |       | 0.00%   |
    | p_20230416    | 4         | 0           | 0           |       | 0.00%   |
    | p_20230418    | 0         | 0           | 0           |       | 0.00%   |
    | p_20230418    | 1         | 0           | 0           |       | 0.00%   |
    | p_20230418    | 2         | 0           | 0           |       | 0.00%   |
    | p_20230418    | 3         | 0           | 0           |       | 0.00%   |
    | p_20230418    | 4         | 0           | 0           |       | 0.00%   |
    +---------------+-----------+-------------+-------------+-------+---------+
    ```

2. For non-partitioned tables

* CREATE-TABLE
    ```sql
    CREATE TABLE test_show_data_skew2
    (
        id int, 
        name string, 
        pdate date
    ) 
    DISTRIBUTED BY HASH(id) BUCKETS 5
    PROPERTIES (
        "replication_num" = "1"
    );
    ```
* View the data skew of the table
    ```sql
    mysql> SHOW DATA SKEW FROM test_show_data_skew2;
    +----------------------+-----------+-------------+-------------+-------+---------+
    | PartitionName        | BucketIdx | AvgRowCount | AvgDataSize | Graph | Percent |
    +----------------------+-----------+-------------+-------------+-------+---------+
    | test_show_data_skew2 | 0         | 0           | 0           |       | 0.00%   |
    | test_show_data_skew2 | 1         | 0           | 0           |       | 0.00%   |
    | test_show_data_skew2 | 2         | 0           | 0           |       | 0.00%   |
    | test_show_data_skew2 | 3         | 0           | 0           |       | 0.00%   |
    | test_show_data_skew2 | 4         | 0           | 0           |       | 0.00%   |
    +----------------------+-----------+-------------+-------------+-------+---------+


    mysql> show data skew from test_show_data_skew2 partition(test_show_data_skew2);
    +----------------------+-----------+-------------+-------------+-------+---------+
    | PartitionName        | BucketIdx | AvgRowCount | AvgDataSize | Graph | Percent |
    +----------------------+-----------+-------------+-------------+-------+---------+
    | test_show_data_skew2 | 0         | 0           | 0           |       | 0.00%   |
    | test_show_data_skew2 | 1         | 0           | 0           |       | 0.00%   |
    | test_show_data_skew2 | 2         | 0           | 0           |       | 0.00%   |
    | test_show_data_skew2 | 3         | 0           | 0           |       | 0.00%   |
    | test_show_data_skew2 | 4         | 0           | 0           |       | 0.00%   |
    +----------------------+-----------+-------------+-------------+-------+---------+
    ```

### Keywords

    SHOW, DATA, SKEW

### Best Practice
