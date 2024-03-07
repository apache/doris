---
{
    "title": "INSERT-OVERWRITE",
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

## INSERT OVERWRITE

### Name

INSERT OVERWRITE

### Description

The function of this statement is to overwrite a table or a partition of a table

```sql
INSERT OVERWRITE table table_name
    [ PARTITION (p1, ...) ]
    [ WITH LABEL label]
    [ (column [, ...]) ]
    [ [ hint [, ...] ] ]
    { VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

 Parameters

> table_name: the destination table to overwrite. This table must exist. It can be of the form `db_name.table_name`
>
> partitions: the table partition that needs to be overwritten must be one of the existing partitions in `table_name` separated by a comma
>
> label: specify a label for the Insert task
>
> column_name: the specified destination column must be one of the existing columns in `table_name`
>
> expression: the corresponding expression that needs to be assigned to a column
>
> DEFAULT: let the column use the default value
>
> query: a common query, the result of the query will overwrite the target.
>
> hint: some indicator used to indicate the execution behavior of `INSERT`. You can choose one of this values: `/*+ STREAMING */`, `/*+ SHUFFLE */` or `/*+ NOSHUFFLE */.
>
> 1. STREAMING: At present, it has no practical effect and is only reserved for compatibility with previous versions. (In the previous version, adding this hint would return a label, but now it defaults to returning a label)
> 2. SHUFFLE: When the target table is a partition table, enabling this hint will do repartiiton.
> 3. NOSHUFFLE: Even if the target table is a partition table, repartiiton will not be performed, but some other operations will be performed to ensure that the data is correctly dropped into each partition.

Notice:

1. In the current version, the session variable `enable_insert_strict` is set to `true` by default. If some data that does not conform to the format of the target table is filtered out during the execution of the `INSERT OVERWRITE` statement, such as when overwriting a partition and not all partition conditions are satisfied, overwriting the target table will fail.
2. If the target table of the INSERT OVERWRITE is an [AUTO-PARTITION-table](../../../../advanced/partition/auto-partition), then new partitions can be created if PARTITION is not specified (that is, rewrite the whole table). If PARTITION for overwrite is specified, then the AUTO PARTITION table behaves as if it were a normal partitioned table during this process, and data that does not satisfy the existing partition conditions is filtered instead of creating a new partition.
3. The `INSERT OVERWRITE` statement first creates a new table, inserts the data to be overwritten into the new table, and then atomically replaces the old table with the new table and modifies its name. Therefore, during the process of overwriting the table, the data in the old table can still be accessed normally until the overwriting is completed.

### Example

Assuming there is a table named `test`. The table contains two columns `c1` and `c2`, and two partitions `p1` and `p2`

```sql
CREATE TABLE IF NOT EXISTS test (
  `c1` int NOT NULL DEFAULT "1",
  `c2` int NOT NULL DEFAULT "4"
) ENGINE=OLAP
UNIQUE KEY(`c1`)
PARTITION BY LIST (`c1`)
(
PARTITION p1 VALUES IN ("1","2","3"),# Partition p1 only allows 1, 2, and 3 to exist.
PARTITION p2 VALUES IN ("4","5","6") # Partition p2 only allows 1, 5, and 6 to exist.
)
DISTRIBUTED BY HASH(`c1`) BUCKETS 3
PROPERTIES (
  "replication_allocation" = "tag.location.default: 1",
  "in_memory" = "false",
  "storage_format" = "V2"
);
```

#### Overwrite Table

1. Overwrite the `test` table using the form of `VALUES`.

   ```sql
   // Single-row overwrite.
   INSERT OVERWRITE table test VALUES (1, 2);
   INSERT OVERWRITE table test (c1, c2) VALUES (1, 2);
   INSERT OVERWRITE table test (c1, c2) VALUES (1, DEFAULT);
   INSERT OVERWRITE table test (c1) VALUES (1);
   // Multi-row overwrite.
   INSERT OVERWRITE table test VALUES (1, 2), (3, 2 + 2);
   INSERT OVERWRITE table test (c1, c2) VALUES (1, 2), (3, 2 * 2);
   INSERT OVERWRITE table test (c1, c2) VALUES (1, DEFAULT), (3, DEFAULT);
   INSERT OVERWRITE table test (c1) VALUES (1), (3);
   ```

- The first and second statements have the same effect. If the target column is not specified during overwriting, the column order in the table will be used as the default target column. After the overwrite is successful, there is only one row of data in the `test` table.
- The third and fourth statements have the same effect. The unspecified column `c2` will be overwritten with the default value 4. After the overwrite is successful, there is only one row of data in the `test` table.
- The fifth and sixth statements have the same effect. Expressions (such as `2+2`, `2*2`) can be used in the statement. The result of the expression will be computed during the execution of the statement and then overwritten into the `test` table. After the overwrite is successful, there are two rows of data in the `test` table.
- The seventh and eighth statements have the same effect. The unspecified column `c2` will be overwritten with the default value 4. After the overwrite is successful, there are two rows of data in the `test` table.

2. Overwrite the `test` table in the form of a query statement. The data format of the `test2` table and the `test` table must be consistent. If they are not consistent, implicit data type conversion will be triggered.

   ```sql
   INSERT OVERWRITE table test SELECT * FROM test2;
   INSERT OVERWRITE table test (c1, c2) SELECT * from test2;
   ```

- The first and second statements have the same effect. The purpose of these statements is to take data from the `test2` table and overwrite the `test` table with the taken data. After the overwrite is successful, the data in the `test` table will be consistent with the data in the `test2` table.

3. Overwrite the `test` table and specify a label.

   ```sql
   INSERT OVERWRITE table test WITH LABEL `label1` SELECT * FROM test2;
   INSERT OVERWRITE table test WITH LABEL `label2` (c1, c2) SELECT * from test2;
   ```

- Using a label will encapsulate this task into an **asynchronous task**. After executing the statement, the relevant operations will be executed asynchronously. Users can use the `SHOW LOAD;` command to check the status of the job imported by this `label`. It should be noted that the label is unique.


#### Overwrite Table Partition

1. Overwrite partitions `P1` and `P2` of the `test` table using the form of `VALUES`.

   ```sql
   // Single-row overwrite.
   INSERT OVERWRITE table test PARTITION(p1,p2) VALUES (1, 2);
   INSERT OVERWRITE table test PARTITION(p1,p2) (c1, c2) VALUES (1, 2);
   INSERT OVERWRITE table test PARTITION(p1,p2) (c1, c2) VALUES (1, DEFAULT);
   INSERT OVERWRITE table test PARTITION(p1,p2) (c1) VALUES (1);
   // Multi-row overwrite.
   INSERT OVERWRITE table test PARTITION(p1,p2) VALUES (1, 2), (4, 2 + 2);
   INSERT OVERWRITE table test PARTITION(p1,p2) (c1, c2) VALUES (1, 2), (4, 2 * 2);
   INSERT OVERWRITE table test PARTITION(p1,p2) (c1, c2) VALUES (1, DEFAULT), (4, DEFAULT);
   INSERT OVERWRITE table test PARTITION(p1,p2) (c1) VALUES (1), (4);
   ```

   Unlike overwriting an entire table, the above statements are overwriting partitions in the table. Partitions can be overwritten one at a time or multiple partitions can be overwritten at once. It should be noted that only data that satisfies the corresponding partition filtering condition can be overwritten successfully. If there is data in the overwritten data that does not satisfy any of the partitions, the overwrite will fail. An example of a failure is shown below.

   ```sql
   INSERT OVERWRITE table test PARTITION(p1,p2) VALUES (7, 2);
   ```

   The data overwritten by the above statements (`c1=7`) does not satisfy the conditions of partitions `P1` and `P2`, so the overwrite will fail.

2. Overwrite partitions `P1` and `P2` of the `test` table in the form of a query statement. The data format of the `test2` table and the `test` table must be consistent. If they are not consistent, implicit data type conversion will be triggered.

   ```sql
   INSERT OVERWRITE table test PARTITION(p1,p2) SELECT * FROM test2;
   INSERT OVERWRITE table test PARTITION(p1,p2) (c1, c2) SELECT * from test2;
   ```

3. Overwrite partitions `P1` and `P2` of the `test` table and specify a label.

   ```sql
   INSERT OVERWRITE table test PARTITION(p1,p2) WITH LABEL `label3` SELECT * FROM test2;
   INSERT OVERWRITE table test PARTITION(p1,p2) WITH LABEL `label4` (c1, c2) SELECT * from test2;
   ```

### Keywords

    INSERT OVERWRITE, OVERWRITE
