---
{
    "title": "Temporary Partition",
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

# Temporary Partition

Since version 0.12, Doris supports temporary partitioning.

A temporary partition belongs to a partitioned table. Only partitioned tables can create temporary partitions.

## Rules

* The partition columns of the temporary partition is the same as the formal partition and cannot be modified.
* The partition ranges of all temporary partitions of a table cannot overlap, but the ranges of temporary partitions and formal partitions can overlap.
* The partition name of the temporary partition cannot be the same as the formal partitions and other temporary partitions.

## Supported operations

The temporary partition supports add, delete, and replace operations.

### Add temporary partition

You can add temporary partitions to a table with the `ALTER TABLE ADD TEMPORARY PARTITION` statement:

```
ALTER TABLE tbl1 ADD TEMPORARY PARTITION tp1 VALUES LESS THAN ("2020-02-01");

ALTER TABLE tbl2 ADD TEMPORARY PARTITION tp1 VALUES [("2020-01-01"), ("2020-02-01"));

ALTER TABLE tbl1 ADD TEMPORARY PARTITION tp1 VALUES LESS THAN ("2020-02-01")
("replication_num" = "1")
DISTRIBUTED BY HASH (k1) BUCKETS 5;

ALTER TABLE tbl3 ADD TEMPORARY PARTITION tp1 VALUES IN ("Beijing", "Shanghai");

ALTER TABLE tbl4 ADD TEMPORARY PARTITION tp1 VALUES IN ((1, "Beijing"), (1, "Shanghai"));

ALTER TABLE tbl3 ADD TEMPORARY PARTITION tp1 VALUES IN ("Beijing", "Shanghai")
("replication_num" = "1")
DISTRIBUTED BY HASH(k1) BUCKETS 5;

```

See `HELP ALTER TABLE;` for more help and examples.

Some instructions for adding operations:

* Adding a temporary partition is similar to adding a formal partition. The partition range of the temporary partition is independent of the formal partition.
* Temporary partition can independently specify some attributes. Includes information such as the number of buckets, the number of replicas, or the storage medium.

### Delete temporary partition

A table's temporary partition can be dropped with the `ALTER TABLE DROP TEMPORARY PARTITION` statement:

```
ALTER TABLE tbl1 DROP TEMPORARY PARTITION tp1;
```

See `HELP ALTER TABLE;` for more help and examples.

Some instructions for the delete operation:

* Deleting the temporary partition will not affect the data of the formal partition.

### Replace partition

You can replace formal partitions of a table with temporary partitions with the `ALTER TABLE REPLACE PARTITION` statement.

```
ALTER TABLE tbl1 REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);

ALTER TABLE tbl1 REPLACE PARTITION (p1, p2) WITH TEMPORARY PARTITION (tp1, tp2, tp3);

ALTER TABLE tbl1 REPLACE PARTITION (p1, p2) WITH TEMPORARY PARTITION (tp1, tp2)
PROPERTIES (
    "strict_range" = "false",
    "use_temp_partition_name" = "true"
);
```

See `HELP ALTER TABLE;` for more help and examples.

The replace operation has two special optional parameters:

1. `strict_range`

    The default is true. 
    
    For Range partition, When this parameter is true, the range union of all formal partitions to be replaced needs to be the same as the range union of the temporary partitions to be replaced. When set to false, you only need to ensure that the range between the new formal partitions does not overlap after replacement. 
    
    For List partition, this parameter is always true, and the enumeration values of all full partitions to be replaced must be identical to the enumeration values of the temporary partitions to be replaced.

    Here are some examples:

    * Example 1

        Range of partitions p1, p2, p3 to be replaced (=> union):

        ```
        (10, 20), [20, 30), [40, 50) => [10, 30), [40, 50)
        ```

        Replace the range of partitions tp1, tp2 (=> union):

        ```
        (10, 30), [40, 45), [45, 50) => [10, 30), [40, 50)
        ```

        The union of ranges is the same, so you can use tp1 and tp2 to replace p1, p2, p3.

    * Example 2

        Range of partition p1 to be replaced (=> union):

        ```
        (10, 50) => [10, 50)
        ```

        Replace the range of partitions tp1, tp2 (=> union):

        ```
        (10, 30), [40, 50) => [10, 30), [40, 50)
        ```

        The union of ranges is not the same. If `strict_range` is true, you cannot use tp1 and tp2 to replace p1. If false, and the two partition ranges `[10, 30), [40, 50)` and the other formal partitions do not overlap, they can be replaced.

    * Example 3

        Enumerated values of partitions p1, p2 to be replaced (=> union).

        ```
        (1, 2, 3), (4, 5, 6) => (1, 2, 3, 4, 5, 6)
        ```

        Replace the enumerated values of partitions tp1, tp2, tp3 (=> union).

        ```
        (1, 2, 3), (4), (5, 6) => (1, 2, 3, 4, 5, 6)
        ```

        The enumeration values are the same, you can use tp1, tp2, tp3 to replace p1, p2

    * Example 4

        Enumerated values of partitions p1, p2, p3 to be replaced (=> union).

        ```
        (("1", "beijing"), ("1", "shanghai")), (("2", "beijing"), ("2", "shanghai")), (("3", "beijing"), ("3", "shanghai")) => (("1", "beijing"), ("3", "shanghai")) "), ("1", "shanghai"), ("2", "beijing"), ("2", "shanghai"), ("3", "beijing"), ("3", "shanghai"))
        ```

        Replace the enumerated values of partitions tp1, tp2 (=> union).

        ```
        (("1", "beijing"), ("1", "shanghai")), (("2", "beijing"), ("2", "shanghai"), ("3", "beijing"), ("3", "shanghai")) => (("1", "beijing") , ("1", "shanghai"), ("2", "beijing"), ("2", "shanghai"), ("3", "beijing"), ("3", "shanghai"))
        ```

        The enumeration values are the same, you can use tp1, tp2 to replace p1, p2, p3

2. `use_temp_partition_name`

    The default is false. When this parameter is false, and the number of partitions to be replaced is the same as the number of replacement partitions, the name of the formal partition after the replacement remains unchanged. If true, after replacement, the name of the formal partition is the name of the replacement partition. Here are some examples:

    * Example 1

        ```
        ALTER TABLE tbl1 REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);
        ```

        `use_temp_partition_name` is false by default. After replacement, the partition name is still p1, but the related data and attributes are replaced with tp1.

        If `use_temp_partition_name` is true by default, the name of the partition is tp1 after replacement. The p1 partition no longer exists.

    * Example 2

        ```
        ALTER TABLE tbl1 REPLACE PARTITION (p1, p2) WITH TEMPORARY PARTITION (tp1);
        ```

        `use_temp_partition_name` is false by default, but this parameter is invalid because the number of partitions to be replaced and the number of replacement partitions are different. After the replacement, the partition name is tp1, and p1 and p2 no longer exist.

Some instructions for the replacement operation:

* After the partition is replaced successfully, the replaced partition will be deleted and cannot be recovered.

## Load and query of temporary partitions

Users can load data into temporary partitions or specify temporary partitions for querying.

1. Load temporary partition

    The syntax for specifying a temporary partition is slightly different depending on the load method. Here is a simple illustration through an example:

    ```
    INSERT INTO tbl TEMPORARY PARTITION (tp1, tp2, ...) SELECT ....
    ```

    ```
    curl --location-trusted -u root: -H "label: 123" -H "temporary_partition: tp1, tp2, ..." -T testData http: // host: port / api / testDb / testTbl / _stream_load
    ```

    ```
    LOAD LABEL example_db.label1
    (
    DATA INFILE ("hdfs: // hdfs_host: hdfs_port / user / palo / data / input / file")
    INTO TABLE `my_table`
    TEMPORARY PARTITION (tp1, tp2, ...)
    ...
    )
    WITH BROKER hdfs ("username" = "hdfs_user", "password" = "hdfs_password");
    ```

    ```
    CREATE ROUTINE LOAD example_db.test1 ON example_tbl
    COLUMNS (k1, k2, k3, v1, v2, v3 = k1 * 100),
    TEMPORARY PARTITIONS (tp1, tp2, ...),
    WHERE k1> 100
    PROPERTIES
    (...)
    FROM KAFKA
    (...);
    ```

2. Query the temporary partition

    ```
    SELECT ... FROM
    tbl1 TEMPORARY PARTITION (tp1, tp2, ...)
    JOIN
    tbl2 TEMPORARY PARTITION (tp1, tp2, ...)
    ON ...
    WHERE ...;
    ```

## Relationship to other operations

### DROP

* After using the `DROP` operation to directly drop the database or table, you can recover the database or table (within a limited time) through the `RECOVER` command, but the temporary partition will not be recovered.
* After the formal partition is dropped using the `ALTER` command, the partition can be recovered by the `RECOVER` command (within a limited time). Operating a formal partition is not related to a temporary partition.
* After the temporary partition is dropped using the `ALTER` command, the temporary partition cannot be recovered through the `RECOVER` command.

### TRUNCATE

* Use the `TRUNCATE` command to empty the table. The temporary partition of the table will be deleted and cannot be recovered.
* When using `TRUNCATE` command to empty the formal partition, it will not affect the temporary partition.
* You cannot use the `TRUNCATE` command to empty the temporary partition.

### ALTER

* When the table has a temporary partition, you cannot use the `ALTER` command to perform Schema Change, Rollup, etc. on the table.
* You cannot add temporary partitions to a table while the table is undergoing a alter operation.


## Best Practices

1. Atomic overwrite

    In some cases, the user wants to be able to rewrite the data of a certain partition, but if it is dropped first and then loaded, there will be a period of time when the data cannot be seen. At this moment, the user can first create a corresponding temporary partition, load new data into the temporary partition, and then replace the original partition atomically through the `REPLACE` operation to achieve the purpose. For atomic overwrite operations of non-partitioned tables, please refer to [Replace Table Document](../../advanced/alter-table/replace-table.md)
    
2. Modify the number of buckets

    In some cases, the user used an inappropriate number of buckets when creating a partition. The user can first create a temporary partition corresponding to the partition range and specify a new number of buckets. Then use the `INSERT INTO` command to load the data of the formal partition into the temporary partition. Through the replacement operation, the original partition is replaced atomically to achieve the purpose.
    
3. Merge or split partitions

    In some cases, users want to modify the range of partitions, such as merging two partitions, or splitting a large partition into multiple smaller partitions. Then the user can first create temporary partitions corresponding to the merged or divided range, and then load the data of the formal partition into the temporary partition through the `INSERT INTO` command. Through the replacement operation, the original partition is replaced atomically to achieve the purpose.
