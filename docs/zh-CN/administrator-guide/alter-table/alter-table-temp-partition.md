---
{
    "title": "临时分区",
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

# 临时分区

在 0.12 版本中，Doris 支持了临时分区功能。

临时分区是归属于某一分区表的。只有分区表可以创建临时分区。

## 规则

* 临时分区的分区列和正式分区相同，且不可修改。
* 一张表所有临时分区之间的分区范围不可重叠，但临时分区的范围和正式分区范围可以重叠。
* 临时分区的分区名称不能和正式分区以及其他临时分区重复。

## 支持的操作

临时分区支持添加、删除、替换操作。

### 添加临时分区

可以通过 `ALTER TABLE ADD TEMPORARY PARTITION` 语句对一个表添加临时分区：

```
ALTER TABLE tbl1 ADD TEMPORARY PARTITION tp1 VALUES LESS THAN("2020-02-01");

ALTER TABLE tbl2 ADD TEMPORARY PARTITION tp1 VALUES [("2020-01-01"), ("2020-02-01"));

ALTER TABLE tbl1 ADD TEMPORARY PARTITION tp1 VALUES LESS THAN("2020-02-01")
("in_memory" = "true", "replication_num" = "1")
DISTRIBUTED BY HASH(k1) BUCKETS 5;

ALTER TABLE tbl3 ADD TEMPORARY PARTITION tp1 VALUES IN ("Beijing", "Shanghai");

ALTER TABLE tbl4 ADD TEMPORARY PARTITION tp1 VALUES IN ((1, "Beijing"), (1, "Shanghai"));

ALTER TABLE tbl3 ADD TEMPORARY PARTITION tp1 VALUES IN ("Beijing", "Shanghai")
("in_memory" = "true", "replication_num" = "1")
DISTRIBUTED BY HASH(k1) BUCKETS 5;

```

通过 `HELP ALTER TABLE;` 查看更多帮助和示例。

添加操作的一些说明：

* 临时分区的添加和正式分区的添加操作相似。临时分区的分区范围独立于正式分区。
* 临时分区可以独立指定一些属性。包括分桶数、副本数、是否是内存表、存储介质等信息。

### 删除临时分区

可以通过 `ALTER TABLE DROP TEMPORARY PARTITION` 语句删除一个表的临时分区：

```
ALTER TABLE tbl1 DROP TEMPORARY PARTITION tp1;
```

通过 `HELP ALTER TABLE;` 查看更多帮助和示例。

删除操作的一些说明：

* 删除临时分区，不影响正式分区的数据。

### 替换分区

可以通过 `ALTER TABLE REPLACE PARTITION` 语句将一个表的正式分区替换为临时分区。

```
ALTER TABLE tbl1 REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);

ALTER TABLE tbl1 REPLACE PARTITION (p1, p2) WITH TEMPORARY PARTITION (tp1, tp2, tp3);

ALTER TABLE tbl1 REPLACE PARTITION (p1, p2) WITH TEMPORARY PARTITION (tp1, tp2)
PROPERTIES (
    "strict_range" = "false",
    "use_temp_partition_name" = "true"
);
```

通过 `HELP ALTER TABLE;` 查看更多帮助和示例。

替换操作有两个特殊的可选参数：

1. `strict_range`

    默认为 true。  

    对于 Range 分区，当该参数为 true 时，表示要被替换的所有正式分区的范围并集需要和替换的临时分区的范围并集完全相同。当置为 false 时，只需要保证替换后，新的正式分区间的范围不重叠即可。

    对于 List 分区，该参数恒为 true。要被替换的所有正式分区的枚举值必须和替换的临时分区枚举值完全相同。
    
    下面举例说明：
    
    * 示例1
    
        待替换的分区 p1, p2, p3 的范围 (=> 并集)：
        
        ```
        [10, 20), [20, 30), [40, 50) => [10, 30), [40, 50)
        ```
    
        替换分区 tp1, tp2 的范围(=> 并集)：
        
        ```
        [10, 30), [40, 45), [45, 50) => [10, 30), [40, 50)
        ```
        
        范围并集相同，则可以使用 tp1 和 tp2 替换 p1, p2, p3。
    
    * 示例2
    
        待替换的分区 p1 的范围 (=> 并集)：
        
        ```
        [10, 50) => [10, 50)
        ```
    
        替换分区 tp1, tp2 的范围(=> 并集)：
        
        ```
        [10, 30), [40, 50) => [10, 30), [40, 50)
        ```
        
        范围并集不相同，如果 `strict_range` 为 true，则不可以使用 tp1 和 tp2 替换 p1。如果为 false，且替换后的两个分区范围 `[10, 30), [40, 50)` 和其他正式分区不重叠，则可以替换。

    * 示例3

        待替换的分区 p1, p2 的枚举值(=> 并集)：

        ```
        (1, 2, 3), (4, 5, 6) => (1, 2, 3, 4, 5, 6)
        ```

        替换分区 tp1, tp2, tp3 的枚举值(=> 并集)：

        ```
        (1, 2, 3), (4), (5, 6) => (1, 2, 3, 4, 5, 6)
        ```

        枚举值并集相同，可以使用 tp1，tp2，tp3 替换 p1，p2

    * 示例4

        待替换的分区 p1, p2，p3 的枚举值(=> 并集)：

        ```
        (("1","beijing"), ("1", "shanghai")), (("2","beijing"), ("2", "shanghai")), (("3","beijing"), ("3", "shanghai")) => (("1","beijing"), ("1", "shanghai"), ("2","beijing"), ("2", "shanghai"), ("3","beijing"), ("3", "shanghai"))
        ```

        替换分区 tp1, tp2 的枚举值(=> 并集)：

        ```
        (("1","beijing"), ("1", "shanghai")), (("2","beijing"), ("2", "shanghai"), ("3","beijing"), ("3", "shanghai")) => (("1","beijing"), ("1", "shanghai"), ("2","beijing"), ("2", "shanghai"), ("3","beijing"), ("3", "shanghai"))
        ```

        枚举值并集相同，可以使用 tp1，tp2 替换 p1，p2，p3

2. `use_temp_partition_name`

    默认为 false。当该参数为 false，并且待替换的分区和替换分区的个数相同时，则替换后的正式分区名称维持不变。如果为 true，则替换后，正式分区的名称为替换分区的名称。下面举例说明：
    
    * 示例1
    
        ```
        ALTER TABLE tbl1 REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);
        ```
        
        `use_temp_partition_name` 默认为 false，则在替换后，分区的名称依然为 p1，但是相关的数据和属性都替换为 tp1 的。
        
        如果 `use_temp_partition_name` 默认为 true，则在替换后，分区的名称为 tp1。p1 分区不再存在。
        
    * 示例2

        ```
        ALTER TABLE tbl1 REPLACE PARTITION (p1, p2) WITH TEMPORARY PARTITION (tp1);
        ```

        `use_temp_partition_name` 默认为 false，但因为待替换分区的个数和替换分区的个数不同，则该参数无效。替换后，分区名称为 tp1，p1 和 p2 不再存在。
        
替换操作的一些说明：

* 分区替换成功后，被替换的分区将被删除且不可恢复。

## 临时分区的导入和查询

用户可以将数据导入到临时分区，也可以指定临时分区进行查询。

1. 导入临时分区

    根据导入方式的不同，指定导入临时分区的语法稍有差别。这里通过示例进行简单说明

    ```
    INSERT INTO tbl TEMPORARY PARTITION(tp1, tp2, ...) SELECT ....
    ```

    ```
    curl --location-trusted -u root: -H "label:123" -H "temporary_partitions: tp1, tp2, ..." -T testData http://host:port/api/testDb/testTbl/_stream_load    
    ```

    ```
    LOAD LABEL example_db.label1
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
    INTO TABLE `my_table`
    TEMPORARY PARTITION (tp1, tp2, ...)
    ...
    )
    WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");
    ```

    ```
    CREATE ROUTINE LOAD example_db.test1 ON example_tbl
    COLUMNS(k1, k2, k3, v1, v2, v3 = k1 * 100),
    TEMPORARY PARTITIONS(tp1, tp2, ...),
    WHERE k1 > 100
    PROPERTIES
    (...)
    FROM KAFKA
    (...);
    ```

2. 查询临时分区

    ```
    SELECT ... FROM
    tbl1 TEMPORARY PARTITION(tp1, tp2, ...)
    JOIN
    tbl2 TEMPORARY PARTITION(tp1, tp2, ...)
    ON ...
    WHERE ...;
    ```

## 和其他操作的关系

### DROP

* 使用 Drop 操作直接删除数据库或表后，可以通过 Recover 命令恢复数据库或表（限定时间内），但临时分区不会被恢复。
* 使用 Alter 命令删除正式分区后，可以通过 Recover 命令恢复分区（限定时间内）。操作正式分区和临时分区无关。
* 使用 Alter 命令删除临时分区后，无法通过 Recover 命令恢复临时分区。

### TRUNCATE

* 使用 Truncate 命令清空表，表的临时分区会被删除，且不可恢复。
* 使用 Truncate 命令清空正式分区时，不影响临时分区。
* 不可使用 Truncate 命令清空临时分区。

### ALTER

* 当表存在临时分区时，无法使用 Alter 命令对表进行 Schema Change、Rollup 等变更操作。
* 当表在进行变更操作时，无法对表添加临时分区。


## 最佳实践

1. 原子的覆盖写操作

    某些情况下，用户希望能够重写某一分区的数据，但如果采用先删除再导入的方式进行，在中间会有一段时间无法查看数据。这时，用户可以先创建一个对应的临时分区，将新的数据导入到临时分区后，通过替换操作，原子的替换原有分区，以达到目的。对于非分区表的原子覆盖写操作，请参阅[替换表文档](./alter-table-replace-table.md)
    
2. 修改分桶数

    某些情况下，用户在创建分区时使用了不合适的分桶数。则用户可以先创建一个对应分区范围的临时分区，并指定新的分桶数。然后通过 `INSERT INTO` 命令将正式分区的数据导入到临时分区中，通过替换操作，原子的替换原有分区，以达到目的。
    
3. 合并或分割分区

    某些情况下，用户希望对分区的范围进行修改，比如合并两个分区，或将一个大分区分割成多个小分区。则用户可以先建立对应合并或分割后范围的临时分区，然后通过 `INSERT INTO` 命令将正式分区的数据导入到临时分区中，通过替换操作，原子的替换原有分区，以达到目的。










