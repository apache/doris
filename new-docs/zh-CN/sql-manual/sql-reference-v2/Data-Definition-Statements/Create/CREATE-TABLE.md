---
{
    "title": "CREATE-TABLE",
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

## CREATE-TABLE

### Description

该命令用于创建一张表。本文档主语介绍创建 Doris 自维护的表的语法。外部表语法请参阅 [CREATE-EXTERNAL-TABLE](./CREATE-EXTERNAL-TABLE.html)文档。

```sql
CREATE TABLE [IF NOT EXISTS] [database.]table
(
    column_definition_list,
    [index_definition_list]
)
[engine_type]
[keys_type]
[table_comment]
[partition_info]
distribution_info
[rollup_list]
[properties]
[extra_properties]
```

* `column_definition_list`

    列定义列表：
    
    `column_definition[, column_definition]`

    * `column_definition`

        列定义：
    
        `column_name column_type [KEY] [aggr_type] [NULL] [default_value] [column_comment]`

        * `column_type`

            列类型，支持以下类型：

            ```
            TINYINT（1字节）
                范围：-2^7 + 1 ~ 2^7 - 1
            SMALLINT（2字节）
                范围：-2^15 + 1 ~ 2^15 - 1
            INT（4字节）
                范围：-2^31 + 1 ~ 2^31 - 1
            BIGINT（8字节）
                范围：-2^63 + 1 ~ 2^63 - 1
            LARGEINT（16字节）
                范围：-2^127 + 1 ~ 2^127 - 1
            FLOAT（4字节）
                支持科学计数法
            DOUBLE（12字节）
                支持科学计数法
            DECIMAL[(precision, scale)] (16字节)
                保证精度的小数类型。默认是 DECIMAL(10, 0)
                precision: 1 ~ 27
                scale: 0 ~ 9
                其中整数部分为 1 ~ 18
                不支持科学计数法
            DATE（3字节）
                范围：0000-01-01 ~ 9999-12-31
            DATETIME（8字节）
                范围：0000-01-01 00:00:00 ~ 9999-12-31 23:59:59
            CHAR[(length)]
                定长字符串。长度范围：1 ~ 255。默认为1
            VARCHAR[(length)]
                变长字符串。长度范围：1 ~ 65533。默认为1
            HLL (1~16385个字节)
                HyperLogLog 列类型，不需要指定长度和默认值。长度根据数据的聚合程度系统内控制。
                必须配合 HLL_UNION 聚合类型使用。
            BITMAP
                bitmap 列类型，不需要指定长度和默认值。表示整型的集合，元素最大支持到2^64 - 1。
                必须配合 BITMAP_UNION 聚合类型使用。
            ```

        * `aggr_type`

            聚合类型，支持以下聚合类型：

            ```
            SUM：求和。适用数值类型。
            MIN：求最小值。适合数值类型。
            MAX：求最大值。适合数值类型。
            REPLACE：替换。对于维度列相同的行，指标列会按照导入的先后顺序，后倒入的替换先导入的。
            REPLACE_IF_NOT_NULL：非空值替换。和 REPLACE 的区别在于对于null值，不做替换。这里要注意的是字段默认值要给NULL，而不能是空字符串，如果是空字符串，会给你替换成空字符串。
            HLL_UNION：HLL 类型的列的聚合方式，通过 HyperLogLog 算法聚合。
            BITMAP_UNION：BIMTAP 类型的列的聚合方式，进行位图的并集聚合。
            ```
            
        
        示例：
        
        ```text
        k1 TINYINT,
        k2 DECIMAL(10,2) DEFAULT "10.5",
        k4 BIGINT NULL DEFAULT VALUE "1000" COMMENT "This is column k4",
        v1 VARCHAR(10) REPLACE NOT NULL,
        v2 BITMAP BITMAP_UNION,
        v3 HLL HLL_UNION,
        v4 INT SUM NOT NULL DEFAULT "1" COMMENT "This is column v4"
        ```
    
*  `index_definition_list`

    索引列表定义：
    
    `index_definition[, index_definition]`

    * `index_definition`

        索引定义：

        ```sql
        INDEX index_name (col_name) [USING BITMAP] COMMENT 'xxxxxx'
        ```

        示例：
        
        ```sql
        INDEX idx1 (k1) USING BITMAP COMMENT "This is a bitmap index1",
        INDEX idx2 (k2) USING BITMAP COMMENT "This is a bitmap index2",
        ...
        ```

* `engine_type`

    表引擎类型。本文档中类型皆为 OLAP。其他外部表引擎类型见 [CREATE EXTERNAL TABLE](./CREATE-EXTERNAL-TABLE.html) 文档。示例：
    
    `ENGINE=olap`
    
* `key_desc`

    数据模型。
    
    `key_type(col1, col2, ...)`
    
    `key_type` 支持以下模型：
    
    * DUPLICATE KEY（默认）：其后指定的列为排序列。
    * AGGREGATE KEY：其后指定的列为维度列。
    * UNIQUE KEY：其后指定的列为主键列。

    示例：
    
    ```
    DUPLICATE KEY(col1, col2),
    AGGREGATE KEY(k1, k2, k3),
    UNIQUE KEY(k1, k2)
    ```
    
* `table_comment`

    表注释。示例：
    
    ```
    COMMENT "This is my first DORIS table"
    ```

* `partition_desc`

    分区信息，支持两种写法：
    
    1. LESS THAN：仅定义分区上界。下界由上一个分区的上界决定。
    
        ```
        PARTITION BY RANGE(col1[, col2, ...])
        (
            PARTITION partition_name1 VALUES LESS THAN MAXVALUE|("value1", "value2", ...),
            PARTITION partition_name2 VALUES LESS THAN MAXVALUE|("value1", "value2", ...)
        )
        ```

    2. FIXED RANGE：定义分区的左闭右开区间。

        ```
        PARTITION BY RANGE(col1[, col2, ...])
        (
            PARTITION partition_name1 VALUES [("k1-lower1", "k2-lower1", "k3-lower1",...), ("k1-upper1", "k2-upper1", "k3-upper1", ...)),
            PARTITION partition_name2 VALUES [("k1-lower1-2", "k2-lower1-2", ...), ("k1-upper1-2", MAXVALUE, ))
        )
        ```

* `distribution_desc`
  
    定义数据分桶方式。

    `DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]`

* `rollup_list`

    建表的同时可以创建多个物化视图（ROLLUP）。

    `ROLLUP (rollup_definition[, rollup_definition, ...])`

    * `rollup_definition`

        `rollup_name (col1[, col2, ...]) [DUPLICATE KEY(col1[, col2, ...])] [PROPERTIES("key" = "value")]`

        示例：

        ```
        ROLLUP (
            r1 (k1, k3, v1, v2),
            r2 (k1, v1)
        )
        ```

* `properties`

    设置表属性。目前支持以下属性：

    * `replication_num`

        副本数。默认副本数为3。如果 BE 节点数量小于3，则需指定副本数小于等于 BE 节点数量。

        在 0.15 版本后，该属性将自动转换成 `replication_allocation` 属性，如：

        `"replication_num" = "3"` 会自动转换成 `"replication_allocation" = "tag.location.default:3"`

    * `replication_allocation`

        根据 Tag 设置副本分布情况。该属性可以完全覆盖 `replication_num` 属性的功能。

    * `storage_medium/storage_cooldown_time`

        数据存储介质。`storage_medium` 用于声明表数据的初始存储介质，而 `storage_cooldown_time` 用于设定到期时间。示例：
        
        ```
        "storage_medium" = "SSD",
        "storage_cooldown_time" = "2020-11-20 00:00:00"
        ```

        这个示例表示数据存放在 SSD 中，并且在 2020-11-20 00:00:00 到期后，会自动迁移到 HDD 存储上。

    * `colocate_with`

        当需要使用 Colocation Join 功能时，使用这个参数设置 Colocation Group。

        `"colocate_with" = "group1"`

    * `bloom_filter_columns`

        用户指定需要添加 Bloom Filter 索引的列名称列表。各个列的 Bloom Filter 索引是独立的，并不是组合索引。

        `"bloom_filter_columns" = "k1, k2, k3"`

    * `in_memory`

        通过此属性设置该表是否为内存表。

        `"in_memory" = "true"`

    * `function_column.sequence_type`

        当使用 UNIQUE KEY 模型时，可以指定一个sequence列，当KEY列相同时，将按照 sequence 列进行 REPLACE(较大值替换较小值，否则无法替换)

        这里我们仅需指定顺序列的类型，支持时间类型或整型。Doris 会创建一个隐藏的顺序列。

        `"function_column.sequence_type" = 'Date'`

    * 动态分区相关

        动态分区相关参数如下：

        * `dynamic_partition.enable`: 用于指定表级别的动态分区功能是否开启。默认为 true。
        * `dynamic_partition.time_unit:` 用于指定动态添加分区的时间单位，可选择为DAY（天），WEEK(周)，MONTH（月），HOUR（时）。
        * `dynamic_partition.start`: 用于指定向前删除多少个分区。值必须小于0。默认为 Integer.MIN_VALUE。
        * `dynamic_partition.end`: 用于指定提前创建的分区数量。值必须大于0。
        * `dynamic_partition.prefix`: 用于指定创建的分区名前缀，例如分区名前缀为p，则自动创建分区名为p20200108。
        * `dynamic_partition.buckets`: 用于指定自动创建的分区分桶数量。
        * `dynamic_partition.create_history_partition`: 是否创建历史分区。
        * `dynamic_partition.history_partition_num`: 指定创建历史分区的数量。
        * `dynamic_partition.reserved_history_periods`: 用于指定保留的历史分区的时间段。

    * 数据排序相关

        数据排序相关参数如下:

        * `data_sort.sort_type`: 数据排序使用的方法，目前支持两种：lexical/z-order，默认是lexical
        * `data_sort.col_num`: 数据排序使用的列数，取最前面几列，不能超过总的key 列数
### Example

1. 创建一个明细模型的表

    ```sql
    CREATE TABLE example_db.table_hash
    (
        k1 TINYINT,
        k2 DECIMAL(10, 2) DEFAULT "10.5",
        k3 CHAR(10) COMMENT "string column",
        k4 INT NOT NULL DEFAULT "1" COMMENT "int column"
    )
    COMMENT "my first table"
    DISTRIBUTED BY HASH(k1) BUCKETS 32
    ```

2. 创建一个明细模型的表，分区，指定排序列，设置副本数为1

    ```sql
    CREATE TABLE example_db.table_hash
    (
        k1 DATE,
        k2 DECIMAL(10, 2) DEFAULT "10.5",
        k3 CHAR(10) COMMENT "string column",
        k4 INT NOT NULL DEFAULT "1" COMMENT "int column"
    )
    DUPLICATE KEY(k1, k2)
    COMMENT "my first table"
    PARTITION BY RANGE(k1)
    (
        PARTITION p1 VALUES LESS THAN ("2020-02-01"),
        PARTITION p1 VALUES LESS THAN ("2020-03-01"),
        PARTITION p1 VALUES LESS THAN ("2020-04-01")
    )
    DISTRIBUTED BY HASH(k1) BUCKETS 32
    PROPERTIES (
        "replication_num" = "1"
    );
    ```

3. 创建一个主键唯一模型的表，设置初始存储介质和冷却时间

    ```sql
    CREATE TABLE example_db.table_hash
    (
        k1 BIGINT,
        k2 LARGEINT,
        v1 VARCHAR(2048) REPLACE,
        v2 SMALLINT SUM DEFAULT "10"
    )
    UNIQUE KEY(k1, k2)
    DISTRIBUTED BY HASH (k1, k2) BUCKETS 32
    PROPERTIES(
        "storage_medium" = "SSD",
        "storage_cooldown_time" = "2015-06-04 00:00:00"
    );
    ```

4. 创建一个聚合模型表，使用固定范围分区描述

    ```sql
    CREATE TABLE table_range
    (
        k1 DATE,
        k2 INT,
        k3 SMALLINT,
        v1 VARCHAR(2048) REPLACE,
        v2 INT SUM DEFAULT "1"
    )
    AGGREGATE KEY(k1, k2, k3)
    PARTITION BY RANGE (k1, k2, k3)
    (
        PARTITION p1 VALUES [("2014-01-01", "10", "200"), ("2014-01-01", "20", "300")),
        PARTITION p2 VALUES [("2014-06-01", "100", "200"), ("2014-07-01", "100", "300"))
    )
    DISTRIBUTED BY HASH(k2) BUCKETS 32
    ```

5. 创建一个包含 HLL 和 BITMAP 列类型的聚合模型表

    ```sql
    CREATE TABLE example_db.example_table
    (
        k1 TINYINT,
        k2 DECIMAL(10, 2) DEFAULT "10.5",
        v1 HLL HLL_UNION,
        v2 BITMAP BITMAP_UNION
    )
    ENGINE=olap
    AGGREGATE KEY(k1, k2)
    DISTRIBUTED BY HASH(k1) BUCKETS 32
    ```

6. 创建两张同一个 Colocation Group 自维护的表。

    ```sql
    CREATE TABLE t1 (
        id int(11) COMMENT "",
        value varchar(8) COMMENT ""
    )
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 10
    PROPERTIES (
        "colocate_with" = "group1"
    );
    
    CREATE TABLE t2 (
        id int(11) COMMENT "",
        value1 varchar(8) COMMENT "",
        value2 varchar(8) COMMENT ""
    )
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 10
    PROPERTIES (
        "colocate_with" = "group1"
    );
    ```

7. 创建一个带有 bitmap 索引以及 bloom filter 索引的内存表

    ```sql
    CREATE TABLE example_db.table_hash
    (
        k1 TINYINT,
        k2 DECIMAL(10, 2) DEFAULT "10.5",
        v1 CHAR(10) REPLACE,
        v2 INT SUM,
        INDEX k1_idx (k1) USING BITMAP COMMENT 'my first index'
    )
    AGGREGATE KEY(k1, k2)
    DISTRIBUTED BY HASH(k1) BUCKETS 32
    PROPERTIES (
        "bloom_filter_columns" = "k2",
        "in_memory" = "true"
    );
    ```

8. 创建一个动态分区表。

    该表每天提前创建3天的分区，并删除3天前的分区。例如今天为`2020-01-08`，则会创建分区名为`p20200108`, `p20200109`, `p20200110`, `p20200111`的分区. 分区范围分别为:

    ```
    [types: [DATE]; keys: [2020-01-08]; ‥types: [DATE]; keys: [2020-01-09]; )
    [types: [DATE]; keys: [2020-01-09]; ‥types: [DATE]; keys: [2020-01-10]; )
    [types: [DATE]; keys: [2020-01-10]; ‥types: [DATE]; keys: [2020-01-11]; )
    [types: [DATE]; keys: [2020-01-11]; ‥types: [DATE]; keys: [2020-01-12]; )
    ```

    ```sql
    CREATE TABLE example_db.dynamic_partition
    (
        k1 DATE,
        k2 INT,
        k3 SMALLINT,
        v1 VARCHAR(2048),
        v2 DATETIME DEFAULT "2014-02-04 15:36:00"
    )
    DUPLICATE KEY(k1, k2, k3)
    PARTITION BY RANGE (k1) ()
    DISTRIBUTED BY HASH(k2) BUCKETS 32
    PROPERTIES(
        "dynamic_partition.time_unit" = "DAY",
        "dynamic_partition.start" = "-3",
        "dynamic_partition.end" = "3",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.buckets" = "32" 
    );
    ```

9. 创建一个带有物化视图（ROLLUP）的表。

    ```sql
    CREATE TABLE example_db.rolup_index_table
    (
        event_day DATE,
        siteid INT DEFAULT '10',
        citycode SMALLINT,
        username VARCHAR(32) DEFAULT '',
        pv BIGINT SUM DEFAULT '0'
    )
    AGGREGATE KEY(event_day, siteid, citycode, username)
    DISTRIBUTED BY HASH(siteid) BUCKETS 10
    ROLLUP (
        r1(event_day,siteid),
        r2(event_day,citycode),
        r3(event_day)
    )
    PROPERTIES("replication_num" = "3");
    ```

10. 通过 `replication_allocation` 属性设置表的副本。

    ```sql
    CREATE TABLE example_db.table_hash
    (
        k1 TINYINT,
        k2 DECIMAL(10, 2) DEFAULT "10.5"
    )
    DISTRIBUTED BY HASH(k1) BUCKETS 32
    PROPERTIES (
        "replication_allocation"="tag.location.group_a:1, tag.location.group_b:2"
    );


    CREATE TABLE example_db.dynamic_partition
    (
        k1 DATE,
        k2 INT,
        k3 SMALLINT,
        v1 VARCHAR(2048),
        v2 DATETIME DEFAULT "2014-02-04 15:36:00"
    )
    PARTITION BY RANGE (k1) ()
    DISTRIBUTED BY HASH(k2) BUCKETS 32
    PROPERTIES(
        "dynamic_partition.time_unit" = "DAY",
        "dynamic_partition.start" = "-3",
        "dynamic_partition.end" = "3",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.buckets" = "32",
        "dynamic_partition."replication_allocation" = "tag.location.group_a:3"
     );
    ```

### Keywords

    CREATE, TABLE

### Best Practice

#### 分区和分桶

一个表必须指定分桶列，但可以不指定分区。关于分区和分桶的具体介绍，可参阅 [数据划分](../../../../data-table/data-partition.html) 文档。

Doris 中的表可以分为分区表和无分区的表。这个属性在建表时确定，之后不可更改。即对于分区表，可以在之后的使用过程中对分区进行增删操作，而对于无分区的表，之后不能再进行增加分区等操作。

同时，分区列和分桶列在表创建之后不可更改，既不能更改分区和分桶列的类型，也不能对这些列进行任何增删操作。

所以建议在建表前，先确认使用方式来进行合理的建表。

#### 动态分区

动态分区功能主要用于帮助用户自动的管理分区。通过设定一定的规则，Doris 系统定期增加新的分区或删除历史分区。可参阅 [动态分区](../../../../advanced/partition/dynamic-partition.html) 文档查看更多帮助。

#### 物化视图

用户可以在建表的同时创建多个物化视图（ROLLUP）。物化视图也可以在建表之后添加。写在建表语句中可以方便用户一次性创建所有物化视图。

如果在建表时创建好物化视图，则后续的所有数据导入操作都会同步生成物化视图的数据。物化视图的数量可能会影响数据导入的效率。

如果在之后的使用过程中添加物化视图，如果表中已有数据，则物化视图的创建时间取决于当前数据量大小。

关于物化视图的介绍，请参阅文档 [物化视图](../../../../advanced/materialized-view.html)。

#### 索引

用户可以在建表的同时创建多个列的索引。索引也可以在建表之后再添加。

如果在之后的使用过程中添加索引，如果表中已有数据，则需要重写所有数据，因此索引的创建时间取决于当前数据量。

#### 内存表

当建表时指定了 `"in_memory" = "true"` 属性。则 Doris 会尽量将该表的数据块缓存在存储引擎的 PageCache 中，已减少磁盘IO。但这个属性不会保证数据块常驻在内存中，仅作为一种尽力而为的标识。
