---
{
    "title": "CREATE-TABLE",
    "language": "zh-CN",
    "toc_min_heading_level": 2,
    "toc_max_heading_level": 4
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

该命令用于创建一张表。本文档主要介绍创建 Doris 自维护的表的语法。外部表语法请参阅 [CREATE-EXTERNAL-TABLE](./CREATE-EXTERNAL-TABLE.md)文档。

```sql
CREATE TABLE [IF NOT EXISTS] [database.]table
(
    column_definition_list
    [, index_definition_list]
)
[engine_type]
[keys_type]
[table_comment]
[partition_info]
distribution_desc
[rollup_list]
[properties]
[extra_properties]
```

#### column_definition_list

列定义列表：

`column_definition[, column_definition]`
* `column_definition`
    列定义：

    `column_name column_type [KEY] [aggr_type] [NULL] [AUTO_INCREMENT] [default_value] [column_comment]`
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
            保证精度的小数类型。默认是 DECIMAL(9, 0)
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
            变长字符串。长度范围：1 ~ 65533。默认为65533
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
        REPLACE：替换。对于维度列相同的行，指标列会按照导入的先后顺序，后导入的替换先导入的。
        REPLACE_IF_NOT_NULL：非空值替换。和 REPLACE 的区别在于对于null值，不做替换。这里要注意的是字段默认值要给NULL，而不能是空字符串，如果是空字符串，会给你替换成空字符串。
        HLL_UNION：HLL 类型的列的聚合方式，通过 HyperLogLog 算法聚合。
        BITMAP_UNION：BIMTAP 类型的列的聚合方式，进行位图的并集聚合。
        ```
    * `AUTO_INCREMENT`(仅在master分支可用)
            
        是否为自增列，自增列可以用来为新插入的行生成一个唯一标识。在插入表数据时如果没有指定自增列的值，则会自动生成一个合法的值。当自增列被显示地插入NULL时，其值也会被替换为生成的合法值。需要注意的是，处于性能考虑，BE会在内存中缓存部分自增列的值，所以自增列自动生成的值只能保证单调性和唯一性，无法保证严格的连续性。
        一张表中至多有一个列是自增列，自增列必须是BIGINT类型，且必须为NOT NULL。
        Duplicate模型表和Unique模型表均支持自增列。

  * `default_value`
        列默认值，当导入数据未指定该列的值时，系统将赋予该列default_value。
          
        语法为`default default_value`。
          
        当前default_value支持两种形式：
        1. 用户指定固定值，如：
        ```SQL
            k1 INT DEFAULT '1',
            k2 CHAR(10) DEFAULT 'aaaa'
        ```
        2. 系统提供的关键字，目前支持以下关键字：
          
        ```SQL
            // 只用于DATETIME类型，导入数据缺失该值时系统将赋予当前时间
            dt DATETIME DEFAULT CURRENT_TIMESTAMP
        ```
      
  示例：
      
  ```text
  k1 TINYINT,
  k2 DECIMAL(10,2) DEFAULT "10.5",
  k4 BIGINT NULL DEFAULT "1000" COMMENT "This is column k4",
  v1 VARCHAR(10) REPLACE NOT NULL,
  v2 BITMAP BITMAP_UNION,
  v3 HLL HLL_UNION,
  v4 INT SUM NOT NULL DEFAULT "1" COMMENT "This is column v4"
  ```
    
#### index_definition_list

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

#### engine_type

表引擎类型。本文档中类型皆为 OLAP。其他外部表引擎类型见 [CREATE EXTERNAL TABLE](./CREATE-EXTERNAL-TABLE.md) 文档。示例：
    
    `ENGINE=olap`
    
#### keys_type

数据模型。

`key_type(col1, col2, ...)`

`key_type` 支持以下模型：

* DUPLICATE KEY（默认）：其后指定的列为排序列。
* AGGREGATE KEY：其后指定的列为维度列。
* UNIQUE KEY：其后指定的列为主键列。

<version since="2.0">
注：当表属性`enable_duplicate_without_keys_by_default = true`时, 默认创建没有排序列的DUPLICATE表。
</version>

示例：

```
DUPLICATE KEY(col1, col2),
AGGREGATE KEY(k1, k2, k3),
UNIQUE KEY(k1, k2)
```
    
#### table_comment

表注释。示例：
    
    ```
    COMMENT "This is my first DORIS table"
    ```

#### partition_info

分区信息，支持三种写法：

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

3. <version since="1.2" type="inline"> MULTI RANGE：批量创建RANGE分区，定义分区的左闭右开区间，设定时间单位和步长，时间单位支持年、月、日、周和小时。</version>

    ```
    PARTITION BY RANGE(col)
    (
       FROM ("2000-11-14") TO ("2021-11-14") INTERVAL 1 YEAR,
       FROM ("2021-11-14") TO ("2022-11-14") INTERVAL 1 MONTH,
       FROM ("2022-11-14") TO ("2023-01-03") INTERVAL 1 WEEK,
       FROM ("2023-01-03") TO ("2023-01-14") INTERVAL 1 DAY
    )
    ```

4. MULTI RANGE：批量创建数字类型的RANGE分区，定义分区的左闭右开区间，设定步长。

    ```
    PARTITION BY RANGE(int_col)
    (
        FROM (1) TO (100) INTERVAL 10
    )
    ```


#### distribution_desc

定义数据分桶方式。

1. Hash 分桶
   语法：
      `DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num|auto]`
   说明：
      使用指定的 key 列进行哈希分桶。
2. Random 分桶
   语法：
      `DISTRIBUTED BY RANDOM [BUCKETS num|auto]`
   说明：
      使用随机数进行分桶。 

#### rollup_list

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

#### properties

设置表属性。目前支持以下属性：

* `replication_num`

    副本数。默认副本数为3。如果 BE 节点数量小于3，则需指定副本数小于等于 BE 节点数量。

    在 0.15 版本后，该属性将自动转换成 `replication_allocation` 属性，如：

    `"replication_num" = "3"` 会自动转换成 `"replication_allocation" = "tag.location.default:3"`

* `replication_allocation`

    根据 Tag 设置副本分布情况。该属性可以完全覆盖 `replication_num` 属性的功能。

* `is_being_synced`  

    用于标识此表是否是被CCR复制而来并且正在被syncer同步，默认为 `false`。  

    如果设置为 `true`：  
    `colocate_with`，`storage_policy`属性将被擦除  
    `dynamic partition`，`auto bucket`功能将会失效，即在`show create table`中显示开启状态，但不会实际生效。当`is_being_synced`被设置为 `false` 时，这些功能将会恢复生效。  

    这个属性仅供CCR外围模块使用，在CCR同步的过程中不要手动设置。

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

    已弃用。只支持设置为'false'。

* `compression`

    Doris 表的默认压缩方式是 LZ4。1.1版本后，支持将压缩方式指定为ZSTD以获得更高的压缩比。

    `"compression"="zstd"`

* `function_column.sequence_col`

    当使用 UNIQUE KEY 模型时，可以指定一个sequence列，当KEY列相同时，将按照 sequence 列进行 REPLACE(较大值替换较小值，否则无法替换)

    `function_column.sequence_col`用来指定sequence列到表中某一列的映射，该列可以为整型和时间类型（DATE、DATETIME），创建后不能更改该列的类型。如果设置了`function_column.sequence_col`, `function_column.sequence_type`将被忽略。

    `"function_column.sequence_col" = 'column_name'`

* `function_column.sequence_type`

    当使用 UNIQUE KEY 模型时，可以指定一个sequence列，当KEY列相同时，将按照 sequence 列进行 REPLACE(较大值替换较小值，否则无法替换)

    这里我们仅需指定顺序列的类型，支持时间类型或整型。Doris 会创建一个隐藏的顺序列。

    `"function_column.sequence_type" = 'Date'`

* `light_schema_change`

    <version since="1.2" type="inline"> 是否使用light schema change优化。</version>

    如果设置成 `true`, 对于值列的加减操作，可以更快地，同步地完成。

    `"light_schema_change" = 'true'`

    该功能在 2.0.0 及之后版本默认开启。

* `disable_auto_compaction`

    是否对这个表禁用自动compaction。

    如果这个属性设置成 `true`, 后台的自动compaction进程会跳过这个表的所有tablet。

    `"disable_auto_compaction" = "false"`

* `enable_single_replica_compaction`

    是否对这个表开启单副本 compaction。

    如果这个属性设置成 `true`, 这个表的 tablet 的所有副本只有一个 do compaction，其他的从该副本拉取 rowset

    `"enable_single_replica_compaction" = "false"`

* `enable_duplicate_without_keys_by_default`

    当配置为`true`时，如果创建表的时候没有指定Unique、Aggregate或Duplicate时，会默认创建一个没有排序列和前缀索引的Duplicate模型的表。

    `"enable_duplicate_without_keys_by_default" = "false"`

* `skip_write_index_on_load`

    是否对这个表开启数据导入时不写索引.

    如果这个属性设置成 `true`, 数据导入的时候不写索引（目前仅对倒排索引生效），而是在compaction的时候延迟写索引。这样可以避免首次写入和compaction
    重复写索引的CPU和IO资源消耗，提升高吞吐导入的性能。

    `"skip_write_index_on_load" = "false"`

* `compaction_policy`

    配置这个表的 compaction 的合并策略，仅支持配置为 time_series 或者 size_based

    time_series: 当 rowset 的磁盘体积积攒到一定大小时进行版本合并。合并后的 rowset 直接晋升到 base compaction 阶段。在时序场景持续导入的情况下有效降低 compact 的写入放大率

    此策略将使用 time_series_compaction 为前缀的参数调整 compaction 的执行

    `"compaction_policy" = ""`

* `time_series_compaction_goal_size_mbytes`

    compaction 的合并策略为 time_series 时，将使用此参数来调整每次 compaction 输入的文件的大小，输出的文件大小和输入相当

    `"time_series_compaction_goal_size_mbytes" = "1024"`

* `time_series_compaction_file_count_threshold`

    compaction 的合并策略为 time_series 时，将使用此参数来调整每次 compaction 输入的文件数量的最小值

    一个 tablet 中，文件数超过该配置，就会触发 compaction

    `"time_series_compaction_file_count_threshold" = "2000"`

* `time_series_compaction_time_threshold_seconds`

    compaction 的合并策略为 time_series 时，将使用此参数来调整 compaction 的最长时间间隔，即长时间未执行过 compaction 时，就会触发一次 compaction，单位为秒

    `"time_series_compaction_time_threshold_seconds" = "3600"`

* 动态分区相关

    动态分区相关参数如下：

    * `dynamic_partition.enable`: 用于指定表级别的动态分区功能是否开启。默认为 true。
    * `dynamic_partition.time_unit:` 用于指定动态添加分区的时间单位，可选择为DAY（天），WEEK(周)，MONTH（月），YEAR（年），HOUR（时）。
    * `dynamic_partition.start`: 用于指定向前删除多少个分区。值必须小于0。默认为 Integer.MIN_VALUE。
    * `dynamic_partition.end`: 用于指定提前创建的分区数量。值必须大于0。
    * `dynamic_partition.prefix`: 用于指定创建的分区名前缀，例如分区名前缀为p，则自动创建分区名为p20200108。
    * `dynamic_partition.buckets`: 用于指定自动创建的分区分桶数量。
    * `dynamic_partition.create_history_partition`: 是否创建历史分区。
    * `dynamic_partition.history_partition_num`: 指定创建历史分区的数量。
    * `dynamic_partition.reserved_history_periods`: 用于指定保留的历史分区的时间段。

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
        PARTITION p2 VALUES LESS THAN ("2020-03-01"),
        PARTITION p3 VALUES LESS THAN ("2020-04-01")
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
        v1 VARCHAR(2048),
        v2 SMALLINT DEFAULT "10"
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

7. 创建一个带有 bitmap 索引以及 bloom filter 索引的表

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
        "bloom_filter_columns" = "k2"
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
    PARTITION BY RANGE (k1) ()
    DISTRIBUTED BY HASH(k2) BUCKETS 32
    PROPERTIES(
        "dynamic_partition.time_unit" = "DAY",
        "dynamic_partition.start" = "-3",
        "dynamic_partition.end" = "3",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.buckets" = "32",
        "dynamic_partition.replication_allocation" = "tag.location.group_a:3"
     );
    ```

11. 通过`storage_policy`属性设置表的冷热分层数据迁移策略
    ```sql
        CREATE TABLE IF NOT EXISTS create_table_use_created_policy 
        (
            k1 BIGINT,
            k2 LARGEINT,
            v1 VARCHAR(2048)
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH (k1) BUCKETS 3
        PROPERTIES(
            "storage_policy" = "test_create_table_use_policy",
            "replication_num" = "1"
        );
    ```
注：需要先创建s3 resource 和 storage policy，表才能关联迁移策略成功

12. 为表的分区添加冷热分层数据迁移策略
    ```sql
        CREATE TABLE create_table_partion_use_created_policy
        (
            k1 DATE,
            k2 INT,
            V1 VARCHAR(2048) REPLACE
        ) PARTITION BY RANGE (k1) (
            PARTITION p1 VALUES LESS THAN ("2022-01-01") ("storage_policy" = "test_create_table_partition_use_policy_1" ,"replication_num"="1"),
            PARTITION p2 VALUES LESS THAN ("2022-02-01") ("storage_policy" = "test_create_table_partition_use_policy_2" ,"replication_num"="1")
        ) DISTRIBUTED BY HASH(k2) BUCKETS 1;
    ```
注：需要先创建s3 resource 和 storage policy，表才能关联迁移策略成功

<version since="1.2.0">

13. 批量创建分区
    ```sql
        CREATE TABLE create_table_multi_partion_date
        (
            k1 DATE,
            k2 INT,
            V1 VARCHAR(20)
        ) PARTITION BY RANGE (k1) (
            FROM ("2000-11-14") TO ("2021-11-14") INTERVAL 1 YEAR,
            FROM ("2021-11-14") TO ("2022-11-14") INTERVAL 1 MONTH,
            FROM ("2022-11-14") TO ("2023-01-03") INTERVAL 1 WEEK,
            FROM ("2023-01-03") TO ("2023-01-14") INTERVAL 1 DAY,
            PARTITION p_20230114 VALUES [('2023-01-14'), ('2023-01-15'))
        ) DISTRIBUTED BY HASH(k2) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1"
        );
    ```
    ```sql
        CREATE TABLE create_table_multi_partion_date_hour
        (
            k1 DATETIME,
            k2 INT,
            V1 VARCHAR(20)
        ) PARTITION BY RANGE (k1) (
            FROM ("2023-01-03 12") TO ("2023-01-14 22") INTERVAL 1 HOUR
        ) DISTRIBUTED BY HASH(k2) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1"
        );
    ```
    ```sql
        CREATE TABLE create_table_multi_partion_integer
        (
            k1 BIGINT,
            k2 INT,
            V1 VARCHAR(20)
        ) PARTITION BY RANGE (k1) (
            FROM (1) TO (100) INTERVAL 10
        ) DISTRIBUTED BY HASH(k2) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1"
        );
    ```

注：批量创建分区可以和常规手动创建分区混用，使用时需要限制分区列只能有一个，批量创建分区实际创建默认最大数量为4096，这个参数可以在fe配置项 `max_multi_partition_num` 调整

</version>

<version since="2.0">

14. 批量无排序列Duplicate表

    ```sql
    CREATE TABLE example_db.table_hash
    (
        k1 DATE,
        k2 DECIMAL(10, 2) DEFAULT "10.5",
        k3 CHAR(10) COMMENT "string column",
        k4 INT NOT NULL DEFAULT "1" COMMENT "int column"
    )
    COMMENT "duplicate without keys"
    PARTITION BY RANGE(k1)
    (
        PARTITION p1 VALUES LESS THAN ("2020-02-01"),
        PARTITION p2 VALUES LESS THAN ("2020-03-01"),
        PARTITION p3 VALUES LESS THAN ("2020-04-01")
    )
    DISTRIBUTED BY HASH(k1) BUCKETS 32
    PROPERTIES (
        "replication_num" = "1",
        "enable_duplicate_without_keys_by_default" = "true"
    );
    ```

</version>

### Keywords

    CREATE, TABLE

### Best Practice

#### 分区和分桶

一个表必须指定分桶列，但可以不指定分区。关于分区和分桶的具体介绍，可参阅 [数据划分](../../../../data-table/data-partition.md) 文档。

Doris 中的表可以分为分区表和无分区的表。这个属性在建表时确定，之后不可更改。即对于分区表，可以在之后的使用过程中对分区进行增删操作，而对于无分区的表，之后不能再进行增加分区等操作。

同时，分区列和分桶列在表创建之后不可更改，既不能更改分区和分桶列的类型，也不能对这些列进行任何增删操作。

所以建议在建表前，先确认使用方式来进行合理的建表。

#### 动态分区

动态分区功能主要用于帮助用户自动的管理分区。通过设定一定的规则，Doris 系统定期增加新的分区或删除历史分区。可参阅 [动态分区](../../../../advanced/partition/dynamic-partition.md) 文档查看更多帮助。

#### 物化视图

用户可以在建表的同时创建多个物化视图（ROLLUP）。物化视图也可以在建表之后添加。写在建表语句中可以方便用户一次性创建所有物化视图。

如果在建表时创建好物化视图，则后续的所有数据导入操作都会同步生成物化视图的数据。物化视图的数量可能会影响数据导入的效率。

如果在之后的使用过程中添加物化视图，如果表中已有数据，则物化视图的创建时间取决于当前数据量大小。

关于物化视图的介绍，请参阅文档 [物化视图](../../../../query-acceleration/materialized-view.md)。

#### 索引

用户可以在建表的同时创建多个列的索引。索引也可以在建表之后再添加。

如果在之后的使用过程中添加索引，如果表中已有数据，则需要重写所有数据，因此索引的创建时间取决于当前数据量。

