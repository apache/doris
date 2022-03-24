---
{
    "title": "CREATE TABLE",
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

# CREATE TABLE

## description

该语句用于创建 table。
语法：

```
    CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name
    (column_definition1[, column_definition2, ...]
    [, index_definition1[, index_definition2, ...]])
    [ENGINE = [olap|mysql|broker|hive|iceberg]]
    [key_desc]
    [COMMENT "table comment"];
    [partition_desc]
    [distribution_desc]
    [rollup_index]
    [PROPERTIES ("key"="value", ...)]
    [BROKER PROPERTIES ("key"="value", ...)]
```

1. column_definition
    语法：
    `col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]`

    说明：
    col_name：列名称
    col_type：列类型

    ```
        BOOLEAN（1字节）
            范围：{0,1}
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
        DOUBLE（8字节）
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
            变长字符串。长度范围：1 ~ 65533
        HLL (1~16385个字节)
            hll列类型，不需要指定长度和默认值、长度根据数据的聚合
            程度系统内控制，并且HLL列只能通过配套的hll_union_agg、Hll_cardinality、hll_hash进行查询或使用
        BITMAP
            bitmap列类型，不需要指定长度和默认值。表示整型的集合，元素最大支持到2^64 - 1
        QUANTILE_STATE
            QUANTILE_STATE列类型，不需要指定长度和默认值，表示分位数预聚合结果。目前仅支持原始数据为数值类型如：TINYINT、INT、FLOAT、DOUBLE、DECIMAL。当元素个数小于2048时存储明细数据，当元素个数大于2048时存储 [TDigest](https://github.com/tdunning/t-digest/blob/main/docs/t-digest-paper/histo.pdf) 算法预聚合的中间结果
    ```

    agg_type：聚合类型，如果不指定，则该列为 key 列。否则，该列为 value 列
    
       * SUM、MAX、MIN、REPLACE
       * HLL_UNION(仅用于HLL列，为HLL独有的聚合方式)、
       * BITMAP_UNION(仅用于 BITMAP 列，为 BITMAP 独有的聚合方式)、
       * QUANTILE_UNION(仅用于 QUANTILE_STATE 列，为 QUANTILE_STATE 独有的聚合方式)
       * REPLACE_IF_NOT_NULL：这个聚合类型的含义是当且仅当新导入数据是非NULL值时会发生替换行为，如果新导入的数据是NULL，那么Doris仍然会保留原值。注意：如果用在建表时REPLACE_IF_NOT_NULL列指定了NOT NULL，那么Doris仍然会将其转化NULL，不会向用户报错。用户可以借助这个类型完成部分列导入的功能。**这里要注意的是字段默认值要给NULL，而不能是空字符串，如果是空字符串，会给你替换成空字符串**。
       * 该类型只对聚合模型(key_desc的type为AGGREGATE KEY)有用，其它模型不需要指这个。

    是否允许为NULL: 默认允许为 NULL。NULL 值在导入数据中用 \N 来表示

    注意：
    
        BITMAP_UNION聚合类型列在导入时的原始数据类型必须是TINYINT,SMALLINT,INT,BIGINT。     
        
        QUANTILE_UNION聚合类型列在导入时的原始数据类型必须是数值类型如:TINYINT、INT、FLOAT、DOUBLE、DECIMAL

2. index_definition
    语法：
        `INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'`
    说明：
        index_name：索引名称
        col_name：列名
    注意：
        当前仅支持BITMAP索引， BITMAP索引仅支持应用于单列

3. ENGINE 类型
    默认为 olap。可选 mysql, broker, hive, iceberg
    1) 如果是 mysql，则需要在 properties 提供以下信息：

```
    PROPERTIES (
        "host" = "mysql_server_host",
        "port" = "mysql_server_port",
        "user" = "your_user_name",
        "password" = "your_password",
        "database" = "database_name",
        "table" = "table_name"
        )
```

    注意：
        "table" 条目中的 "table_name" 是 mysql 中的真实表名。
        而 CREATE TABLE 语句中的 table_name 是该 mysql 表在 Doris 中的名字，可以不同。
    
    在 Doris 创建 mysql 表的目的是可以通过 Doris 访问 mysql 数据库。
        而 Doris 本身并不维护、存储任何 mysql 数据。
    2) 如果是 broker，表示表的访问需要通过指定的broker, 需要在 properties 提供以下信息：
        ```
        PROPERTIES (
        "broker_name" = "broker_name",
        "path" = "file_path1[,file_path2]",
        "column_separator" = "value_separator"
        "line_delimiter" = "value_delimiter"
        )
        ```
        另外还需要提供Broker需要的Property信息，通过BROKER PROPERTIES来传递，例如HDFS需要传入
        ```
        BROKER PROPERTIES(
            "username" = "name",
            "password" = "password"
        )
        ```
        这个根据不同的Broker类型，需要传入的内容也不相同
    注意：
        "path" 中如果有多个文件，用逗号[,]分割。如果文件名中包含逗号，那么使用 %2c 来替代。如果文件名中包含 %，使用 %25 代替
        现在文件内容格式支持CSV，支持GZ，BZ2，LZ4，LZO(LZOP) 压缩格式。
    
    3) 如果是 hive，则需要在 properties 提供以下信息：
    ```
    PROPERTIES (
        "database" = "hive_db_name",
        "table" = "hive_table_name",
        "hive.metastore.uris" = "thrift://127.0.0.1:9083"
    )
    
    ```
    其中 database 是 hive 表对应的库名字，table 是 hive 表的名字，hive.metastore.uris 是 hive metastore 服务地址。

    4）如果是 iceberg，则需要在 properties 中提供以下信息：
    ```
        PROPERTIES (
            "iceberg.database" = "iceberg_db_name",
            "iceberg.table" = "iceberg_table_name",
            "iceberg.hive.metastore.uris" = "thrift://127.0.0.1:9083",
            "iceberg.catalog.type" = "HIVE_CATALOG"
            )

    ```
    其中 database 是 Iceberg 对应的库名；  
    table 是 Iceberg 中对应的表名；
    hive.metastore.uris 是 hive metastore 服务地址；  
    catalog.type 默认为 HIVE_CATALOG。当前仅支持 HIVE_CATALOG，后续会支持更多 Iceberg catalog 类型。


4. key_desc
    语法：
        `key_type(k1[,k2 ...])`
    说明：
        数据按照指定的key列进行排序，且根据不同的key_type具有不同特性。
        key_type支持以下类型：
                AGGREGATE KEY:key列相同的记录，value列按照指定的聚合类型进行聚合，
                             适合报表、多维分析等业务场景。
                UNIQUE KEY:key列相同的记录，value列按导入顺序进行覆盖，
                             适合按key列进行增删改查的点查询业务。
                DUPLICATE KEY:key列相同的记录，同时存在于Doris中，
                             适合存储明细数据或者数据无聚合特性的业务场景。
        默认为DUPLICATE KEY，key列为列定义中前36个字节, 如果前36个字节的列数小于3，将使用前三列。
    注意：
        除AGGREGATE KEY外，其他key_type在建表时，value列不需要指定聚合类型。

5. partition_desc
    目前支持 RANGE 和 LIST 两种分区方式。
    5.1 RANGE 分区
        RANGE partition描述有两种使用方式
        1) LESS THAN
            语法：

            ```
                PARTITION BY RANGE (k1, k2, ...)
                (
                PARTITION partition_name1 VALUES LESS THAN MAXVALUE|("value1", "value2", ...),
                PARTITION partition_name2 VALUES LESS THAN MAXVALUE|("value1", "value2", ...)
                ...
                )
            ```
            
            说明：
                使用指定的 key 列和指定的数值范围进行分区。
                1) 分区名称仅支持字母开头，字母、数字和下划线组成
                2) 目前仅支持以下类型的列作为 Range 分区列
                    TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, DATETIME
                3) 分区为左闭右开区间，首个分区的左边界为做最小值
                4) NULL 值只会存放在包含最小值的分区中。当包含最小值的分区被删除后，NULL 值将无法导入。
                5) 可以指定一列或多列作为分区列。如果分区值缺省，则会默认填充最小值。
            
            注意：
                1) 分区一般用于时间维度的数据管理
                2) 有数据回溯需求的，可以考虑首个分区为空分区，以便后续增加分区
        
        2）Fixed Range
            语法：
            ```
                PARTITION BY RANGE (k1, k2, k3, ...)
                (
                PARTITION partition_name1 VALUES [("k1-lower1", "k2-lower1", "k3-lower1",...), ("k1-upper1", "k2-upper1", "k3-upper1", ...)),
                PARTITION partition_name2 VALUES [("k1-lower1-2", "k2-lower1-2", ...), ("k1-upper1-2", MAXVALUE, ))
                "k3-upper1-2", ...
                )
            ```
            说明：
                1）Fixed Range比LESS THAN相对灵活些，左右区间完全由用户自己确定
                2）其他与LESS THAN保持同步

    5.2 LIST 分区
        LIST partition分为单列分区和多列分区
        1) 单列分区
            语法：

            ```
                PARTITION BY LIST(k1)
                (
                PARTITION partition_name1 VALUES IN ("value1", "value2", ...),
                PARTITION partition_name2 VALUES IN ("value1", "value2", ...)
                ...
                )
            ```
        
            说明：
                使用指定的 key 列和制定的枚举值进行分区。
                1) 分区名称仅支持字母开头，字母、数字和下划线组成
                2) 目前仅支持以下类型的列作为 List 分区列
                    BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, DATETIME, CHAR, VARCHAR
                3) 分区为枚举值集合，各个分区之间分区值不能重复
                4) 不可导入 NULL 值
                5) 分区值不能缺省，必须指定至少一个
        
        2) 多列分区
            语法：
        
            ```
                PARTITION BY LIST(k1, k2)
                (
                PARTITION partition_name1 VALUES IN (("value1", "value2"), ("value1", "value2"), ...),
                PARTITION partition_name2 VALUES IN (("value1", "value2"), ("value1", "value2"), ...)
                ...
                )
            ```
        
            说明：
                1) 多列分区的分区是元组枚举值的集合
                2) 每个元组值的个数必须与分区列个数相等
                3) 其他与单列分区保持同步

6. distribution_desc
        1) Hash 分桶
        语法：
            `DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]`
        说明：
            使用指定的 key 列进行哈希分桶。
        2) Random 分桶
        语法：
            `DISTRIBUTED BY RANDOM [BUCKETS num]`
        说明：
            使用随机数进行分桶。  
    建议: 当没有合适的key做哈希分桶使得表的数据均匀分布的时候，建议使用RANDOM分桶方式。

7. PROPERTIES
    1) 如果 ENGINE 类型为 olap
           可以在 properties 设置该表数据的初始存储介质、存储到期时间和副本数。

    ```
       PROPERTIES (
           "storage_medium" = "[SSD|HDD]",
           ["storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss"],
           ["replication_num" = "3"]
           ["replication_allocation" = "xxx"]
           )
    ```

       storage_medium：        用于指定该分区的初始存储介质，可选择 SSD 或 HDD。默认初始存储介质可通过fe的配置文件 `fe.conf` 中指定 `default_storage_medium=xxx`，如果没有指定，则默认为 HDD。
                               注意：当FE配置项 `enable_strict_storage_medium_check` 为 `True` 时，若集群中没有设置对应的存储介质时，建表语句会报错 `Failed to find enough host in all backends with storage medium is SSD|HDD`. 
       storage_cooldown_time： 当设置存储介质为 SSD 时，指定该分区在 SSD 上的存储到期时间。
                               默认存放 30 天。
                               格式为："yyyy-MM-dd HH:mm:ss"
       replication_num:        指定分区的副本数。默认为 3。
       replication_allocation:     按照资源标签来指定副本分布。
    
       当表为单分区表时，这些属性为表的属性。
           当表为两级分区时，这些属性为附属于每一个分区。
           如果希望不同分区有不同属性。可以通过 ADD PARTITION 或 MODIFY PARTITION 进行操作

    2) 如果 Engine 类型为 olap, 可以指定某列使用 bloom filter 索引
           bloom filter 索引仅适用于查询条件为 in 和 equal 的情况，该列的值越分散效果越好
           目前只支持以下情况的列:除了 TINYINT FLOAT DOUBLE 类型以外的 key 列及聚合方法为 REPLACE 的 value 列

```
       PROPERTIES (
           "bloom_filter_columns"="k1,k2,k3"
           )
```

    3) 如果希望使用 Colocate Join 特性，需要在 properties 中指定

```
       PROPERTIES (
           "colocate_with"="table1"
           )
```

    4) 如果希望使用动态分区特性，需要在properties 中指定。注意：动态分区只支持 RANGE 分区

```
      PROPERTIES (
          "dynamic_partition.enable" = "true|false",
          "dynamic_partition.time_unit" = "HOUR|DAY|WEEK|MONTH",
          "dynamic_partition.start" = "${integer_value}",
          "dynamic_partition.end" = "${integer_value}",
          "dynamic_partition.prefix" = "${string_value}",
          "dynamic_partition.buckets" = "${integer_value}
```
    dynamic_partition.enable: 用于指定表级别的动态分区功能是否开启。默认为 true。
    dynamic_partition.time_unit: 用于指定动态添加分区的时间单位，可选择为HOUR（小时），DAY（天），WEEK(周)，MONTH（月）。
                                 注意：以小时为单位的分区列，数据类型不能为 DATE。
    dynamic_partition.start: 用于指定向前删除多少个分区。值必须小于0。默认为 Integer.MIN_VALUE。
    dynamic_partition.end: 用于指定提前创建的分区数量。值必须大于0。
    dynamic_partition.prefix: 用于指定创建的分区名前缀，例如分区名前缀为p，则自动创建分区名为p20200108
    dynamic_partition.buckets: 用于指定自动创建的分区分桶数量
    dynamic_partition.create_history_partition: 用于创建历史分区功能是否开启。默认为 false。
    dynamic_partition.history_partition_num: 当开启创建历史分区功能时，用于指定创建历史分区数量。
    dynamic_partition.reserved_history_periods: 用于指定保留的历史分区的时间段。
    
    5) 建表时可以批量创建多个 Rollup
    语法：
    ```
        ROLLUP (rollup_name (column_name1, column_name2, ...)
               [FROM from_index_name]
                [PROPERTIES ("key"="value", ...)],...)
    ```
    
    6) 如果希望使用 内存表 特性，需要在 properties 中指定

```
        PROPERTIES (
           "in_memory"="true"
        )   
```
    当 in_memory 属性为 true 时，Doris会尽可能将该表的数据和索引Cache到BE 内存中
    
    7) 创建UNIQUE_KEYS表时，可以指定一个sequence列，当KEY列相同时，将按照sequence列进行REPLACE(较大值替换较小值，否则无法替换)

```
        PROPERTIES (
            "function_column.sequence_type" = 'Date',
        );
```
    sequence_type用来指定sequence列的类型，可以为整型和时间类型
## example

1. 创建一个 olap 表，使用 HASH 分桶，使用列存，相同key的记录进行聚合

    ```
    CREATE TABLE example_db.table_hash
    (
    k1 BOOLEAN,
    k2 TINYINT,
    k3 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM
    )
    ENGINE=olap
    AGGREGATE KEY(k1, k2, k3)
    COMMENT "my first doris table"
    DISTRIBUTED BY HASH(k1) BUCKETS 32;
    ```

2. 创建一个 olap 表，使用 Hash 分桶，使用列存，相同key的记录进行覆盖，
   设置初始存储介质和冷却时间

   ```
    CREATE TABLE example_db.table_hash
    (
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048) REPLACE,
    v2 SMALLINT SUM DEFAULT "10"
    )
    ENGINE=olap
    AGGREGATE KEY(k1, k2)
    DISTRIBUTED BY HASH (k1, k2) BUCKETS 32
    PROPERTIES(
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2015-06-04 00:00:00"
    );
   ```

3. 创建一个 olap 表，使用 Range 分区，使用Hash分桶，默认使用列存，
   相同key的记录同时存在，设置初始存储介质和冷却时间

    1）LESS THAN

    ```
    CREATE TABLE example_db.table_range
    (
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
    )
    ENGINE=olap
    DUPLICATE KEY(k1, k2, k3)
    PARTITION BY RANGE (k1)
    (
    PARTITION p1 VALUES LESS THAN ("2014-01-01"),
    PARTITION p2 VALUES LESS THAN ("2014-06-01"),
    PARTITION p3 VALUES LESS THAN ("2014-12-01")
    )
    DISTRIBUTED BY HASH(k2) BUCKETS 32
    PROPERTIES(
    "storage_medium" = "SSD", "storage_cooldown_time" = "2015-06-04 00:00:00"
    );
    ```

    说明：
    这个语句会将数据划分成如下3个分区：

    ```
    ( {    MIN     },   {"2014-01-01"} )
    [ {"2014-01-01"},   {"2014-06-01"} )
    [ {"2014-06-01"},   {"2014-12-01"} )
    ```

    不在这些分区范围内的数据将视为非法数据被过滤

   2) Fixed Range

    ```
    CREATE TABLE table_range
    (
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
    )
    ENGINE=olap
    DUPLICATE KEY(k1, k2, k3)
    PARTITION BY RANGE (k1, k2, k3)
    (
    PARTITION p1 VALUES [("2014-01-01", "10", "200"), ("2014-01-01", "20", "300")),
    PARTITION p2 VALUES [("2014-06-01", "100", "200"), ("2014-07-01", "100", "300"))
    )
    DISTRIBUTED BY HASH(k2) BUCKETS 32
    PROPERTIES(
    "storage_medium" = "SSD"
    );
    ```

4. 创建一个 olap 表，使用 List 分区，使用Hash分桶，默认使用列存，
   相同key的记录同时存在，设置初始存储介质和冷却时间

    1）单列分区

    ```
    CREATE TABLE example_db.table_list
    (
    k1 INT,
    k2 VARCHAR(128),
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
    )
    ENGINE=olap
    DUPLICATE KEY(k1, k2, k3)
    PARTITION BY LIST (k1)
    (
    PARTITION p1 VALUES IN ("1", "2", "3"),
    PARTITION p2 VALUES IN ("4", "5", "6"),
    PARTITION p3 VALUES IN ("7", "8", "9")
    )
    DISTRIBUTED BY HASH(k2) BUCKETS 32
    PROPERTIES(
    "storage_medium" = "SSD", "storage_cooldown_time" = "2022-06-04 00:00:00"
    );
    ```

    说明：
    这个语句会将数据划分成如下3个分区：

    ```
    ("1", "2", "3")
    ("4", "5", "6")
    ("7", "8", "9")
    ```

    不在这些分区枚举值内的数据将视为非法数据被过滤

    2) 多列分区

    ```
    CREATE TABLE example_db.table_list
    (
    k1 INT,
    k2 VARCHAR(128),
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
    )
    ENGINE=olap
    DUPLICATE KEY(k1, k2, k3)
    PARTITION BY LIST (k1, k2)
    (
    PARTITION p1 VALUES IN (("1","beijing"), ("1", "shanghai")),
    PARTITION p2 VALUES IN (("2","beijing"), ("2", "shanghai")),
    PARTITION p3 VALUES IN (("3","beijing"), ("3", "shanghai"))
    )
    DISTRIBUTED BY HASH(k2) BUCKETS 32
    PROPERTIES(
    "storage_medium" = "SSD", "storage_cooldown_time" = "2022-06-04 00:00:00"
    );
    ```

    说明：
    这个语句会将数据划分成如下3个分区：

    ```
    (("1","beijing"), ("1", "shanghai"))
    (("2","beijing"), ("2", "shanghai"))
    (("3","beijing"), ("3", "shanghai"))
    ```

    不在这些分区枚举值内的数据将视为非法数据被过滤

5. 创建一个 mysql 表

   5.1 直接通过外表信息创建mysql表
```
    CREATE EXTERNAL TABLE example_db.table_mysql
    (
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
    )
    ENGINE=mysql
    PROPERTIES
    (
    "host" = "127.0.0.1",
    "port" = "8239",
    "user" = "mysql_user",
    "password" = "mysql_passwd",
    "database" = "mysql_db_test",
    "table" = "mysql_table_test"
    )
```

   5.2 通过External Catalog Resource创建mysql表
```
   CREATE EXTERNAL RESOURCE "mysql_resource" 
   PROPERTIES
   (
     "type" = "odbc_catalog",
     "user" = "mysql_user",
     "password" = "mysql_passwd",
     "host" = "127.0.0.1",
      "port" = "8239"			
   );
```
```
    CREATE EXTERNAL TABLE example_db.table_mysql
    (
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
    )
    ENGINE=mysql
    PROPERTIES
    (
    "odbc_catalog_resource" = "mysql_resource",
    "database" = "mysql_db_test",
    "table" = "mysql_table_test"
    )
```

6. 创建一个数据文件存储在HDFS上的 broker 外部表, 数据使用 "|" 分割，"\n" 换行

```
    CREATE EXTERNAL TABLE example_db.table_broker (
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
    )
    ENGINE=broker
    PROPERTIES (
    "broker_name" = "hdfs",
    "path" = "hdfs://hdfs_host:hdfs_port/data1,hdfs://hdfs_host:hdfs_port/data2,hdfs://hdfs_host:hdfs_port/data3%2c4",
    "column_separator" = "|",
    "line_delimiter" = "\n"
    )
    BROKER PROPERTIES (
    "username" = "hdfs_user",
    "password" = "hdfs_password"
    )
```

7. 创建一张含有HLL列的表

```
    CREATE TABLE example_db.example_table
    (
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 HLL HLL_UNION,
    v2 HLL HLL_UNION
    )
    ENGINE=olap
    AGGREGATE KEY(k1, k2)
    DISTRIBUTED BY HASH(k1) BUCKETS 32;
```

8. 创建一张含有BITMAP_UNION聚合类型的表（v1和v2列的原始数据类型必须是TINYINT,SMALLINT,INT）

```
    CREATE TABLE example_db.example_table
    (
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 BITMAP BITMAP_UNION,
    v2 BITMAP BITMAP_UNION
    )
    ENGINE=olap
    AGGREGATE KEY(k1, k2)
    DISTRIBUTED BY HASH(k1) BUCKETS 32;
```

1. 创建一张含有QUANTILE_UNION聚合类型的表（v1和v2列的原始数据类型必须是数值类型）

```
    CREATE TABLE example_db.example_table
    (
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 QUANTILE_STATE QUANTILE_UNION,
    v2 QUANTILE_STATE QUANTILE_UNION
    )
    ENGINE=olap
    AGGREGATE KEY(k1, k2)
    DISTRIBUTED BY HASH(k1) BUCKETS 32;
```

10. 创建两张支持Colocate Join的表t1 和t2

```
    CREATE TABLE `t1` (
    `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 10
    PROPERTIES (
    "colocate_with" = "t1"
    );

    CREATE TABLE `t2` (
    `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 10
    PROPERTIES (
    "colocate_with" = "t1"
    );
```

11. 创建一个数据文件存储在BOS上的 broker 外部表

```
    CREATE EXTERNAL TABLE example_db.table_broker (
    k1 DATE
    )
    ENGINE=broker
    PROPERTIES (
    "broker_name" = "bos",
    "path" = "bos://my_bucket/input/file",
    )
    BROKER PROPERTIES (
      "bos_endpoint" = "http://bj.bcebos.com",
      "bos_accesskey" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
      "bos_secret_accesskey"="yyyyyyyyyyyyyyyyyyyy"
    )
```

12. 创建一个带有bitmap 索引的表

```
    CREATE TABLE example_db.table_hash
    (
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM,
    INDEX k1_idx (k1) USING BITMAP COMMENT 'xxxxxx'
    )
    ENGINE=olap
    AGGREGATE KEY(k1, k2)
    COMMENT "my first doris table"
    DISTRIBUTED BY HASH(k1) BUCKETS 32;
```

13. 创建一个动态分区表(需要在FE配置中开启动态分区功能)，该表每天提前创建3天的分区，并删除3天前的分区。例如今天为`2020-01-08`，则会创建分区名为`p20200108`, `p20200109`, `p20200110`, `p20200111`的分区. 分区范围分别为: 

```
[types: [DATE]; keys: [2020-01-08]; ‥types: [DATE]; keys: [2020-01-09]; )
[types: [DATE]; keys: [2020-01-09]; ‥types: [DATE]; keys: [2020-01-10]; )
[types: [DATE]; keys: [2020-01-10]; ‥types: [DATE]; keys: [2020-01-11]; )
[types: [DATE]; keys: [2020-01-11]; ‥types: [DATE]; keys: [2020-01-12]; )
```

```
    CREATE TABLE example_db.dynamic_partition
    (
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
    )
    ENGINE=olap
    DUPLICATE KEY(k1, k2, k3)
    PARTITION BY RANGE (k1) ()
    DISTRIBUTED BY HASH(k2) BUCKETS 32
    PROPERTIES(
    "storage_medium" = "SSD",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32"
     );
```

14. 创建一个带有rollup索引的表
```
    CREATE TABLE example_db.rollup_index_table
    (
        event_day DATE,
        siteid INT DEFAULT '10',
        citycode SMALLINT,
        username VARCHAR(32) DEFAULT '',
        pv BIGINT SUM DEFAULT '0'
    )
    AGGREGATE KEY(event_day, siteid, citycode, username)
    DISTRIBUTED BY HASH(siteid) BUCKETS 10
    rollup (
    r1(event_day,siteid),
    r2(event_day,citycode),
    r3(event_day)
    )
    PROPERTIES("replication_num" = "3");
```
15. 创建一个内存表

```
    CREATE TABLE example_db.table_hash
    (
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM,
    INDEX k1_idx (k1) USING BITMAP COMMENT 'xxxxxx'
    )
    ENGINE=olap
    AGGREGATE KEY(k1, k2)
    COMMENT "my first doris table"
    DISTRIBUTED BY HASH(k1) BUCKETS 32
    PROPERTIES ("in_memory"="true");
```

16. 创建一个hive外部表

```
    CREATE TABLE example_db.table_hive
    (
      k1 TINYINT,
      k2 VARCHAR(50),
      v INT
    )
    ENGINE=hive
    PROPERTIES
    (
      "database" = "hive_db_name",
      "table" = "hive_table_name",
      "hive.metastore.uris" = "thrift://127.0.0.1:9083"
    );
```

17. 通过 replication_allocation 指定表的副本分布

```	
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

17. 创建一个 Iceberg 外表

```
    CREATE TABLE example_db.t_iceberg 
    ENGINE=ICEBERG
    PROPERTIES (
    "iceberg.database" = "iceberg_db",
    "iceberg.table" = "iceberg_table",
    "iceberg.hive.metastore.uris"  =  "thrift://127.0.0.1:9083",
    "iceberg.catalog.type"  =  "HIVE_CATALOG"
    );
```

## keyword

    CREATE,TABLE
