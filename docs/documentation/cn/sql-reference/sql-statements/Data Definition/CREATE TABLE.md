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
        CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name
        (column_definition1[, column_definition2, ...])
        [ENGINE = [olap|mysql|broker]]
        [key_desc]
        [COMMENT "table comment"];
        [partition_desc]
        [distribution_desc]
        [PROPERTIES ("key"="value", ...)]
        [BROKER PROPERTIES ("key"="value", ...)]
        
    1. column_definition
        语法：
        col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
        
        说明：
        col_name：列名称
        col_type：列类型
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
                                范围：1900-01-01 ~ 9999-12-31
                            DATETIME（8字节）
                                范围：1900-01-01 00:00:00 ~ 9999-12-31 23:59:59
                            CHAR[(length)]
                                定长字符串。长度范围：1 ~ 255。默认为1
                            VARCHAR[(length)]
                                变长字符串。长度范围：1 ~ 65533
                            HLL (1~16385个字节)
                                hll列类型，不需要指定长度和默认值、长度根据数据的聚合
                                程度系统内控制，并且HLL列只能通过配套的hll_union_agg、Hll_cardinality、hll_hash进行查询或使用
                            BITMAP
                                bitmap 列类型，不需要指定长度和默认值
                                BITMAP 列只能通过配套的 BITMAP_UNION、BITMAP_COUNT、TO_BITMAP 进行查询或使用
                                
        agg_type：聚合类型，如果不指定，则该列为 key 列。否则，该列为 value 列
                            
                            * SUM、MAX、MIN、REPLACE
                            * HLL_UNION(仅用于HLL列，为HLL独有的聚合方式)、
                            * BITMAP_UNION(仅用于 BITMAP 列，为 BITMAP 独有的聚合方式)、
                            * REPLACE_IF_NOT_NULL：这个聚合类型的含义是当且仅当新导入数据是非NULL值时才会发生替换行为，如果新导入的数据是NULL，那么Doris仍然会保留原值。注意：如果用户在建表时REPLACE_IF_NOT_NULL列指定了NOT NULL，那么Doris仍然会将其转化为NULL，不会向用户报错。用户可以借助这个类型完成部分列导入的功能。
                            *该类型只对聚合模型(key_desc的type为AGGREGATE KEY)有用，其它模型不需要指定这个。

        是否允许为NULL: 默认不允许为 NULL。NULL 值在导入数据中用 \N 来表示

        注意： 
            BITMAP_UNION聚合类型列在导入时的原始数据类型必须是TINYINT,SMALLINT,INT。

    2. ENGINE 类型
        默认为 olap。可选 mysql, broker
        1) 如果是 mysql，则需要在 properties 提供以下信息：
        
            PROPERTIES (
            "host" = "mysql_server_host",
            "port" = "mysql_server_port",
            "user" = "your_user_name",
            "password" = "your_password",
            "database" = "database_name",
            "table" = "table_name"
            )
        
        注意：
            "table" 条目中的 "table_name" 是 mysql 中的真实表名。
            而 CREATE TABLE 语句中的 table_name 是该 mysql 表在 Palo 中的名字，可以不同。
            
            在 Palo 创建 mysql 表的目的是可以通过 Palo 访问 mysql 数据库。
            而 Palo 本身并不维护、存储任何 mysql 数据。
        2) 如果是 broker，表示表的访问需要通过指定的broker, 需要在 properties 提供以下信息：
            PROPERTIES (
            "broker_name" = "broker_name",
            "path" = "file_path1[,file_path2]",
            "column_separator" = "value_separator"
            "line_delimiter" = "value_delimiter"
            )
            另外还需要提供Broker需要的Property信息，通过BROKER PROPERTIES来传递，例如HDFS需要传入
            BROKER PROPERTIES(
                "username" = "name", 
                "password" = "password"
            )
            这个根据不同的Broker类型，需要传入的内容也不相同
        注意：
            "path" 中如果有多个文件，用逗号[,]分割。如果文件名中包含逗号，那么使用 %2c 来替代。如果文件名中包含 %，使用 %25 代替
            现在文件内容格式支持CSV，支持GZ，BZ2，LZ4，LZO(LZOP) 压缩格式。
    
    3. key_desc
        语法：
            key_type(k1[,k2 ...])
        说明：
            数据按照指定的key列进行排序，且根据不同的key_type具有不同特性。
            key_type支持一下类型：
                    AGGREGATE KEY:key列相同的记录，value列按照指定的聚合类型进行聚合，
                                 适合报表、多维分析等业务场景。
                    UNIQUE KEY:key列相同的记录，value列按导入顺序进行覆盖，
                                 适合按key列进行增删改查的点查询业务。
                    DUPLICATE KEY:key列相同的记录，同时存在于Palo中，
                                 适合存储明细数据或者数据无聚合特性的业务场景。
            默认为DUPLICATE KEY，key列为列定义中前36个字节, 如果前36个字节的列数小于3，将使用前三列。
        注意：
            除AGGREGATE KEY外，其他key_type在建表时，value列不需要指定聚合类型。

    4. partition_desc
        partition描述有两种使用方式
        1) LESS THAN 
        语法：
            PARTITION BY RANGE (k1, k2, ...)
            (
            PARTITION partition_name1 VALUES LESS THAN MAXVALUE|("value1", "value2", ...),
            PARTITION partition_name2 VALUES LESS THAN MAXVALUE|("value1", "value2", ...)
            ...
            )
        说明：
            使用指定的 key 列和指定的数值范围进行分区。
            1) 分区名称仅支持字母开头，字母、数字和下划线组成
            2) 目前仅支持以下类型的列作为 Range 分区列，且只能指定一个分区列
                TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, DATETIME
            3) 分区为左闭右开区间，首个分区的左边界为做最小值
            4) NULL 值只会存放在包含最小值的分区中。当包含最小值的分区被删除后，NULL 值将无法导入。
            5) 可以指定一列或多列作为分区列。如果分区值缺省，则会默认填充最小值。
                             
        注意：
            1) 分区一般用于时间维度的数据管理
            2) 有数据回溯需求的，可以考虑首个分区为空分区，以便后续增加分区
            
        2）Fixed Range
        语法：
            PARTITION BY RANGE (k1, k2, k3, ...)
            (
            PARTITION partition_name1 VALUES [("k1-lower1", "k2-lower1", "k3-lower1",...), ("k1-upper1", "k2-upper1", "k3-upper1", ...)),
            PARTITION partition_name2 VALUES [("k1-lower1-2", "k2-lower1-2", ...), ("k1-upper1-2", MAXVALUE, ))
            "k3-upper1-2", ...
            )
        说明：
            1）Fixed Range比LESS THAN相对灵活些，左右区间完全由用户自己确定
            2）其他与LESS THAN保持同步

    5. distribution_desc
        1) Hash 分桶
        语法：
            DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
        说明：
            使用指定的 key 列进行哈希分桶。默认分区数为10

        建议:建议使用Hash分桶方式

    6. PROPERTIES
        1) 如果 ENGINE 类型为 olap，则可以在 properties 中指定列存（目前我们仅支持列存）

            PROPERTIES (
            "storage_type" = "[column]",
            )
        
        2) 如果 ENGINE 类型为 olap
           可以在 properties 设置该表数据的初始存储介质、存储到期时间和副本数。
           
           PROPERTIES (
           "storage_medium" = "[SSD|HDD]",
           ["storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss"],
           ["replication_num" = "3"]
           )
           
           storage_medium：        用于指定该分区的初始存储介质，可选择 SSD 或 HDD。默认为 HDD。
           storage_cooldown_time： 当设置存储介质为 SSD 时，指定该分区在 SSD 上的存储到期时间。
                                   默认存放 7 天。
                                   格式为："yyyy-MM-dd HH:mm:ss"
           replication_num:        指定分区的副本数。默认为 3
           
           当表为单分区表时，这些属性为表的属性。
           当表为两级分区时，这些属性为附属于每一个分区。
           如果希望不同分区有不同属性。可以通过 ADD PARTITION 或 MODIFY PARTITION 进行操作

        3) 如果 Engine 类型为 olap, 并且 storage_type 为 column, 可以指定某列使用 bloom filter 索引
           bloom filter 索引仅适用于查询条件为 in 和 equal 的情况，该列的值越分散效果越好
           目前只支持以下情况的列:除了 TINYINT FLOAT DOUBLE 类型以外的 key 列及聚合方法为 REPLACE 的 value 列
           
           PROPERTIES (
           "bloom_filter_columns"="k1,k2,k3"
           )
        4) 如果希望使用Colocate Join 特性，需要在 properties 中指定

           PROPERTIES (
           "colocate_with"="table1"
           )
    
## example
    1. 创建一个 olap 表，使用 HASH 分桶，使用列存，相同key的记录进行聚合
        CREATE TABLE example_db.table_hash
        (
        k1 TINYINT,
        k2 DECIMAL(10, 2) DEFAULT "10.5",
        v1 CHAR(10) REPLACE,
        v2 INT SUM
        )
        ENGINE=olap
        AGGREGATE KEY(k1, k2)
        COMMENT "my first doris table"
        DISTRIBUTED BY HASH(k1) BUCKETS 32
        PROPERTIES ("storage_type"="column");
        
    2. 创建一个 olap 表，使用 Hash 分桶，使用列存，相同key的记录进行覆盖，
       设置初始存储介质和冷却时间
        CREATE TABLE example_db.table_hash
        (
        k1 BIGINT,
        k2 LARGEINT,
        v1 VARCHAR(2048) REPLACE,
        v2 SMALLINT SUM DEFAULT "10"
        )
        ENGINE=olap
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH (k1, k2) BUCKETS 32
        PROPERTIES(
        "storage_type"="column"，
        "storage_medium" = "SSD",
        "storage_cooldown_time" = "2015-06-04 00:00:00"
        );
    
    3. 创建一个 olap 表，使用 Range 分区，使用Hash分桶，默认使用列存，
       相同key的记录同时存在，设置初始存储介质和冷却时间
       
    1）LESS THAN
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
        
        说明：
        这个语句会将数据划分成如下3个分区：
        ( {    MIN     },   {"2014-01-01"} )
        [ {"2014-01-01"},   {"2014-06-01"} )
        [ {"2014-06-01"},   {"2014-12-01"} )
        
        不在这些分区范围内的数据将视为非法数据被过滤
        
    2) Fixed Range
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

    4. 创建一个 mysql 表
        CREATE TABLE example_db.table_mysql
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
        
    5. 创建一个数据文件存储在HDFS上的 broker 外部表, 数据使用 "|" 分割，"\n" 换行
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

    6. 创建一张含有HLL列的表
        CREATE TABLE example_db.example_table
        (
        k1 TINYINT,
        k2 DECIMAL(10, 2) DEFAULT "10.5",
        v1 HLL HLL_UNION,
        v2 HLL HLL_UNION
        )
        ENGINE=olap
        AGGREGATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 32
        PROPERTIES ("storage_type"="column");

    7. 创建一张含有BITMAP_UNION聚合类型的表（v1和v2列的原始数据类型必须是TINYINT,SMALLINT,INT）
        CREATE TABLE example_db.example_table
        (
        k1 TINYINT,
        k2 DECIMAL(10, 2) DEFAULT "10.5",
        v1 BITMAP BITMAP_UNION,
        v2 BITMAP BITMAP_UNION
        )
        ENGINE=olap
        AGGREGATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 32
        PROPERTIES ("storage_type"="column");

    8. 创建两张支持Colocat Join的表t1 和t2
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

    9. 创建一个数据文件存储在BOS上的 broker 外部表
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

## keyword
    CREATE,TABLE
        
