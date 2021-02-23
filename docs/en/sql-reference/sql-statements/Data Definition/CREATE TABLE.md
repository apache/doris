---
{
    "title": "CREATE TABLE",
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

# CREATE TABLE

## description

This statement is used to create table
Syntax:

```
    CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name
    (column_definition1[, column_definition2, ...]
    [, index_definition1[, ndex_definition12,]])
    [ENGINE = [olap|mysql|broker|hive]]
    [key_desc]
    [COMMENT "table comment"]
    [partition_desc]
    [distribution_desc]
    [rollup_index]
    [PROPERTIES ("key"="value", ...)]
    [BROKER PROPERTIES ("key"="value", ...)];
```

1. column_definition
    Syntax:
    `col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]`
    Explain:
    col_name: Name of column
    col_type: Type of column
    ```
        TINYINT(1 Byte)
            Range: -2^7 + 1 ~ 2^7 - 1
        SMALLINT(2 Bytes)
            Range: -2^15 + 1 ~ 2^15 - 1
        INT(4 Bytes)
            Range: -2^31 + 1 ~ 2^31 - 1
        BIGINT(8 Bytes)
            Range: -2^63 + 1 ~ 2^63 - 1
        LARGEINT(16 Bytes)
            Range: -2^127 + 1 ~ 2^127 - 1
        FLOAT(4 Bytes)
            Support scientific notation
        DOUBLE(8 Bytes)
            Support scientific notation
        DECIMAL[(precision, scale)] (16 Bytes)
            Default is DECIMAL(10, 0)
            precision: 1 ~ 27
            scale: 0 ~ 9
            integer part: 1 ~ 18
            fractional part: 0 ~ 9
            Not support scientific notation
        DATE(3 Bytes)
            Range: 0000-01-01 ~ 9999-12-31
        DATETIME(8 Bytes)
            Range: 0000-01-01 00:00:00 ~ 9999-12-31 23:59:59
        CHAR[(length)]
            Fixed length string. Range: 1 ~ 255. Default: 1
        VARCHAR[(length)]
            Variable length string. Range: 1 ~ 65533
        HLL (1~16385 Bytes)
            HLL tpye, No need to specify length.
            This type can only be queried by hll_union_agg, hll_cardinality, hll_hash functions.
        BITMAP
            BITMAP type, No need to specify length. Represent a set of unsigned bigint numbers, the largest element could be 2^64 - 1
    ```
    agg_type: Aggregation type. If not specified, the column is key column. Otherwise, the column   is value column.
       * SUM、MAX、MIN、REPLACE
       * HLL_UNION: Only for HLL type
       * REPLACE_IF_NOT_NULL: The meaning of this aggregation type is that substitution will   occur if and only if the newly imported data is a non-null value. If the newly imported   data is null, Doris will still retain the original value. Note: if NOT NULL is specified  in the REPLACE_IF_NOT_NULL column when the user creates the table, Doris will convert it     to NULL and will not report an error to the user. Users can leverage this aggregate type    to achieve importing some of columns.
       * BITMAP_UNION: Only for BITMAP type
    Allow NULL: Default is NOT NULL. NULL value should be represented as `\N` in load source file.
    Notice:  
    
        The origin value of BITMAP_UNION column should be TINYINT, SMALLINT, INT, BIGINT.
2. index_definition
    Syntax:
        `INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'`
    Explain:
        index_name：index name
        col_name：column name
    Notice:
        Only support BITMAP index in current version, BITMAP can only apply to single column
3. ENGINE type
    Default is olap. Options are: olap, mysql, broker, hive
    1) For mysql, properties should include:

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

    Notice:
        "table_name" is the real table name in MySQL database.
        table_name in CREATE TABLE stmt is table is Doris. They can be different or same.
        MySQL table created in Doris is for accessing data in MySQL database.
        Doris does not maintain and store any data from MySQL table.
    2) For broker, properties should include:

        ```
        PROPERTIES (
            "broker_name" = "broker_name",
            "path" = "file_path1[,file_path2]",
            "column_separator" = "value_separator"
            "line_delimiter" = "value_delimiter"
        )
        ```
    
        ```
        BROKER PROPERTIES(
            "username" = "name",
            "password" = "password"
        )
        ```
    
        For different broker, the broker properties are different
    Notice:
        Files name in "path" is separated by ",". If file name includes ",", use "%2c" instead.     If file name includes "%", use "%25" instead.
        Support CSV and Parquet. Support GZ, BZ2, LZ4, LZO(LZOP)
    3) For hive, properties should include:
        ```
        PROPERTIES (
            "database" = "hive_db_name",
            "table" = "hive_table_name",
            "hive.metastore.uris" = "thrift://127.0.0.1:9083"
        )
        ```
        "database" is the name of the database corresponding to the hive table, "table" is the name of the hive table, and "hive.metastore.uris" is the hive metastore service address.
        Notice: At present, hive external tables are only used for Spark Load and query is not supported.
4. key_desc
    Syntax:
        key_type(k1[,k2 ...])
    Explain:
        Data is order by specified key columns. And has different behaviors for different key  desc.
            AGGREGATE KEY:
                    value columns will be aggregated is key columns are same.
            UNIQUE KEY:
                    The new incoming rows will replace the old rows if key columns are same.
            DUPLICATE KEY:
                    All incoming rows will be saved.
        the default key_type is DUPLICATE KEY, and key columns are first 36 bytes of the columns    in define order.
         If the number of columns in the first 36 is less than 3, the first 3 columns will be   used.
    NOTICE:
        Except for AGGREGATE KEY, no need to specify aggregation type for value columns.
5. partition_desc
    Partition has two ways to use:
    1) LESS THAN
    Syntax:

        ```
        PARTITION BY RANGE (k1, k2, ...)
        (
        PARTITION partition_name1 VALUES LESS THAN MAXVALUE|("value1", "value2", ...),
        PARTITION partition_name2 VALUES LESS THAN MAXVALUE|("value1", "value2", ...)
        ...
        )
        ```

    Explain:
        1) Partition name only support [A-z0-9_]
        2) Partition key column's type should be:
            TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, DATETIME
        3) The range is [closed, open). And the lower bound of first partition is MIN VALUE of  specified column type.
        4) NULL values should be save in partition which includes MIN VALUE.
        5) Support multi partition columns, the the default partition value is MIN VALUE.
    2）Fixed Range
    Syntax:
        ```
        PARTITION BY RANGE (k1, k2, k3, ...)
        (
        PARTITION partition_name1 VALUES [("k1-lower1", "k2-lower1", "k3-lower1",...),  ("k1-upper1", "k2-upper1", "k3-upper1", ...)),
        PARTITION partition_name2 VALUES [("k1-lower1-2", "k2-lower1-2", ...), ("k1-upper1-2",  MAXVALUE, ))
        "k3-upper1-2", ...
        )
        ```
    Explain:
        1）The Fixed Range is more flexible than the LESS THAN, and the left and right intervals    are completely determined by the user.
        2）Others are consistent with LESS THAN.
6. distribution_desc
    1) Hash
    Syntax:
        `DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]`
    Explain:
        The default buckets is 10.
7. PROPERTIES
    1) If ENGINE type is olap. User can specify storage medium, cooldown time and replication   number:

        ```
        PROPERTIES (
            "storage_medium" = "[SSD|HDD]",
            ["storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss"],
            ["replication_num" = "3"]
            )
        ```
    
        storage_medium:         SSD or HDD, The default initial storage media can be specified by `default_storage_medium= XXX` in the fe configuration file `fe.conf`, or, if not, by default, HDD.
                                Note: when FE configuration 'enable_strict_storage_medium_check' is' True ', if the corresponding storage medium is not set in the cluster, the construction clause 'Failed to find enough host in all backends with storage medium is SSD|HDD'.
        storage_cooldown_time:  If storage_medium is SSD, data will be automatically moved to HDD   when timeout.
                                Default is 30 days.
                                Format: "yyyy-MM-dd HH:mm:ss"
        replication_num:        Replication number of a partition. Default is 3.
        If table is not range partitions. This property takes on Table level. Or it will takes on   Partition level.
        User can specify different properties for different partition by `ADD PARTITION` or     `MODIFY PARTITION` statements.
    2) If Engine type is olap, user can set bloom filter index for column.
        Bloom filter index will be used when query contains `IN` or `EQUAL`.
        Bloom filter index support key columns with type except TINYINT FLOAT DOUBLE, also  support value with REPLACE aggregation type.

        ```
        PROPERTIES (
            "bloom_filter_columns"="k1,k2,k3"
        )
        ```

    3) For Colocation Join:

        ```
        PROPERTIES (
            "colocate_with"="table1"
        )
        ```
    
    4) if you want to use the dynamic partitioning feature, specify it in properties
    
        ```
        PROPERTIES (
            "dynamic_partition.enable" = "true|false",
            "dynamic_partition.time_unit" = "HOUR|DAY|WEEK|MONTH",
            "dynamic_partitoin.end" = "${integer_value}",
            "dynamic_partition.prefix" = "${string_value}",
            "dynamic_partition.buckets" = "${integer_value}
        )    
       ```
       
       Dynamic_partition. Enable: specifies whether dynamic partitioning at the table level is enabled
       
       Dynamic_partition. Time_unit: used to specify the time unit for dynamically adding partitions, which can be selected as HOUR, DAY, WEEK, and MONTH.
                                     Attention: When the time unit is HOUR, the data type of partition column cannot be DATE.
       
       Dynamic_partition. End: used to specify the number of partitions created in advance
       
       Dynamic_partition. Prefix: used to specify the partition name prefix to be created, such as the partition name prefix p, automatically creates the partition name p20200108
       
       Dynamic_partition. Buckets: specifies the number of partition buckets that are automatically created
       ```
8. rollup_index
    grammar:
    ```
      ROLLUP (rollup_name (column_name1, column_name2, ...)
                     [FROM from_index_name]
                      [PROPERTIES ("key"="value", ...)],...)
    ```

    5) if you want to use the inmemory table feature, specify it in properties

        ```
        PROPERTIES (
           "in_memory"="true"
        )   
        ```
## example

1. Create an olap table, distributed by hash, with aggregation type.

    ```
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
    ```

2. Create an olap table, distributed by hash, with aggregation type. Also set storage medium and cooldown time.

    ```
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
    "storage_type"="column",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2015-06-04 00:00:00"
    );
    ```

3. Create an olap table, with range partitioned, distributed by hash.

1) LESS THAN

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
    
    Explain:
    This statement will create 3 partitions:
    
    ```
    ( {    MIN     },   {"2014-01-01"} )
    [ {"2014-01-01"},   {"2014-06-01"} )
    [ {"2014-06-01"},   {"2014-12-01"} )
    ```
    
    Data outside these ranges will not be loaded.

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

4. Create a mysql table
   4.1 Create MySQL table directly from external table information
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

   4.2 Create MySQL table with external ODBC catalog resource
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

5. Create a broker table, with file on HDFS, line delimit by "|", column separated by "\n"

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
    );
    ```

6. Create table will HLL column

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

7. Create a table will BITMAP_UNION column

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

8. Create 2 colocate join table.

    ```
    CREATE TABLE `t1` (
    `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 10
    PROPERTIES (
    "colocate_with" = "group1"
    );
    CREATE TABLE `t2` (
    `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 10
    PROPERTIES (
    "colocate_with" = "group1"
    );
    ```

9. Create a broker table, with file on BOS.

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
    );
    ```

10. Create a table with a bitmap index 

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
    PROPERTIES ("storage_type"="column");
    ```
    
11. Create a dynamic partitioning table (dynamic partitioning needs to be enabled in FE configuration), which creates partitions 3 days in advance every day. For example, if today is' 2020-01-08 ', partitions named 'p20200108', 'p20200109', 'p20200110', 'p20200111' will be created.

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
        PARTITION BY RANGE (k1)
        (
        PARTITION p1 VALUES LESS THAN ("2014-01-01"),
        PARTITION p2 VALUES LESS THAN ("2014-06-01"),
        PARTITION p3 VALUES LESS THAN ("2014-12-01")
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 32
        PROPERTIES(
        "storage_medium" = "SSD",
        "dynamic_partition.time_unit" = "DAY",
        "dynamic_partition.end" = "3",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.buckets" = "32"
         );
     ```
12. Create a table with rollup index
```
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
    rollup (
    r1(event_day,siteid),
    r2(event_day,citycode),
    r3(event_day)
    )
    PROPERTIES("replication_num" = "3");
```

13. Create a inmemory table:

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

14. Create a hive external table
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

## keyword

    CREATE,TABLE
