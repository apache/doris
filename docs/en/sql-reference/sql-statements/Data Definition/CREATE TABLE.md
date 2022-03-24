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
    [ENGINE = [olap|mysql|broker|hive|iceberg]]
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
        BOOLEAN(1 Byte)
            Range: {0,1}
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
        QUANTILE_STATE
            QUANTILE_STATE type, No need to specify length. Represents the quantile pre-aggregation result. Currently, only numerical raw data types are supported such as `int`,`float`,`double`, etc.
            If the number of elements is less than 2048, the explict data is stored. 
            If the number of elements is greater than 2048, the intermediate result of the pre-aggregation of the TDigest algorithm is stored.
            
    ```
    agg_type: Aggregation type. If not specified, the column is key column. Otherwise, the column   is value column.

       * SUM、MAX、MIN、REPLACE
       * HLL_UNION: Only for HLL type
       * REPLACE_IF_NOT_NULL: The meaning of this aggregation type is that substitution will occur if and only if the newly imported data is a non-null value. If the newly imported data is null, Doris will still retain the original value. Note: if NOT NULL is specified in the REPLACE_IF_NOT_NULL column when the user creates the table, Doris will convert it to NULL and will not report an error to the user. Users can leverage this aggregate type to achieve importing some of columns .**It should be noted here that the default value should be NULL, not an empty string. If it is an empty string, you should replace it with an empty string**.
       * BITMAP_UNION: Only for BITMAP type
       * QUANTILE_UNION: Only for QUANTILE_STATE type
    Allow NULL: Default is NOT NULL. NULL value should be represented as `\N` in load source file.
    
    Notice: 
    
        The origin value of BITMAP_UNION column should be TINYINT, SMALLINT, INT, BIGINT.
        
        The origin value of QUANTILE_UNION column should be a numeric type such as TINYINT, INT, FLOAT, DOUBLE, DECIMAL, etc.
2. index_definition
    Syntax:
        `INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'`
    Explain:
        index_name: index name
        col_name: column name
    Notice:
        Only support BITMAP index in current version, BITMAP can only apply to single column
3. ENGINE type
    Default is olap. Options are: olap, mysql, broker, hive, iceberg
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

    4) For iceberg, properties should include:
        ```
        PROPERTIES (
            "iceberg.database" = "iceberg_db_name",
            "iceberg.table" = "iceberg_table_name",
            "iceberg.hive.metastore.uris" = "thrift://127.0.0.1:9083",
            "iceberg.catalog.type" = "HIVE_CATALOG"
            )

        ```
        database is the name of the database corresponding to Iceberg.  
        table is the name of the table corresponding to Iceberg.
        hive.metastore.uris is the address of the hive metastore service.  
        catalog.type defaults to HIVE_CATALOG. Currently, only HIVE_CATALOG is supported, more Iceberg catalog types will be supported later.
        
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
    Currently, both RANGE and LIST partitioning methods are supported.
    5.1 RANGE partition 
        RANGE Partition has two ways to use:
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
            Use the specified key column and the specified range of values for partitioning.
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

    5.2 LIST partition
        LIST partition is divided into single column partition and multi-column partition
        1) Single column partition
            Syntax.

            ```
                PARTITION BY LIST(k1)
                (
                PARTITION partition_name1 VALUES IN ("value1", "value2", ...) ,
                PARTITION partition_name2 VALUES IN ("value1", "value2", ...)
                ...
                )
            ```
        
            Explain:
                Use the specified key column and the formulated enumeration value for partitioning.
                1) Partition name only support [A-z0-9_]
                2) Partition key column's type should be:
                    BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, DATETIME, CHAR, VARCHAR
                3) Partition is a collection of enumerated values, partition values cannot be duplicated between partitions
                4) NULL values cannot be imported
                5) partition values cannot be defaulted, at least one must be specified
        
        2) Multi-column partition
            Syntax.
        
            ```
                PARTITION BY LIST(k1, k2)
                (
                PARTITION partition_name1 VALUES IN (("value1", "value2"), ("value1", "value2"), ...) ,
                PARTITION partition_name2 VALUES IN (("value1", "value2"), ("value1", "value2"), ...)
                ...
                )
            ```
        
            Explain:
                1) the partition of a multi-column partition is a collection of tuple enumeration values
                2) The number of tuple values per partition must be equal to the number of columns in the partition
                3) The other partitions are synchronized with the single column partition

6. distribution_desc
    1) Hash
       Syntax:
        `DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]`
       Explain:
         Hash bucketing using the specified key column.
    2) Random
       Syntax:
        `DISTRIBUTED BY RANDOM [BUCKETS num]`
       Explain:
         Use random numbers for bucketing.
    Suggestion: It is recommended to use random bucketing when there is no suitable key for hash bucketing to make the data of the table evenly distributed.   

7. PROPERTIES
    1) If ENGINE type is olap. User can specify storage medium, cooldown time and replication   number:

        ```
        PROPERTIES (
            "storage_medium" = "[SSD|HDD]",
            ["storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss"],
            ["replication_num" = "3"],
			["replication_allocation" = "xxx"]
            )
        ```
    
        storage_medium:         SSD or HDD, The default initial storage media can be specified by `default_storage_medium= XXX` in the fe configuration file `fe.conf`, or, if not, by default, HDD.
                                Note: when FE configuration 'enable_strict_storage_medium_check' is' True ', if the corresponding storage medium is not set in the cluster, the construction clause 'Failed to find enough host in all backends with storage medium is SSD|HDD'.
        storage_cooldown_time:  If storage_medium is SSD, data will be automatically moved to HDD   when timeout.
                                Default is 30 days.
                                Format: "yyyy-MM-dd HH:mm:ss"
        replication_num:        Replication number of a partition. Default is 3.
        replication_allocation:     Specify the distribution of replicas according to the resource tag.

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
    
    4) if you want to use the dynamic partitioning feature, specify it in properties. Note: Dynamic partitioning only supports RANGE partitions
    
        ```
        PROPERTIES (
            "dynamic_partition.enable" = "true|false",
            "dynamic_partition.time_unit" = "HOUR|DAY|WEEK|MONTH",
            "dynamic_partition.end" = "${integer_value}",
            "dynamic_partition.prefix" = "${string_value}",
            "dynamic_partition.buckets" = "${integer_value}
        )    
       ```
       
       dynamic_partition.enable: specifies whether dynamic partitioning at the table level is enabled
       dynamic_partition.time_unit: used to specify the time unit for dynamically adding partitions, which can be selected as HOUR, DAY, WEEK, and MONTH.
                                     Attention: When the time unit is HOUR, the data type of partition column cannot be DATE.
       dynamic_partition.end: used to specify the number of partitions created in advance
       dynamic_partition.prefix: used to specify the partition name prefix to be created, such as the partition name prefix p, automatically creates the partition name p20200108
       dynamic_partition.buckets: specifies the number of partition buckets that are automatically created
       dynamic_partition.create_history_partition: specifies whether create history partitions, default value is false
       dynamic_partition.history_partition_num: used to specify the number of history partitions when enable create_history_partition
       dynamic_partition.reserved_history_periods: Used to specify the range of reserved history periods
       
       ```
    5)  You can create multiple Rollups in bulk when building a table
    grammar:
    ```
      ROLLUP (rollup_name (column_name1, column_name2, ...)
                     [FROM from_index_name]
                      [PROPERTIES ("key"="value", ...)],...)
    ```
    
    6) if you want to use the inmemory table feature, specify it in properties
    
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
    AGGREGATE KEY(k1, k2)
    DISTRIBUTED BY HASH (k1, k2) BUCKETS 32
    PROPERTIES(
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2015-06-04 00:00:00"
    );
    ```

3. Create an olap table, with range partitioned, distributed by hash. Records with the same key exist at the same time, set the initial storage medium and cooling time, use default column storage.

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
4. Create an olap table, with list partitioned, distributed by hash. Records with the same key exist at the same time, set the initial storage medium and cooling time, use default column storage.

    1) Single column partition

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

    Explain:
    This statement will divide the data into 3 partitions as follows.

    ```
    ("1", "2", "3")
    ("4", "5", "6")
    ("7", "8", "9")
    ```

    Data that does not fall within these partition enumeration values will be filtered as illegal data

    2) Multi-column partition

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
    PARTITION p1 VALUES IN (("1", "beijing"), ("1", "shanghai")),
    PARTITION p2 VALUES IN (("2", "beijing"), ("2", "shanghai")),
    PARTITION p3 VALUES IN (("3", "beijing"), ("3", "shanghai"))
    )
    DISTRIBUTED BY HASH(k2) BUCKETS 32
    PROPERTIES(
    "storage_medium" = "SSD", "storage_cooldown_time" = "2022-06-04 00:00:00"
    );
    ```

    Explain:
    This statement will divide the data into 3 partitions as follows.

    ```
    (("1", "beijing"), ("1", "shanghai"))
    (("2", "beijing"), ("2", "shanghai"))
    (("3", "beijing"), ("3", "shanghai"))
    ```

    Data that is not within these partition enumeration values will be filtered as illegal data

5. Create a mysql table
   5.1 Create MySQL table directly from external table information
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

   5.2 Create MySQL table with external ODBC catalog resource
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

6. Create a broker table, with file on HDFS, line delimit by "|", column separated by "\n"

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

7. Create table will HLL column

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

8. Create a table will BITMAP_UNION column

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
9. Create a table with QUANTILE_UNION column (the origin value of **v1** and **v2** columns must be **numeric** types）
    
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
10. Create 2 colocate join table.

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

11. Create a broker table, with file on BOS.

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

12. Create a table with a bitmap index 

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
    
13. Create a dynamic partitioning table (dynamic partitioning needs to be enabled in FE configuration), which creates partitions 3 days in advance every day. For example, if today is' 2020-01-08 ', partitions named 'p20200108', 'p20200109', 'p20200110', 'p20200111' will be created.

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
        "dynamic_partition.end" = "3",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.buckets" = "32"
         );
     ```
14. Create a table with rollup index
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

15. Create a inmemory table:

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

16. Create a hive external table
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

17. Specify the replica distribution of the table through replication_allocation

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

17. Create an Iceberg external table

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
