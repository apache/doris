# CREATE TABLE
Description
This statement is used to create a table.
Grammar:
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name
(column_definition1[, column_definition2, ...])
[ENGINE = [olap|mysql|broker]]
[key squeaks]
[Distriction & UDESC]
[Distribution & UDESC]
[PROPERTIES ("key"="value", ...)];
[BROKER PROPERTIES ("key"="value", ...)];

1. column_definition
Grammar:
col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]

Explain:
Col_name: Column name
Col_type: Column type
TINYINT (1 byte)
Scope: - 2 ^ 7 + 1 - 2 ^ 7 - 1
SMALLINT (2 bytes)
Scope: - 2 ^ 15 + 1 - 2 ^ 15 - 1
INT (4 bytes)
Scope: - 2 ^ 31 + 1 - 2 ^ 31 - 1
BIGINT (8 bytes)
Scope: - 2 ^ 63 + 1 - 2 ^ 63 - 1
LARGEINT (16 bytes)
Scope: 0 - 2 ^ 127 - 1
FLOAT (4 bytes)
Supporting scientific counting
DOUBLE (12 bytes)
Supporting scientific counting
Decima [(precision, scale)] (4023383; 33410)
The decimal type guaranteeing accuracy. Default is DECIMAL (10, 0)
Precision: 1 ~27
scale: 0 ~ 9
The integer part is 1 - 18
No support for scientific counting
DATE (3 bytes)
Scope: 1900-01-01-9999-12-31
DATETIME (8 bytes)
Scope: 1900-01:00:00-9999-12-31:23:59:59
CHAR[(length)]
Fixed-length string. Length range: 1 - 255. Default 1
VARCHAR[(length)]
Variable length string. Length range: 1 - 65533
HLL (1 ~1638520010;* 33410s)
HLL column type, no need to specify length and default value, length aggregation based on data
Degree system internal control, and HLL columns can only be queried or used by matching hll_union_agg, Hll_cardinality, hll_hash

Agg_type: The aggregation type, if not specified, is listed as the key column. Otherwise, it is listed as value column
SUM, MAX, MIN, REPLACE, HLL_UNION (only for HLL columns, unique aggregation of HLL)
This type is only useful for aggregation models (the type of key_desc is AGGREGATE KEY), and other models do not need to specify this.

Whether NULL is allowed or not: NULL is not allowed by default. NULL values are represented in imported data byN

2. ENGINE 类型
The default is olap. Optional mysql, broker
1) If it is mysql, you need to provide the following information in properties:

PROPERTIES (
"host" ="mysql server" host,
"port" = "mysql_server_port",
"user" = "your_user_name",
"password" = "your_password",
"database" ="database" u name,
"table" = "table_name"
)

Be careful:
The "table_name" in the "table" entry is the real table name in mysql.
The table_name in the CREATE TABLE statement is the name of the MySQL table in Palo, which can be different.

The purpose of creating MySQL tables in Palo is to access the MySQL database through Palo.
Palo itself does not maintain or store any MySQL data.
2) If it is a broker, it means that the access to tables needs to be through the specified broker, and the following information needs to be provided in properties:
PROPERTIES (
"broker"u name "="broker "u name",
"paths" = "file_path1[,file_path2]",
Columbus@U separator="value@u separator"
"line_delimiter" = "value_delimiter"
)
In addition, you need to provide the property information Broker needs to pass through BROKER PROPERTIES, such as HDFS needs to be imported.
BROKER PROPERTIES (
"Username" = "name"
"password" = "password"
)
Depending on the Broker type, the content that needs to be passed in is different.
Be careful:
If there are multiple files in "paths", split them with commas [,]. If the file name contains commas, use% 2C instead. If the file name is included, use% 25 instead.
Now the file content format supports CSV, GZ, BZ2, LZ4, LZO (LZOP) compression format.

THREE. Key u descu
Grammar:
Key type (k1 [,k2...])
Explain:
The data is sorted according to the specified key column and has different characteristics according to different key_types.
Key_type supports some types:
AGGREGATE KEY: The key column has the same record, and the value column aggregates according to the specified aggregation type.
Suitable for business scenarios such as report, multi-dimensional analysis, etc.
UNIQUE KEY: The key column has the same record, and the value column is overwritten in the import order.
It is suitable for the point query business of adding, deleting and modifying by key column.
DUPLICATE KEY: Key column has the same record and exists in Palo.
Suitable for business scenarios where detailed data is stored or data is not aggregated.
Be careful:
Except for AGGREGATE KEY, other key_types do not require the value column to specify the aggregation type when building tables.

Four Division
1) Range 分区
Grammar:
PARTITION BY RANGE (k1, k2, ...)
(
PARTITION partition_name VALUES LESS THAN MAXVALUE|("value1", "value2", ...)
PARTITION partition_name VALUES LESS THAN MAXVALUE|("value1", "value2", ...)
...
)
Explain:
Partitioning using the specified key column and the specified range of values.
1) The partition name only supports letters at the beginning, letters, numbers, and underscores
2) Currently, only the following types of columns are supported as Range partition columns, and only one partition column can be specified.
Tinyint, smallint, int, bigint, largeinet, date, date
3) The partition is left-closed and right-open, and the left boundary of the first partition is the minimum.
4) NULL values are stored only in partitions containing minimum values. When the partition containing the minimum value is deleted, the NULL value cannot be imported.
5) You can specify one or more columns as partition columns. If the partition value is default, the minimum value is filled by default.

Be careful:
1) Partitions are generally used for data management in time dimension
2) If there is a need for data backtracking, the first partition can be considered as an empty partition in order to increase the number of partitions in the future.

Five distribution
(1) Hash -20998;` 26742;
Grammar:
DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
Explain:
Hash buckets using the specified key column. The default partition number is 10

Suggestion: Hash bucket dividing method is recommended.

6. PROPERTIES
1) If the ENGINE type is olap, you can specify a column store in properties (currently we only support column store)

PROPERTIES (
"storage_type" = "[column]",
)

2) If ENGINE type is OLAP
You can set the initial storage medium, storage expiration time, and number of copies of the table data in properties.

PROPERTIES (
"storage_medium" = "[SSD|HDD]",
["Storage = U cooldown = time" = "YYY-MM-DD HH: mm: ss"],
["Replication = Unum" = "3"]
)

Storage_medium: The initial storage medium used to specify the partition can be SSD or HDD. The default is HDD.
Storage_cooldown_time: When the storage medium is set to SSD, specify the storage expiration time of the partition on SSD.
Store by default for 7 days.
The format is: "yyyy-MM-dd HH: mm: ss"
Replication_num: The number of copies of the specified partition. Default 3

When a table is a single partitioned table, these attributes are attributes of the table.
When tables are two-level partitions, these attributes are attached to each partition.
If you want different partitions to have different attributes. It can be operated through ADD PARTITION or MODIFY PARTITION

3) 如果 Engine 类型为 olap, 并且 storage_type 为 column, 可以指定某列使用 bloom filter 索引
Blooming filter index is only applicable to the case where the query conditions are in and equal. The more decentralized the values of the column, the better the effect.
Currently, only the following columns are supported: key columns other than TINYINT FLOAT DOUBLE type and value columns aggregated by REPLACE

PROPERTIES (
"bloom_filter_columns"="k1,k2,k3"
)
4) If you want to use the Colocate Join feature, you need to specify it in properties

PROPERTIES (
"colocate_with"="table1"
)

'35;'35; example
1. Create an OLAP table, use HASH buckets, use column storage, aggregate records of the same key
CREATE TABLE example_db.table_hash
(
k1 DURATION,
K2 Decima (10,2) Default "10.5",
v1 CHAR(10) REPLACE,
v2 INT SUM
)
ENGINE=olap
AGGREGATE KEY (k1, k2)
DISTRIBUTED BY HASH(k1) BUCKETS 32
PROPERTIES ("storage_type"="column");

2. Create an OLAP table, use Hash bucket, use column storage, and overwrite the same key record.
Setting initial storage medium and cooling time
CREATE TABLE example_db.table_hash
(
k1 BIGINT
k2 LARGEINT,
v1 VARCHAR(2048) REPLACE,
v2 SMALLINT SUM DEFAULT "10"
)
ENGINE=olap
KEY (k1, K2) UNIT
DISTRIBUTED BY HASH (k1, k2) BUCKETS 32
PROPERTIES(
"storage_type"="column"，
"storage_medium" = "SSD",
"Storage = U cooldown  time" = "2015-06-04:00:00:00:00:
);

3. Create an OLAP table, use Key Range partition, use Hash bucket, default column storage.
Records of the same key coexist, setting the initial storage medium and cooling time
CREATE TABLE example_db.table_range
(
k1 DATE,
k2 INT
k3 SMALL
v1 VARCHAR (2048),
V2 DATETIME DEFAULT "2014 -02 -04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY (k1, k2, k3)
PARTITION BY RANGE (k1)
(
The partition value of P1 is less than ("2014-01-01").
The segmentation value of P2 is lower than that of ("2014-06-01").
The partition value of P3 is less than ("2014-12-01")
)
DISTRIBUTED BY HASH(k2) BUCKETS 32
PROPERTIES(
"Storage = U Medium"= "SSD", "Storage = U Cooldown = U Time"= "2015-06-04:00:00:00:00:
);

Explain:
This statement divides the data into the following three partitions:
({MIN}, {"2014 -01 -01"}}
[ {"2014-01-01"},   {"2014-06-01"} )
[ {"2014-06-01"},   {"2014-12-01"} )

Data that is not within these partitions will be treated as illegal data being filtered

4. Create a MySQL table
CREATE TABLE example_db.table_mysql
(
k1 DATE,
k2 INT
k3 SMALL
k4 VARCHAR (2048),
K5 DATE
)
ENGINE=mysql
PROPERTIES
(
"host" = "127.0.0.1",
"port" = "8239",
"user" = "mysql_user",
"password" = "mysql_passwd",
"database" ="mysql" db test,
"table" = "mysql_table_test"
)

5. Create a broker external table where the data file is stored on HDFS, and the data is split by "|" and "\ n" newline
CREATE EXTERNAL TABLE example_db.table_broker (
k1 DATE,
k2 INT
k3 SMALL
k4 VARCHAR (2048),
K5 DATE
)
ENGINE=broker
PROPERTIES (
"broker" u name ="hdfs",
"path" ="hdfs http://hdfs -u -host:hdfs" port /data1,hdfs http://hdfs -u -host:hdfs -u -port /data3%2c4 ",
"Column"= U separator"= 124;"
"line_delimiter" = "\n"
)
BROKER PROPERTIES (
"Username" = "HDFS\\ user"
"password" = "hdfs_password"
)

6. Create a table with HLL columns
CREATE TABLE example_db.example_table
(
k1 DURATION,
K2 Decima (10,2) Default "10.5",
THE EUROPEAN UNION,
V2 HLL HLL UNION
)
ENGINE=olap
AGGREGATE KEY (k1, k2)
DISTRIBUTED BY HASH(k1) BUCKETS 32
PROPERTIES ("storage_type"="column");

7. Create two tables T1 and T2 that support Colocat Join
CREATE TABLE `t1` (
`id` int(11) COMMENT "",
'value ` varchar (8) COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"colocate_with" = "t1"
);

CREATE TABLE `t2` (
`id` int(11) COMMENT "",
'value ` varchar (8) COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"colocate_with" = "t1"
);

8. Create a broker external table with data files stored on BOS
CREATE EXTERNAL TABLE example_db.table_broker (
date
)
ENGINE=broker
PROPERTIES (
"broker_name" = "bos",
"path" = "bos://my_bucket/input/file",
)
BROKER PROPERTIES (
"bosu endpoint" ="http://bj.bcebos.com",
"bos_accesskey" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
"bos_secret_accesskey"="yyyyyyyyyyyyyyyyyyyy"
)

## keyword
CREATE,TABLE

