create database if not exists partition_tables;
use partition_tables;

CREATE TABLE decimal_partition_table (
    id INT,
    name STRING,
    value FLOAT
)
PARTITIONED BY (partition_col DECIMAL(10, 2))
STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/partition_tables/decimal_partition_table';

CREATE TABLE int_partition_table (
    id INT,
    name STRING,
    value FLOAT
)
PARTITIONED BY (partition_col INT)
STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/partition_tables/int_partition_table';

CREATE TABLE string_partition_table (
    id INT,
    name STRING,
    value FLOAT
)
PARTITIONED BY (partition_col STRING)
STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/partition_tables/string_partition_table';

CREATE TABLE date_partition_table (
    id INT,
    name STRING,
    value FLOAT
)
PARTITIONED BY (partition_col DATE)
STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/partition_tables/date_partition_table';

CREATE TABLE string_partition_table_with_comma (
    id INT,
    name STRING,
    value FLOAT
)
PARTITIONED BY (partition_col STRING)
STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/partition_tables/string_partition_table_with_comma';

msck repair table decimal_partition_table;
msck repair table int_partition_table;
msck repair table string_partition_table;
msck repair table date_partition_table;
msck repair table string_partition_table_with_comma;
