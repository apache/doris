use default;


CREATE TABLE orc_partition_multi_stripe (
    col1 STRING,
    col2 INT,
    col3 DOUBLE
) PARTITIONED BY (
    partition_col1 STRING,
    partition_col2 INT
)
STORED AS ORC
LOCATION '/user/doris/preinstalled_data/orc_table/orc_partition_multi_stripe';
;
msck repair table orc_partition_multi_stripe;

CREATE TABLE parquet_partition_multi_row_group (
    col1 STRING,
    col2 INT,
    col3 DOUBLE
) PARTITIONED BY (
    partition_col1 STRING,
    partition_col2 INT
)
STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/parquet_partition_multi_row_group';
;
msck repair table parquet_partition_multi_row_group;
