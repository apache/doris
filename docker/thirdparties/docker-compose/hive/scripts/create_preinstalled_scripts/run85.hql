use `multi_catalog`;

CREATE TABLE test_parquet_lazy_read_struct(
    id INT,
    name STRING,
    col STRUCT<
        a: INT,
        b: STRING,
        c: STRUCT<
            aa: INT
        >
    >
)
STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/test_parquet_lazy_read_struct';
