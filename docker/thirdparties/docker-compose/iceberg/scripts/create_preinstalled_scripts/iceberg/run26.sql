create database if not exists demo.test_stats_order;
USE demo.test_stats_order;

DROP TABLE IF EXISTS iceberg_all_types_orc;
DROP TABLE IF EXISTS iceberg_all_types_parquet;

CREATE TABLE iceberg_all_types_orc (
    `boolean_col` boolean,
    `int_col` int,
    `bigint_col` bigint,
    `float_col` float,
    `double_col` double,
    `decimal_col1` decimal(9,0),
    `decimal_col2` decimal(8,4),
    `decimal_col3` decimal(18,6),
    `decimal_col4` decimal(38,12),
    `string_col` string,
    `date_col` date,
    `datetime_col1` TIMESTAMP_NTZ
)
USING iceberg
TBLPROPERTIES(
    'write.format.default' = 'orc',
    'format-version' = '2'
);

CREATE TABLE iceberg_all_types_parquet (
    `boolean_col` boolean,
    `int_col` int,
    `bigint_col` bigint,
    `float_col` float,
    `double_col` double,
    `decimal_col1` decimal(9,0),
    `decimal_col2` decimal(8,4),
    `decimal_col3` decimal(18,6),
    `decimal_col4` decimal(38,12),
    `string_col` string,
    `date_col` date,
    `datetime_col1` TIMESTAMP_NTZ
)
USING iceberg
TBLPROPERTIES(
    'write.format.default' = 'parquet',
    'format-version' = '2'
);

ALTER TABLE iceberg_all_types_parquet WRITE ORDERED BY int_col ASC;
ALTER TABLE iceberg_all_types_orc WRITE ORDERED BY int_col ASC;

CREATE TABLE iceberg_int_order (
    int_col int
)
USING iceberg
TBLPROPERTIES(
    'write.format.default' = 'parquet',
    'format-version' = '2'
);


CREATE TABLE iceberg_int_no_order (
    int_col int
)
USING iceberg
TBLPROPERTIES(
    'write.format.default' = 'parquet',
    'format-version' = '2'
);

ALTER TABLE iceberg_int_order WRITE ORDERED BY int_col ASC;




