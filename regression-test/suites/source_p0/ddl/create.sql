CREATE TABLE doris_source
(
    bigint_1   BIGINT,
    char_1     char(10),
    date_1     DATE,
    datetime_1 DATETIME,
    decimal_1  DECIMAL(5, 2),
    double_1   DOUBLE,
    float_1    FLOAT,
    int_1      INT,
    largeint_1 STRING,
    smallint_1 SMALLINT,
    tinyint_1  TINYINT,
    varchar_1  VARCHAR(255)
) DUPLICATE KEY(`bigint_1`)
DISTRIBUTED BY HASH(`bigint_1`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
