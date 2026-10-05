CREATE TABLE basic_data
(
    k00 INT             NOT NULL,
    k01 DATE            NOT NULL,
    k02 BOOLEAN         NULL,
    k03 TINYINT         NULL,
    k04 SMALLINT        NULL,
    k05 INT             NULL,
    k06 BIGINT          NULL,
    k07 LARGEINT        NULL,
    k08 FLOAT           NULL,
    k09 DOUBLE          NULL,
    k10 DECIMAL(9,1)    NULL,
    k11 DECIMALV3(9,1)  NULL,
    k12 DATETIME        NULL,
    k13 DATEV2          NULL,
    k14 DATETIMEV2      NULL,
    k15 CHAR            NULL,
    k16 VARCHAR         NULL,
    k17 STRING          NULL,
    k18 JSON            NULL

)
DUPLICATE KEY(k00)
DISTRIBUTED BY HASH(k00) BUCKETS 32
PROPERTIES (
    "bloom_filter_columns"="k05",
    "replication_num" = "1"
);