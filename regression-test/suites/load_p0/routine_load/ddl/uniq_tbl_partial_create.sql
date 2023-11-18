CREATE TABLE routine_load_uniq_tbl_partial
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
    k18 JSON            NULL,

    INDEX idx_bitmap_k104 (`k02`) USING BITMAP,
    INDEX idx_bitmap_k113 (`k13`) USING BITMAP,
    INDEX idx_bitmap_k114 (`k14`) USING BITMAP,
    INDEX idx_bitmap_k117 (`k17`) USING BITMAP
)
UNIQUE KEY(k00,k01)
PARTITION BY RANGE(k01)
(
    PARTITION p1 VALUES [('2023-08-01'), ('2023-08-11')),
    PARTITION p2 VALUES [('2023-08-11'), ('2023-08-21')),
    PARTITION p3 VALUES [('2023-08-21'), ('2023-09-01'))
)
DISTRIBUTED BY HASH(k00) BUCKETS 32
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);
