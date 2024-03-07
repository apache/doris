CREATE TABLE routine_load_dup_tbl_basic_multi_table
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
    
    INDEX idx_inverted_k104 (`k05`) USING INVERTED,
    INDEX idx_inverted_k110 (`k11`) USING INVERTED,
    INDEX idx_inverted_k113 (`k13`) USING INVERTED,
    INDEX idx_inverted_k114 (`k14`) USING INVERTED,
    INDEX idx_inverted_k117 (`k17`) USING INVERTED PROPERTIES("parser" = "english"),
    INDEX idx_ngrambf_k115 (`k15`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
    INDEX idx_ngrambf_k116 (`k16`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
    INDEX idx_ngrambf_k117 (`k17`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),

    INDEX idx_bitmap_k104 (`k02`) USING BITMAP
    
)
DUPLICATE KEY(k00)
PARTITION BY RANGE(k01)
(
    PARTITION p1 VALUES [('2023-08-01'), ('2023-08-11')),
    PARTITION p2 VALUES [('2023-08-11'), ('2023-08-21')),
    PARTITION p3 VALUES [('2023-08-21'), ('2023-09-01'))
)
DISTRIBUTED BY HASH(k00) BUCKETS 32
PROPERTIES (
    "bloom_filter_columns"="k05",
    "replication_num" = "1"
);