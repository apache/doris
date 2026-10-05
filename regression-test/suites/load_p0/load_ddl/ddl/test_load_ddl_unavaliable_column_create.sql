CREATE TABLE test_load_ddl_unavaliable_column
(
    k00 INT             NOT NULL,
    k01 DATE            NOT NULL,
    k02 BOOLEAN         NOT NULL,
    k03 VARCHAR         NOT NULL,

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
    "replication_num" = "1"
);