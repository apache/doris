CREATE TABLE test_load_ddl_hidden_column
(
    k00 INT             NOT NULL,
    k01 DATE            NOT NULL,
    k02 BOOLEAN         NOT NULL,
    k03 VARCHAR         NOT NULL

)
UNIQUE KEY(k00,k01)
PARTITION BY RANGE(k01)
(
    PARTITION p1 VALUES [('2023-08-01'), ('2023-08-11')),
    PARTITION p2 VALUES [('2023-08-11'), ('2023-08-21')),
    PARTITION p3 VALUES [('2023-08-21'), ('2023-09-01'))
)
DISTRIBUTED BY HASH(k00) BUCKETS 6
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true", 
    "function_column.sequence_col" = "k00",
    "replication_num" = "1"
);