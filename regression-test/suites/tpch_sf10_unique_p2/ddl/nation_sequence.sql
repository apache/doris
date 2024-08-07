CREATE TABLE IF NOT EXISTS nation  (
    `n_nationkey` int(11) NOT NULL,
    `n_name`      varchar(25) NOT NULL,
    `n_regionkey` int(11) NOT NULL,
    `n_comment`   varchar(152) NULL
) ENGINE=OLAP
UNIQUE KEY(`N_NATIONKEY`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1
PROPERTIES (
    "enable_mow_light_delete" = "true",
    "function_column.sequence_type" = 'int',
    "replication_num" = "3"
);

