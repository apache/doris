create TABLE if not exists `quantile_state_basic_agg` (
    `k1` int(11) NULL,
    `k2` QUANTILE_STATE QUANTILE_UNION NOT NULL
    )AGGREGATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES("replication_num" = "1");
