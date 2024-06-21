create TABLE if not exists `bitmap_basic_agg` (
  `k1` int(11) NULL,
  `k2` bitmap BITMAP_UNION 
)AGGREGATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES("replication_num" = "1");
