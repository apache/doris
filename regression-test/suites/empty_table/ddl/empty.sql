CREATE TABLE `empty` (
  `c1` INT,
  `c2` String
)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES("replication_num" = "1");
