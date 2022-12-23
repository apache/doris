CREATE TABLE IF NOT EXISTS `empty` (
  `c1` INT,
  `c2` String,
  `c3` Decimal(15, 2) NOT NULL
)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES("replication_num" = "1");
