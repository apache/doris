CREATE TABLE IF NOT EXISTS `nation` (
  `n_nationkey` integer NOT NULL,
  `n_name` char(25) NOT NULL,
  `n_regionkey` integer NOT NULL,
  `n_comment` varchar(152)
)
DISTRIBUTED BY HASH(`n_nationkey`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
