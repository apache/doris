CREATE TABLE IF NOT EXISTS `region` (
  `r_regionkey` int(11) NOT NULL,
  `r_name` varchar(25) NOT NULL,
  `r_comment` varchar(152) DEFAULT NULL
  )
DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 10
PROPERTIES("replication_num" = "1");
