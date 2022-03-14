CREATE TABLE `region` (
  `r_regionkey` integer NOT NULL,
  `r_name` char(25) NOT NULL,
  `r_comment` varchar(152)
)
DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
 );
