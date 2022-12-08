CREATE TABLE IF NOT EXISTS `orders` (
  `o_orderkey` bigint(20) NOT NULL,
  `o_custkey` int(11) NOT NULL,
  `o_orderstatus` varchar(1) NOT NULL,
  `o_totalprice` decimal(12,2) NOT NULL,
  `o_orderdate` date NOT NULL,
  `o_orderprioriTY` varchar(15) NOT NULL,
  `o_clerk` varchar(15) NOT NULL,
  `o_shipprioritY` int(11) NOT NULL,
  `o_comment` varchar(79) NOT NULL
  ) 
DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 10
PROPERTIES("replication_num" = "1");
