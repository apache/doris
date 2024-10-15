CREATE TABLE IF NOT EXISTS `lineorder` (
  `lo_orderkey` bigint(20) NOT NULL COMMENT "",
  `lo_linenumber` bigint(20) NOT NULL COMMENT "",
  `lo_custkey` int(11) NOT NULL COMMENT "",
  `lo_partkey` int(11) NOT NULL COMMENT "",
  `lo_suppkey` int(11) NOT NULL COMMENT "",
  `lo_orderdate` int(11) NOT NULL COMMENT "",
  `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
  `lo_shippriority` int(11) NOT NULL COMMENT "",
  `lo_quantity` bigint(20) NOT NULL COMMENT "",
  `lo_extendedprice` bigint(20) NOT NULL COMMENT "",
  `lo_ordtotalprice` bigint(20) NOT NULL COMMENT "",
  `lo_discount` bigint(20) NOT NULL COMMENT "",
  `lo_revenue` bigint(20) NOT NULL COMMENT "",
  `lo_supplycost` bigint(20) NOT NULL COMMENT "",
  `lo_tax` bigint(20) NOT NULL COMMENT "",
  `lo_commitdate` bigint(20) NOT NULL COMMENT "",
  `lo_shipmode` varchar(11) NOT NULL COMMENT ""
)
UNIQUE KEY (`lo_orderkey`, `lo_linenumber`)
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"storage_vault_name" = "ssb_sf1_p2"
);