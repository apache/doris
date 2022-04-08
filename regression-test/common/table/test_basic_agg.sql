CREATE TABLE `test_basic_agg` (
  `k1` tinyint(4) NULL COMMENT "",
  `k2` smallint(6) NULL COMMENT "",
  `k3` int(11) NULL COMMENT "",
  `k4` bigint(20) NULL COMMENT "",
  `k5` decimal(9, 3) NULL COMMENT "",
  `k6` char(5) NULL COMMENT "",
  `k10` date NULL COMMENT "",
  `k11` datetime NULL COMMENT "",
  `k7` varchar(20) NULL COMMENT "",
  `k8` double MAX NULL COMMENT "",
  `k9` float SUM NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)
COMMENT "OLAP"
PARTITION BY RANGE(`k1`)
(PARTITION p1 VALUES [("-128"), ("-64")),
PARTITION p2 VALUES [("-64"), ("0")),
PARTITION p3 VALUES [("0"), ("64")),
PARTITION p4 VALUES [("64"), (MAXVALUE)))
DISTRIBUTED BY HASH(`k1`) BUCKETS 5
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
);