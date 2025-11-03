CREATE TABLE IF NOT EXISTS upper_case (
  `P_PARTKEY` integer NOT NULL,
  `P_NAME` varchar(55) NOT NULL,
  `P_MFGR` char(25) NOT NULL,
  `P_BRAND` char(10) NOT NULL,
  `P_TYPE` varchar(25) NOT NULL,
  `P_SIZE` integer NOT NULL,
  `P_CONTAINER` char(10) NOT NULL,
  `P_RETAILPRICE` decimal(12, 2) NOT NULL,
  `P_COMMENT` varchar(23) NOT NULL
)ENGINE=OLAP
DISTRIBUTED BY HASH(p_partkey) BUCKETS 24
PROPERTIES (
    "replication_num" = "1"
);
