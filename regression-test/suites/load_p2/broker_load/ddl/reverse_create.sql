CREATE TABLE IF NOT EXISTS reverse (
  `comment` varchar(23) NOT NULL,
  `retailprice` decimal(12, 2) NOT NULL,
  `container` char(10) NOT NULL,
  `size` integer NOT NULL,
  `type` varchar(25) NOT NULL,
  `brand` char(10) NOT NULL,
  `mfgr` char(25) NOT NULL,
  `name` varchar(55) NOT NULL,
  `partkey` integer NOT NULL
)ENGINE=OLAP
DISTRIBUTED BY HASH(partkey) BUCKETS 24
PROPERTIES (
    "replication_num" = "1"
);
