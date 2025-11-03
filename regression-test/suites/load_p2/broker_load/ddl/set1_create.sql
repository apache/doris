CREATE TABLE IF NOT EXISTS set1 (
  `p_partkey` integer NOT NULL default '0',
  `p_name` varchar(55) NOT NULL default 'aaa',
  `p_size` integer NOT NULL default '0',
  `p_greatest` integer NOT NULL
)ENGINE=OLAP
DISTRIBUTED BY HASH(p_partkey) BUCKETS 24
PROPERTIES (
    "replication_num" = "1"
);
