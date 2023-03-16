CREATE TABLE IF NOT EXISTS set4 (
  `partkey` integer NOT NULL default '0'
)ENGINE=OLAP
DISTRIBUTED BY HASH(partkey) BUCKETS 24
PROPERTIES (
    "replication_num" = "1"
);
