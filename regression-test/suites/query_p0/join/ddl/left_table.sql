CREATE TABLE IF NOT EXISTS left_table (
k1 int(11) NULL COMMENT "",
no varchar(50) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(k1)
COMMENT "OLAP"
DISTRIBUTED BY HASH(k1) BUCKETS 2
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
);
