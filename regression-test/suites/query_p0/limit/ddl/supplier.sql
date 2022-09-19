CREATE TABLE IF NOT EXISTS tpch_tiny_supplier (
	s_suppkey INTEGER NOT NULL,
	s_name CHAR(25) NOT NULL,
	s_address VARCHAR(40) NOT NULL,
	s_nationkey INTEGER NOT NULL,
	s_phone CHAR(15) NOT NULL,
	s_acctbal DECIMAL(15, 2) NOT NULL,
	s_comment VARCHAR(101) NOT NULL
) DUPLICATE KEY(s_suppkey) DISTRIBUTED BY HASH(s_suppkey) BUCKETS 3 PROPERTIES ("replication_num" = "1")