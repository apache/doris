CREATE TABLE IF NOT EXISTS tpch_tiny_part (
	p_partkey INTEGER NOT NULL,
	p_name VARCHAR(55) NOT NULL,
	p_mfgr CHAR(25) NOT NULL,
	p_brand CHAR(10) NOT NULL,
	p_type VARCHAR(25) NOT NULL,
	p_size INTEGER NOT NULL,
	p_container CHAR(10) NOT NULL,
	p_retailprice DECIMAL(15, 2) NOT NULL,
	p_comment VARCHAR(23) NOT NULL
) DUPLICATE KEY(p_partkey) DISTRIBUTED BY HASH(p_partkey) BUCKETS 3 PROPERTIES ("replication_num" = "1")