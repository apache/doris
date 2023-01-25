CREATE TABLE IF NOT EXISTS tpch_tiny_lineitem (
	orderkey bigint,
	partkey bigint,
	suppkey bigint,
	linenumber integer,
	quantity double,
	extendedprice double,
	discount double,
	tax double,
	returnflag varchar(1),
	linestatus varchar(1),
	shipdate date,
	commitdate date,
	receiptdate date,
	shipinstruct varchar(25),
	shipmode varchar(10),
	comment varchar(44)
) DUPLICATE KEY(orderkey) DISTRIBUTED BY HASH(orderkey) BUCKETS 3 PROPERTIES ("replication_num" = "1")