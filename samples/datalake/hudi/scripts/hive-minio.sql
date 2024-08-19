USE default;

CREATE EXTERNAL TABLE `customer`(
  `c_custkey` int,
  `c_name` varchar(25),
  `c_address` varchar(40),
  `c_nationkey` int,
  `c_phone` char(15),
  `c_acctbal` decimal(12,2),
  `c_mktsegment` char(10),
  `c_comment` varchar(117))
STORED AS parquet
LOCATION 's3://data/customer';
