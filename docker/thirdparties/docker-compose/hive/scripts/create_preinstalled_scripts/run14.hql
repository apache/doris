CREATE TABLE `lineorder` (
  `lo_orderkey` int,
  `lo_linenumber` int,
  `lo_custkey` int,
  `lo_partkey` int,
  `lo_suppkey` int,
  `lo_orderdate` int,
  `lo_orderpriority` varchar(16),
  `lo_shippriority` int,
  `lo_quantity` int,
  `lo_extendedprice` int,
  `lo_ordtotalprice` int,
  `lo_discount` int,
  `lo_revenue` int,
  `lo_supplycost` int,
  `lo_tax` int,
  `lo_commitdate` int,
  `lo_shipmode` varchar(11)
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
LOCATION '/user/doris/preinstalled_data/data_case/lineorder'
TBLPROPERTIES ('transient_lastDdlTime'='1658816839');

