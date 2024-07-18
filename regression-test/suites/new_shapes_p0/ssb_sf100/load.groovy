// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("load") {
    if (isCloudMode()) {
        return
    }
    String database = context.config.getDbNameByFile(context.file)
    sql "drop database if exists ${database}"
    sql "create database ${database}"
    sql "use ${database}"
    sql """
CREATE TABLE IF NOT EXISTS `lineorder` (
  `lo_orderkey` int(11) NOT NULL COMMENT '',
  `lo_linenumber` int(11) NOT NULL COMMENT '',
  `lo_custkey` int(11) NOT NULL COMMENT '',
  `lo_partkey` int(11) NOT NULL COMMENT '',
  `lo_suppkey` int(11) NOT NULL COMMENT '',
  `lo_orderdate` int(11) NOT NULL COMMENT '',
  `lo_orderpriority` varchar(16) NOT NULL COMMENT '',
  `lo_shippriority` int(11) NOT NULL COMMENT '',
  `lo_quantity` int(11) NOT NULL COMMENT '',
  `lo_extendedprice` int(11) NOT NULL COMMENT '',
  `lo_ordtotalprice` int(11) NOT NULL COMMENT '',
  `lo_discount` int(11) NOT NULL COMMENT '',
  `lo_revenue` int(11) NOT NULL COMMENT '',
  `lo_supplycost` int(11) NOT NULL COMMENT '',
  `lo_tax` int(11) NOT NULL COMMENT '',
  `lo_commitdate` int(11) NOT NULL COMMENT '',
  `lo_shipmode` varchar(11) NOT NULL COMMENT ''
) ENGINE=OLAP
DUPLICATE KEY(`lo_orderkey`)
COMMENT "OLAP"
PARTITION BY RANGE(`lo_orderdate`)
(PARTITION p1 VALUES [("-2147483648"), ("19930101")),
PARTITION p2 VALUES [("19930101"), ("19940101")),
PARTITION p3 VALUES [("19940101"), ("19950101")),
PARTITION p4 VALUES [("19950101"), ("19960101")),
PARTITION p5 VALUES [("19960101"), ("19970101")),
PARTITION p6 VALUES [("19970101"), ("19980101")),
PARTITION p7 VALUES [("19980101"), ("19990101")))
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "groupa1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);"""

sql """
CREATE TABLE IF NOT EXISTS `customer` (
  `c_custkey` int(11) NOT NULL COMMENT '',
  `c_name` varchar(26) NOT NULL COMMENT '',
  `c_address` varchar(41) NOT NULL COMMENT '',
  `c_city` varchar(11) NOT NULL COMMENT '',
  `c_nation` varchar(16) NOT NULL COMMENT '',
  `c_region` varchar(13) NOT NULL COMMENT '',
  `c_phone` varchar(16) NOT NULL COMMENT '',
  `c_mktsegment` varchar(11) NOT NULL COMMENT ''
) ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "groupa2",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);"""

sql """
CREATE TABLE IF NOT EXISTS `dates` (
  `d_datekey` int(11) NOT NULL COMMENT '',
  `d_date` varchar(20) NOT NULL COMMENT '',
  `d_dayofweek` varchar(10) NOT NULL COMMENT '',
  `d_month` varchar(11) NOT NULL COMMENT '',
  `d_year` int(11) NOT NULL COMMENT '',
  `d_yearmonthnum` int(11) NOT NULL COMMENT '',
  `d_yearmonth` varchar(9) NOT NULL COMMENT '',
  `d_daynuminweek` int(11) NOT NULL COMMENT '',
  `d_daynuminmonth` int(11) NOT NULL COMMENT '',
  `d_daynuminyear` int(11) NOT NULL COMMENT '',
  `d_monthnuminyear` int(11) NOT NULL COMMENT '',
  `d_weeknuminyear` int(11) NOT NULL COMMENT '',
  `d_sellingseason` varchar(14) NOT NULL COMMENT '',
  `d_lastdayinweekfl` int(11) NOT NULL COMMENT '',
  `d_lastdayinmonthfl` int(11) NOT NULL COMMENT '',
  `d_holidayfl` int(11) NOT NULL COMMENT '',
  `d_weekdayfl` int(11) NOT NULL COMMENT ''
) ENGINE=OLAP
DUPLICATE KEY(`d_datekey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`d_datekey`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"colocate_with" = "groupa3",
"storage_format" = "DEFAULT"
);"""

sql """

 CREATE TABLE IF NOT EXISTS `supplier` (
  `s_suppkey` int(11) NOT NULL COMMENT '',
  `s_name` varchar(26) NOT NULL COMMENT '',
  `s_address` varchar(26) NOT NULL COMMENT '',
  `s_city` varchar(11) NOT NULL COMMENT '',
  `s_nation` varchar(16) NOT NULL COMMENT '',
  `s_region` varchar(13) NOT NULL COMMENT '',
  `s_phone` varchar(16) NOT NULL COMMENT ''
) ENGINE=OLAP
DUPLICATE KEY(`s_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "groupa4",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);"""

sql """
CREATE TABLE IF NOT EXISTS `part` (
  `p_partkey` int(11) NOT NULL COMMENT '',
  `p_name` varchar(23) NOT NULL COMMENT '',
  `p_mfgr` varchar(7) NOT NULL COMMENT '',
  `p_category` varchar(8) NOT NULL COMMENT '',
  `p_brand` varchar(10) NOT NULL COMMENT '',
  `p_color` varchar(12) NOT NULL COMMENT '',
  `p_type` varchar(26) NOT NULL COMMENT '',
  `p_size` int(11) NOT NULL COMMENT '',
  `p_container` varchar(11) NOT NULL COMMENT ''
) ENGINE=OLAP
DUPLICATE KEY(`p_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 12
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "groupa5",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);"""

sql """alter table dates modify column d_lastdayinweekfl set stats ('row_count'='2556', 'ndv'='2', 'num_nulls'='0', 'min_value'='0', 'max_value'='1', 'data_size'='10224');"""
sql """alter table supplier modify column s_suppkey set stats ('row_count'='200000', 'ndv'='196099', 'num_nulls'='0', 'min_value'='1', 'max_value'='200000', 'data_size'='800000');"""
sql """alter table lineorder modify column lo_quantity set stats ('row_count'='600037902', 'ndv'='50', 'num_nulls'='0', 'min_value'='1', 'max_value'='50', 'data_size'='2400151608');"""
sql """alter table lineorder modify column lo_shipmode set stats ('row_count'='600037902', 'ndv'='7', 'num_nulls'='0', 'min_value'='AIR', 'max_value'='TRUCK', 'data_size'='2571562204');"""
sql """alter table customer modify column c_name set stats ('row_count'='3000000', 'ndv'='3017713', 'num_nulls'='0', 'min_value'='Customer#000000001', 'max_value'='Customer#003000000', 'data_size'='54000000');"""
sql """alter table dates modify column d_date set stats ('row_count'='2556', 'ndv'='2539', 'num_nulls'='0', 'min_value'='April 1, 1992', 'max_value'='September 9, 1998', 'data_size'='38181');"""
sql """alter table dates modify column d_daynuminyear set stats ('row_count'='2556', 'ndv'='366', 'num_nulls'='0', 'min_value'='1', 'max_value'='366', 'data_size'='10224');"""
sql """alter table dates modify column d_yearmonth set stats ('row_count'='2556', 'ndv'='84', 'num_nulls'='0', 'min_value'='Apr1992', 'max_value'='Sep1998', 'data_size'='17892');"""
sql """alter table part modify column p_mfgr set stats ('row_count'='1400000', 'ndv'='5', 'num_nulls'='0', 'min_value'='MFGR#1', 'max_value'='MFGR#5', 'data_size'='8400000');"""
sql """alter table part modify column p_name set stats ('row_count'='1400000', 'ndv'='8417', 'num_nulls'='0', 'min_value'='almond antique', 'max_value'='yellow white', 'data_size'='17705366');"""
sql """alter table lineorder modify column lo_extendedprice set stats ('row_count'='600037902', 'ndv'='1135983', 'num_nulls'='0', 'min_value'='90096', 'max_value'='10494950', 'data_size'='2400151608');"""
sql """alter table lineorder modify column lo_linenumber set stats ('row_count'='600037902', 'ndv'='7', 'num_nulls'='0', 'min_value'='1', 'max_value'='7', 'data_size'='2400151608');"""
sql """alter table lineorder modify column lo_partkey set stats ('row_count'='600037902', 'ndv'='999528', 'num_nulls'='0', 'min_value'='1', 'max_value'='1000000', 'data_size'='2400151608');"""
sql """alter table lineorder modify column lo_shippriority set stats ('row_count'='600037902', 'ndv'='1', 'num_nulls'='0', 'min_value'='0', 'max_value'='0', 'data_size'='2400151608');"""
sql """alter table customer modify column c_mktsegment set stats ('row_count'='3000000', 'ndv'='5', 'num_nulls'='0', 'min_value'='AUTOMOBILE', 'max_value'='MACHINERY', 'data_size'='26999329');"""
sql """alter table dates modify column d_dayofweek set stats ('row_count'='2556', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='18258');"""
sql """alter table dates modify column d_sellingseason set stats ('row_count'='2556', 'ndv'='5', 'num_nulls'='0', 'min_value'='Christmas', 'max_value'='Winter', 'data_size'='15760');"""
sql """alter table dates modify column d_weekdayfl set stats ('row_count'='2556', 'ndv'='2', 'num_nulls'='0', 'min_value'='0', 'max_value'='1', 'data_size'='10224');"""
sql """alter table supplier modify column s_city set stats ('row_count'='200000', 'ndv'='250', 'num_nulls'='0', 'min_value'='ALGERIA  0', 'max_value'='VIETNAM  9', 'data_size'='2000000');"""
sql """alter table part modify column p_category set stats ('row_count'='1400000', 'ndv'='25', 'num_nulls'='0', 'min_value'='MFGR#11', 'max_value'='MFGR#55', 'data_size'='9800000');"""
sql """alter table part modify column p_size set stats ('row_count'='1400000', 'ndv'='50', 'num_nulls'='0', 'min_value'='1', 'max_value'='50', 'data_size'='5600000');"""
sql """alter table part modify column p_type set stats ('row_count'='1400000', 'ndv'='150', 'num_nulls'='0', 'min_value'='ECONOMY ANODIZED BRASS', 'max_value'='STANDARD POLISHED TIN', 'data_size'='28837497');"""
sql """alter table lineorder modify column lo_orderkey set stats ('row_count'='600037902', 'ndv'='148064528', 'num_nulls'='0', 'min_value'='1', 'max_value'='600000000', 'data_size'='2400151608');"""
sql """alter table lineorder modify column lo_revenue set stats ('row_count'='600037902', 'ndv'='6280312', 'num_nulls'='0', 'min_value'='81087', 'max_value'='10494950', 'data_size'='2400151608');"""
sql """alter table lineorder modify column lo_suppkey set stats ('row_count'='600037902', 'ndv'='196099', 'num_nulls'='0', 'min_value'='1', 'max_value'='200000', 'data_size'='2400151608');"""
sql """alter table lineorder modify column lo_supplycost set stats ('row_count'='600037902', 'ndv'='15824', 'num_nulls'='0', 'min_value'='54057', 'max_value'='125939', 'data_size'='2400151608');"""
sql """alter table customer modify column c_address set stats ('row_count'='3000000', 'ndv'='3011483', 'num_nulls'='0', 'min_value'='    yaP00NZn4mxv', 'max_value'='zzzzsVRceYXRDisV3RC', 'data_size'='44994193');"""
sql """alter table dates modify column d_datekey set stats ('row_count'='2556', 'ndv'='2560', 'num_nulls'='0', 'min_value'='19920101', 'max_value'='19981230', 'data_size'='10224');"""
sql """alter table dates modify column d_daynuminmonth set stats ('row_count'='2556', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='10224');"""
sql """alter table dates modify column d_year set stats ('row_count'='2556', 'ndv'='7', 'num_nulls'='0', 'min_value'='1992', 'max_value'='1998', 'data_size'='10224');"""
sql """alter table supplier modify column s_address set stats ('row_count'='200000', 'ndv'='197960', 'num_nulls'='0', 'min_value'='  2MrUy', 'max_value'='zzzqXhTdKxT0RAR8yxbc', 'data_size'='2998285');"""
sql """alter table lineorder modify column lo_commitdate set stats ('row_count'='600037902', 'ndv'='2469', 'num_nulls'='0', 'min_value'='19920131', 'max_value'='19981031', 'data_size'='2400151608');"""
sql """alter table lineorder modify column lo_tax set stats ('row_count'='600037902', 'ndv'='9', 'num_nulls'='0', 'min_value'='0', 'max_value'='8', 'data_size'='2400151608');"""
sql """alter table customer modify column c_city set stats ('row_count'='3000000', 'ndv'='250', 'num_nulls'='0', 'min_value'='ALGERIA  0', 'max_value'='VIETNAM  9', 'data_size'='30000000');"""
sql """alter table customer modify column c_custkey set stats ('row_count'='3000000', 'ndv'='2985828', 'num_nulls'='0', 'min_value'='1', 'max_value'='3000000', 'data_size'='12000000');"""
sql """alter table dates modify column d_daynuminweek set stats ('row_count'='2556', 'ndv'='7', 'num_nulls'='0', 'min_value'='1', 'max_value'='7', 'data_size'='10224');"""
sql """alter table dates modify column d_lastdayinmonthfl set stats ('row_count'='2556', 'ndv'='2', 'num_nulls'='0', 'min_value'='0', 'max_value'='1', 'data_size'='10224');"""
sql """alter table dates modify column d_month set stats ('row_count'='2556', 'ndv'='12', 'num_nulls'='0', 'min_value'='April', 'max_value'='September', 'data_size'='15933');"""
sql """alter table dates modify column d_yearmonthnum set stats ('row_count'='2556', 'ndv'='84', 'num_nulls'='0', 'min_value'='199201', 'max_value'='199812', 'data_size'='10224');"""
sql """alter table supplier modify column s_phone set stats ('row_count'='200000', 'ndv'='199261', 'num_nulls'='0', 'min_value'='10-100-177-2350', 'max_value'='34-999-827-8511', 'data_size'='3000000');"""
sql """alter table part modify column p_partkey set stats ('row_count'='1400000', 'ndv'='1394881', 'num_nulls'='0', 'min_value'='1', 'max_value'='1400000', 'data_size'='5600000');"""
sql """alter table lineorder modify column lo_custkey set stats ('row_count'='600037902', 'ndv'='1962895', 'num_nulls'='0', 'min_value'='1', 'max_value'='2999999', 'data_size'='2400151608');"""
sql """alter table lineorder modify column lo_orderdate set stats ('row_count'='600037902', 'ndv'='2408', 'num_nulls'='0', 'min_value'='19920101', 'max_value'='19980802', 'data_size'='2400151608');"""
sql """alter table lineorder modify column lo_ordtotalprice set stats ('row_count'='600037902', 'ndv'='35026888', 'num_nulls'='0', 'min_value'='81806', 'max_value'='60690215', 'data_size'='2400151608');"""
sql """alter table customer modify column c_nation set stats ('row_count'='3000000', 'ndv'='25', 'num_nulls'='0', 'min_value'='ALGERIA', 'max_value'='VIETNAM', 'data_size'='21248112');"""
sql """alter table customer modify column c_phone set stats ('row_count'='3000000', 'ndv'='3012496', 'num_nulls'='0', 'min_value'='10-100-106-1617', 'max_value'='34-999-998-5763', 'data_size'='45000000');"""
sql """alter table customer modify column c_region set stats ('row_count'='3000000', 'ndv'='5', 'num_nulls'='0', 'min_value'='AFRICA', 'max_value'='MIDDLE EAST', 'data_size'='20398797');"""
sql """alter table dates modify column d_holidayfl set stats ('row_count'='2556', 'ndv'='2', 'num_nulls'='0', 'min_value'='0', 'max_value'='1', 'data_size'='10224');"""
sql """alter table dates modify column d_weeknuminyear set stats ('row_count'='2556', 'ndv'='53', 'num_nulls'='0', 'min_value'='1', 'max_value'='53', 'data_size'='10224');"""
sql """alter table supplier modify column s_nation set stats ('row_count'='200000', 'ndv'='25', 'num_nulls'='0', 'min_value'='ALGERIA', 'max_value'='VIETNAM', 'data_size'='1415335');"""
sql """alter table part modify column p_brand set stats ('row_count'='1400000', 'ndv'='1002', 'num_nulls'='0', 'min_value'='MFGR#111', 'max_value'='MFGR#559', 'data_size'='12285135');"""
sql """alter table part modify column p_color set stats ('row_count'='1400000', 'ndv'='92', 'num_nulls'='0', 'min_value'='almond', 'max_value'='yellow', 'data_size'='8170588');"""
sql """alter table part modify column p_container set stats ('row_count'='1400000', 'ndv'='40', 'num_nulls'='0', 'min_value'='JUMBO BAG', 'max_value'='WRAP PKG', 'data_size'='10606696');"""
sql """alter table lineorder modify column lo_discount set stats ('row_count'='600037902', 'ndv'='11', 'num_nulls'='0', 'min_value'='0', 'max_value'='10', 'data_size'='2400151608');"""
sql """alter table lineorder modify column lo_orderpriority set stats ('row_count'='600037902', 'ndv'='5', 'num_nulls'='0', 'min_value'='1-URGENT', 'max_value'='5-LOW', 'data_size'='5040804567');"""
sql """alter table dates modify column d_monthnuminyear set stats ('row_count'='2556', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='10224');"""
sql """alter table supplier modify column s_name set stats ('row_count'='200000', 'ndv'='201596', 'num_nulls'='0', 'min_value'='Supplier#000000001', 'max_value'='Supplier#000200000', 'data_size'='3600000');"""
sql """alter table supplier modify column s_region set stats ('row_count'='200000', 'ndv'='5', 'num_nulls'='0', 'min_value'='AFRICA', 'max_value'='MIDDLE EAST', 'data_size'='1360337');"""


}
