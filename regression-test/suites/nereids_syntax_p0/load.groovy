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
    sql """
        DROP TABLE IF EXISTS `customer`;
    """

    sql """
        DROP TABLE IF EXISTS `dates`;
    """

    sql """
        DROP TABLE IF EXISTS `lineorder`;
    """

    sql """
        DROP TABLE IF EXISTS `part`;
    """

    sql """
        DROP TABLE IF EXISTS `supplier`;
    """

    sql """
        CREATE TABLE IF NOT EXISTS `customer` (
          `c_custkey` int(11) NOT NULL COMMENT "",
          `c_name` varchar(26) NOT NULL COMMENT "",
          `c_address` varchar(41) NOT NULL COMMENT "",
          `c_city` varchar(11) NOT NULL COMMENT "",
          `c_nation` varchar(16) NOT NULL COMMENT "",
          `c_region` varchar(13) NOT NULL COMMENT "",
          `c_phone` varchar(16) NOT NULL COMMENT "",
          `c_mktsegment` varchar(11) NOT NULL COMMENT ""
        )
        DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """
        CREATE TABLE IF NOT EXISTS `dates` (
          `d_datekey` int(11) NOT NULL COMMENT "",
          `d_date` varchar(20) NOT NULL COMMENT "",
          `d_dayofweek` varchar(10) NOT NULL COMMENT "",
          `d_month` varchar(11) NOT NULL COMMENT "",
          `d_year` int(11) NOT NULL COMMENT "",
          `d_yearmonthnum` int(11) NOT NULL COMMENT "",
          `d_yearmonth` varchar(9) NOT NULL COMMENT "",
          `d_daynuminweek` int(11) NOT NULL COMMENT "",
          `d_daynuminmonth` int(11) NOT NULL COMMENT "",
          `d_daynuminyear` int(11) NOT NULL COMMENT "",
          `d_monthnuminyear` int(11) NOT NULL COMMENT "",
          `d_weeknuminyear` int(11) NOT NULL COMMENT "",
          `d_sellingseason` varchar(14) NOT NULL COMMENT "",
          `d_lastdayinweekfl` int(11) NOT NULL COMMENT "",
          `d_lastdayinmonthfl` int(11) NOT NULL COMMENT "",
          `d_holidayfl` int(11) NOT NULL COMMENT "",
          `d_weekdayfl` int(11) NOT NULL COMMENT ""
        )
        DISTRIBUTED BY HASH(`d_datekey`) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """
        CREATE TABLE IF NOT EXISTS `lineorder` (
          `lo_orderkey` bigint(20) NOT NULL COMMENT "",
          `lo_linenumber` bigint(20) NOT NULL COMMENT "",
          `lo_custkey` int(11) NOT NULL COMMENT "",
          `lo_partkey` int(11) NOT NULL COMMENT "",
          `lo_suppkey` int(11) NOT NULL COMMENT "",
          `lo_orderdate` int(11) NOT NULL COMMENT "",
          `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
          `lo_shippriority` int(11) NOT NULL COMMENT "",
          `lo_quantity` bigint(20) NOT NULL COMMENT "",
          `lo_extendedprice` bigint(20) NOT NULL COMMENT "",
          `lo_ordtotalprice` bigint(20) NOT NULL COMMENT "",
          `lo_discount` bigint(20) NOT NULL COMMENT "",
          `lo_revenue` bigint(20) NOT NULL COMMENT "",
          `lo_supplycost` bigint(20) NOT NULL COMMENT "",
          `lo_tax` bigint(20) NOT NULL COMMENT "",
          `lo_commitdate` bigint(20) NOT NULL COMMENT "",
          `lo_shipmode` varchar(11) NOT NULL COMMENT ""
        )
        PARTITION BY RANGE(`lo_orderdate`)
        (PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
        PARTITION p1993 VALUES [("19930101"), ("19940101")),
        PARTITION p1994 VALUES [("19940101"), ("19950101")),
        PARTITION p1995 VALUES [("19950101"), ("19960101")),
        PARTITION p1996 VALUES [("19960101"), ("19970101")),
        PARTITION p1997 VALUES [("19970101"), ("19980101")),
        PARTITION p1998 VALUES [("19980101"), ("19990101")))
        DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """
        CREATE TABLE IF NOT EXISTS `part` (
          `p_partkey` int(11) NOT NULL COMMENT "",
          `p_name` varchar(23) NOT NULL COMMENT "",
          `p_mfgr` varchar(7) NOT NULL COMMENT "",
          `p_category` varchar(8) NOT NULL COMMENT "",
          `p_brand` varchar(10) NOT NULL COMMENT "",
          `p_color` varchar(12) NOT NULL COMMENT "",
          `p_type` varchar(26) NOT NULL COMMENT "",
          `p_size` int(11) NOT NULL COMMENT "",
          `p_container` varchar(11) NOT NULL COMMENT ""
        )
        DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 10
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """
        CREATE TABLE IF NOT EXISTS `supplier` (
          `s_suppkey` int(11) NOT NULL COMMENT "",
          `s_name` varchar(26) NOT NULL COMMENT "",
          `s_address` varchar(26) NOT NULL COMMENT "",
          `s_city` varchar(11) NOT NULL COMMENT "",
          `s_nation` varchar(16) NOT NULL COMMENT "",
          `s_region` varchar(13) NOT NULL COMMENT "",
          `s_phone` varchar(16) NOT NULL COMMENT ""
        )
        DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """
        INSERT INTO customer VALUES
        (1303, "Customer#000001303", "fQ Lp,FoozZe1", "ETHIOPIA 3", "ETHIOPIA", "AFRICA", "15-658-234-7985", "MACHINERY"),
        (1309, "Customer#000001309", "vQcJGUXPHMH2 5OWs1XUP0kx", "IRAN     2", "IRAN", "MIDDLE EAST", "20-821-905-5952", "AUTOMOBILE"),
        (1312, "Customer#000001312", "MVsKeqWejff8jQ30", "CANADA   9", "CANADA", "AMERICA", "13-153-492-9898", "BUILDING");
    """

    sql """
        INSERT INTO dates VALUES
        (19920410, "April 10, 1992", "Saturday", "April", 1992, 199204, "Apr1992", 7, 10, 101, 4, 15, "Spring", 1, 1, 0, 0),
        (19920411, "April 11, 1992", "Sunday", "April", 1992, 199204, "Apr1992", 1, 11, 102, 4, 15, "Spring", 0, 1, 0, 0),
        (19920412, "April 12, 1992", "Monday", "April", 1992, 199204, "Apr1992", 2, 12, 103, 4, 15, "Spring", 0, 1, 0, 1);
    """

    sql """
        INSERT INTO part VALUES
        (1165, "linen midnight", "MFGR#3", "MFGR#31", "MFGR#3117", "seashell", "MEDIUM BURNISHED NICKEL", 1, "LG PACK"),
        (1432, "metallic bisque", "MFGR#3", "MFGR#35", "MFGR#352", "aquamarine", "LARGE BURNISHED TIN", 21, "MED PKG"),
        (1455, "magenta blush", "MFGR#1", "MFGR#13", "MFGR#1324", "blue", "LARGE BRUSHED STEEL", 42, "SM PACK");
    """

    sql """
        INSERT INTO supplier VALUES
        (9, "Supplier#000000009", ",gJ6K2MKveYxQT", "IRAN     6", "IRAN", "MIDDLE EAST", "20-338-906-3675"),
        (15, "Supplier#000000015", "DF35PepL5saAK", "INDIA    0", "INDIA", "ASIA", "18-687-542-7601"),
        (29, "Supplier#000000029", "VVSymB3fbwaN", "ARGENTINA4", "ARGENTINA", "AMERICA", "11-773-203-7342");
    """

    sql """
        INSERT INTO lineorder VALUES
        (1309892, 2, 1303, 1165, 9, 19920517, "3-MEDIUM", 0, 21, 2404899, 5119906, 8, 2212507, 68711, 7, 19920616, "RAIL"),
        (1309892, 1, 1303, 1432, 15, 19920517, "3-MEDIUM", 0, 24, 2959704, 5119906, 7, 2752524, 73992, 0, 19920619, "TRUCK"),
        (1310179, 6, 1312, 1455, 29, 19921110, "3-MEDIUM", 0, 15, 1705830, 20506457, 10, 1535247, 68233, 8, 19930114, "FOB");
    """
}
