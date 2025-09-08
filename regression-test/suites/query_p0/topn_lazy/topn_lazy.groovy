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

suite("topn_lazy") {
    sql """
        set topn_lazy_materialization_threshold=1024;
        set runtime_filter_mode=GLOBAL;
        set TOPN_FILTER_RATIO=0.5;
        set disable_join_reorder=true;
        """
    // ========single table ===========
    //single table select all slots
    explain {
        sql "select * from lineorder where lo_orderkey>100 order by lo_orderkey  limit 1; "
        contains("projectList:[lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice, lo_ordtotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax, lo_commitdate, lo_shipmode]")
        contains("column_descs_lists[[`lo_linenumber` bigint NOT NULL, `lo_custkey` int NOT NULL, `lo_partkey` int NOT NULL, `lo_suppkey` int NOT NULL, `lo_orderdate` int NOT NULL, `lo_orderpriority` varchar(16) NOT NULL, `lo_shippriority` int NOT NULL, `lo_quantity` bigint NOT NULL, `lo_extendedprice` bigint NOT NULL, `lo_ordtotalprice` bigint NOT NULL, `lo_discount` bigint NOT NULL, `lo_revenue` bigint NOT NULL, `lo_supplycost` bigint NOT NULL, `lo_tax` bigint NOT NULL, `lo_commitdate` bigint NOT NULL, `lo_shipmode` varchar(11) NOT NULL]]")
        contains("locations: [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]]")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__lineorder]")
    }

    // no topn lazy since huge limit
    explain {
        sql "select lo_suppkey, lo_commitdate from lineorder where lo_orderkey>100 order by lo_orderkey  limit 1025;"
        notContains("VMaterializeNode")
    }

    // single table select some slots
    explain {
        sql "select lo_suppkey, lo_commitdate from lineorder where lo_orderkey>100 order by lo_orderkey  limit 1; "
        contains("projectList:[lo_suppkey, lo_commitdate]")
        contains("column_descs_lists[[`lo_suppkey` int NOT NULL, `lo_commitdate` bigint NOT NULL]]")
        contains("locations: [[1, 2]]")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__lineorder]")
    }

    // switch output slot order
    explain {
        sql("select lo_commitdate, lo_suppkey from lineorder where lo_orderkey>100 order by lo_orderkey  limit 1; ")
        contains("projectList:[lo_commitdate, lo_suppkey]")
        contains("column_descs_lists[[`lo_suppkey` int NOT NULL, `lo_commitdate` bigint NOT NULL]]")
        contains("locations: [[1, 2]]")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__lineorder]")
    }
    

    //============ join ================
    explain {
        sql("select * from lineorder, date where d_datekey > 0 and lo_orderdate = d_datekey order by d_date limit 5;")
        contains("projectList:[lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice, lo_ordtotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax, lo_commitdate, lo_shipmode, d_datekey, d_date, d_dayofweek, d_month, d_year, d_yearmonthnum, d_yearmonth, d_daynuminweek, d_daynuminmonth, d_daynuminyear, d_monthnuminyear, d_weeknuminyear, d_sellingseason, d_lastdayinweekfl, d_lastdayinmonthfl, d_holidayfl, d_weekdayfl]")
        contains("column_descs_lists[[`lo_orderkey` bigint NOT NULL, `lo_linenumber` bigint NOT NULL, `lo_custkey` int NOT NULL, `lo_partkey` int NOT NULL, `lo_suppkey` int NOT NULL, `lo_orderpriority` varchar(16) NOT NULL, `lo_shippriority` int NOT NULL, `lo_quantity` bigint NOT NULL, `lo_extendedprice` bigint NOT NULL, `lo_ordtotalprice` bigint NOT NULL, `lo_discount` bigint NOT NULL, `lo_revenue` bigint NOT NULL, `lo_supplycost` bigint NOT NULL, `lo_tax` bigint NOT NULL, `lo_commitdate` bigint NOT NULL, `lo_shipmode` varchar(11) NOT NULL], [`d_dayofweek` varchar(10) NOT NULL, `d_month` varchar(11) NOT NULL, `d_year` int NOT NULL, `d_yearmonthnum` int NOT NULL, `d_yearmonth` varchar(9) NOT NULL, `d_daynuminweek` int NOT NULL, `d_daynuminmonth` int NOT NULL, `d_daynuminyear` int NOT NULL, `d_monthnuminyear` int NOT NULL, `d_weeknuminyear` int NOT NULL, `d_sellingseason` varchar(14) NOT NULL, `d_lastdayinweekfl` int NOT NULL, `d_lastdayinmonthfl` int NOT NULL, `d_holidayfl` int NOT NULL, `d_weekdayfl` int NOT NULL]]")
        contains("locations: [[3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18], [19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33]")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__lineorder, __DORIS_GLOBAL_ROWID_COL__date]")
    }
    

    explain {
        sql "select lineorder.*, date.* from lineorder, date where d_datekey > 0 and lo_orderdate = d_datekey order by d_date limit 5;"
        contains("projectList:[lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice, lo_ordtotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax, lo_commitdate, lo_shipmode, d_datekey, d_date, d_dayofweek, d_month, d_year, d_yearmonthnum, d_yearmonth, d_daynuminweek, d_daynuminmonth, d_daynuminyear, d_monthnuminyear, d_weeknuminyear, d_sellingseason, d_lastdayinweekfl, d_lastdayinmonthfl, d_holidayfl, d_weekdayfl]")
        contains("column_descs_lists[[`lo_orderkey` bigint NOT NULL, `lo_linenumber` bigint NOT NULL, `lo_custkey` int NOT NULL, `lo_partkey` int NOT NULL, `lo_suppkey` int NOT NULL, `lo_orderpriority` varchar(16) NOT NULL, `lo_shippriority` int NOT NULL, `lo_quantity` bigint NOT NULL, `lo_extendedprice` bigint NOT NULL, `lo_ordtotalprice` bigint NOT NULL, `lo_discount` bigint NOT NULL, `lo_revenue` bigint NOT NULL, `lo_supplycost` bigint NOT NULL, `lo_tax` bigint NOT NULL, `lo_commitdate` bigint NOT NULL, `lo_shipmode` varchar(11) NOT NULL], [`d_dayofweek` varchar(10) NOT NULL, `d_month` varchar(11) NOT NULL, `d_year` int NOT NULL, `d_yearmonthnum` int NOT NULL, `d_yearmonth` varchar(9) NOT NULL, `d_daynuminweek` int NOT NULL, `d_daynuminmonth` int NOT NULL, `d_daynuminyear` int NOT NULL, `d_monthnuminyear` int NOT NULL, `d_weeknuminyear` int NOT NULL, `d_sellingseason` varchar(14) NOT NULL, `d_lastdayinweekfl` int NOT NULL, `d_lastdayinmonthfl` int NOT NULL, `d_holidayfl` int NOT NULL, `d_weekdayfl` int NOT NULL]]")
        contains("locations: [[3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18], [19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33]]")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__lineorder, __DORIS_GLOBAL_ROWID_COL__date]")
    }

    explain{
        sql "select date.*, lineorder.* from date, lineorder where d_datekey > 0 and lo_orderdate = d_datekey order by d_date limit 5;"
        contains("projectList:[d_datekey, d_date, d_dayofweek, d_month, d_year, d_yearmonthnum, d_yearmonth, d_daynuminweek, d_daynuminmonth, d_daynuminyear, d_monthnuminyear, d_weeknuminyear, d_sellingseason, d_lastdayinweekfl, d_lastdayinmonthfl, d_holidayfl, d_weekdayfl, lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice, lo_ordtotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax, lo_commitdate, lo_shipmode]")
        contains("column_descs_lists[[`d_dayofweek` varchar(10) NOT NULL, `d_month` varchar(11) NOT NULL, `d_year` int NOT NULL, `d_yearmonthnum` int NOT NULL, `d_yearmonth` varchar(9) NOT NULL, `d_daynuminweek` int NOT NULL, `d_daynuminmonth` int NOT NULL, `d_daynuminyear` int NOT NULL, `d_monthnuminyear` int NOT NULL, `d_weeknuminyear` int NOT NULL, `d_sellingseason` varchar(14) NOT NULL, `d_lastdayinweekfl` int NOT NULL, `d_lastdayinmonthfl` int NOT NULL, `d_holidayfl` int NOT NULL, `d_weekdayfl` int NOT NULL], [`lo_orderkey` bigint NOT NULL, `lo_linenumber` bigint NOT NULL, `lo_custkey` int NOT NULL, `lo_partkey` int NOT NULL, `lo_suppkey` int NOT NULL, `lo_orderpriority` varchar(16) NOT NULL, `lo_shippriority` int NOT NULL, `lo_quantity` bigint NOT NULL, `lo_extendedprice` bigint NOT NULL, `lo_ordtotalprice` bigint NOT NULL, `lo_discount` bigint NOT NULL, `lo_revenue` bigint NOT NULL, `lo_supplycost` bigint NOT NULL, `lo_tax` bigint NOT NULL, `lo_commitdate` bigint NOT NULL, `lo_shipmode` varchar(11) NOT NULL]]")
        contains("locations: [[3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17], [18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33]]")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__date, __DORIS_GLOBAL_ROWID_COL__lineorder]")
    }

    //======multi topn====
    explain {
        sql """    select *
                    from (
                        select * from lineorder order by lo_custkey limit 100
                    ) T join customer on c_custkey=lo_partkey
                    order by c_name limit 1;
                    """
        multiContains("VMaterializeNode", 1)
    }

    explain {
        sql """    select *
                    from 
                    customer left semi join (
                        select * from lineorder order by lo_custkey limit 100
                    ) T on c_custkey=lo_partkey
                    order by c_name limit 1;
                    """
        multiContains("VMaterializeNode", 1)
    }
    
        explain {
        sql """    select *
            from 
                    customer left semi join (
                        select * from lineorder order by lo_custkey limit 100
                    ) T on c_custkey=lo_partkey
                    order by c_name limit 1;
                    """
        multiContains("VMaterializeNode", 1)
    }
    
    qt_test_lazy1 """select * from date order by d_date limit 10;"""

    qt_test_lazy2 """SELECT d_datekey, d_date, d_dayofweek, d_month, d_year, d_yearmonthnum, d_daynuminweek, d_monthnuminyear, d_sellingseason FROM date ORDER BY d_date LIMIT 10;"""

    // test topn lazy materialization not blocked by offset
    explain {
        sql """
            select * from lineorder where lo_orderkey>100 order by lo_orderkey limit 10 offset 10001024;
            """
        contains("VMaterializeNode")
    }

    // test topn with row store
    sql """ DROP TABLE IF EXISTS date_row_store """
    sql """ 
    CREATE TABLE `date_row_store` (
        `d_datekey` int NOT NULL,
        `d_date` varchar(20) NOT NULL,
        `d_dayofweek` varchar(10) NOT NULL,
        `d_month` varchar(11) NOT NULL,
        `d_year` int NOT NULL,
        `d_yearmonthnum` int NOT NULL,
        `d_yearmonth` varchar(9) NOT NULL,
        `d_daynuminweek` int NOT NULL,
        `d_daynuminmonth` int NOT NULL,
        `d_daynuminyear` int NOT NULL,
        `d_monthnuminyear` int NOT NULL,
        `d_weeknuminyear` int NOT NULL,
        `d_sellingseason` varchar(14) NOT NULL,
        `d_lastdayinweekfl` int NOT NULL,
        `d_lastdayinmonthfl` int NOT NULL,
        `d_holidayfl` int NOT NULL,
        `d_weekdayfl` int NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`d_datekey`)
    DISTRIBUTED BY HASH(`d_datekey`) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "store_row_column" = "true"
    ); """
    sql """ INSERT INTO date_row_store select * from date; """

    qt_test_lazy3 """ select * from date_row_store order by d_date limit 10; """

    qt_test_lazy4 """SELECT d_datekey, d_date, d_dayofweek, d_month, d_year, d_yearmonthnum, d_daynuminweek, d_monthnuminyear, d_sellingseason FROM date_row_store ORDER BY d_date LIMIT 10;"""

    // Add new test cases for LEFT JOIN with different column orders
    sql """ DROP TABLE IF EXISTS users """
    sql """ CREATE TABLE users (
        user_id INT,
        user_name VARCHAR(50)
    ) DISTRIBUTED BY HASH(user_id) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    ); """

    sql """ DROP TABLE IF EXISTS orders """
    sql """ CREATE TABLE orders (
        order_id INT,
        user_id INT,
        order_amount DECIMAL(10,2)
    ) DISTRIBUTED BY HASH(user_id) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    ); """

    sql """ INSERT INTO users VALUES 
        (1, 'Alice'),
        (2, 'Bob'),
        (3, 'Charlie'),
        (4, 'David'),
        (5, 'Eve'); """

    sql """ INSERT INTO orders VALUES 
        (101, 1, 100.50),
        (102, 2, 200.75),
        (103, 3, 150.00); """

    // Test case 1: Original column order
    qt_test_lazy5 """
        SELECT u.user_id, u.user_name, o.order_id, o.order_amount 
        FROM users u LEFT JOIN orders o ON u.user_id = o.user_id 
        ORDER BY u.user_id LIMIT 5;
    """

    // Test case 2: Different column order
    qt_test_lazy6 """
        SELECT o.order_amount, u.user_name, o.order_id, u.user_id 
        FROM users u LEFT JOIN orders o ON u.user_id = o.user_id 
        ORDER BY u.user_id LIMIT 5;
    """

    // Test case 3: Another column order variation
    qt_test_lazy7 """
        SELECT u.user_name, o.order_id, u.user_id, o.order_amount 
        FROM users u LEFT JOIN orders o ON u.user_id = o.user_id 
        ORDER BY u.user_id LIMIT 5;
    """

    // Cleanup tables
    sql """ DROP TABLE IF EXISTS users """
    sql """ DROP TABLE IF EXISTS orders """

    sql """
drop table if exists table_100_undef_partitions2_keys3_properties4_distributed_by52;
create table table_100_undef_partitions2_keys3_properties4_distributed_by52 (
pk int,
col_int_undef_signed int    ,
col_int_undef_signed__index_inverted int    ,
col_decimal_20_5__undef_signed decimal(20,5)    ,
col_decimal_20_5__undef_signed__index_inverted decimal(20,5)    ,
col_decimal_76__20__undef_signed decimal(37, 20)    ,
col_decimal_76__20__undef_signed__index_inverted decimal(37, 20)    ,
col_varchar_100__undef_signed varchar(100)    ,
col_varchar_100__undef_signed__index_inverted varchar(100)    ,
col_char_50__undef_signed char(50)    ,
col_char_50__undef_signed__index_inverted char(50)    ,
col_date_undef_signed date    ,
col_date_undef_signed__index_inverted date    ,
col_datetime_undef_signed datetime    ,
col_datetime_undef_signed__index_inverted datetime    ,
col_tinyint_undef_signed tinyint    ,
col_tinyint_undef_signed__index_inverted tinyint    ,
INDEX col_int_undef_signed__index_inverted_idx (`col_int_undef_signed__index_inverted`) USING INVERTED,
INDEX col_decimal_20_5__undef_signed__index_inverted_idx (`col_decimal_20_5__undef_signed__index_inverted`) USING INVERTED,
INDEX col_decimal_76__20__undef_signed__index_inverted_idx (`col_decimal_76__20__undef_signed__index_inverted`) USING INVERTED,
INDEX col_varchar_100__undef_signed__index_inverted_idx (`col_varchar_100__undef_signed__index_inverted`) USING INVERTED,
INDEX col_char_50__undef_signed__index_inverted_idx (`col_char_50__undef_signed__index_inverted`) USING INVERTED,
INDEX col_date_undef_signed__index_inverted_idx (`col_date_undef_signed__index_inverted`) USING INVERTED,
INDEX col_datetime_undef_signed__index_inverted_idx (`col_datetime_undef_signed__index_inverted`) USING INVERTED,
INDEX col_tinyint_undef_signed__index_inverted_idx (`col_tinyint_undef_signed__index_inverted`) USING INVERTED
) engine=olap
DUPLICATE KEY(pk)
distributed by hash(pk) buckets 10
properties("replication_num" = "1");
insert into table_100_undef_partitions2_keys3_properties4_distributed_by52(pk,col_int_undef_signed,col_int_undef_signed__index_inverted,col_decimal_20_5__undef_signed,col_decimal_20_5__undef_signed__index_inverted,col_decimal_76__20__undef_signed,col_decimal_76__20__undef_signed__index_inverted,col_varchar_100__undef_signed,col_varchar_100__undef_signed__index_inverted,col_char_50__undef_signed,col_char_50__undef_signed__index_inverted,col_date_undef_signed,col_date_undef_signed__index_inverted,col_datetime_undef_signed,col_datetime_undef_signed__index_inverted,col_tinyint_undef_signed,col_tinyint_undef_signed__index_inverted) values (99,96,10.0245,54.1981,-1831021433,null,-27601,'2001-04-02 10:53:11','rnw',null,null,'2018-09-02 15:39:59','2013-08-13','2013-05-02','2004-01-12',null,5),(98,930109687,101,null,3.1322,52.0983,22509,'÷','rnw','some','2008-03-27','2002-06-06','2025-09-01','2006-08-12','2015-07-01',5,5),(97,null,37.1195,242126287,101,35.0750,-657037509,null,'him','one','l','2024-12-31','2006-05-08','2024-12-31 12:01:00','2005-03-06',5,4),(96,-26,-76,-1792374929,24696,101,101,'2003-06-06','how','2002-09-27','rnw','2025-09-01','2009-01-26','2011-11-24','2025-01-01',5,4),(95,null,-72,101,2.1144,-18,15637,'rnw',null,'2011-07-27','x','2025-01-01','2009-09-19','2024-12-31 12:01:00','2024-01-05 16:40:00',1,5),(94,101,182674832,29982,101,62,101,'rnw','2008-03-05','rnw','2017-07-26 14:24:26','2001-08-12','2024-12-31','2025-09-01','2025-01-01',5,2),(93,null,46.1013,null,null,32.1405,null,'2005-01-06 07:18:39','i','2002-04-27 17:59:59','would','2000-12-20 06:34:58','2025-09-01','2025-09-01','2019-04-11',null,1),(92,-959883076,101,83.0503,null,101,101,null,'rnw','did','j','2019-12-04','2013-06-24 07:41:35','2025-09-01','2014-07-17 12:08:38',5,5),(91,101,24,1209967381,2022142382,-48,101,'rnw','h','rnw','m','2008-10-28','2002-10-26','2025-09-01','2024-12-31 12:01:00',3,1),(90,25,-20838,101,73.0018,-8,-85,'ب','but','u','up','2010-12-19 14:25:33','2024-01-05','2000-06-26','2003-07-19 14:54:37',5,3),(89,30.0472,-13,46.1132,101,54354290,-12706,'rnw','2001-11-22','2004-03-09 17:25:40','2018-07-04 03:17:51','2014-01-22','2002-05-01','2025-01-01','2018-11-20',null,1),(88,9852,101,12.1926,null,-93,-15319,null,'him','it''s','2008-11-02 23:48:48','2012-11-14','2025-01-01','2024-12-31 12:01:00','2008-03-07',4,3),(87,2029780350,null,-15678,-12,30211,33,'rnw','2009-08-09','g','2015-08-08','2025-01-01','2019-08-23','2025-01-01','2015-07-04',4,2),(86,13917,1375383765,1793486448,-1429126865,283348690,-1138,';',';','so',null,'2004-06-28','2015-11-02','2025-09-01','2024-12-31 12:01:00',4,4),(85,-40,101,-27902,101,null,-38,'d','y','why','2015-02-07 12:18:55','2013-08-08','2014-10-20','2025-01-01','2006-12-24',3,4),(84,null,41.0754,-1404996759,5.1582,null,56.1351,'2004-07-12','2014-11-12 18:13:09','tell','2012-03-17 00:52:32','2005-01-26','2011-08-27 14:34:08','2005-06-14','2009-08-27 06:18:41',4,3),(83,-31003,228719825,-322340149,48,null,-164955896,'2013-08-07 13:00:03','rnw','m',null,'2014-03-08','2012-01-16 10:12:04','2009-03-15','2008-11-19 01:19:46',4,4),(82,null,101,49.0531,-29433,null,87.1118,'v',null,'∞','not','2024-12-31','2024-12-31','2000-08-21 00:54:14','2008-08-02 03:06:00',1,1),(81,101,10195,33,101,101,null,'as','-','z','2005-06-17','2024-12-31','2002-11-21','2008-11-11','2025-09-01',3,2),(80,null,-100795903,101,6160,null,-7,'u','right','rnw',null,'2002-01-01 03:35:00','2016-01-19','2018-05-04','2002-08-13',2,4),(79,null,null,9392,-10358,99.0928,null,'2006-07-14','2009-11-16 06:10:52','2002-02-28',null,'2006-10-09','2025-09-01','2001-10-13','2025-09-01',3,3),(78,9607,101,100.1260,-79,null,372750981,'been',null,'2005-12-19 08:15:27','that''s','2005-01-10','2017-10-04','2013-01-13 13:58:37','2010-08-06 12:16:49',1,4),(77,null,null,-8324,101,null,69.0938,'2000-10-26','2011-12-10','not','rnw','2025-01-01','2010-02-22','2000-03-26','2014-05-14 12:11:51',null,1),(76,-26609,-1739809511,null,null,null,66.0745,'≠','f','2010-07-23','okay','2006-03-18','2018-12-23','2015-08-01','2019-11-25 06:17:13',null,null),(75,null,null,-1641643444,47,null,-22559,'one','of','rnw','¥','2024-12-31','2024-01-05','2002-11-08 08:27:42','2024-12-31 12:01:00',3,3),(74,101,74.1831,-60,41.0928,43.0388,null,'2010-01-24',null,'2008-09-24 09:40:05','2005-03-22 19:06:02','2007-01-12 16:47:35','2025-01-01','2000-09-17 22:07:52','2024-01-05 16:40:00',3,5),(73,227868312,-921802792,23073,-69,-91984583,101,'p','rnw','look','out','2016-04-28','2013-02-17','2016-03-24','2024-12-31 12:01:00',null,null),(72,-1942,13909,-430288485,25153,67,28.1108,'2014-05-28','z','2010-02-01','2002-08-19 07:07:05','2025-01-01','2025-09-01','2024-01-05 16:40:00','2017-10-25',3,4),(71,101,-99,-1968729210,66.1977,-1961018600,1532028300,'\t','and','and','£','2013-07-21 07:38:55','2002-09-23 05:07:56','2004-06-06','2017-07-05 07:47:26',2,null),(70,null,null,20.1564,-9041,-1476497981,96.1325,'a','2016-07-22','with','!','2002-01-23 16:13:19','2019-03-08','2015-06-20','2025-09-01',3,4),(69,101,14.0279,-25959,101,null,101,'z','2016-02-08 21:27:56',null,null,'2024-01-05','2025-09-01','2016-10-27','2024-12-31 12:01:00',5,1),(68,-1852981503,2100535395,null,31834,-238597916,null,'are','rnw','2001-01-19','j','2024-01-05','2002-06-09','2001-12-03','2006-03-02',1,3),(67,101,-13899,null,-2042560281,19631,88,'n','go','2016-02-28','2011-04-14','2025-01-01','2002-04-18','2001-11-20 17:58:41','2010-01-01',5,null),(66,-24375,-53,null,3637,-30993,34,',','p',null,'2009-04-11 07:04:07','2010-04-14 11:43:43','2004-01-23','2017-06-02','2004-02-28',3,5),(65,-110439692,null,-966852224,-45,8709,101,'x','good',null,'hey','2024-12-31','2015-12-03 23:02:13','2024-12-31 12:01:00','2014-09-20',null,3),(64,101,5.0607,77.0525,-1041530656,null,101,'i','yes','2006-07-22','e','2005-08-24','2017-07-27','2015-08-08','2025-01-01',5,4),(63,-20798,-65,101,101,59.1061,101,'rnw','t','2012-11-18 03:16:56','rnw','2005-06-05','2005-11-26','2025-01-01','2024-12-31 12:01:00',1,5),(62,99,-113,-44,15,76.0490,76.0544,'rnw','rnw','rnw','time','2024-12-31','2024-12-31','2019-02-10 14:35:04','2024-12-31 12:01:00',3,null),(61,null,null,-21984,19495,536907096,107,'have','−','b','g','2024-12-31','2012-05-13','2011-06-05','2005-09-15',3,1),(60,-63,578647617,-88,-5757,-19937,-1411057885,'be',null,null,'rnw','2014-03-14','2025-01-01','2025-01-01','2008-12-12',4,null),(59,101,null,null,83.0025,-128,73.1448,'rnw','2011-07-14 03:26:45','all','here','2024-01-05','2016-11-18','2024-12-31 12:01:00','2005-06-09',4,null),(58,101,5512,null,101,null,null,'rnw','2009-03-19 16:00:25','there','+','2012-04-20','2025-09-01','2008-04-20','2025-01-01',null,5),(57,null,115,20175,126,101,null,'2009-12-18','2003-07-23',null,'“','2003-06-26 07:32:09','2025-09-01','2012-07-22 23:52:56','2006-05-26',1,4),(56,92.0298,-1998138065,-2108929095,null,null,1996704426,null,'now','2001-09-20','rnw','2004-09-26','2006-02-12','2009-10-03','2024-01-05 16:40:00',null,3),(55,null,null,-1333896545,null,1602812377,-1301187766,'p','he''s','2002-09-22 07:49:40','we','2014-11-11','2007-05-03','2024-01-05 16:40:00','2025-01-01',1,2),(54,-121,921831233,-60,31417,-714058163,101,'2013-02-24','x','rnw','this','2009-04-27','2003-12-01','2005-07-26','2015-11-22 18:47:03',5,1),(53,23.0916,-65,94.1610,null,-11774,101,'i',null,'2016-08-10','ok','2013-07-03','2011-02-08 19:53:06','2009-04-10','2013-07-19',3,1),(52,10.0036,69,101,-25,2078055611,-1193089339,null,'2007-01-17','rnw','2012-11-21','2024-12-31','2019-10-17','2002-01-17','2006-10-20',1,4),(51,490641712,101,-1208425665,1704944216,101,35.1109,'2001-10-24 13:04:02','2018-11-03','h','rnw','2008-03-08 00:51:14','2024-12-31','2013-12-09','2024-12-31 12:01:00',4,5),(50,101,-30892,85.0626,null,2046625791,19477,'2009-01-16 21:00:31',null,'then','rnw','2024-01-05','2019-10-09 21:12:04','2009-10-17','2008-10-09 10:36:02',4,4),(49,-7948,null,-1237358454,-24289,101,101,null,'don''t','2006-08-26 13:33:08','right','2024-12-31','2003-11-23 15:09:42','2011-05-09','2017-10-28',1,4),(48,101,1212190129,101,101,-16955,2024921712,'2015-03-13 16:38:24','well','♪','2017-02-26','2024-12-31','2025-09-01','2015-12-01','2011-04-06',2,2),(47,-86,-1183247462,-28494,53.1596,-99,null,'z','e','rnw','y','2014-10-11','2012-02-19','2004-12-03','2017-10-27 21:56:52',5,2),(46,816733664,30802,null,1590,16.1304,-1926619389,null,'yeah','rnw',null,'2025-09-01','2005-11-10','2006-02-03 04:12:12','2024-01-05 16:40:00',4,2),(45,25113,94.1092,101,-18569,-14,null,'got','rnw','rnw','that''s','2010-10-15','2019-05-10','2006-12-28','2006-09-03',4,2),(44,10400,null,101,101,101,101,'rnw',null,'rnw','2014-02-18','2025-01-01','2011-01-09 00:25:02','2024-01-05 16:40:00','2016-03-05',2,3),(43,-32740,14.1968,122,16805,28029,null,'y','then','or','2007-01-19 21:41:58','2025-01-01','2011-05-08 05:03:05','2025-01-01','2024-12-31 12:01:00',2,2),(42,-2014681343,null,1.1275,1773220791,91.1451,null,'2001-12-08 21:43:13',null,null,'up','2018-03-07 11:06:43','2024-12-31','2025-01-01','2019-07-01',2,4),(41,118,957360875,-17259,null,-2106787953,-9,'b','rnw',null,'-','2025-09-01','2024-01-05','2024-01-05 16:40:00','2025-09-01',4,null),(40,26178,37,-103,-1276334519,101,90.1959,'u','that''s','2005-12-07 20:01:22','?','2018-12-11','2024-12-31','2002-01-06 02:12:09','2000-12-24 10:51:46',4,5),(39,-88,101,101,-29198,69.1583,-1743131560,'2000-07-24','what',null,'2009-05-03','2018-05-23 07:21:11','2018-04-15 20:51:41','2008-07-20','2024-12-31 12:01:00',3,null),(38,null,101,-90,101,11270,-17602,'k','well','2015-03-06',null,'2025-09-01','2007-05-12','2014-05-13','2004-04-11',null,1),(37,43.0554,null,-73,-15,null,null,'rnw','there',null,'2002-06-08','2009-08-25 00:01:37','2006-04-20 19:53:05','2025-09-01','2024-12-31 12:01:00',2,null),(36,31,3147,-7857,-28137,-127267983,1979222536,'rnw','2002-04-28','2010-05-13',null,'2024-01-05','2011-02-06','2025-01-01','2006-02-07',null,5),(35,-7487,887749819,2146643194,101,26,101,'?','2009-03-01','2017-10-18 16:38:48','2010-08-14','2007-06-24 16:59:19','2019-02-16','2007-07-04','2016-12-10',null,null),(34,76.1479,null,101,-1249311707,93.0460,null,null,'for','2000-02-16','on','2018-04-04','2025-09-01','2025-01-01','2018-12-28',null,1),(33,-26,2085464347,95.1138,8797,null,101,'∞','k','2002-10-27 00:17:02','rnw','2008-05-13','2003-11-28','2013-03-01','2003-05-21',4,5),(32,254455493,17610,61.1001,101,null,2420,'rnw','say','2018-05-18',null,'2004-07-02','2024-01-05','2024-12-31 12:01:00','2003-07-21 00:22:05',2,1),(31,51.0660,101,4.1063,null,34.1067,27802,'rnw','going','rnw','2011-05-11','2015-05-27','2006-02-12','2010-06-17','2005-11-17 18:51:56',5,2),(30,101,33.1534,75.1832,3200,101,16.1574,'2008-08-06 10:47:16','o','rnw','when','2006-04-28','2000-03-13 06:14:22','2013-02-23','2013-10-09',1,4),(29,-45,101,null,48.1770,101,-45,'2018-12-22 06:09:20','2003-03-16','2002-12-26',null,'2008-04-28','2025-09-01','2009-06-03','2025-01-01',2,3),(28,-45,-1099209347,20.0518,37.1517,null,1355916103,'it','rnw','g','rnw','2011-04-24','2024-12-31','2024-01-05 16:40:00','2002-01-17',5,3),(27,51.0374,110,101,null,1425908152,null,'2009-02-22','=','q',null,'2007-10-25 00:17:03','2006-10-18 22:14:53','2009-01-17 05:32:53','2024-12-31 12:01:00',4,4),(26,null,101,null,null,172556776,34,'2008-06-22 12:49:38','rnw','2001-01-05','.','2001-04-14','2014-11-14','2010-03-18 07:04:12','2024-12-31 12:01:00',3,1),(25,57,101,-32660,101,-975011907,-24273,'2005-09-06','2007-11-14 11:49:50','at','rnw','2014-07-27','2015-11-28','2025-09-01','2007-04-25',5,2),(24,607616076,6268,21,-792714526,-3178,43.0493,'rnw','2011-08-04','2011-10-01 11:53:33','2004-07-11','2024-01-05','2007-10-12','2003-09-14 10:16:03','2024-12-31 12:01:00',null,2),(23,101,-1798512625,604898133,54,1593803284,-20,'they','2016-02-09 18:11:09',null,'ع','2024-12-31','2005-09-07 10:46:33','2025-09-01','2007-08-16 07:07:33',4,1),(22,null,1410623173,null,-29,1306843248,-690953271,'really','2008-05-04 21:12:54','2018-10-19 12:40:12','that','2025-01-01','2009-07-12','2025-01-01','2007-12-07',3,4),(21,-42,-18302,101,null,-1670597842,14557,'s','then',null,'2015-06-03 03:52:57','2004-07-05 20:50:36','2017-09-25 18:33:18','2017-02-01','2024-12-31 12:01:00',1,4),(20,-726471646,null,105,1263164908,101,1201066505,'2003-03-18 04:04:44','a',null,'c','2003-07-11','2004-08-22 01:20:52','2000-05-16 23:29:31','2017-04-09',2,4),(19,null,101,40.1426,-103,null,null,'2003-02-12',null,'2010-01-04','p','2006-07-22','2014-05-06 01:10:52','2025-09-01','2002-01-19',2,null),(18,1235631086,-929061630,120,82.0120,-88,null,null,'t','2013-10-17','2010-08-18','2016-07-24 20:41:40','2017-01-11','2024-12-31 12:01:00','2024-12-31 12:01:00',3,3),(17,101,64.1884,-27565,15622,-1742992154,383354227,'2004-07-13 11:40:21','p',null,'=','2005-07-12','2006-12-19','2024-12-31 12:01:00','2012-09-24',2,3),(16,-52,-520555179,null,-10045,-14134,-1571551312,'2007-09-17 02:50:51','j','know','d','2005-03-22','2004-09-11','2025-09-01','2009-01-28',5,2),(15,null,20154,48.1534,-18907,101,67,null,'rnw','2013-07-20','2018-12-27 13:07:55','2025-01-01','2025-01-01','2025-01-01','2024-12-31 12:01:00',2,5),(14,-16,101,962081387,-29004,63.0021,1976714324,null,'rnw','u',null,'2025-01-01','2007-01-10 08:51:48','2015-12-01','2025-09-01',1,4),(13,null,16825,null,3.0322,37.1650,null,'⭐','m','2000-08-17','2004-03-17 07:08:51','2013-03-16','2024-12-31','2002-03-05','2024-01-05 16:40:00',4,2),(12,28327,17.1838,-1453335927,null,-1618796614,25094,'2015-10-17 04:41:45',null,null,'DEL','2000-08-15 06:30:29','2016-05-20 04:29:07','2001-09-05','2025-01-01',2,4),(11,101,101,null,102,101,-4,'2001-04-16','when','2018-06-27 15:17:52','okay','2024-12-31','2004-07-16','2009-05-10','2007-03-26',5,2),(10,null,-19386,18.0074,149095826,66.0910,null,'your','really','you',null,'2025-01-01','2025-09-01','2010-01-07','2002-03-02 14:32:04',1,5),(9,-59,null,null,17.1956,null,null,'out','¥','you''re','2002-08-12 02:29:29','2013-03-12','2001-09-18 12:39:49','2005-06-10','2025-01-01',null,3),(8,114,null,null,14,28564,3559,null,'2019-03-21 14:06:27','just','2013-02-26','2001-11-03 15:18:51','2019-06-15 16:13:07','2025-09-01','2012-09-22 08:11:08',null,1),(7,-23183,101,1178947591,55,null,null,'2000-08-12','rnw','ع','ñ','2001-02-26','2015-09-04','2025-09-01','2025-09-01',null,null),(6,-859677910,101,101,101,-20552,101,'rnw','her','=','!','2003-12-06','2003-12-17 14:27:41','2005-01-15','2000-12-22',null,5),(5,66,101,2000724422,88,12.0571,59.0938,'e','2000-02-24','he''s','rnw','2014-12-27 12:04:24','2024-12-31','2024-01-05 16:40:00','2006-05-08',3,2),(4,-5294,26,1881027595,29091,1461076672,null,'2014-09-01','rnw','2006-10-02 20:11:28','from','2006-11-26','2010-08-24 07:32:29','2025-01-01','2018-02-18 04:31:42',null,2),(3,1977476414,-56,18881,737869454,9408,19.0698,'o','rnw','the',null,'2006-06-09','2015-06-08','2025-09-01','2014-05-03',3,5),(2,-14461,5224,-66,23.0247,-1071337819,-5713,'2013-08-19','2002-11-06','2000-04-17 05:07:04',null,'2025-09-01','2015-02-02','2002-01-25 14:28:49','2004-06-04',3,5),(1,-36,93.1244,null,101,72.0543,1051586608,'can''t','you',null,'©','2014-03-03 10:25:40','2024-01-05','2007-06-22 05:11:54','2001-11-22',null,3),(0,-118,null,1979415894,26.1640,-1573185656,21.0770,'rnw','rnw','rnw','©','2012-03-12','2010-08-21','2025-09-01','2012-07-22',null,3);

drop table if exists table_100_undef_partitions2_keys3_properties4_distributed_by55;
create table table_100_undef_partitions2_keys3_properties4_distributed_by55 (
pk int,
col_int_undef_signed int    ,
col_int_undef_signed__index_inverted int    ,
col_decimal_20_5__undef_signed decimal(20,5)    ,
col_decimal_20_5__undef_signed__index_inverted decimal(20,5)    ,
col_decimal_76__20__undef_signed decimal(37, 20)    ,
col_decimal_76__20__undef_signed__index_inverted decimal(37, 20)    ,
col_varchar_100__undef_signed varchar(100)    ,
col_varchar_100__undef_signed__index_inverted varchar(100)    ,
col_char_50__undef_signed char(50)    ,
col_char_50__undef_signed__index_inverted char(50)    ,
col_date_undef_signed date    ,
col_date_undef_signed__index_inverted date    ,
col_datetime_undef_signed datetime    ,
col_datetime_undef_signed__index_inverted datetime    ,
col_tinyint_undef_signed tinyint    ,
col_tinyint_undef_signed__index_inverted tinyint    ,
INDEX col_int_undef_signed__index_inverted_idx (`col_int_undef_signed__index_inverted`) USING INVERTED,
INDEX col_decimal_20_5__undef_signed__index_inverted_idx (`col_decimal_20_5__undef_signed__index_inverted`) USING INVERTED,
INDEX col_decimal_76__20__undef_signed__index_inverted_idx (`col_decimal_76__20__undef_signed__index_inverted`) USING INVERTED,
INDEX col_varchar_100__undef_signed__index_inverted_idx (`col_varchar_100__undef_signed__index_inverted`) USING INVERTED,
INDEX col_char_50__undef_signed__index_inverted_idx (`col_char_50__undef_signed__index_inverted`) USING INVERTED,
INDEX col_date_undef_signed__index_inverted_idx (`col_date_undef_signed__index_inverted`) USING INVERTED,
INDEX col_datetime_undef_signed__index_inverted_idx (`col_datetime_undef_signed__index_inverted`) USING INVERTED,
INDEX col_tinyint_undef_signed__index_inverted_idx (`col_tinyint_undef_signed__index_inverted`) USING INVERTED
) engine=olap
DUPLICATE KEY(pk)
distributed by hash(pk) buckets 10
properties("replication_num" = "1");
insert into table_100_undef_partitions2_keys3_properties4_distributed_by55(pk,col_int_undef_signed,col_int_undef_signed__index_inverted,col_decimal_20_5__undef_signed,col_decimal_20_5__undef_signed__index_inverted,col_decimal_76__20__undef_signed,col_decimal_76__20__undef_signed__index_inverted,col_varchar_100__undef_signed,col_varchar_100__undef_signed__index_inverted,col_char_50__undef_signed,col_char_50__undef_signed__index_inverted,col_date_undef_signed,col_date_undef_signed__index_inverted,col_datetime_undef_signed,col_datetime_undef_signed__index_inverted,col_tinyint_undef_signed,col_tinyint_undef_signed__index_inverted) values (99,1192057302,-53,97.1041,4369,101,101,'2003-04-14','want','2000-10-28 07:25:51','-','2024-01-05','2016-05-22','2025-09-01','2025-09-01',4,3),(98,182528010,29.1486,96.0760,45.1118,-18749,24712,'2009-05-01','if','all','because','2024-12-31','2010-10-19','2024-12-31 12:01:00','2011-02-19',4,1),(97,93.0034,101,29.1942,1803103912,793884609,null,'rnw','2009-11-06','rnw','now','2015-10-05','2010-09-06','2012-09-06 11:35:28','2007-12-21',2,4),(96,15989,null,17.0338,-10690,8857,-86,'2016-10-20','time','p','rnw','2000-08-26','2024-01-05','2017-02-19','2006-06-13',null,1),(95,-29336,null,63.0362,50.1693,101,-83,null,'rnw',null,'2008-08-07','2011-02-26 20:33:04','2005-10-22 08:24:03','2010-03-24 19:21:28','2018-04-28',4,null),(94,null,61,101,80.0507,29316,null,null,'2003-01-07','n','2001-02-19 13:51:33','2025-09-01','2024-01-05','2024-01-05 16:40:00','2024-01-05 16:40:00',2,2),(93,40,611614098,-16681,-47,101,-961832056,null,'❤️','i','rnw','2007-04-20','2014-03-18','2025-01-01','2002-10-28',3,2),(92,31171,101,-2117054704,25025,null,null,'rnw','come','2004-08-13 22:25:04','rnw','2005-02-26 00:53:16','2018-09-16','2008-05-03','2008-11-23',3,3),(91,64.0762,88.1203,94,-765098767,101,116,'2009-07-23','2007-09-10 05:46:41','v',null,'2003-02-27','2024-12-31','2024-01-05 16:40:00','2002-03-25 18:59:58',2,5),(90,-66,101,75.0782,37.0999,null,null,null,null,'him','2008-07-18','2024-01-05','2024-12-31','2018-04-09','2012-02-18',4,1),(89,-3697,862,1002401442,19.0431,105,21.1165,'yes',null,'of','at','2024-12-31','2010-10-12','2005-11-26','2010-02-25',2,5),(88,2,40,740349019,71.0260,83.0888,82.0980,'ñ','2013-09-27','e','rnw','2024-12-31','2024-01-05','2017-03-14','2007-11-11',5,2),(87,null,2141896382,101,-678890122,9.0390,94,'2010-06-03 08:00:46','2000-06-18','2012-06-01 01:02:11',null,'2019-10-22 16:29:22','2002-08-10','2024-01-05 16:40:00','2009-10-17',null,2),(86,null,35,101,101,null,73.0405,'k','⭐','2002-05-17 16:26:03','rnw','2013-07-17','2013-11-28','2017-01-16 02:28:53','2003-04-27',3,5),(85,null,-1781710580,1431596902,8.1871,22,9465,null,'rnw','on',null,'2025-09-01','2012-11-11','2014-07-01','2004-01-11 05:12:18',5,2),(84,-3,21.0128,12,-209,101,795978218,null,'m',null,'×','2006-01-09','2008-09-07','2006-11-24 14:43:05','2006-08-16',2,2),(83,69.0562,101,20187,-897641004,-20657,-706254776,'rnw','because','your','y','2016-07-25','2008-08-19','2014-07-19 04:24:38','2025-09-01',null,4),(82,44.0944,101,null,-1693736993,-9,-1059004549,'2016-02-09 11:43:31',null,'⭐','rnw','2025-09-01','2003-08-05','2005-09-17','2024-12-31 12:01:00',4,4),(81,40.1184,18,101,-3961,101,-54,'2013-08-16 14:51:20','u','2008-05-28 10:30:13','⭐','2014-10-05 19:31:22','2000-03-10 04:13:58','2012-02-16','2004-09-21',1,3),(80,101,101,101,67.1883,18.1814,null,'2014-09-07 22:01:29','r','h','2005-08-23','2015-10-05','2015-02-08','2004-08-20 12:30:24','2003-12-26',4,2),(79,-9231,1926682969,76,-44382961,101,101,'2002-09-18','but','rnw','rnw','2024-01-05','2005-11-27','2024-12-31 12:01:00','2000-01-26 15:09:45',null,1),(78,-128,58,101,13.0705,-284763404,null,null,'2016-12-27','say','2007-02-06 17:41:10','2019-11-11 19:56:41','2007-01-20 01:45:57','2011-05-13 05:36:13','2008-11-27 04:11:17',1,3),(77,101,null,76,-15,-1239528980,126,'to','rnw',null,null,'2010-11-28 20:05:46','2009-06-17','2019-07-15','2002-03-20',3,3),(76,null,15627,-1491156576,-1651067311,28,-26,'can''t','been','2002-04-08 07:21:00','they','2011-07-22','2025-09-01','2002-11-17 04:42:15','2007-08-06',5,3),(75,null,-8898,null,31186,-23,-80,'q','got','−','oh','2024-01-05','2010-09-16','2009-06-08','2007-07-19 16:35:27',1,4),(74,-110,68.1838,97.0554,-1530450175,-126,-97427501,'rnw','2007-06-03 07:58:29',null,'rnw','2025-01-01','2024-01-05','2015-10-14','2004-11-25',5,1),(73,70,77.1732,null,19461,18.0406,11.1157,'2013-12-18','her','in','2007-11-19','2000-04-26 10:14:36','2024-01-05','2002-06-08','2010-07-20',4,5),(72,null,-566520587,101,null,null,97.1238,'2019-03-26','you','2018-12-17 18:58:40','2013-03-23 18:53:03','2025-01-01','2014-03-04 18:52:06','2025-09-01','2024-01-05 16:40:00',4,4),(71,-484204015,34,-352,-447308270,28691,-56,'2005-07-18 19:03:23','2019-07-27 13:07:47','2014-08-09',null,'2025-01-01','2024-12-31','2017-06-28','2019-07-27 14:17:49',2,2),(70,848164599,100.1048,13309,null,39,null,'2003-06-10 10:09:06',':','something','¥','2025-09-01','2012-11-18','2002-05-28','2015-11-03',2,2),(69,1895325637,101,null,null,-878042324,1821774218,null,'rnw','t','2013-10-16','2011-12-25 08:44:31','2014-07-06','2018-02-09 01:42:48','2025-09-01',5,3),(68,-1254151053,4,52,2.1581,null,-8520,'2011-01-01 04:32:39','it''s','okay','mean','2024-12-31','2016-10-24','2016-05-06','2025-01-01',2,2),(67,98,null,null,null,2043308113,null,'this',null,'rnw','s','2006-10-21 10:18:06','2025-01-01','2019-06-19 03:28:44','2018-09-11 19:59:37',null,null),(66,16615,101,101,38.0974,null,-9175,'2017-07-03','”','2015-04-02 20:11:55','”','2024-01-05','2024-01-05','2015-08-06 01:51:04','2008-06-08 09:17:28',2,null),(65,null,-400332668,null,-1919241029,-597580778,-172877564,'2014-07-12','2001-10-04','2015-11-25','your','2025-09-01','2005-12-15','2002-08-19','2002-12-15',4,4),(64,101,101,84,null,101,70.1069,'2005-04-13','2006-11-02','2010-04-03 13:19:57','2017-12-04 09:52:01','2004-05-28 10:08:19','2024-12-31','2011-09-07','2012-02-05',1,1),(63,-1528362172,61.0966,-320,3,19465,-794675418,'i','2015-01-05',null,'2004-08-13 21:30:22','2024-01-05','2024-12-31','2001-04-23','2010-04-26 13:29:13',null,5),(62,101,477087066,73.1544,null,106,1710755242,null,null,'2001-07-13 02:21:30','2003-05-23','2025-09-01','2007-02-21','2002-03-21','2000-02-28 11:21:35',null,5),(61,-604327882,null,101,15871,65.0472,494147977,'z','q',null,'2015-05-26 13:56:21','2007-09-28','2019-10-04','2004-04-06 21:57:04','2008-12-02 05:17:00',null,4),(60,101,-1560980862,12730,-1746768071,40.0751,40.1159,'2017-08-08','2007-03-08','you''re','rnw','2017-09-26 00:30:33','2005-06-23 16:02:18','2025-01-01','2005-09-22',4,2),(59,-176447130,-9,73.1381,1940327035,-60,-24789675,'rnw','w','rnw','2003-12-05 13:45:56','2000-03-05 01:31:26','2024-01-05','2015-11-06 04:02:46','2010-04-21',1,5),(58,null,95,null,null,101,30786032,'rnw','2004-08-04','v','ok','2003-06-19','2012-01-27 12:30:52','2012-01-02','2025-01-01',3,2),(57,-948768394,34.0548,null,77.1832,7.0781,-121,'“',null,'2009-09-14','h','2006-01-28','2025-01-01','2009-04-15','2003-01-21',1,4),(56,-1379560203,1413312208,-1892411462,-519293415,63.0768,-20639,'w','2001-12-06 16:47:09',null,'rnw','2006-08-04','2011-06-19','2010-08-14','2001-06-25',3,1),(55,120,297800030,null,1120217109,101,738872126,null,null,'2009-09-10','a','2002-07-03','2000-07-24','2025-01-01','2005-11-15 07:39:16',5,1),(54,105,null,1441777520,98.0865,56,23.1370,'b','g','tell','rnw','2024-12-31','2016-04-07','2007-12-24','2000-06-22',1,null),(53,17.0567,36.1696,13.0322,-1353421680,null,-33,'rnw',null,'rnw','rnw','2005-01-18 20:38:49','2024-01-05','2012-04-24','2000-11-21',2,2),(52,-17411,-491137373,null,101,-50,101,null,'2012-10-13 08:49:41','2018-01-05 17:41:18',null,'2016-09-04 18:47:10','2001-05-07','2025-09-01','2016-03-06',3,4),(51,1444433911,101,18274,-7930,1369070994,24.0359,'2012-02-10','then','good','hey','2025-09-01','2025-01-01','2024-12-31 12:01:00','2017-11-27',2,4),(50,-9440,100.0759,71,101,101,1174092323,'2014-11-25 16:40:10',null,null,'2003-09-05 22:43:05','2015-07-19','2024-01-05','2024-12-31 12:01:00','2015-06-12 13:35:53',2,1),(49,null,null,1518721323,1796291024,-67,27754,'mean',null,'been',null,'2024-12-31','2025-01-01','2016-11-05','2000-01-24',null,3),(48,-10,101,30462,-30885,-67,101,'2002-12-24','”','had','2005-12-12','2025-01-01','2025-01-01','2011-05-10','2001-06-02',5,4),(47,101,51.1182,null,null,101,45.1427,'h',null,'did','2014-08-20 11:38:05','2006-07-25','2024-01-05','2007-08-27 20:33:59','2007-09-13',2,2),(46,-99,187677740,-23,-6,25.1936,8112,'2017-08-06','rnw','q','2019-10-21','2005-01-25 00:22:35','2013-08-07 23:32:22','2017-09-15','2000-05-06',2,1);

drop table if exists table_20_undef_partitions2_keys3_properties4_distributed_by55;
create table table_20_undef_partitions2_keys3_properties4_distributed_by55 (
pk int,
col_int_undef_signed int    ,
col_int_undef_signed__index_inverted int    ,
col_decimal_20_5__undef_signed decimal(20,5)    ,
col_decimal_20_5__undef_signed__index_inverted decimal(20,5)    ,
col_decimal_76__20__undef_signed decimal(37, 20)    ,
col_decimal_76__20__undef_signed__index_inverted decimal(37, 20)    ,
col_varchar_100__undef_signed varchar(100)    ,
col_varchar_100__undef_signed__index_inverted varchar(100)    ,
col_char_50__undef_signed char(50)    ,
col_char_50__undef_signed__index_inverted char(50)    ,
col_date_undef_signed date    ,
col_date_undef_signed__index_inverted date    ,
col_datetime_undef_signed datetime    ,
col_datetime_undef_signed__index_inverted datetime    ,
col_tinyint_undef_signed tinyint    ,
col_tinyint_undef_signed__index_inverted tinyint    ,
INDEX col_int_undef_signed__index_inverted_idx (`col_int_undef_signed__index_inverted`) USING INVERTED,
INDEX col_decimal_20_5__undef_signed__index_inverted_idx (`col_decimal_20_5__undef_signed__index_inverted`) USING INVERTED,
INDEX col_decimal_76__20__undef_signed__index_inverted_idx (`col_decimal_76__20__undef_signed__index_inverted`) USING INVERTED,
INDEX col_varchar_100__undef_signed__index_inverted_idx (`col_varchar_100__undef_signed__index_inverted`) USING INVERTED,
INDEX col_char_50__undef_signed__index_inverted_idx (`col_char_50__undef_signed__index_inverted`) USING INVERTED,
INDEX col_date_undef_signed__index_inverted_idx (`col_date_undef_signed__index_inverted`) USING INVERTED,
INDEX col_datetime_undef_signed__index_inverted_idx (`col_datetime_undef_signed__index_inverted`) USING INVERTED,
INDEX col_tinyint_undef_signed__index_inverted_idx (`col_tinyint_undef_signed__index_inverted`) USING INVERTED
) engine=olap
DUPLICATE KEY(pk)
distributed by hash(pk) buckets 10
properties("replication_num" = "1");
insert into table_20_undef_partitions2_keys3_properties4_distributed_by55(pk,col_int_undef_signed,col_int_undef_signed__index_inverted,col_decimal_20_5__undef_signed,col_decimal_20_5__undef_signed__index_inverted,col_decimal_76__20__undef_signed,col_decimal_76__20__undef_signed__index_inverted,col_varchar_100__undef_signed,col_varchar_100__undef_signed__index_inverted,col_char_50__undef_signed,col_char_50__undef_signed__index_inverted,col_date_undef_signed,col_date_undef_signed__index_inverted,col_datetime_undef_signed,col_datetime_undef_signed__index_inverted,col_tinyint_undef_signed,col_tinyint_undef_signed__index_inverted) values (19,-122,33.0031,101,19.0096,101,91.1381,'2009-07-14 21:44:43','2006-12-12','2008-02-19','2001-08-11 02:12:28','2007-10-08','2024-12-31','2003-07-25','2013-12-02',2,5),(18,-70,13189,101,1394605574,1617304998,1570,'say','can','+','2011-08-19','2018-04-14','2015-09-28','2010-08-07 19:15:11','2017-05-21',null,null);

    """
    // verify that topn.output should be reseted before create materializate
    sql """
    select
        t1.col_tinyint_undef_signed,
        t2.col_tinyint_undef_signed__index_inverted,
        t3.col_tinyint_undef_signed,
        t1.col_tinyint_undef_signed,
        t2.col_decimal_20_5__undef_signed__index_inverted,
        t3.col_date_undef_signed__index_inverted
    from table_100_undef_partitions2_keys3_properties4_distributed_by55 as t1
    right join
        table_20_undef_partitions2_keys3_properties4_distributed_by55 as t2
        on t1.col_tinyint_undef_signed = t2.col_tinyint_undef_signed__index_inverted
    left join
        table_100_undef_partitions2_keys3_properties4_distributed_by52 as t3
        on t2.col_tinyint_undef_signed__index_inverted
        = t3.col_tinyint_undef_signed__index_inverted
    order by t3.col_date_undef_signed desc
    limit 100
    ;
    """
}
