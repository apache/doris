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
        set enable_topn_lazy_materialization=true;
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
    
    qt_test_lazy1 """select * from date order by d_date limit 10;"""

    qt_test_lazy2 """SELECT d_datekey, d_date, d_dayofweek, d_month, d_year, d_yearmonthnum, d_daynuminweek, d_monthnuminyear, d_sellingseason FROM date ORDER BY d_date LIMIT 10;"""

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
}
