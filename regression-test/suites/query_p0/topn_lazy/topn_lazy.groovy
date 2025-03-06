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
        set enable_lazy_materialization=true;
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
}
