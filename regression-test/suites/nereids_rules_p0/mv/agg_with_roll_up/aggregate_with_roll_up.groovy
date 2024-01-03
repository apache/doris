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

suite("aggregate_with_roll_up") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_timeout = false"
    // tmp disable to rewrite, will be removed in the future
    sql "SET disable_nereids_rules = 'ELIMINATE_OUTER_JOIN'"

    sql """
    drop table if exists orders
    """

    sql """
    CREATE TABLE IF NOT EXISTS orders  (
      o_orderkey       INTEGER NOT NULL,
      o_custkey        INTEGER NOT NULL,
      o_orderstatus    CHAR(1) NOT NULL,
      o_totalprice     DECIMALV3(15,2) NOT NULL,
      o_orderdate      DATE NOT NULL,
      o_orderpriority  CHAR(15) NOT NULL,  
      o_clerk          CHAR(15) NOT NULL, 
      o_shippriority   INTEGER NOT NULL,
      O_COMMENT        VARCHAR(79) NOT NULL
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    PARTITION BY RANGE(o_orderdate) (
    PARTITION `day_2` VALUES LESS THAN ('2023-12-9'),
    PARTITION `day_3` VALUES LESS THAN ("2023-12-11"),
    PARTITION `day_4` VALUES LESS THAN ("2023-12-30")
    )
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """
    drop table if exists lineitem
    """

    sql"""
    CREATE TABLE IF NOT EXISTS lineitem (
      l_orderkey    INTEGER NOT NULL,
      l_partkey     INTEGER NOT NULL,
      l_suppkey     INTEGER NOT NULL,
      l_linenumber  INTEGER NOT NULL,
      l_quantity    DECIMALV3(15,2) NOT NULL,
      l_extendedprice  DECIMALV3(15,2) NOT NULL,
      l_discount    DECIMALV3(15,2) NOT NULL,
      l_tax         DECIMALV3(15,2) NOT NULL,
      l_returnflag  CHAR(1) NOT NULL,
      l_linestatus  CHAR(1) NOT NULL,
      l_shipdate    DATE NOT NULL,
      l_commitdate  DATE NOT NULL,
      l_receiptdate DATE NOT NULL,
      l_shipinstruct CHAR(25) NOT NULL,
      l_shipmode     CHAR(10) NOT NULL,
      l_comment      VARCHAR(44) NOT NULL
    )
    DUPLICATE KEY(l_orderkey, l_partkey, l_suppkey, l_linenumber)
    PARTITION BY RANGE(l_shipdate) (
    PARTITION `day_1` VALUES LESS THAN ('2023-12-9'),
    PARTITION `day_2` VALUES LESS THAN ("2023-12-11"),
    PARTITION `day_3` VALUES LESS THAN ("2023-12-30"))
    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """
    drop table if exists partsupp
    """

    sql """
    CREATE TABLE IF NOT EXISTS partsupp (
      ps_partkey     INTEGER NOT NULL,
      ps_suppkey     INTEGER NOT NULL,
      ps_availqty    INTEGER NOT NULL,
      ps_supplycost  DECIMALV3(15,2)  NOT NULL,
      ps_comment     VARCHAR(199) NOT NULL 
    )
    DUPLICATE KEY(ps_partkey, ps_suppkey)
    DISTRIBUTED BY HASH(ps_partkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """ insert into lineitem values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-08', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (2, 4, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-09', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 4, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-10', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (4, 3, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-11', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (5, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-12', '2023-12-12', '2023-12-13', 'c', 'd', 'xxxxxxxxx');
    """

    sql """
    insert into orders values
    (1, 1, 'o', 9.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 33.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 1.2, '2023-12-12', 'c','d',2, 'mi');  
    """

    sql """
    insert into partsupp values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    def check_rewrite = { mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """

        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        explain {
            sql("${query_sql}")
            contains "(${mv_name})"
        }
    }

    def check_rewrite_with_mv_partition = { mv_sql, query_sql, mv_name, partition_column ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY (${partition_column})
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """

        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        explain {
            sql("${query_sql}")
            contains "(${mv_name})"
        }
    }

    def check_rewrite_with_force_analyze = { mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """

        sql "analyze table ${mv_name} with sync;"
        sql "analyze table lineitem with sync;"
        sql "analyze table orders with sync;"
        sql "analyze table partsupp with sync;"

        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        explain {
            sql("${query_sql}")
            contains "(${mv_name})"
        }
    }

    def check_not_match = { mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """

        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        explain {
            sql("${query_sql}")
            notContains "(${mv_name})"
        }
    }

    // multi table
    // filter inside + left + use roll up dimension
    def mv13_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic " +
            "from lineitem " +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query13_0 = "select t1.l_partkey, t1.l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from (select * from lineitem where l_shipdate = '2023-12-11') t1 " +
            "left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query13_0_before "${query13_0}"
    check_rewrite(mv13_0, query13_0, "mv13_0")
    order_qt_query13_0_after "${query13_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv13_0"""


    // filter inside + right + use roll up dimension
    def mv14_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic " +
            "from lineitem " +
            "left join (select * from orders where o_orderdate = '2023-12-08') t2 " +
            "on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query14_0 = "select l_partkey, l_suppkey, l_shipdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from lineitem t1 " +
            "left join (select * from orders where o_orderdate = '2023-12-08') t2 " +
            "on t1.l_orderkey = o_orderkey and t1.l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query14_0_before "${query14_0}"
    check_rewrite(mv14_0, query14_0, "mv14_0")
    order_qt_query14_0_after "${query14_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv14_0"""


    // filter inside + right + left + use roll up dimension
    def mv15_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic " +
            "from lineitem " +
            "left join (select * from orders where o_orderstatus = 'o') t2 " +
            "on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query15_0 = "select t1.l_partkey, t1.l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from (select * from lineitem where l_shipdate = '2023-12-11') t1 " +
            "left join (select * from orders where o_orderstatus = 'o') t2 " +
            "on t1.l_orderkey = o_orderkey and t1.l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query15_0_before "${query15_0}"
    check_rewrite_with_mv_partition(mv15_0, query15_0, "mv15_0", "l_shipdate")
    order_qt_query15_0_after "${query15_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv15_0"""


    // filter outside + left + use roll up dimension
    def mv16_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic " +
            "from lineitem " +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query16_0 = "select t1.l_partkey, t1.l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from lineitem t1 " +
            "left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate " +
            "where l_shipdate = '2023-12-11' " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query16_0_before "${query16_0}"
    check_rewrite(mv16_0, query16_0, "mv16_0")
    order_qt_query16_0_after "${query16_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv16_0"""


    // filter outside + right + use roll up dimension
    def mv17_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic " +
            "from lineitem " +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query17_0 = "select t1.l_partkey, t1.l_suppkey, l_shipdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from lineitem t1 " +
            "left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate " +
            "where o_orderdate = '2023-12-11' " +
            "group by " +
            "l_shipdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query17_0_before "${query17_0}"
    check_rewrite(mv17_0, query17_0, "mv17_0")
    order_qt_query17_0_after "${query17_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv17_0"""

    // filter outside + left + right + use roll up dimension
    def mv18_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic " +
            "from lineitem " +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query18_0 = "select t1.l_suppkey, l_shipdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from lineitem t1 " +
            "left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate " +
            "where o_orderdate = '2023-12-11' and l_partkey = 3 " +
            "group by " +
            "l_shipdate, " +
            "l_suppkey"
    order_qt_query18_0_before "${query18_0}"
    check_rewrite(mv18_0, query18_0, "mv18_0")
    order_qt_query18_0_after "${query18_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18_0"""


    // filter inside + left + use not roll up dimension
    def mv19_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total " +
            "from lineitem " +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query19_0 = "select t1.l_partkey, t1.l_suppkey, o_orderdate, sum(o_totalprice) " +
            "from (select * from lineitem where l_partkey = 2 ) t1 " +
            "left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query19_0_before "${query19_0}"
    check_rewrite(mv19_0, query19_0, "mv19_0")
    order_qt_query19_0_after "${query19_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv19_0"""


    def mv19_1 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic " +
            "from (select * from lineitem where l_partkey = 2) t1 " +
            "left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query19_1 = "select t1.l_partkey, t1.l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end), " +
            "count(*) " +
            "from (select * from lineitem where l_partkey = 2 and l_suppkey = 3) t1 " +
            "left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
//    // Should pass but not, tmp
//    order_qt_query19_1_before "${query19_1}"
//    check_rewrite(mv19_1, query19_1, "mv19_1")
//    order_qt_query19_1_after "${query19_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv19_1"""


    // filter inside + right + use not roll up dimension
    def mv20_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic " +
            "from lineitem " +
            "left join (select * from orders where o_orderstatus = 'o') t2 " +
            "on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query20_0 = "select l_shipdate, t1.l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from lineitem t1 " +
            "left join (select * from orders where o_orderstatus = 'o') t2 " +
            "on t1.l_orderkey = o_orderkey and t1.l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_suppkey"
    order_qt_query20_0_before "${query20_0}"
    check_rewrite(mv20_0, query20_0, "mv20_0")
    order_qt_query20_0_after "${query20_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv20_0"""

    // filter inside + right + left + use not roll up dimension
    def mv21_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic " +
            "from lineitem " +
            "left join (select * from orders where o_orderstatus = 'o') t2 " +
            "on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query21_0 = "select t1.l_partkey, t1.l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from (select * from lineitem where l_partkey = 2) t1 " +
            "left join (select * from orders where o_orderstatus = 'o') t2 " +
            "on t1.l_orderkey = o_orderkey and t1.l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query21_0_before "${query21_0}"
    check_rewrite(mv21_0, query21_0, "mv21_0")
    order_qt_query21_0_after "${query21_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv21_0"""


    // filter outside + left + use not roll up dimension
    def mv22_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic " +
            "from lineitem " +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query22_0 = "select t1.l_partkey, t1.l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from lineitem t1 " +
            "left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate " +
            "where l_partkey = 2 or l_suppkey = 3  " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query22_0_before "${query22_0}"
    check_rewrite(mv22_0, query22_0, "mv22_0")
    order_qt_query22_0_after "${query22_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv22_0"""


    def mv22_1 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic " +
            "from lineitem " +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate " +
            "where l_partkey = 2 " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query22_1 = "select t1.l_partkey, t1.l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from lineitem t1 " +
            "left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate " +
            "where l_partkey = 2 and l_suppkey = 3  " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    // Should pass but not, tmp
//    order_qt_query22_1_before "${query22_1}"
//    check_rewrite(mv22_1, query22_1, "mv22_1")
//    order_qt_query22_1_after "${query22_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv22_0"""


    // filter outside + right + use not roll up dimension
    def mv23_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderstatus, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic " +
            "from lineitem " +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey, " +
            "o_orderstatus"
    def query23_0 = "select t1.l_partkey, t1.l_suppkey, l_shipdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from lineitem t1 " +
            "left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate " +
            "where o_orderdate = '2023-12-08' and o_orderstatus = 'o' " +
            "group by " +
            "l_shipdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query23_0_before "${query23_0}"
    check_rewrite(mv23_0, query23_0, "mv23_0")
    order_qt_query23_0_after "${query23_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv23_0"""


    // filter outside + left + right + not use roll up dimension
    def mv24_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic " +
            "from lineitem " +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query24_0 = "select t1.l_suppkey, l_shipdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from lineitem t1 " +
            "left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate " +
            "where l_suppkey = 3 " +
            "group by " +
            "l_shipdate, " +
            "l_suppkey"
    order_qt_query24_0_before "${query24_0}"
    check_rewrite(mv24_0, query24_0, "mv24_0")
    order_qt_query24_0_after "${query24_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv24_0"""



    // without filter
    def mv25_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all " +
            "from lineitem " +
            "left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query25_0 = "select l_partkey, l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*) " +
            "from lineitem " +
            "left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query25_0_before "${query25_0}"
    check_rewrite(mv25_0, query25_0, "mv25_0")
    order_qt_query25_0_after "${query25_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv25_0"""

    // single table
    // filter + use roll up dimension
    def mv1_1 = "select o_orderdate, o_shippriority, o_comment, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 " +
            "from orders " +
            "group by " +
            "o_orderdate, " +
            "o_shippriority, " +
            "o_comment "
    def query1_1 = "select o_shippriority, o_comment, " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*) " +
            "from orders " +
            "where o_orderdate = '2023-12-09' " +
            "group by " +
            "o_shippriority, " +
            "o_comment "
    order_qt_query1_1_before "${query1_1}"
    // rewrite success, for cbo chose, should force analyze
    // because data volume is small and mv plan is almost same to query plan
    check_rewrite_with_force_analyze(mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""


    // filter + not use roll up dimension
    def mv2_0 = "select o_orderdate, o_shippriority, o_comment, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, " +
            "bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 " +
            "from orders " +
            "group by " +
            "o_orderdate, " +
            "o_shippriority, " +
            "o_comment "
    def query2_0 = "select o_shippriority, o_comment, " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*) " +
            "from orders " +
            "where o_shippriority = 2 " +
            "group by " +
            "o_shippriority, " +
            "o_comment "

    order_qt_query2_0_before "${query2_0}"
    // rewrite success, for cbo chose, should force analyze
    // because data volume is small and mv plan is almost same to query plan
    check_rewrite_with_force_analyze(mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""

    // can not rewrite, todo
}
