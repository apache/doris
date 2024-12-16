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

suite("materialized_view_grouping_sets") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_planner=true"


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
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql """
    drop table if exists lineitem
    """

    sql """
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

    sql """analyze table lineitem with sync;"""
    sql """analyze table orders with sync;"""
    sql """analyze table partsupp with sync;"""

    sql """alter table orders modify column o_comment set stats ('row_count'='10');"""
    sql """alter table lineitem modify column l_comment set stats ('row_count'='7');"""

    // query has group sets, and mv doesn't
    // single table grouping sets without grouping scalar function
    def mv1_0 =
            """
            select o_orderstatus, o_orderdate, o_orderpriority,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from orders
            group by
            o_orderstatus, o_orderdate, o_orderpriority;
            """
    def query1_0 =
            """
            select o_orderstatus, o_orderdate, o_orderpriority,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from orders
            group by
            GROUPING SETS ((o_orderstatus, o_orderdate), (o_orderpriority), (o_orderstatus), ());
            """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""

    // single table grouping sets with grouping scalar function
    def mv2_0 =
            """
            select o_orderstatus, o_orderdate, o_orderpriority,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from orders
            group by
            o_orderstatus, o_orderdate, o_orderpriority;
            """
    def query2_0 =
            """
            select o_orderstatus, o_orderdate, o_orderpriority,
            grouping_id(o_orderstatus, o_orderdate, o_orderpriority),
            grouping_id(o_orderstatus, o_orderdate),
            grouping(o_orderdate),
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from orders
            group by
            GROUPING SETS ((o_orderstatus, o_orderdate), (o_orderpriority), (o_orderstatus), ());
            """
    order_qt_query2_0_before "${query2_0}"
    async_mv_rewrite_success(db, mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    // multi table grouping sets without grouping scalar function
    def mv3_0 =
            """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from lineitem
            left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
            """
    def query3_0 =
            """
            select t1.l_partkey, t1.l_suppkey, o_orderdate,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from (select * from lineitem where l_shipdate = '2023-12-11') t1
            left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            group by
            GROUPING SETS ((l_shipdate, o_orderdate, l_partkey), (l_partkey, l_suppkey), (l_suppkey), ());
            """
    order_qt_query3_0_before "${query3_0}"
    async_mv_rewrite_success(db, mv3_0, query3_0, "mv3_0")
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""


    // multi table grouping sets with grouping scalar function
    def mv4_0 =
            """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from lineitem
            left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
            """
    def query4_0 =
            """
            select t1.l_partkey, t1.l_suppkey, o_orderdate,
            grouping(t1.l_suppkey),
            grouping(o_orderdate),
            grouping_id(t1.l_partkey, t1.l_suppkey),
            grouping_id(t1.l_partkey, t1.l_suppkey, o_orderdate),
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from (select * from lineitem where l_shipdate = '2023-12-11') t1
            left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            group by
            GROUPING SETS ((l_shipdate, o_orderdate, l_partkey), (l_partkey, l_suppkey), (l_suppkey), ());
            """
    order_qt_query4_0_before "${query4_0}"
    async_mv_rewrite_success(db, mv4_0, query4_0, "mv4_0")
    order_qt_query4_0_after "${query4_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_0"""


    // single table cube without grouping scalar function
    def mv5_0 =
            """
            select o_orderstatus, o_orderdate, o_orderpriority,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from orders
            group by
            o_orderstatus, o_orderdate, o_orderpriority;
            """
    def query5_0 =
            """
            select o_orderstatus, o_orderpriority,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from orders
            group by
            CUBE (o_orderstatus, o_orderpriority);
            """
    order_qt_query5_0_before "${query5_0}"
    async_mv_rewrite_success(db, mv5_0, query5_0, "mv5_0")
    order_qt_query5_0_after "${query5_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_0"""

    // single table cube with grouping scalar function
    def mv6_0 =
            """
            select o_orderstatus, o_orderdate, o_orderpriority,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from orders
            group by
            o_orderstatus, o_orderdate, o_orderpriority;
            """
    def query6_0 =
            """
            select o_orderstatus, o_orderpriority,
            grouping_id(o_orderstatus, o_orderpriority),
            grouping_id(o_orderstatus),
            grouping(o_orderstatus),
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from orders
            group by
            CUBE (o_orderstatus, o_orderpriority);
            """
    order_qt_query6_0_before "${query6_0}"
    async_mv_rewrite_success(db, mv6_0, query6_0, "mv6_0")
    order_qt_query6_0_after "${query6_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_0"""

    // multi table cube without grouping scalar function
    def mv7_0 =
            """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from lineitem
            left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
            """
    def query7_0 =
            """
            select t1.l_partkey, t1.l_suppkey, o_orderdate,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from (select * from lineitem where l_shipdate = '2023-12-11') t1
            left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            group by
            CUBE (t1.l_partkey, t1.l_suppkey, o_orderdate);
            """
    order_qt_query7_0_before "${query7_0}"
    async_mv_rewrite_success(db, mv7_0, query7_0, "mv7_0")
    order_qt_query7_0_after "${query7_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_0"""

    // multi table cube with grouping scalar function
    def mv8_0 =
            """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from lineitem
            left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
            """
    def query8_0 =
            """
            select t1.l_partkey, t1.l_suppkey, o_orderdate,
            grouping(t1.l_suppkey),
            grouping(o_orderdate),
            grouping_id(t1.l_partkey, t1.l_suppkey),
            grouping_id(t1.l_partkey, t1.l_suppkey, o_orderdate),
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from (select * from lineitem where l_shipdate = '2023-12-11') t1
            left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            group by
            CUBE (t1.l_partkey, t1.l_suppkey, o_orderdate);
            """
    order_qt_query8_0_before "${query8_0}"
    async_mv_rewrite_success(db, mv8_0, query8_0, "mv8_0")
    order_qt_query8_0_after "${query8_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8_0"""



    // single table rollup without grouping scalar function
    def mv9_0 =
            """
            select o_orderstatus, o_orderdate, o_orderpriority,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from orders
            group by
            o_orderstatus, o_orderdate, o_orderpriority;
            """
    def query9_0 =
            """
            select o_orderstatus, o_orderpriority,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from orders
            group by
            ROLLUP (o_orderstatus, o_orderpriority);
            """
    order_qt_query9_0_before "${query9_0}"
    async_mv_rewrite_success(db, mv9_0, query9_0, "mv9_0")
    order_qt_query9_0_after "${query9_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_0"""

    // single table rollup with grouping scalar function
    def mv10_0 =
            """
            select o_orderstatus, o_orderdate, o_orderpriority,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from orders
            group by
            o_orderstatus, o_orderdate, o_orderpriority;
            """
    def query10_0 =
            """
            select o_orderstatus, o_orderpriority,
            grouping_id(o_orderstatus, o_orderpriority),
            grouping_id(o_orderstatus),
            grouping(o_orderstatus),
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from orders
            group by
            ROLLUP (o_orderstatus, o_orderpriority);
            """
    order_qt_query10_0_before "${query10_0}"
    async_mv_rewrite_success(db, mv10_0, query10_0, "mv10_0")
    order_qt_query10_0_after "${query10_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv10_0"""

    // multi table rollup without grouping scalar function
    def mv11_0 =
            """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from lineitem
            left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
            """
    def query11_0 =
            """
            select t1.l_partkey, t1.l_suppkey, o_orderdate,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from (select * from lineitem where l_shipdate = '2023-12-11') t1
            left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            group by
            ROLLUP (t1.l_partkey, t1.l_suppkey, o_orderdate);
            """
    order_qt_query11_0_before "${query11_0}"
    async_mv_rewrite_success(db, mv11_0, query11_0, "mv11_0")
    order_qt_query11_0_after "${query11_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv11_0"""

    // multi table rollup with grouping scalar function
    def mv12_0 =
            """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from lineitem
            left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
            """
    def query12_0 =
            """
            select t1.l_partkey, t1.l_suppkey, o_orderdate,
            grouping(t1.l_suppkey),
            grouping(o_orderdate),
            grouping_id(t1.l_partkey, t1.l_suppkey),
            grouping_id(t1.l_partkey, t1.l_suppkey, o_orderdate),
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from (select * from lineitem where l_shipdate = '2023-12-11') t1
            left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            group by
            ROLLUP (t1.l_partkey, t1.l_suppkey, o_orderdate);
            """
    order_qt_query12_0_before "${query12_0}"
    async_mv_rewrite_success(db, mv12_0, query12_0, "mv12_0")
    order_qt_query12_0_after "${query12_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv12_0"""


    // both query and mv has group sets
    // not support
    def mv13_0 =
            """
            select o_orderstatus, o_orderdate, o_orderpriority,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from orders
            group by
            GROUPING SETS ((o_orderstatus, o_orderdate), (o_orderpriority), (o_orderstatus), ());
            """
    def query13_0 =
            """
            select o_orderstatus, o_orderdate, o_orderpriority,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from orders
            group by
            GROUPING SETS ((o_orderstatus, o_orderdate), (o_orderpriority), (o_orderstatus), ());
            """
    order_qt_query13_0_before "${query13_0}"
    async_mv_rewrite_fail(db, mv13_0, query13_0, "mv13_0")
    order_qt_query13_0_after "${query13_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv13_0"""


    // mv has group sets, and query doesn't
    // not support
    def mv14_0 =
            """
            select o_orderstatus, o_orderdate, o_orderpriority,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from orders
            group by
            GROUPING SETS ((o_orderstatus, o_orderdate), (o_orderpriority), (o_orderstatus), ());
            """
    def query14_0 =
            """
            select o_orderstatus, o_orderdate, o_orderpriority,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from orders
            group by
            o_orderstatus, o_orderdate, o_orderpriority;
            """
    order_qt_query14_0_before "${query14_0}"
    async_mv_rewrite_fail(db, mv14_0, query14_0, "mv14_0")
    order_qt_query14_0_after "${query14_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv14_0"""
}

