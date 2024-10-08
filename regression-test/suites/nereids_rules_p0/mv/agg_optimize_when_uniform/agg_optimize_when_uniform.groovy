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

suite("agg_optimize_when_uniform") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql """set enable_agg_state=true"""

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
    );
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

    sql """analyze table lineitem with sync;"""
    sql """analyze table orders with sync;"""
    sql """analyze table partsupp with sync;"""

    // single table
    // filter cover all roll up dimensions and contains agg function in mapping, combinator handler
    def mv1_0 = """
            select o_orderkey, o_custkey, o_orderdate,
            sum_union(sum_state(o_totalprice)),
            max_union(max_state(o_totalprice)),
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from orders
            group by
            o_orderkey,
            o_custkey,
            o_orderdate;
            """
    def query1_0 =
            """
            select o_orderdate,
            sum(o_totalprice),
            max(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from orders
            where o_orderkey = 3 and o_custkey = 1
            group by
            o_orderdate;
            """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    qt_shape1_0_after """explain shape plan ${query1_0}"""
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""

    // filter cover all roll up dimensions and contains agg function in distinct handler
    def mv2_0 = """
             select
             count(o_totalprice),
             o_shippriority,
             o_orderstatus,
             bin(o_orderkey),
             o_orderkey
             from orders
             group by
             o_orderkey,
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
            """
    def query2_0 =
            """
             select 
             count(o_totalprice),
             max(distinct o_shippriority + o_orderkey),
             min(distinct o_shippriority + o_orderkey),
             avg(distinct o_shippriority),
             sum(distinct o_shippriority) / count(distinct o_shippriority),
             o_shippriority,
             bin(o_orderkey)
             from orders
             where o_orderkey = 1 and o_orderstatus = 'o'
             group by
             o_shippriority,
             bin(o_orderkey);
            """
    order_qt_query2_0_before "${query2_0}"
    async_mv_rewrite_success(db, mv2_0, query2_0, "mv2_0")
    qt_shape2_0_after """explain shape plan ${query2_0}"""
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""

    // filter cover all roll up dimensions and only contains agg function in direct handler
    def mv3_0 = """
            select o_orderdate, o_shippriority, o_comment,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all
            from orders
            group by
            o_orderdate,
            o_shippriority,
            o_comment;
            """
    def query3_0 =
            """
            select o_comment,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*)
            from orders
            where o_orderdate = '2023-12-09' and o_shippriority = 1
            group by
            o_comment;
            """
    order_qt_query3_0_before "${query3_0}"
    async_mv_rewrite_success(db, mv3_0, query3_0, "mv3_0")
    // query success and doesn't add aggregate
    qt_shape3_0_after """explain shape plan ${query3_0}"""
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""


    def mv3_1 = """
            select o_orderdate, o_shippriority, o_comment,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all
            from orders
            group by
            o_orderdate,
            o_shippriority,
            o_comment;
            """
    def query3_1 =
            """
            select o_comment,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*)
            from orders
            where o_orderdate = '2023-12-12' and o_shippriority = 2 and o_totalprice = 56.2
            group by
            o_comment;
            """
    order_qt_query3_1_before "${query3_1}"
    // query where has a column not in agg output
    async_mv_rewrite_fail(db, mv3_1, query3_1, "mv3_1")
    qt_shape3_1_after """explain shape plan ${query3_1}"""
    order_qt_query3_1_after "${query3_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_1"""


    // filter does not cover all roll up dimensions
    def mv4_0 = """
            select o_orderdate, o_shippriority, o_comment,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all
            from orders
            group by
            o_orderdate,
            o_shippriority,
            o_comment;
            """
    def query4_0 =
            """
            select o_comment,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*)
            from orders
            where o_orderdate = '2023-12-09'
            group by
            o_comment;
            """
    order_qt_query4_0_before "${query4_0}"
    // query success but add agg
    async_mv_rewrite_success_without_check_chosen(db, mv4_0, query4_0, "mv4_0")
    order_qt_query4_0_after "${query4_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_0"""


    // multi table
    // filter cover all roll up dimensions and contains agg function in mapping, combinator handler
    def mv5_0 = """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum_union(sum_state(o_totalprice)),
            max_union(max_state(o_totalprice)),
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
    def query5_0 =
            """
            select t1.l_suppkey, o_orderdate,
            sum(o_totalprice),
            max(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from lineitem t1
            left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            where l_partkey = 2 and l_shipdate = '2023-12-12'
            group by
            o_orderdate,
            l_suppkey;
            """
    order_qt_query5_0_before "${query5_0}"
    async_mv_rewrite_success(db, mv5_0, query5_0, "mv5_0")
    qt_shape5_0_after """explain shape plan ${query5_0}"""
    order_qt_query5_0_after "${query5_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_0"""

    // filter cover all roll up dimensions and contains agg function in distinct handler
    def mv6_0 = """
             select
             l_partkey,
             count(o_totalprice),
             o_shippriority,
             o_orderstatus,
             bin(o_orderkey),
             o_orderkey
             from lineitem t1
             left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
             group by
             o_orderkey,
             o_orderstatus,
             o_shippriority,
             l_partkey,
             bin(o_orderkey);
            """
    def query6_0 =
            """
             select 
             l_partkey,
             count(o_totalprice),
             max(distinct o_shippriority + o_orderkey),
             min(distinct o_shippriority + o_orderkey),
             avg(distinct o_shippriority),
             sum(distinct o_shippriority) / count(distinct o_shippriority),
             o_shippriority,
             bin(o_orderkey)
             from lineitem t1
             left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
             where o_orderkey = 1 and o_orderstatus = 'o'
             group by
             l_partkey,
             o_shippriority,
             bin(o_orderkey);
            """
    order_qt_query6_0_before "${query6_0}"
    async_mv_rewrite_success(db, mv6_0, query6_0, "mv6_0")
    qt_shape6_0_after """explain shape plan ${query6_0}"""
    order_qt_query6_0_after "${query6_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_0"""


    // filter cover all roll up dimensions and only contains agg function in direct handler
    def mv7_0 = """
            select o_orderdate, o_shippriority, o_comment, l_partkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all
             from lineitem t1
             left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            group by
            l_partkey,
            o_orderdate,
            o_shippriority,
            o_comment;
            """
    def query7_0 =
            """
            select o_comment, l_partkey,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*)
             from lineitem t1
             left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            where o_orderdate = '2023-12-09' and o_shippriority = 1
            group by
            l_partkey,
            o_comment;
            """
    order_qt_query7_0_before "${query7_0}"
    async_mv_rewrite_success(db, mv7_0, query7_0, "mv7_0")
    // query success and doesn't add aggregate
    qt_shape7_0_after """explain shape plan ${query7_0}"""
    order_qt_query7_0_after "${query7_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_0"""


    def mv7_1 = """
            select o_orderdate, o_shippriority, o_comment, l_partkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all
             from lineitem t1
             left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            group by
            l_partkey,
            o_orderdate,
            o_shippriority,
            o_comment;
            """
    def query7_1 =
            """
            select o_comment, l_partkey,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*)
             from lineitem t1
             left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            where o_orderdate = '2023-12-09' and o_shippriority = 1 and o_totalprice = 11.5
            group by
            l_partkey,
            o_comment;
            """
    order_qt_query7_1_before "${query7_1}"
    // query where has a column not in agg output
    async_mv_rewrite_fail(db, mv7_1, query7_1, "mv7_1")
    qt_shape7_1_after """explain shape plan ${query7_1}"""
    order_qt_query7_1_after "${query7_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_1"""

    // filter does not cover all roll up dimensions
    def mv8_0 = """
            select o_orderdate, o_shippriority, o_comment, l_partkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all
             from lineitem t1
             left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            group by
            l_partkey,
            o_orderdate,
            o_shippriority,
            o_comment;
            """
    def query8_0 =
            """
            select o_comment, l_partkey,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*)
             from lineitem t1
             left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            where o_orderdate = '2023-12-09'
            group by
            l_partkey,
            o_comment;
            """
    order_qt_query8_0_before "${query8_0}"
    // query success but add agg
    async_mv_rewrite_success(db, mv8_0, query8_0, "mv8_0")
    qt_shape8_0_after """explain shape plan ${query8_0}"""
    order_qt_query8_0_after "${query8_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8_0"""
}
