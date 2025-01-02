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
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

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
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 33.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 1.2, '2023-12-12', 'c','d',2, 'mi');  
    """

    sql """
    insert into partsupp values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    sql """analyze table partsupp with sync"""
    sql """analyze table lineitem with sync"""
    sql """analyze table orders with sync"""
    sql """alter table lineitem modify column l_comment set stats ('row_count'='5');"""
    sql """alter table orders modify column O_COMMENT set stats ('row_count'='8');"""
    sql """alter table partsupp modify column ps_comment set stats ('row_count'='2');"""

    // multi table
    // filter inside + left + use roll up dimension
    def mv13_0 =
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
            l_suppkey
            """
    def query13_0 =
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
            o_orderdate,
            l_partkey,
            l_suppkey
            """
    order_qt_query13_0_before "${query13_0}"
    async_mv_rewrite_success(db, mv13_0, query13_0, "mv13_0")
    order_qt_query13_0_after "${query13_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv13_0"""


    def mv13_1 = """
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
    def query13_1 = """
            select t1.l_partkey, t1.l_suppkey, o_orderdate,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 10 and o_orderkey IN (1, 3) then o_custkey else null end)
            from (select * from lineitem where l_shipdate = '2023-12-11') t1
            left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    order_qt_query13_1_before "${query13_1}"
    async_mv_rewrite_fail(db, mv13_1, query13_1, "mv13_1")
    order_qt_query13_1_after "${query13_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv13_1"""


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
    async_mv_rewrite_success(db, mv14_0, query14_0, "mv14_0")
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
    async_mv_rewrite_success(db, mv15_0, query15_0, "mv15_0")
    order_qt_query15_0_after "${query15_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv15_0"""

    def mv15_1 = """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from lineitem
            left join (select * from orders where o_orderstatus = 'o') t2
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query15_1 = """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            bitmap_union_count(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)),
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from (select * from lineitem where l_shipdate = '2023-12-11') t1
            left join (select * from orders where o_orderstatus = 'o') t2
            on t1.l_orderkey = o_orderkey and t1.l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
    """

    order_qt_query15_1_before "${query15_1}"
    async_mv_rewrite_success(db, mv15_1, query15_1, "mv15_1")
    order_qt_query15_1_after "${query15_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv15_1"""

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
    async_mv_rewrite_success(db, mv16_0, query16_0, "mv16_0")
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
    def query17_0 = """
            select t1.l_partkey, t1.l_suppkey, l_shipdate,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from lineitem t1
            left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            where o_orderdate = '2023-12-11'
            group by
            l_shipdate,
            l_partkey,
            l_suppkey;
    """
    order_qt_query17_0_before "${query17_0}"
    async_mv_rewrite_success(db, mv17_0, query17_0, "mv17_0")
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
    async_mv_rewrite_success(db, mv18_0, query18_0, "mv18_0")
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
    async_mv_rewrite_success(db, mv19_0, query19_0, "mv19_0")
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
    order_qt_query19_1_before "${query19_1}"
    async_mv_rewrite_success(db, mv19_1, query19_1, "mv19_1")
    order_qt_query19_1_after "${query19_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv19_1"""


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
    async_mv_rewrite_success(db, mv20_0, query20_0, "mv20_0")
    order_qt_query20_0_after "${query20_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv20_0"""

    def mv20_1 = """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as bitmap_union_basic
            from lineitem
            left join (select * from orders where o_orderstatus = 'o') t2
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
            """

    def query20_1 = """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            bitmap_union_count(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)),
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from lineitem
            left join (select * from orders where o_orderstatus = 'o') t2
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
            """
    order_qt_query20_1_before "${query20_1}"
    async_mv_rewrite_success(db, mv20_1, query20_1, "mv20_1")
    order_qt_query20_1_after "${query20_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv20_1"""


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
    async_mv_rewrite_success(db, mv21_0, query21_0, "mv21_0")
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
    async_mv_rewrite_success(db, mv22_0, query22_0, "mv22_0")
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
    order_qt_query22_1_before "${query22_1}"
    async_mv_rewrite_success(db, mv22_1, query22_1, "mv22_1")
    order_qt_query22_1_after "${query22_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv22_1"""


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
    async_mv_rewrite_success(db, mv23_0, query23_0, "mv23_0")
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
    async_mv_rewrite_success(db, mv24_0, query24_0, "mv24_0")
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
    async_mv_rewrite_success(db, mv25_0, query25_0, "mv25_0")
    order_qt_query25_0_after "${query25_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv25_0"""


    // bitmap_union roll up to bitmap_union
    def mv25_1 = """
           select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 2, 3) then o_custkey else null end)) cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (3, 4, 5) then o_custkey else null end)) as cnt_2
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query25_1 = """
            select o_orderdate, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 2, 3) then o_custkey else null end)) cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (3, 4, 5) then o_custkey else null end)) as cnt_2
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_suppkey;
    """
    order_qt_query25_1_before "${query25_1}"
    async_mv_rewrite_success(db, mv25_1, query25_1, "mv25_1")
    order_qt_query25_1_after "${query25_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv25_1"""

    // bitmap_union roll up to bitmap_union_count
    def mv25_2 = """
           select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 0 and o_orderkey IN (1, 2, 3) then o_custkey else null end)) cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (3, 4, 5) then o_custkey else null end)) as cnt_2
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query25_2 = """
            select o_orderdate, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 0 and o_orderkey IN (1, 2, 3) then o_custkey else null end)),
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (3, 4, 5) then o_custkey else null end)),
            bitmap_union_count(to_bitmap(case when o_shippriority > 0 and o_orderkey IN (1, 2, 3) then o_custkey else null end)),
            bitmap_union_count(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (3, 4, 5) then o_custkey else null end)),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (3, 4, 5) then o_custkey else null end)
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_suppkey;
    """
    order_qt_query25_2_before "${query25_2}"
    async_mv_rewrite_success(db, mv25_2, query25_2, "mv25_2")
    order_qt_query25_2_after "${query25_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv25_2"""


    // bitmap_union roll up to bitmap_union_count
    def mv25_3 = """
           select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 0 and o_orderkey IN (1, 2, 3) then o_custkey else null end)) cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (3, 4, 5) then o_custkey else null end)) as cnt_2
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query25_3 = """
            select o_orderdate,  l_suppkey + l_partkey,
            sum(o_totalprice) + max(o_totalprice) - min(o_totalprice),
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 0 and o_orderkey IN (1, 2, 3) then o_custkey else null end)),
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (3, 4, 5) then o_custkey else null end)),
            bitmap_union_count(to_bitmap(case when o_shippriority > 0 and o_orderkey IN (1, 2, 3) then o_custkey else null end)),
            bitmap_union_count(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (3, 4, 5) then o_custkey else null end)),
            bitmap_union_count(to_bitmap(case when o_shippriority > 0 and o_orderkey IN (1, 2, 3) then o_custkey else null end)) + bitmap_union_count(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (3, 4, 5) then o_custkey else null end)),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (3, 4, 5) then o_custkey else null end)
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_suppkey + l_partkey;
    """
    order_qt_query25_3_before "${query25_3}"
    async_mv_rewrite_success(db, mv25_3, query25_3, "mv25_3")
    order_qt_query25_3_after "${query25_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv25_3"""


    def mv25_4 = """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total,
            sum(o_totalprice) + l_suppkey
            from lineitem
            left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
    """

    def query25_4 = """
            select t1.l_partkey, t1.l_suppkey, o_orderdate, sum(o_totalprice), sum(o_totalprice) + t1.l_suppkey
            from (select * from lineitem where l_partkey = 2 ) t1
            left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_partkey,
            l_suppkey;
    """

    order_qt_query25_4_before "${query25_4}"
    async_mv_rewrite_success(db, mv25_4, query25_4, "mv25_4")
    order_qt_query25_4_after "${query25_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv25_4"""

    // hll roll up with column
    def mv25_5 =
            """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            bitmap_union(to_bitmap(l_partkey)),
            hll_union(hll_hash(l_partkey))
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
            """

    def query25_5 =
            """
            select l_partkey, l_suppkey, o_orderdate,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(distinct l_partkey),
            approx_count_distinct(l_partkey),
            hll_union_agg(hll_hash(l_partkey)),
            hll_cardinality(hll_union(hll_hash(l_partkey))),
            hll_cardinality(hll_raw_agg(hll_hash(l_partkey))),
            hll_raw_agg(hll_hash(l_partkey)),
            hll_union(hll_hash(l_partkey))
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_partkey,
            l_suppkey;
            """
    order_qt_query25_5_before "${query25_5}"
    async_mv_rewrite_success(db, mv25_5, query25_5, "mv25_5")
    order_qt_query25_5_after "${query25_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv25_5"""

    // hll roll up with complex expression
    def mv25_6 =
            """
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            bitmap_union(to_bitmap(case when o_shippriority > 0 and o_orderkey IN (1, 3) then o_custkey else null end)),
            hll_union(hll_hash(case when o_shippriority > 0 and o_orderkey IN (1, 3) then o_custkey else null end))
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
            """

    def query25_6 =
            """
            select l_partkey, l_suppkey, o_orderdate,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(distinct case when o_shippriority > 0 and o_orderkey IN (1, 3) then o_custkey else null end) as count_1,
            approx_count_distinct(case when o_shippriority > 0 and o_orderkey IN (1, 3) then o_custkey else null end) as count_2,
            hll_union_agg(hll_hash(case when o_shippriority > 0 and o_orderkey IN (1, 3) then o_custkey else null end)) as count_3,
            hll_cardinality(hll_union(hll_hash(case when o_shippriority > 0 and o_orderkey IN (1, 3) then o_custkey else null end))) as count_4,
            hll_cardinality(hll_raw_agg(hll_hash(case when o_shippriority > 0 and o_orderkey IN (1, 3) then o_custkey else null end))) as count_5,
            hll_raw_agg(hll_hash(case when o_shippriority > 0 and o_orderkey IN (1, 3) then o_custkey else null end)) as count_6,
            hll_union(hll_hash(case when o_shippriority > 0 and o_orderkey IN (1, 3) then o_custkey else null end))  as count_7
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_partkey,
            l_suppkey;
            """
    order_qt_query25_6_before "${query25_6}"
    async_mv_rewrite_success(db, mv25_6, query25_6, "mv25_6")
    order_qt_query25_6_after "${query25_6}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv25_6"""


    // single table
    // filter + use roll up dimension
    def mv1_1 = """
            select o_orderdate, o_shippriority, o_comment,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
            from orders
            group by
            o_orderdate,
            o_shippriority,
            o_comment;
    """
    def query1_1 = """
            select o_shippriority, o_comment,
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
            count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*)
            from orders
            where o_orderdate = '2023-12-09'
            group by
            o_shippriority,
            o_comment;
            """
    order_qt_query1_1_before "${query1_1}"
    async_mv_rewrite_success(db, mv1_1, query1_1, "mv1_1")
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
    async_mv_rewrite_success(db, mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""

    // can not rewrite
    // bitmap_union_count is aggregate function but not support roll up
    def mv26_0 = """
            select o_orderdate, o_shippriority, o_comment,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union_count(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
            bitmap_union_count(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
            from orders
            group by
            o_orderdate,
            o_shippriority,
            o_comment;
            """
    def query26_0 = """
            select o_orderdate, o_shippriority,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union_count(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
            bitmap_union_count(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
            from orders
            group by
            o_orderdate,
            o_shippriority;
    """
    order_qt_query26_0_before "${query26_0}"
    async_mv_rewrite_fail(db, mv26_0, query26_0, "mv26_0")
    order_qt_query26_0_after "${query26_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv26_0"""


    // bitmap_count is not aggregate function, so doesn't not support roll up
    def mv27_0 = """
            select o_orderdate, o_shippriority, o_comment,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_count(bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 2, 3) then o_custkey else null end))) cnt_1,
            bitmap_count(bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (3, 4, 5) then o_custkey else null end))) as cnt_2
            from orders
            group by
            o_orderdate,
            o_shippriority,
            o_comment;
            """
    def query27_0 = """
            select o_orderdate, o_shippriority,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_count(bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 2, 3) then o_custkey else null end))) cnt_1,
            bitmap_count(bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (3, 4, 5) then o_custkey else null end))) as cnt_2
            from orders
            group by
            o_orderdate,
            o_shippriority;
            """
    order_qt_query27_0_before "${query27_0}"
    async_mv_rewrite_fail(db, mv27_0, query27_0, "mv27_0")
    order_qt_query27_0_after "${query27_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv27_0"""

    // with cte
    // left join + aggregate
    def mv28_0 = """
           select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            sum(ifnull(o_totalprice, 0)) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 2, 3) then o_custkey else null end)) as  cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (3, 4, 5) then o_custkey else null end)) as cnt_2
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query28_0 = """
        with cte_view_1 as (
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            ifnull(o_totalprice, 0) as price_with_no_null
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        )
        select
          l_shipdate,
          sum(price_with_no_null)
        from
          cte_view_1 cte_view
        group by
          l_shipdate
        order by
          l_shipdate
        limit 10;
    """
    order_qt_query28_0_before "${query28_0}"
    async_mv_rewrite_success(db, mv28_0, query28_0, "mv28_0")
    order_qt_query28_0_after "${query28_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv28_0"""


    // scalar aggregate
    def mv29_0 = """
            select count(*)
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate;
    """
    def query29_0 = """
        select *
        from
        (
           with cte_view_1 as (
            select l_shipdate, o_orderdate, l_partkey, l_suppkey,
            ifnull(o_totalprice, 0) as price_with_no_null
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            )
          select
            count(1) count_all
          from
            cte_view_1 cte_view
          limit 1
        ) as t;
    """
    order_qt_query29_0_before "${query29_0}"
    async_mv_rewrite_success(db, mv29_0, query29_0, "mv29_0")
    order_qt_query29_0_after "${query29_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv29_0"""

    // mv and query both are scalar aggregate
    def mv29_1 = """
            select
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate;
    """
    def query29_1 = """
            select
            count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2,
            sum(o_totalprice),
            min(o_totalprice),
            count(*)
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate;
    """

    order_qt_query29_1_before "${query29_1}"
    async_mv_rewrite_success(db, mv29_1, query29_1, "mv29_1")
    order_qt_query29_1_after "${query29_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv29_1"""


    // mv and query both are scalar aggregate, and query calc the aggregate function
    def mv29_2 = """
            select
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate;
    """
    def query29_2 = """
            select
            count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2,
            (sum(o_totalprice) + min(o_totalprice)) * count(*),
            min(o_totalprice) + count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end)
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate;
    """
    order_qt_query29_2_before "${query29_2}"
    async_mv_rewrite_success(db, mv29_2, query29_2, "mv29_2")
    order_qt_query29_2_after "${query29_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv29_2"""


    // join input has simple agg, simple agg which can not contains rollup, cube
    // can not rewrite, because avg doesn't support roll up now
    def mv30_0 = """
            select
            l_linenumber,
            l_quantity,
            count(distinct l_orderkey),
            sum(case when l_orderkey in (1,2,3) then l_suppkey * l_linenumber else 0 end),
            max(case when l_orderkey in (4, 5) then (l_quantity *2 + part_supp_a.qty_max) * 0.88 else 100 end),
            avg(case when l_partkey in (2, 3, 4) then l_discount + o_totalprice + part_supp_a.qty_sum else 50 end)
            from lineitem
            left join orders on l_orderkey = o_orderkey
            left join
            (select ps_partkey, ps_suppkey, sum(ps_availqty) qty_sum, max(ps_availqty) qty_max,
                min(ps_availqty) qty_min,
                avg(ps_supplycost) cost_avg
                from partsupp
                group by ps_partkey,ps_suppkey) part_supp_a
            on l_partkey = part_supp_a.ps_partkey
            and l_suppkey = part_supp_a.ps_suppkey
            group by l_linenumber, l_quantity;
    """
    def query30_0 = """
            select
            l_linenumber,
            count(distinct l_orderkey),
            sum(case when l_orderkey in (1,2,3) then l_suppkey * l_linenumber else 0 end),
            max(case when l_orderkey in (4, 5) then (l_quantity *2 + part_supp_a.qty_max) * 0.88 else 100 end),
            avg(case when l_partkey in (2, 3, 4) then l_discount + o_totalprice + part_supp_a.qty_sum else 50 end)
            from lineitem
            left join orders on l_orderkey = o_orderkey
            left join
            (select ps_partkey, ps_suppkey, sum(ps_availqty) qty_sum, max(ps_availqty) qty_max,
                min(ps_availqty) qty_min,
                avg(ps_supplycost) cost_avg
                from partsupp
                group by ps_partkey,ps_suppkey) part_supp_a
            on l_partkey = part_supp_a.ps_partkey
            and l_suppkey = part_supp_a.ps_suppkey
            group by l_linenumber;
    """
    order_qt_query30_0_before "${query30_0}"
    async_mv_rewrite_fail(db, mv30_0, query30_0, "mv30_0")
    order_qt_query30_0_after "${query30_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv30_0"""

    // should rewrite fail, because the part of query is join but mv is aggregate
    def mv31_0 = """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              o_orderkey,
              count(*)
            from
              orders
              left join lineitem on l_orderkey = o_orderkey
              group by o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              o_orderkey;
    """
    def query31_0 = """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey,
              count(*)
            from
              orders left
              join lineitem on l_orderkey = o_orderkey
              left join partsupp on ps_partkey = l_orderkey
              group by
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey;
    """
    order_qt_query31_0_before "${query31_0}"
    async_mv_rewrite_fail(db, mv31_0, query31_0, "mv31_0")
    order_qt_query31_0_after "${query31_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv31_0"""

    // should rewrite fail, because the group by dimension query used is not in mv group by dimension
    def mv32_0 = """
            select
              o_orderdate,
              count(*)
            from
              orders
            group by
              o_orderdate;
    """
    def query32_0 = """
            select
              o_orderdate,
              count(*)
            from
              orders
            group by
              o_orderdate,
              o_shippriority;
    """
    order_qt_query32_0_before "${query32_0}"
    async_mv_rewrite_fail(db, mv32_0, query32_0, "mv32_0")
    order_qt_query32_0_after "${query32_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv32_0"""

    // should rewrite fail, because the group by dimension query used is not in mv group by dimension
    def mv32_1 = """
            select o_orderdate
            from orders
            group by o_orderdate;
    """
    def query32_1 = """
            select
            1
            from  orders 
            group by
            o_orderdate;
    """
    order_qt_query32_1_before "${query32_1}"
    async_mv_rewrite_success(db, mv32_1, query32_1, "mv32_1")
    order_qt_query32_1_after "${query32_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv32_1"""

    def mv32_2 = """
            select o_orderdate, o_orderkey
            from orders
            group by o_orderdate, o_orderkey;
    """
    def query32_2 = """
            select
            1
            from orders 
            group by
            o_orderdate;
    """
    order_qt_query32_2_before "${query32_2}"
    async_mv_rewrite_success(db, mv32_2, query32_2, "mv32_2")
    order_qt_query32_2_after "${query32_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv32_2"""

    // test combinator aggregate function rewrite
    sql """set enable_agg_state=true"""
    // query has no combinator and mv has combinator
    // mv is union
    def mv33_0 = """
            select
            o_orderstatus,
            l_partkey,
            l_suppkey,
            sum_union(sum_state(o_shippriority)),
            group_concat_union(group_concat_state(o_orderstatus)),
            avg_union(avg_state(l_linenumber)),
            max_by_union(max_by_state(O_COMMENT,o_totalprice)),
            count_union(count_state(l_orderkey)),
            multi_distinct_count_union(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_partkey,
            l_suppkey;
    """
    def query33_0 = """
            select
            o_orderstatus,
            l_suppkey,
            sum(o_shippriority),
            group_concat(o_orderstatus),
            avg(l_linenumber),
            max_by(O_COMMENT,o_totalprice),
            count(l_orderkey),
            multi_distinct_count(l_shipmode)
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_suppkey;
    """
    order_qt_query33_0_before "${query33_0}"
    async_mv_rewrite_success(db, mv33_0, query33_0, "mv33_0")
    order_qt_query33_0_after "${query33_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv33_0"""


    // mv is merge
    def mv33_1 = """
            select
            o_orderstatus,
            l_partkey,
            l_suppkey,
            sum_merge(sum_state(o_shippriority)),
            group_concat_merge(group_concat_state(o_orderstatus)),
            avg_merge(avg_state(l_linenumber)),
            max_by_merge(max_by_state(O_COMMENT,o_totalprice)),
            count_merge(count_state(l_orderkey)),
            multi_distinct_count_merge(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_partkey,
            l_suppkey;
    """
    def query33_1 = """
             select
            o_orderstatus,
            l_suppkey,
            sum(o_shippriority),
            group_concat(o_orderstatus),
            avg(l_linenumber),
            max_by(O_COMMENT,o_totalprice),
            count(l_orderkey),
            multi_distinct_count(l_shipmode)
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_suppkey
            order by o_orderstatus;
    """
    order_qt_query33_1_before "${query33_1}"
    async_mv_rewrite_fail(db, mv33_1, query33_1, "mv33_1")
    order_qt_query33_1_after "${query33_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv33_1"""



    // both query and mv are combinator
    // mv is union, query is union
    def mv34_0 = """
            select
            o_orderstatus,
            l_partkey,
            l_suppkey,
            sum_union(sum_state(o_shippriority)),
            group_concat_union(group_concat_state(o_orderstatus)),
            avg_union(avg_state(l_linenumber)),
            max_by_union(max_by_state(O_COMMENT,o_totalprice)),
            count_union(count_state(l_orderkey)),
            multi_distinct_count_union(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_partkey,
            l_suppkey;
    """
    def query34_0 = """
            select
            o_orderstatus,
            l_suppkey,
            sum_union(sum_state(o_shippriority)),
            group_concat_union(group_concat_state(o_orderstatus)),
            avg_union(avg_state(l_linenumber)),
            max_by_union(max_by_state(O_COMMENT,o_totalprice)),
            count_union(count_state(l_orderkey)),
            multi_distinct_count_union(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_suppkey;
    """
    async_mv_rewrite_success(db, mv34_0, query34_0, "mv34_0")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv34_0"""


    // mv is union, query is merge
    def mv35_0 = """
            select
            o_orderstatus,
            l_partkey,
            l_suppkey,
            sum_union(sum_state(o_shippriority)),
            group_concat_union(group_concat_state(o_orderstatus)),
            avg_union(avg_state(l_linenumber)),
            max_by_union(max_by_state(O_COMMENT,o_totalprice)),
            count_union(count_state(l_orderkey)),
            multi_distinct_count_union(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_partkey,
            l_suppkey;
    """
    def query35_0 = """
            select
            o_orderstatus,
            l_suppkey,
            sum_merge(sum_state(o_shippriority)),
            group_concat_merge(group_concat_state(o_orderstatus)),
            avg_merge(avg_state(l_linenumber)),
            max_by_merge(max_by_state(O_COMMENT,o_totalprice)),
            count_merge(count_state(l_orderkey)),
            multi_distinct_count_merge(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_suppkey
            order by o_orderstatus;
    """
    order_qt_query35_0_before "${query35_0}"
    async_mv_rewrite_success(db, mv35_0, query35_0, "mv35_0")
    order_qt_query35_0_after "${query35_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv35_0"""


    // mv is merge, query is merge
    def mv36_0 = """
            select
            o_orderstatus,
            l_partkey,
            l_suppkey,
            sum_merge(sum_state(o_shippriority)),
            group_concat_merge(group_concat_state(o_orderstatus)),
            avg_merge(avg_state(l_linenumber)),
            max_by_merge(max_by_state(O_COMMENT,o_totalprice)),
            count_merge(count_state(l_orderkey)),
            multi_distinct_count_merge(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_partkey,
            l_suppkey;
    """
    def query36_0 = """
            select
            o_orderstatus,
            l_suppkey,
            sum_merge(sum_state(o_shippriority)),
            group_concat_merge(group_concat_state(o_orderstatus)),
            avg_merge(avg_state(l_linenumber)),
            max_by_merge(max_by_state(O_COMMENT,o_totalprice)),
            count_merge(count_state(l_orderkey)),
            multi_distinct_count_merge(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_suppkey
            order by o_orderstatus;
    """
    order_qt_query36_0_before "${query36_0}"
    async_mv_rewrite_fail(db, mv36_0, query36_0, "mv36_0")
    order_qt_query36_0_after "${query36_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv36_0"""


    // mv is merge, query is union
    def mv37_0 = """
            select
            o_orderstatus,
            l_partkey,
            l_suppkey,
            sum_merge(sum_state(o_shippriority)),
            group_concat_merge(group_concat_state(o_orderstatus)),
            avg_merge(avg_state(l_linenumber)),
            max_by_merge(max_by_state(O_COMMENT,o_totalprice)),
            count_merge(count_state(l_orderkey)),
            multi_distinct_count_merge(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_partkey,
            l_suppkey;
    """
    def query37_0 = """
            select
            o_orderstatus,
            l_suppkey,
            sum_union(sum_state(o_shippriority)),
            group_concat_union(group_concat_state(o_orderstatus)),
            avg_union(avg_state(l_linenumber)),
            max_by_union(max_by_state(O_COMMENT,o_totalprice)),
            count_union(count_state(l_orderkey)),
            multi_distinct_count_union(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_suppkey;
    """
    async_mv_rewrite_fail(db, mv37_0, query37_0, "mv37_0")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv37_0"""
}
