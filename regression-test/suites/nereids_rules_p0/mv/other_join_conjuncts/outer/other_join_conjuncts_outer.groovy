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

suite("other_join_conjuncts_outer") {
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
      O_COMMENT        VARCHAR(79) NOT NULL,
      lo_orderdate    DATE NOT NULL
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
      l_comment      VARCHAR(44) NOT NULL,
      lo_orderdate    DATE NOT NULL
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
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-08', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy' ,'2023-12-08'),
    (1, 2, 3, 4, 5.5, 6.5, 7.6, 8.5, 'o', 'k', '2023-12-08', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy' ,'2023-12-08'),
    (2, 4, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-09', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy' ,'2023-12-09'),
    (2, 4, 3, 4, 5.5, 6.5, 7.6, 8.5, 'o', 'k', '2023-12-09', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy' ,'2023-12-09'),
    (3, 2, 4, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-10', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy' ,'2023-12-10'),
    (3, 2, 4, 4, 5.5, 6.6, 7.5, 8.5, 'o', 'k', '2023-12-10', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy' ,'2023-12-10'),
    (4, 3, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-11', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy' ,'2023-12-11'),
    (4, 3, 3, 4, 5.5, 6.6, 7.5, 8.5, 'o', 'k', '2023-12-11', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy' ,'2023-12-11'),
    (5, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-12', '2023-12-12', '2023-12-13', 'c', 'd', 'xxxxxxxxx', '2023-12-12'),
    (5, 2, 3, 6, 7.6, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-12', '2023-12-12', '2023-12-13', 'c', 'd', 'xxxxxxxxx', '2023-12-12');
    """

    sql """
    insert into orders values
    (1, 1, 'o', 9.5,  '2023-12-08', 'a', 'b', 1, 'yy','2023-12-08'),
    (1, 1, 'o', 10.5, '2023-12-09', 'a', 'b', 1, 'yy','2023-12-09'),
    (1, 1, 'o', 10.5, '2023-12-07', 'a', 'b', 1, 'yy','2023-12-07'),
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy','2023-12-08'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy','2023-12-09'),
    (2, 1, 'o', 11.5, '2023-12-08', 'a', 'b', 1, 'yy','2023-12-08'),
    (2, 1, 'o', 11.5, '2023-12-11', 'a', 'b', 1, 'yy','2023-12-11'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy','2023-12-10'),
    (3, 1, 'o', 12.5, '2023-12-09', 'a', 'b', 1, 'yy','2023-12-09'),
    (3, 1, 'o', 12.5, '2023-12-12', 'a', 'b', 1, 'yy','2023-12-12'),
    (3, 1, 'o', 33.5, '2023-12-13', 'a', 'b', 1, 'yy','2023-12-13'),
    (4, 2, 'o', 43.2, '2023-12-10', 'c','d',2, 'mm'  ,'2023-12-10'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'  ,'2023-12-11'),
    (4, 2, 'o', 43.2, '2023-12-13', 'c','d',2, 'mm'  ,'2023-12-13'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'  ,'2023-12-12'),
    (5, 2, 'o', 56.2, '2023-12-14', 'c','d',2, 'mi'  ,'2023-12-14'),
    (5, 2, 'o', 56.2, '2023-12-16', 'c','d',2, 'mi'  ,'2023-12-16'),
    (5, 2, 'o', 1.2,  '2023-12-12', 'c','d',2, 'mi'  ,'2023-12-12');  
    """

    sql """
    insert into partsupp values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    sql """analyze table partsupp with sync"""
    sql """analyze table lineitem with sync"""
    sql """analyze table orders with sync"""

    sql """alter table orders modify column lo_orderdate set stats ('row_count'='18');"""
    sql """alter table lineitem modify column lo_orderdate set stats ('row_count'='10');"""
    sql """alter table partsupp modify column ps_comment set stats ('row_count'='2');"""

    // =, !=, >, <, <=, >=
    // left outer join
    // other conjuncts in join condition
    def mv1_0 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            left outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            left outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    def query1_0 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            left outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            left outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""

    def mv1_4 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            left outer join
            orders on l_shipdate <= o_orderdate and l_orderkey = o_orderkey 
            left outer join partsupp on l_orderkey + o_orderkey != ps_availqty and ps_partkey = l_partkey;
            """
    def query1_4 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            left outer join
            orders on l_shipdate <= o_orderdate and l_orderkey = o_orderkey
            left outer join partsupp on l_orderkey + o_orderkey != ps_availqty and ps_partkey = l_partkey;
            """
    order_qt_query1_4_before "${query1_4}"
    // other conjuncts is before equal conjuncts, should success
    async_mv_rewrite_success(db, mv1_4, query1_4, "mv1_4")
    order_qt_query1_4_after "${query1_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_4"""


    def mv1_5 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            left outer join
            orders on l_shipdate <= o_orderdate and l_orderkey = o_orderkey and lineitem.lo_orderdate <= orders.lo_orderdate
            left outer join partsupp on l_orderkey + o_orderkey != ps_availqty and ps_partkey = l_partkey;
            """
    def query1_5 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            left outer join
            orders on l_shipdate <= o_orderdate and l_orderkey = o_orderkey and lineitem.lo_orderdate <= orders.lo_orderdate
            left outer join partsupp on l_orderkey + o_orderkey != ps_availqty and ps_partkey = l_partkey;
            """
    order_qt_query1_5_before "${query1_5}"
    // other conjuncts has the same column name
    async_mv_rewrite_success(db, mv1_5, query1_5, "mv1_5")
    order_qt_query1_5_after "${query1_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_5"""


    def mv1_1 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            left outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            left outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    def query1_1 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            left outer join
            orders on l_orderkey = o_orderkey and l_shipdate < o_orderdate
            left outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query1_1_before "${query1_1}"
    async_mv_rewrite_fail(db, mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""


    def mv1_2 =
            """
            select l_orderkey, l_shipdate, o_orderdate
            from
            lineitem
            left outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            left outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    def query1_2 =
            """
            select l_orderkey, l_shipdate, o_orderdate
            from
            lineitem
            left outer join
            orders on l_orderkey = o_orderkey and l_shipdate < o_orderdate
            left outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query1_2_before "${query1_2}"
    // though select has the compensate filter column, should fail
    async_mv_rewrite_fail(db, mv1_2, query1_2, "mv1_2")
    order_qt_query1_2_after "${query1_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_2"""


    // self join
    def mv1_3 =
            """
            select l1.l_orderkey, l2.l_shipdate
            from
            lineitem l1
            left outer join
            lineitem l2 on l1.l_orderkey = l2.l_orderkey and l1.l_shipdate <= l2.l_receiptdate and l1.l_extendedprice != l2.l_discount;
            """
    def query1_3 =
            """
            select l1.l_orderkey, l2.l_shipdate
            from
            lineitem l1
            left outer join
            lineitem l2 on l1.l_orderkey = l2.l_orderkey and l1.l_shipdate <= l2.l_receiptdate and l1.l_extendedprice != l2.l_discount;
            """
    order_qt_query1_3_before "${query1_3}"
    async_mv_rewrite_success(db, mv1_3, query1_3, "mv1_3")
    order_qt_query1_3_after "${query1_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3"""

    // other conjuncts above join
    def mv2_0 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate <= o_orderdate;
            """
    def query2_0 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders 
              left outer join lineitem on l_orderkey = o_orderkey
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate <= o_orderdate;
            """
    order_qt_query2_0_before "${query2_0}"
    async_mv_rewrite_success(db, mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    def mv2_1 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate <= o_orderdate;
            """
    def query2_1 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate < o_orderdate;
            """
    order_qt_query2_1_before "${query2_1}"
    async_mv_rewrite_fail(db, mv2_1, query2_1, "mv2_1")
    order_qt_query2_1_after "${query2_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_1"""


    def mv2_2 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate <= o_orderdate;
            """
    def query2_2 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate < o_orderdate;
            """
    order_qt_query2_2_before "${query2_2}"
    // though select has the compensate filter column, should fail
    async_mv_rewrite_fail(db, mv2_2, query2_2, "mv2_2")
    order_qt_query2_2_after "${query2_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_2"""


    // other conjuncts both above join and in join other conjuncts
    def mv3_0 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query3_0 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query3_0_before "${query3_0}"
    async_mv_rewrite_success(db, mv3_0, query3_0, "mv3_0")
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""


    def mv3_1 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query3_1 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey and l_shipdate < o_orderdate
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query3_1_before "${query3_1}"
    async_mv_rewrite_fail(db, mv3_1, query3_1, "mv3_1")
    order_qt_query3_1_after "${query3_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_1"""


    def mv3_2 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query3_2 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey and l_shipdate < o_orderdate
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query3_2_before "${query3_2}"
    // though select has the compensate filter column, should fail
    async_mv_rewrite_fail(db, mv3_2, query3_2, "mv3_2")
    order_qt_query3_2_after "${query3_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_2"""


    def mv3_3 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query3_3 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey
              left outer join partsupp on ps_partkey = l_partkey;
            """
    order_qt_query3_3_before "${query3_3}"
    // mv has other conjuncts but query not
    async_mv_rewrite_fail(db, mv3_3, query3_3, "mv3_3")
    order_qt_query3_3_after "${query3_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_3"""


    def mv3_4 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey 
              left outer join partsupp on ps_partkey = l_partkey;
            """
    def query3_4 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey and l_shipdate < o_orderdate
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query3_4_before "${query3_4}"
    // query has other conjuncts but mv not
    async_mv_rewrite_fail(db, mv3_4, query3_4, "mv3_4")
    order_qt_query3_4_after "${query3_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_4"""


    def mv3_5 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query3_5 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query3_5_before "${query3_5}"
    // Combinations of different join types
    async_mv_rewrite_success(db, mv3_5, query3_5, "mv3_5")
    order_qt_query3_5_after "${query3_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_5"""


    def mv3_6 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey and date_trunc(l_shipdate, 'day') <= o_orderdate
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query3_6 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey and date_trunc(l_shipdate, 'day') <= o_orderdate
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query3_6_before "${query3_6}"
    // Complex expressions
    async_mv_rewrite_success(db, mv3_6, query3_6, "mv3_6")
    order_qt_query3_6_after "${query3_6}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_6"""


    // right outer join
    // other conjuncts in join condition
    def mv4_0 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            right outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            right outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    def query4_0 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            right outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            right outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query4_0_before "${query4_0}"
    async_mv_rewrite_success(db, mv4_0, query4_0, "mv4_0")
    order_qt_query4_0_after "${query4_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_0"""


    def mv4_1 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            right outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            right outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    def query4_1 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            right outer join
            orders on l_orderkey = o_orderkey and l_shipdate < o_orderdate
            right outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query4_1_before "${query4_1}"
    async_mv_rewrite_fail(db, mv4_1, query4_1, "mv4_1")
    order_qt_query4_1_after "${query4_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_1"""


    def mv4_2 =
            """
            select l_orderkey, l_shipdate, o_orderdate
            from
            lineitem
            right outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            right outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    def query4_2 =
            """
            select l_orderkey, l_shipdate, o_orderdate
            from
            lineitem
            right outer join
            orders on l_orderkey = o_orderkey and l_shipdate < o_orderdate
            right outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query4_2_before "${query4_2}"
    // though select has the compensate filter column, should fail
    async_mv_rewrite_fail(db, mv4_2, query4_2, "mv4_2")
    order_qt_query4_2_after "${query4_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_2"""

    def mv4_3 =
            """
            select l_orderkEY, o_orderdate
            from
            lineitem
            right outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            right outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderKey != ps_availQty;
            """
    def query4_3 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            right outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            right outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query4_3_before "${query4_3}"
    // Case sensitivity of column names in query and mv, should success
    async_mv_rewrite_success(db, mv4_3, query4_3, "mv4_3")
    order_qt_query4_3_after "${query4_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_3"""


    def mv4_4 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            right outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            left outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    def query4_4 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            right outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            left outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query4_4_before "${query4_4}"
    // Combinations of different join types
    async_mv_rewrite_success(db, mv4_4, query4_4, "mv4_4")
    order_qt_query4_4_after "${query4_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_4"""

    // other conjuncts above join
    def mv5_0 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate <= o_orderdate;
            """
    def query5_0 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders 
              right outer join lineitem on l_orderkey = o_orderkey
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate <= o_orderdate;
            """
    order_qt_query5_0_before "${query5_0}"
    async_mv_rewrite_success(db, mv5_0, query5_0, "mv5_0")
    order_qt_query5_0_after "${query5_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_0"""


    def mv5_1 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate <= o_orderdate;
            """
    def query5_1 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate < o_orderdate;
            """
    order_qt_query5_1_before "${query5_1}"
    async_mv_rewrite_fail(db, mv5_1, query5_1, "mv5_1")
    order_qt_query5_1_after "${query5_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_1"""


    def mv5_2 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate <= o_orderdate;
            """
    def query5_2 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate < o_orderdate;
            """
    order_qt_query_5_2_before "${query5_2}"
    // though select has the compensate filter column, should fail
    async_mv_rewrite_fail(db, mv5_2, query5_2, "mv5_2")
    order_qt_query5_2_after "${query5_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_2"""

    // self join
    def mv5_3 =
            """
            select l1.l_orderkey, l2.l_shipdate
            from
            lineitem l1
            right outer join
            lineitem l2 on l1.l_orderkey = l2.l_orderkey
            where l1.l_shipdate <= l2.l_receiptdate and l1.l_extendedprice != l2.l_discount;
            """
    def query5_3 =
            """
            select l1.l_orderkey, l2.l_shipdate
            from
            lineitem l1
            right outer join
            lineitem l2 on l1.l_orderkey = l2.l_orderkey
            where l1.l_shipdate <= l2.l_receiptdate and l1.l_extendedprice != l2.l_discount;
            """
    order_qt_query5_3_before "${query5_3}"
    async_mv_rewrite_success(db, mv5_3, query5_3, "mv5_3")
    order_qt_query5_3_after "${query5_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_3"""


    // other conjuncts both above join and in join other conjuncts
    def mv6_0 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query6_0 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query6_0_before "${query6_0}"
    async_mv_rewrite_success(db, mv6_0, query6_0, "mv6_0")
    order_qt_query6_0_after "${query6_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_0"""


    def mv6_1 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query6_1 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey and l_shipdate < o_orderdate
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query6_1_before "${query6_1}"
    async_mv_rewrite_fail(db, mv6_1, query6_1, "mv6_1")
    order_qt_query6_1_after "${query6_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_1"""


    def mv6_2 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query6_2 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey and l_shipdate < o_orderdate
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query6_2_before "${query6_2}"
    // though select has the compensate filter column, should fail
    async_mv_rewrite_fail(db, mv6_2, query6_2, "mv6_2")
    order_qt_query6_2_after "${query6_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_2"""


    def mv6_3 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query6_3 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey
              right outer join partsupp on ps_partkey = l_partkey;
            """
    order_qt_query6_3_before "${query6_3}"
    // mv has other conjuncts but query not
    async_mv_rewrite_fail(db, mv6_3, query6_3, "mv6_3")
    order_qt_query6_3_after "${query6_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_3"""


    def mv6_4 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey
              right outer join partsupp on ps_partkey = l_partkey;
            """
    def query6_4 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              right outer join lineitem on l_orderkey = o_orderkey and l_shipdate < o_orderdate
              right outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query6_4_before "${query6_4}"
    // query has other conjuncts but mv not
    async_mv_rewrite_fail(db, mv6_4, query6_4, "mv6_4")
    order_qt_query6_4_after "${query6_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_4"""


    // full outer join
    // other conjuncts in join condition
    def mv7_0 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            full outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            full outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    def query7_0 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            full outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            full outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query7_0_before "${query7_0}"
    async_mv_rewrite_success(db, mv7_0, query7_0, "mv7_0")
    order_qt_query7_0_after "${query7_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_0"""


    def mv7_1 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            full outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            full outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    def query7_1 =
            """
            select l_orderkey, o_orderdate
            from
            lineitem
            full outer join
            orders on l_orderkey = o_orderkey and l_shipdate < o_orderdate
            full outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query7_7_before "${query7_1}"
    async_mv_rewrite_fail(db, mv7_1, query7_1, "mv7_1")
    order_qt_query7_7_after "${query7_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_1"""


    def mv7_2 =
            """
            select l_orderkey, l_shipdate, o_orderdate
            from
            lineitem
            full outer join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            full outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    def query7_2 =
            """
            select l_orderkey, l_shipdate, o_orderdate
            from
            lineitem
            full outer join
            orders on l_orderkey = o_orderkey and l_shipdate < o_orderdate
            full outer join partsupp on ps_partkey = l_partkey and l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query7_2_before "${query7_2}"
    // though select has the compensate filter column, should fail
    async_mv_rewrite_fail(db, mv7_2, query7_2, "mv7_2")
    order_qt_query7_2_after "${query7_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_2"""

    // other conjuncts above join
    def mv8_0 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate <= o_orderdate;
            """
    def query8_0 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders 
              full outer join lineitem on l_orderkey = o_orderkey
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate <= o_orderdate;
            """
    order_qt_query8_0_before "${query8_0}"
    async_mv_rewrite_success(db, mv8_0, query8_0, "mv8_0")
    order_qt_query8_0_after "${query8_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8_0"""


    def mv8_1 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate <= o_orderdate;
            """
    def query8_1 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate < o_orderdate;
            """
    order_qt_query8_1_before "${query8_1}"
    async_mv_rewrite_fail(db, mv8_1, query8_1, "mv8_1")
    order_qt_query8_1_after "${query8_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8_1"""


    def mv8_2 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate <= o_orderdate;
            """
    def query8_2 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty and l_shipdate < o_orderdate;
            """
    order_qt_query8_2_before "${query8_2}"
    // though select has the compensate filter column, should fail
    async_mv_rewrite_fail(db, mv8_2, query8_2, "mv8_2")
    order_qt_query8_2_after "${query8_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8_2"""


    // other conjuncts both above join and in join other conjuncts
    def mv9_0 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query9_0 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query9_0_before "${query9_0}"
    async_mv_rewrite_success(db, mv9_0, query9_0, "mv9_0")
    order_qt_query9_0_after "${query9_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_0"""


    def mv9_1 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query9_1 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey and l_shipdate < o_orderdate
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query9_1_before "${query9_1}"
    async_mv_rewrite_fail(db, mv9_1, query9_1, "mv9_1")
    order_qt_query9_1_after "${query9_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_1"""


    def mv9_2 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query9_2 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey and l_shipdate < o_orderdate
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query9_2_before "${query9_2}"
    // though select has the compensate filter column, should fail
    async_mv_rewrite_fail(db, mv9_2, query9_2, "mv9_2")
    order_qt_query9_2_after "${query9_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_2"""


    // self join
    def mv9_3 =
            """
            select l1.l_orderkey, l2.l_shipdate
            from
            lineitem l1
            full outer join
            lineitem l2 on l1.l_orderkey = l2.l_orderkey
            where l1.l_shipdate <= l2.l_receiptdate and l1.l_extendedprice != l2.l_discount;
            """
    def query9_3 =
            """
            select l1.l_orderkey, l2.l_shipdate
            from
            lineitem l1
            full outer join
            lineitem l2 on l1.l_orderkey = l2.l_orderkey
            where l1.l_shipdate <= l2.l_receiptdate and l1.l_extendedprice != l2.l_discount;
            """
    order_qt_query9_3_before "${query9_3}"
    async_mv_rewrite_success(db, mv9_3, query9_3, "mv9_3")
    order_qt_query9_3_after "${query9_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_3"""

    def mv9_4 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query9_4 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey 
              full outer join partsupp on ps_partkey = l_partkey;
            """
    order_qt_query9_4_before "${query9_4}"
    // mv has other conjuncts but query not
    async_mv_rewrite_fail(db, mv9_4, query9_4, "mv9_4")
    order_qt_query9_4_after "${query9_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_4"""


    def mv9_5 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey 
              full outer join partsupp on ps_partkey = l_partkey;
            """
    def query9_5 =
            """
            select
              o_orderdate,
              l_shipdate,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey and l_shipdate < o_orderdate
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query9_5_before "${query9_5}"
    // query has other conjuncts but mv not
    async_mv_rewrite_fail(db, mv9_5, query9_5, "mv9_5")
    order_qt_query9_5_after "${query9_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_5"""


    def mv9_6 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query9_6 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              left outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              full outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query9_6_before "${query9_6}"
    // Combinations of different join types
    async_mv_rewrite_success(db, mv9_6, query9_6, "mv9_6")
    order_qt_query9_6_after "${query9_6}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_6"""


    def mv9_7 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    def query9_7 =
            """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey
            from
              orders
              full outer join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
              left outer join partsupp on ps_partkey = l_partkey
              where l_orderkey + o_orderkey != ps_availqty;
            """
    order_qt_query9_7_before "${query9_7}"
    // Combinations of different join types
    async_mv_rewrite_success(db, mv9_7, query9_7, "mv9_7")
    order_qt_query9_7_after "${query9_7}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_7"""
}


