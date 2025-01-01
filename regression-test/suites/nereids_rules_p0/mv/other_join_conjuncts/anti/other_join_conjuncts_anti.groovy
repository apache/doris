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

suite("other_join_conjuncts_anti") {
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
    // left anti join other conjuncts in join condition
    def mv1_0 =
            """
            select l_orderkey
            from
            lineitem
            left anti join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            left anti join partsupp on ps_partkey = l_partkey and l_orderkey != ps_availqty;
            """
    def query1_0 =
            """
            select l_orderkey
            from
            lineitem
            left anti join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            left anti join partsupp on ps_partkey = l_partkey and l_orderkey != ps_availqty;
            """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_success_without_check_chosen(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""


    def mv1_4 =
            """
            select l_orderkey
            from
            lineitem
            left anti join
            orders on l_shipdate <= o_orderdate and l_orderkey = o_orderkey 
            left anti join partsupp on l_orderkey != ps_availqty and ps_partkey = l_partkey;
            """
    def query1_4 =
            """
            select l_orderkey
            from
            lineitem
            left anti join
            orders on l_shipdate <= o_orderdate and l_orderkey = o_orderkey
            left anti join partsupp on l_orderkey != ps_availqty and ps_partkey = l_partkey;
            """
    order_qt_query1_4_before "${query1_4}"
    // other conjuncts is before equal conjuncts, should success
    async_mv_rewrite_success_without_check_chosen(db, mv1_4, query1_4, "mv1_4")
    order_qt_query1_4_after "${query1_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_4"""



    def mv1_5 =
            """
            select l_orderkey
            from
            lineitem
            left anti join
            orders on l_shipdate <= o_orderdate and l_orderkey = o_orderkey and lineitem.lo_orderdate <= orders.lo_orderdate
            left anti join partsupp on l_orderkey != ps_availqty and ps_partkey = l_partkey;
            """
    def query1_5 =
            """
            select l_orderkey
            from
            lineitem
            left anti join
            orders on l_shipdate <= o_orderdate and l_orderkey = o_orderkey and lineitem.lo_orderdate <= orders.lo_orderdate
            left anti join partsupp on l_orderkey != ps_availqty and ps_partkey = l_partkey;
            """
    order_qt_query1_5_before "${query1_5}"
    // other conjuncts has the same column name
    async_mv_rewrite_success_without_check_chosen(db, mv1_5, query1_5, "mv1_5")
    order_qt_query1_5_after "${query1_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_5"""


    def mv1_1 =
            """
            select l_orderkey
            from
            lineitem
            left anti join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            left anti join partsupp on ps_partkey = l_partkey and l_orderkey != ps_availqty;
            """
    def query1_1 =
            """
            select l_orderkey
            from
            lineitem
            left anti join
            orders on l_orderkey = o_orderkey and l_shipdate < o_orderdate
            left anti join partsupp on ps_partkey = l_partkey and l_orderkey != ps_availqty;
            """
    order_qt_query1_1_before "${query1_1}"
    async_mv_rewrite_fail(db, mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""

    def mv1_2 =
            """
            select l_orderkey
            from
            lineitem
            left anti join
            orders on l_orderkey = o_orderkey and l_shipdate <= o_orderdate
            left anti join partsupp on ps_partkey = l_partkey and l_orderkey != ps_availqty;
            """
    def query1_2 =
            """
            select l_orderkey
            from
            lineitem
            left anti join
            orders on l_orderkey = o_orderkey
            left anti join partsupp on ps_partkey = l_partkey;
            """
    order_qt_query1_2_before "${query1_2}"
    // mv has other conjuncts but query not
    async_mv_rewrite_fail(db, mv1_2, query1_2, "mv1_2")
    order_qt_query1_2_after "${query1_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_2"""

    def mv1_3 =
            """
            select l_orderkey
            from
            lineitem
            left anti join
            orders on l_orderkey = o_orderkey 
            left anti join partsupp on ps_partkey = l_partkey;
            """
    def query1_3 =
            """
            select l_orderkey
            from
            lineitem
            left anti join
            orders on l_orderkey = o_orderkey and l_shipdate < o_orderdate
            left anti join partsupp on ps_partkey = l_partkey and l_orderkey != ps_availqty;
            """
    order_qt_query1_3_before "${query1_3}"
    // query has other conjuncts but mv not
    async_mv_rewrite_fail(db, mv1_3, query1_3, "mv1_3")
    order_qt_query1_3_after "${query1_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3"""

    // right anti join other conjuncts in join condition
    def mv2_0 =
            """
            select l_orderkey
            from
            orders
            right anti join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate;
            """
    def query2_0 =
            """
            select l_orderkey
            from
            orders
            right anti join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate;
            """
    order_qt_query2_0_before "${query2_0}"
    async_mv_rewrite_success_without_check_chosen(db, mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    def mv2_1 =
            """
            select l_orderkey
            from
            orders
            right anti join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate;
            """
    def query2_1 =
            """
            select l_orderkey
            from
            orders
            right anti join lineitem on l_orderkey = o_orderkey and l_shipdate < o_orderdate;
            """
    order_qt_query2_1_before "${query2_1}"
    async_mv_rewrite_fail(db, mv2_1, query2_1, "mv2_1")
    order_qt_query2_1_after "${query2_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_1"""

    def mv2_2 =
            """
            select l_orderkey
            from
            orders
            right anti join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate;
            """
    def query2_2 =
            """
            select l_orderkey
            from
            orders
            right anti join lineitem on l_orderkey = o_orderkey;
            """
    order_qt_query2_2_before "${query2_2}"
    // mv has other conjuncts but query not
    async_mv_rewrite_fail(db, mv2_2, query2_2, "mv2_2")
    order_qt_query2_2_after "${query2_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_2"""


    def mv2_3 =
            """
            select l_orderkey
            from
            orders
            right anti join lineitem on l_orderkey = o_orderkey;
            """
    def query2_3 =
            """
            select l_orderkey
            from
            orders
            right anti join lineitem on l_orderkey = o_orderkey and l_shipdate < o_orderdate;
            """
    order_qt_query2_3_before "${query2_3}"
    // query has other conjuncts but mv not
    async_mv_rewrite_fail(db, mv2_3, query2_3, "mv2_3")
    order_qt_query2_3_after "${query2_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_3"""


    def mv2_4 =
            """
            select L_orderkeY
            from
            orders
            right anti join lineitem on l_orderkey = o_orderkey and l_shipdatE <= o_orderdatE;
            """
    def query2_4 =
            """
            select l_orderkey
            from
            orders
            right anti join lineitem on l_orderkey = o_orderkey and l_shipdate <= o_orderdate;
            """
    order_qt_query2_4_before "${query2_4}"
    // Case sensitivity of column names in query and mv, should success
    async_mv_rewrite_success_without_check_chosen(db, mv2_4, query2_4, "mv2_4")
    order_qt_query2_4_after "${query2_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_4"""


    def mv2_5 =
            """
            select L_orderkeY
            from
            orders
            right anti join lineitem on l_orderkey = o_orderkey and date_trunc(l_shipdatE, 'day') <= o_orderdatE;
            """
    def query2_5 =
            """
            select l_orderkey
            from
            orders
            right anti join lineitem on l_orderkey = o_orderkey and date_trunc(l_shipdate, 'day') <= o_orderdate;
            """
    order_qt_query2_5_before "${query2_5}"
    // Complex expressions
    async_mv_rewrite_success_without_check_chosen(db, mv2_5, query2_5, "mv2_5")
    order_qt_query2_5_after "${query2_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_5"""


    // left self join
    def mv3_0 =
            """
            select l1.l_orderkey
            from
            lineitem l1
            left anti join
            lineitem l2 on l1.l_orderkey = l2.l_orderkey and l1.l_shipdate <= l2.l_receiptdate and l1.l_extendedprice != l2.l_discount
            """
    def query3_0 =
            """
            select l1.l_orderkey
            from
            lineitem l1
            left anti join
            lineitem l2 on l1.l_orderkey = l2.l_orderkey and l1.l_shipdate <= l2.l_receiptdate and l1.l_extendedprice != l2.l_discount;
            """
    order_qt_query3_0_before "${query3_0}"
    async_mv_rewrite_success(db, mv3_0, query3_0, "mv3_0")
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""

    // right self join
    def mv4_0 =
            """
            select l2.l_orderkey
            from
            lineitem l1
            right anti join
            lineitem l2 on l1.l_orderkey = l2.l_orderkey and l1.l_shipdate <= l2.l_receiptdate and l1.l_extendedprice != l2.l_discount
            """
    def query4_0 =
            """
            select l2.l_orderkey
            from
            lineitem l1
            right anti join
            lineitem l2 on l1.l_orderkey = l2.l_orderkey and l1.l_shipdate <= l2.l_receiptdate and l1.l_extendedprice != l2.l_discount;
            """
    order_qt_query4_0_before "${query4_0}"
    async_mv_rewrite_success(db, mv4_0, query4_0, "mv4_0")
    order_qt_query4_0_after "${query4_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_0"""

}
