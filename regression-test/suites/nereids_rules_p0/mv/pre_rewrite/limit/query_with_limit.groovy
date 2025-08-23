package mv.pre_rewrite.limit
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

suite("query_with_limit") {
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

    // test limit without offset
    def mv1_0 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderkey > 1;
            """
    def query1_0 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderkey > 1
            limit 2;
            """
    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv1_0, query1_0, "mv1_0", [NOT_IN_RBO])
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""

    // test normal limit with offset
    def mv1_1 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderkey > 1;
            """
    def query1_1 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            where o_orderkey > 1
            limit 2 offset 3;
            """
    async_mv_rewrite_success(db, mv1_1, query1_1, "mv1_1", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv1_1, query1_1, "mv1_1", [NOT_IN_RBO])
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""

    // test mv with limit in from subquery, should fail
    def mv1_2 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            (select * from orders limit 2) t
            left join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey;
            """
    def query1_2 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            limit 2;
            """
    async_mv_rewrite_fail(db, mv1_2, query1_2, "mv1_2")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_2"""

    // test limit and offset in from subquery, and query is the same with mv
    // should fail, no support limit in mv, multi table
    def mv1_3 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            (select * from orders limit 2 offset 3) t
            left join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey;
            """
    def query1_3 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            (select * from orders limit 2 offset 3) t
            left join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey;
            """
    async_mv_rewrite_fail(db, mv1_3, query1_3, "mv1_3")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3"""


    // test limit offset in where subquery, and query is the same with mv
    // should fail, no support topN in mv, single table
    def mv1_4 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            where o_orderkey in (select l_orderkey from lineitem limit 2 offset 3);
            """
    def query1_4 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            where o_orderkey in (select l_orderkey from lineitem limit 2 offset 3);
            """
    async_mv_rewrite_fail(db, mv1_4, query1_4, "mv1_4")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_4"""


    // test normal limit offset + group by
    def mv1_5 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey;
            """
    def query1_5 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            limit 2 offset 3;
            """
    async_mv_rewrite_success(db, mv1_5, query1_5, "mv1_5", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv1_5, query1_5, "mv1_5", [NOT_IN_RBO])
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_5"""


    // test normal limit without offset + group by
    def mv1_5_0 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey;
            """
    def query1_5_0 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            limit 2;
            """
    async_mv_rewrite_success(db, mv1_5_0, query1_5_0, "mv1_5_0", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv1_5_0, query1_5_0, "mv1_5_0", [NOT_IN_RBO])
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_5_0"""


    // test mv limit in from subquery, should fail
    def mv1_6 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            (select * from orders limit 2 offset 3) t
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey;
            """
    def query1_6 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            limit 2 offset 3;
            """
    async_mv_rewrite_fail(db, mv1_6, query1_6, "mv1_6")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_6"""


    // test limit offset in from subquery, and query is the same with mv
    // should fail, no support topN in mv, multi table
    def mv1_7 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            (select * from orders limit 2 offset 3) t
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey;
            """
    def query1_7 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            (select * from orders limit 2 offset 3) t
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey;
            """
    async_mv_rewrite_fail(db, mv1_7, query1_7, "mv1_7")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_7"""


    // test limit offset in where subquery, and query is the same with mv
    // should fail, no support topN in mv, single table
    def mv1_8 =
            """
            select
            o_orderkey,
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            where o_orderkey in (select l_orderkey from lineitem limit 2 offset 3)
            group by
            o_orderkey,
            o_orderdate,
            o_shippriority,
            o_comment;
            """
    def query1_8 =
            """
            select
            o_orderkey,
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            where o_orderkey in (select l_orderkey from lineitem limit 2 offset 3)
            group by
            o_orderkey,
            o_orderdate,
            o_shippriority,
            o_comment;
            """
    async_mv_rewrite_fail(db, mv1_8, query1_8, "mv1_8")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_8"""


    // test topN rewrite with offset
    def mv2_0 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey;
            """
    def query2_0 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            order by l_orderkey
            limit 2
            offset 1;
            """
    order_qt_query2_0_before "${query2_0}"
    async_mv_rewrite_success(db, mv2_0, query2_0, "mv2_0", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv2_0, query2_0, "mv2_0", [NOT_IN_RBO])
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    // test topN rewrite without offset
    def mv2_1 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey;
            """
    def query2_1 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            order by l_orderkey
            limit 2;
            """
    order_qt_query2_1_before "${query2_1}"
    async_mv_rewrite_success(db, mv2_1, query2_1, "mv2_1", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv2_1, query2_1, "mv2_1", [NOT_IN_RBO])
    order_qt_query2_1_after "${query2_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_1"""

    // test topN rewrite, mv from subquery contains limit, should fail
    def mv2_2 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            ( select * from 
            orders left
            join lineitem on l_orderkey = o_orderkey
            order by l_orderkey
            limit 2) t
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey;
            """
    def query2_2 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            order by l_orderkey
            limit 2;
            """
    order_qt_query2_2_before "${query2_2}"
    async_mv_rewrite_fail(db, mv2_2, query2_2, "mv2_2")
    order_qt_query2_2_after "${query2_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_2"""

    // test topN in from subquery, and query is the same with mv, multi table
    def mv2_3 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            ( select * from 
            orders left
            join lineitem on l_orderkey = o_orderkey
            order by l_orderkey
            limit 2) t
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey;
            """
    def query2_3 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey,
            l_partkey
            from
            ( select * from 
            orders left
            join lineitem on l_orderkey = o_orderkey
            order by l_orderkey
            limit 2) t
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey;
            """
    order_qt_query2_3_before "${query2_3}"
    async_mv_rewrite_fail(db, mv2_3, query2_3, "mv2_3")
    order_qt_query2_3_after "${query2_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_3"""


    // test topN in where subquery, and query is the same with mv, single table
    def mv2_4 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            where o_orderkey in (select l_orderkey from lineitem order by l_orderkey limit 2 offset 3);
            """
    def query2_4 =
            """
            select
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            where o_orderkey in (select l_orderkey from lineitem order by l_orderkey limit 2 offset 3);
            """
    async_mv_rewrite_fail(db, mv2_4, query2_4, "mv2_4")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_4"""


    // test normal topN + group by, with offset
    def mv2_5 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey;
            """
    // order keys should contains all join child slot
    def query2_5 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            order by 
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            limit 2 offset 3;
            """
    async_mv_rewrite_success(db, mv2_5, query2_5, "mv2_5", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv2_5, query2_5, "mv2_5", [NOT_IN_RBO])
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_5"""


    // test normal topN + group by, without offset
    def mv2_5_0 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey;
            """
    // order keys should contains all join child slot
    def query2_5_0 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            order by 
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            limit 2;
            """
    async_mv_rewrite_success(db, mv2_5_0, query2_5_0, "mv2_5_0", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv2_5_0, query2_5_0, "mv2_5_0", [NOT_IN_RBO])
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_5_0"""


    // test mv with topN in from subquery, should fail
    def mv2_6 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            (select * from orders order by o_orderkey limit 2 offset 3) t
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey;
            """
    // order keys should contains all join child slot
    def query2_6 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            orders left
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            order by o_orderkey
            limit 2 offset 3;
            """
    async_mv_rewrite_fail(db, mv2_6, query2_6, "mv2_6")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_6"""


    // test limit offset in from subquery, and query is the same with mv, multi table
    def mv2_7 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            (select * from orders order by o_orderkey limit 2 offset 3) t
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey;
            """
    def query2_7 =
            """
            select
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey
            from
            (select * from orders order by o_orderkey limit 2 offset 3) t
            join lineitem on l_orderkey = o_orderkey
            left join partsupp on ps_partkey = l_partkey and l_suppkey = ps_suppkey
            group by
            l_orderkey,
            o_orderkey,
            l_partkey,
            l_suppkey;
            """
    async_mv_rewrite_fail(db, mv2_7, query2_7, "mv2_7")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_7"""


    // test limit offset in where subquery, and query is the same with mv, single table
    def mv2_8 =
            """
            select
            o_orderkey,
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            where o_orderkey in (select l_orderkey from lineitem order by l_orderkey limit 2 offset 3)
            group by
            o_orderkey,
            o_orderdate,
            o_shippriority,
            o_comment;
            """
    def query2_8 =
            """
            select
            o_orderkey,
            o_orderdate,
            o_shippriority,
            o_comment
            from
            orders
            where o_orderkey in (select l_orderkey from lineitem order by l_orderkey limit 2 offset 3)
            group by
            o_orderkey,
            o_orderdate,
            o_shippriority,
            o_comment;
            """
    async_mv_rewrite_fail(db, mv2_8, query2_8, "mv2_8")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_8"""


    // test window rewrite, should fail, not supported
    def mv3_0 =
            """
            select 
              * 
            from 
              (
                select 
                  row_number() over (
                    partition by l_orderkey 
                    order by 
                      l_partkey
                  ) as row_number_win, 
                  l_orderkey, 
                  l_partkey 
                from 
                  lineitem
              ) t 
            where 
              row_number_win <=1;
            """
    def query3_0 =
            """
            select 
              * 
            from 
              (
                select 
                  row_number() over (
                    partition by l_orderkey 
                    order by 
                      l_partkey
                  ) as row_number_win, 
                  l_orderkey, 
                  l_partkey 
                from 
                  lineitem
              ) t 
            where 
              row_number_win <=1;
            """
    order_qt_query3_0_before "${query3_0}"
    async_mv_rewrite_fail(db, mv3_0, query3_0, "mv3_0")
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""


    // test window with limit offset
    def mv3_1 =
            """
                select 
                  row_number() over (
                    partition by l_orderkey 
                    order by 
                      l_partkey
                  ) as row_number_win, 
                  l_orderkey, 
                  l_partkey,
                   o_shippriority
                from 
                  lineitem
                left join
                  orders on l_orderkey = o_orderkey;
            """
    def query3_1 =
            """
                select 
                  row_number() over (
                    partition by l_orderkey 
                    order by 
                      l_partkey
                  ) as row_number_win, 
                  l_orderkey, 
                  l_partkey,
                   o_shippriority
                from 
                  lineitem
                left join
                  orders on l_orderkey = o_orderkey
                limit 2 offset 1;
            """
    async_mv_rewrite_fail(db, mv3_1, query3_1, "mv3_1")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_1"""


    // test window with topN
    def mv3_2 =
            """
                select 
                  row_number() over (
                    partition by l_orderkey 
                    order by 
                      l_partkey
                  ) as row_number_win, 
                  l_orderkey, 
                  l_partkey,
                   o_shippriority
                from 
                  lineitem
                left join
                  orders on l_orderkey = o_orderkey;
            """
    def query3_2 =
            """
                select 
                  row_number() over (
                    partition by l_orderkey 
                    order by 
                      l_partkey
                  ) as row_number_win, 
                  l_orderkey, 
                  l_partkey,
                   o_shippriority
                from 
                  lineitem
                left join
                  orders on l_orderkey = o_orderkey
                order by l_orderkey
                limit 2 offset 1;
            """
    async_mv_rewrite_fail(db, mv3_2, query3_2, "mv3_2")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_2"""

    // query is union all + limit offset
    def mv4_0 =
            """
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
            """
    def query4_0 =
            """
            select *
            from (
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
                union all
                select 
                  o_orderkey
                  from
                  orders
                ) t  
                  limit 2 offset 5;
            """
    async_mv_rewrite_success(db, mv4_0, query4_0, "mv4_0", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv4_0, query4_0, "mv4_0", [NOT_IN_RBO])
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_0"""

    // query is union all + limit without offset
    def mv4_0_0 =
            """
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
            """
    def query4_0_0 =
            """
            select *
            from (
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
                union all
                select 
                  o_orderkey
                  from
                  orders
                ) t  
                  limit 2 offset 5;
            """
    async_mv_rewrite_success(db, mv4_0_0, query4_0_0, "mv4_0_0", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv4_0_0, query4_0_0, "mv4_0_0", [NOT_IN_RBO])
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_0_0"""


    // query is union all + group by + limit offset
    def mv4_1 =
            """
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
                  group by l_orderkey 
            """
    def query4_1 =
            """
            select *
            from (
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
                group by l_orderkey  
                union all
                select 
                  o_orderkey
                  from
                  orders
                ) t 
                group by l_orderkey 
                  limit 2 offset 0;
            """
    async_mv_rewrite_success(db, mv4_1, query4_1, "mv4_1", [TRY_IN_RBO, FORCE_IN_RBO])
    // because limit not pushed through aggregate, so success
    async_mv_rewrite_success(db, mv4_1, query4_1, "mv4_1", [NOT_IN_RBO])
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_1"""

    // query is union all + group by + limit without offset
    def mv4_1_0 =
            """
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
                  group by l_orderkey 
            """
    def query4_1_0 =
            """
            select *
            from (
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
                group by l_orderkey  
                union all
                select 
                  o_orderkey
                  from
                  orders
                ) t 
                group by l_orderkey 
                  limit 2;
            """
    async_mv_rewrite_success(db, mv4_1_0, query4_1_0, "mv4_1_0", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_success(db, mv4_1_0, query4_1_0, "mv4_1_0", [NOT_IN_RBO])
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_1_0"""


    // query is union all + topN
    def mv4_2 =
            """
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
                  order by l_orderkey
            """
    def query4_2 =
            """
            select *
            from (
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
                union all
                select 
                  o_orderkey
                  from
                  orders
                ) t 
                order by l_orderkey 
                  limit 2 offset 5;
            """
    async_mv_rewrite_success(db, mv4_2, query4_2, "mv4_2", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv4_2, query4_2, "mv4_2", [NOT_IN_RBO])
    order_qt_query4_2_after "${query4_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_2"""


    // query is union all + topN, without offset
    def mv4_2_0 =
            """
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
                  order by l_orderkey
            """
    def query4_2_0 =
            """
            select *
            from (
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
                union all
                select 
                  o_orderkey
                  from
                  orders
                ) t 
                order by l_orderkey 
                  limit 2;
            """
    async_mv_rewrite_success(db, mv4_2_0, query4_2_0, "mv4_2_0", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv4_2_0, query4_2_0, "mv4_2_0", [NOT_IN_RBO])
    order_qt_query4_2_0_after "${query4_2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_2_0"""


    // query is union all + group by + topN
    def mv4_3 =
            """
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
                  group by l_orderkey 
                  order by l_orderkey
            """
    def query4_3 =
            """
            select *
            from (
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
                group by l_orderkey  
                union all
                select 
                  o_orderkey
                  from
                  orders
                ) t  
                  group by l_orderkey 
                  order by l_orderkey
                  limit 2 offset 5;
            """
    async_mv_rewrite_success(db, mv4_3, query4_3, "mv4_3", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv4_3, query4_3, "mv4_3", [NOT_IN_RBO])
    order_qt_query4_3_after "${query4_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_3"""


    // query is union all + group by + topN without offset
    def mv4_3_0 =
            """
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
                  group by l_orderkey 
                  order by l_orderkey
            """
    def query4_3_0 =
            """
            select *
            from (
                select 
                  l_orderkey
                from 
                  lineitem
                  left join
                  orders on l_orderkey = o_orderkey
                group by l_orderkey  
                union all
                select 
                  o_orderkey
                  from
                  orders
                ) t  
                  group by l_orderkey 
                  order by l_orderkey
                  limit 2 offset 5;
            """
    async_mv_rewrite_success(db, mv4_3_0, query4_3_0, "mv4_3_0", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv4_3_0, query4_3_0, "mv4_3_0", [NOT_IN_RBO])
    order_qt_query4_3_0_after "${query4_3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_3_0"""
}

