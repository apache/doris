package mv.window
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

suite("window_above_scan") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "set disable_nereids_rules='ELIMINATE_CONST_JOIN_CONDITION,CONSTANT_PROPAGATION'"

    sql """
    drop table if exists orders1
    """

    sql """
    CREATE TABLE IF NOT EXISTS orders1  (
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
    drop table if exists lineitem1
    """

    sql"""
    CREATE TABLE IF NOT EXISTS lineitem1 (
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
    drop table if exists partsupp1
    """

    sql """
    CREATE TABLE IF NOT EXISTS partsupp1 (
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

    sql """ insert into lineitem1 values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-08', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (2, 4, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-09', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 4, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-10', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (4, 3, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-11', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (5, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-12', '2023-12-12', '2023-12-13', 'c', 'd', 'xxxxxxxxx');
    """

    sql """
    insert into orders1 values
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
    insert into partsupp1 values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    sql """analyze table partsupp1 with sync"""
    sql """analyze table lineitem1 with sync"""
    sql """analyze table orders1 with sync"""
    sql """alter table lineitem1 modify column l_comment set stats ('row_count'='5');"""
    sql """alter table orders1 modify column O_COMMENT set stats ('row_count'='18');"""
    sql """alter table partsupp1 modify column ps_comment set stats ('row_count'='2');"""

    // top filter(project) + window + bottom filter(project) + scan
    // query has only top filter, view has both top and bottom filter
    def mv1_0 =
            """
            select * from (
            select 
            o_orderkey, o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderdate > '2023-12-10'
            ) t
            where o_orderkey > 6
            """
    def query1_0 =
            """
            select * from (
            select 
            o_orderkey,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            ) t
            where o_orderkey > 6;
            """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_fail(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""


    def mv1_1 =
            """
            select * from (
            select 
            o_orderkey, o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 6
            ) t
            where o_orderkey > 7
            """
    def query1_1 =
            """
            select 
            *
            from
            (
            select 
            o_orderkey,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            ) t
            where o_orderkey > 6;
            """
    order_qt_query1_1_before "${query1_1}"
    async_mv_rewrite_fail(db, mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""


    // query has only top filter, view has only top filter
    def mv2_0 =
            """
            select *
            from (
            select 
            o_orderkey,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            ) t
            where o_orderkey > 1;
            """
    def query2_0 =
            """
            select *
            from (
            select 
            o_orderkey,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            ) t
            where o_orderkey > 2;
            """
    order_qt_query2_0_before "${query2_0}"
    async_mv_rewrite_success(db, mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    def mv2_1 =
            """
            select *
            from (
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            ) t
            where o_orderkey > 1;
            """
    def query2_1 =
            """
            select *
            from (
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            ) t
            where o_orderdate > '2023-12-10';
            """
    order_qt_query2_1_before "${query2_1}"
    async_mv_rewrite_fail(db, mv2_1, query2_1, "mv2_1")
    order_qt_query2_1_after "${query2_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_1"""

    // query has only top filter, view has only bottom filter
    def mv3_0 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderdate > '2023-12-09';
            """
    def query3_0 =
            """
            select *
            from (
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            ) t
            where o_orderdate > '2023-12-10';
            """
    order_qt_query3_0_before "${query3_0}"
    // filter can be pushed down, should success
    async_mv_rewrite_success(db, mv3_0, query3_0, "mv3_0")
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""


    def mv3_1 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 3;
            """
    def query3_1 =
            """
            select *
            from (
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            ) t
            where o_orderkey > 3;
            """
    order_qt_query3_1_before "${query3_1}"
    // filter can not be pushed down, should success
    async_mv_rewrite_fail(db, mv3_1, query3_1, "mv3_1")
    order_qt_query3_1after "${query3_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_1"""


    // query has only top filter, view has no filter
    def mv4_0 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1;
            """
    def query4_0 =
            """
            select *
            from (
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            ) t
            where o_orderkey > 3;
            """
    order_qt_query4_0_before "${query4_0}"
    async_mv_rewrite_success(db, mv4_0, query4_0, "mv4_0")
    order_qt_query4_0after "${query4_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_0"""


    // query has only bottom filter, view has both top and bottom filter
    def mv5_0 =
            """
            select * from (
            select 
            o_orderkey, o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderdate > '2023-12-10'
            ) t
            where o_orderkey > 6
            """
    def query5_0 =
            """
            select 
            o_orderkey,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 6;
            """
    order_qt_query5_0_before "${query5_0}"
    async_mv_rewrite_fail(db, mv5_0, query5_0, "mv5_0")
    order_qt_query5_0_after "${query5_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_0"""


    def mv5_1 =
            """
            select * from (
            select 
            o_orderkey, o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 6
            ) t
            where o_orderkey > 7
            """
    def query5_1 =
            """
            select 
            o_orderkey,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 6;
            """
    order_qt_query5_1_before "${query5_1}"
    async_mv_rewrite_fail(db, mv5_1, query5_1, "mv5_1")
    order_qt_query5_1_after "${query5_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_1"""

    // query has only bottom filter, view has only top filter
    def mv6_0 =
            """
            select *
            from (
            select 
            o_orderkey,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            ) t
            where o_orderkey > 1;
            """
    def query6_0 =
            """
            select 
            o_orderkey,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 2;
            """
    order_qt_query6_0_before "${query6_0}"
    async_mv_rewrite_fail(db, mv6_0, query6_0, "mv6_0")
    order_qt_query6_0_after "${query6_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_0"""


    def mv6_1 =
            """
            select *
            from (
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            ) t
            where o_orderkey > 1;
            """
    def query6_1 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderdate > '2023-12-10';
            """
    order_qt_query6_1_before "${query6_1}"
    async_mv_rewrite_fail(db, mv6_1, query6_1, "mv6_1")
    order_qt_query6_1_after "${query6_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_1"""

    // query has only bottom filter, view has only bottom filter
    def mv7_0 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderdate > '2023-12-09';
            """
    def query7_0 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderdate > '2023-12-10';
            """
    order_qt_query7_0_before "${query7_0}"
    // filter can be pushed down, should success
    async_mv_rewrite_success(db, mv7_0, query7_0, "mv7_0")
    order_qt_query7_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_0"""

    def mv7_1 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 3;
            """
    def query7_1 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 3;
            """
    order_qt_query7_1_before "${query7_1}"
    // filter can not be pushed down, should success
    async_mv_rewrite_success(db, mv7_1, query7_1, "mv7_1")
    order_qt_query7_1after "${query7_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_1"""


    def mv7_2 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 3;
            """
    def query7_2 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 4;
            """
    order_qt_query7_2_before "${query7_2}"
    // filter can not be pushed down, should success
    async_mv_rewrite_fail(db, mv7_2, query7_2, "mv7_2")
    order_qt_query7_2after "${query7_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_2"""

    // query has only bottom filter, view has no filter
    def mv8_0 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1;
            """
    def query8_0 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 3;
            """
    order_qt_query8_0_before "${query8_0}"
    async_mv_rewrite_fail(db, mv8_0, query8_0, "mv8_0")
    order_qt_query8_0after "${query8_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8_0"""


    def mv8_1 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1;
            """
    def query8_1 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderdate > '2023-12-10';
            """
    order_qt_query8_1_before "${query8_1}"
    async_mv_rewrite_success(db, mv8_1, query8_1, "mv8_1")
    order_qt_query8_1after "${query8_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8_1"""


    // query has both top and bottom filter, view has both top and bottom filter
    def mv9_0 =
            """
            select * from (
            select 
            o_orderkey, o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderdate > '2023-12-10'
            ) t
            where o_orderkey > 6
            """
    def query9_0 =
            """
            select * from (
            select 
            o_orderkey, o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderdate > '2023-12-10'
            ) t
            where o_orderkey > 6
            """
    order_qt_query9_0_before "${query9_0}"
    async_mv_rewrite_success_without_check_chosen(db, mv9_0, query9_0, "mv9_0")
    order_qt_query9_0_after "${query9_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_0"""


    def mv9_1 =
            """
            select * from (
            select 
            o_orderkey, o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderdate > '2023-12-10'
            ) t
            where o_orderkey > 6
            """
    def query9_1 =
            """
                        select * from (
            select 
            o_orderkey, o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderdate > '2023-12-10'
            ) t
            where o_orderkey > 7
            """
    order_qt_query9_1_before "${query9_1}"
    async_mv_rewrite_success_without_check_chosen(db, mv9_1, query9_1, "mv9_1")
    order_qt_query9_1_after "${query9_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_1"""


    def mv9_2 =
            """
            select * from (
            select 
            o_orderkey, o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 6
            ) t
            where o_orderkey > 6
            """
    def query9_2 =
            """
            select * from (
            select 
            o_orderkey, o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 7
            ) t
            where o_orderkey > 6
            """
    order_qt_query9_2_before "${query9_2}"
    async_mv_rewrite_fail(db, mv9_2, query9_2, "mv9_2")
    order_qt_query9_2_after "${query9_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_2"""


    // query has both top and bottom filter, view has only top filter
    def mv10_0 =
            """
            select *
            from (
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            ) t
            where o_orderkey > 1;
            """
    def query10_0 =
            """
            select *
            from (
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderdate > '2023-12-10'
            ) t
            where o_orderkey > 2;
            """
    order_qt_query10_0_before "${query10_0}"
    async_mv_rewrite_success(db, mv10_0, query10_0, "mv10_0")
    order_qt_query10_0_after "${query10_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv10_0"""


    def mv10_1 =
            """
            select *
            from (
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            ) t
            where o_orderkey > 1;
            """
    def query10_1 =
            """
            select *
            from (
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 1
            ) t
            where o_orderdate > '2023-12-10';
            """
    order_qt_query10_1_before "${query10_1}"
    async_mv_rewrite_fail(db, mv10_1, query10_1, "mv10_1")
    order_qt_query10_1_after "${query10_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv10_1"""


    // query has both top and bottom filter, view has only bottom filter
    def mv11_0 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderdate > '2023-12-09';
            """
    def query11_0 =
            """
            select *
            from (
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 1
            ) t
            where o_orderdate > '2023-12-10';
            """
    order_qt_query11_0_before "${query11_0}"
    // filter can be pushed down, should success
    async_mv_rewrite_fail(db, mv11_0, query11_0, "mv11_0")
    order_qt_query11_0_after "${query11_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv11_0"""


    def mv11_1 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 3;
            """
    def query11_1 =
            """
            select *
            from (
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 3
            ) t
            where o_orderkey > 7;
            """
    order_qt_query11_1_before "${query11_1}"
    // filter can not be pushed down, should success
    async_mv_rewrite_success(db, mv11_1, query11_1, "mv11_1")
    order_qt_query11_1after "${query11_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv11_1"""

    // query has both top and bottom filter, view has no filter
    def mv12_0 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1;
            """
    def query12_0 =
            """
            select
            *
            from
            (
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderdate > '2023-12-10'
            ) t
            where o_orderkey > 3;
            """
    order_qt_query12_0_before "${query12_0}"
    async_mv_rewrite_success(db, mv12_0, query12_0, "mv12_0")
    order_qt_query12_0after "${query12_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv12_0"""


    def mv12_1 =
            """
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1;
            """
    def query12_1 =
            """
            select
            *
            from
            (
            select 
            o_orderkey,
            o_orderdate,
            FIRST_VALUE(o_custkey) OVER (
                    PARTITION BY o_orderdate 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS first_value,
            RANK() OVER (
                    PARTITION BY o_orderdate, o_orderstatus 
                    ORDER BY o_totalprice NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rank_value
            from 
            orders1
            where o_orderkey > 3
            ) t
            where o_orderdate > '2023-12-10';
            """
    order_qt_query12_1_before "${query12_1}"
    async_mv_rewrite_fail(db, mv12_1, query12_1, "mv12_1")
    order_qt_query12_1after "${query12_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv12_1"""

}
