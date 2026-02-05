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

suite("window_above_join") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "set disable_nereids_rules='ELIMINATE_CONST_JOIN_CONDITION,CONSTANT_PROPAGATION'"

    sql """
    drop table if exists orders2
    """

    sql """
    CREATE TABLE IF NOT EXISTS orders2  (
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
    drop table if exists lineitem2
    """

    sql"""
    CREATE TABLE IF NOT EXISTS lineitem2 (
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
    drop table if exists partsupp2
    """

    sql """
    CREATE TABLE IF NOT EXISTS partsupp2 (
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

    sql """ insert into lineitem2 values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-08', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (2, 4, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-09', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 4, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-10', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (4, 3, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-11', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (5, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-12', '2023-12-12', '2023-12-13', 'c', 'd', 'xxxxxxxxx');
    """

    sql """
    insert into orders2 values
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
    insert into partsupp2 values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    sql """analyze table partsupp2 with sync"""
    sql """analyze table lineitem2 with sync"""
    sql """analyze table orders2 with sync"""
    sql """alter table lineitem2 modify column l_comment set stats ('row_count'='5');"""
    sql """alter table orders2 modify column O_COMMENT set stats ('row_count'='18');"""
    sql """alter table partsupp2 modify column ps_comment set stats ('row_count'='2');"""


    // top filter(project) + window + bottom filter(project) + join
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderdate > '2023-12-10'
            ) t
            where o_orderkey > 3;
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            ) t
            where o_orderkey > 3;
            """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_fail(db, mv1_0, query1_0, "join_mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv1_0"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            ) t
            where o_orderkey > 6;
            """
    order_qt_query1_1_before "${query1_1}"
    async_mv_rewrite_fail(db, mv1_1, query1_1, "join_mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv1_1"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            ) t
            where o_orderkey > 2;
            """
    order_qt_query2_0_before "${query2_0}"
    async_mv_rewrite_success(db, mv2_0, query2_0, "join_mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv2_0"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            ) t
            where o_orderdate > '2023-12-10';
            """
    order_qt_query2_1_before "${query2_1}"
    async_mv_rewrite_fail(db, mv2_1, query2_1, "join_mv2_1")
    order_qt_query2_1_after "${query2_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv2_1"""


    def mv2_2 =
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value,
            max_suppkey
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            left join (
                select ps_partkey, 
                ps_suppkey,
                MAX(ps_suppkey) OVER (
                    PARTITION BY ps_partkey 
                    ORDER BY ps_supplycost NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS max_suppkey
                from
                partsupp2
                where ps_suppkey > 1
            ) window_partsupp2 ON l_partkey = ps_partkey
            ) t
            where o_orderkey > 1;
            """
    def query2_2 =
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value,
            max_suppkey
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            left join (
                select ps_partkey, 
                ps_suppkey,
                MAX(ps_suppkey) OVER (
                    PARTITION BY ps_partkey 
                    ORDER BY ps_supplycost NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS max_suppkey
                from
                partsupp2
                where ps_suppkey > 1
            ) window_partsupp2 ON l_partkey = ps_partkey
            ) t
            where o_orderkey > 2;
            """
    order_qt_query2_2_before "${query2_2}"
    async_mv_rewrite_success(db, mv2_2, query2_2, "join_mv2_2")
    order_qt_query2_2_after "${query2_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv2_2"""


    def mv2_3 =
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value,
            max_suppkey
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            left join (
                select ps_partkey, 
                ps_suppkey,
                MAX(ps_suppkey) OVER (
                    PARTITION BY ps_partkey 
                    ORDER BY ps_supplycost NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS max_suppkey
                from
                partsupp2
                where ps_suppkey > 1
            ) window_partsupp2 ON l_partkey = ps_partkey
            ) t
            where o_orderkey > 1;
            """
    def query2_3 =
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value,
            max_suppkey
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            left join (
                select *
                from
                (
                select ps_partkey, 
                ps_suppkey,
                MAX(ps_suppkey) OVER (
                    PARTITION BY ps_partkey 
                    ORDER BY ps_supplycost NULLS LAST
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS max_suppkey
                from
                partsupp2
                ) ps_sub
                where ps_suppkey > 1
            ) window_partsupp2 ON l_partkey = ps_partkey
            ) t
            where o_orderkey > 1;
            """
    order_qt_query2_3_before "${query2_3}"
    // the filter ps_suppkey > 1 on the partsupp2 can not be pushed down, so fail
    async_mv_rewrite_fail(db, mv2_3, query2_3, "join_mv2_3")
    order_qt_query2_3_after "${query2_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv2_3"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            ) t
            where o_orderdate > '2023-12-10';
            """
    order_qt_query3_0_before "${query3_0}"
    // filter can be pushed down, should success
    async_mv_rewrite_success(db, mv3_0, query3_0, "join_mv3_0")
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv3_0"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            ) t
            where o_orderkey > 3;
            """
    order_qt_query3_1_before "${query3_1}"
    // filter can not be pushed down, should success
    async_mv_rewrite_fail(db, mv3_1, query3_1, "join_mv3_1")
    order_qt_query3_1after "${query3_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv3_1"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate;
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            ) t
            where o_orderkey > 3;
            """
    order_qt_query4_0_before "${query4_0}"
    async_mv_rewrite_success(db, mv4_0, query4_0, "join_mv4_0")
    order_qt_query4_0after "${query4_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv4_0"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderkey > 6;
            """
    order_qt_query5_0_before "${query5_0}"
    async_mv_rewrite_fail(db, mv5_0, query5_0, "join_mv5_0")
    order_qt_query5_0_after "${query5_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv5_0"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderkey > 6;
            """
    order_qt_query5_1_before "${query5_1}"
    async_mv_rewrite_fail(db, mv5_1, query5_1, "join_mv5_1")
    order_qt_query5_1_after "${query5_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv5_1"""

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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderkey > 2;
            """
    order_qt_query6_0_before "${query6_0}"
    async_mv_rewrite_fail(db, mv6_0, query6_0, "join_mv6_0")
    order_qt_query6_0_after "${query6_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv6_0"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderdate > '2023-12-10';
            """
    order_qt_query6_1_before "${query6_1}"
    async_mv_rewrite_fail(db, mv6_1, query6_1, "join_mv6_1")
    order_qt_query6_1_after "${query6_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv6_1"""

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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderdate > '2023-12-10';
            """
    order_qt_query7_0_before "${query7_0}"
    // filter can be pushed down, should success
    async_mv_rewrite_success(db, mv7_0, query7_0, "join_mv7_0")
    order_qt_query7_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv7_0"""

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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderkey > 3;
            """
    order_qt_query7_1_before "${query7_1}"
    // filter can not be pushed down, should success
    async_mv_rewrite_success(db, mv7_1, query7_1, "join_mv7_1")
    order_qt_query7_1after "${query7_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv7_1"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderkey > 4;
            """
    order_qt_query7_2_before "${query7_2}"
    // filter can not be pushed down, should success
    async_mv_rewrite_fail(db, mv7_2, query7_2, "join_mv7_2")
    order_qt_query7_2after "${query7_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv7_2"""

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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate;
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderkey > 3;
            """
    order_qt_query8_0_before "${query8_0}"
    async_mv_rewrite_fail(db, mv8_0, query8_0, "join_mv8_0")
    order_qt_query8_0after "${query8_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv8_0"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate;
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderdate > '2023-12-10';
            """
    order_qt_query8_1_before "${query8_1}"
    async_mv_rewrite_success(db, mv8_1, query8_1, "join_mv8_1")
    order_qt_query8_1after "${query8_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv8_1"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderdate > '2023-12-10'
            ) t
            where o_orderkey > 6
            """
    order_qt_query9_0_before "${query9_0}"
    async_mv_rewrite_success_without_check_chosen(db, mv9_0, query9_0, "join_mv9_0")
    order_qt_query9_0_after "${query9_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv9_0"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderdate > '2023-12-10'
            ) t
            where o_orderkey > 7
            """
    order_qt_query9_1_before "${query9_1}"
    async_mv_rewrite_success_without_check_chosen(db, mv9_1, query9_1, "join_mv9_1")
    order_qt_query9_1_after "${query9_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv9_1"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderkey > 7
            ) t
            where o_orderkey > 6
            """
    order_qt_query9_2_before "${query9_2}"
    async_mv_rewrite_fail(db, mv9_2, query9_2, "join_mv9_2")
    order_qt_query9_2_after "${query9_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv9_2"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderdate > '2023-12-10'
            ) t
            where o_orderkey > 2;
            """
    order_qt_query10_0_before "${query10_0}"
    async_mv_rewrite_success(db, mv10_0, query10_0, "join_mv10_0")
    order_qt_query10_0_after "${query10_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv10_0"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderkey > 1
            ) t
            where o_orderdate > '2023-12-10';
            """
    order_qt_query10_1_before "${query10_1}"
    async_mv_rewrite_fail(db, mv10_1, query10_1, "join_mv10_1")
    order_qt_query10_1_after "${query10_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv10_1"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderkey > 1
            ) t
            where o_orderdate > '2023-12-10';
            """
    order_qt_query11_0_before "${query11_0}"
    // filter can be pushed down, should success
    async_mv_rewrite_fail(db, mv11_0, query11_0, "join_mv11_0")
    order_qt_query11_0_after "${query11_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv11_0"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderkey > 3
            ) t
            where o_orderkey > 7;
            """
    order_qt_query11_1_before "${query11_1}"
    // filter can not be pushed down, should success
    async_mv_rewrite_success(db, mv11_1, query11_1, "join_mv11_1")
    order_qt_query11_1after "${query11_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv11_1"""

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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate;
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderdate > '2023-12-10'
            ) t
            where o_orderkey > 3;
            """
    order_qt_query12_0_before "${query12_0}"
    async_mv_rewrite_success(db, mv12_0, query12_0, "join_mv12_0")
    order_qt_query12_0after "${query12_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv12_0"""


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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate;
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
                ) AS rank_value,
            LAG(l_extendedprice, 1, 0) over (partition by o_orderdate, l_shipdate order by l_quantity) AS lag_value 
            from 
            lineitem2
            left join orders2 on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderkey > 3
            ) t
            where o_orderdate > '2023-12-10';
            """
    order_qt_query12_1_before "${query12_1}"
    async_mv_rewrite_fail(db, mv12_1, query12_1, "join_mv12_1")
    order_qt_query12_1after "${query12_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS join_mv12_1"""
}
