package mv.agg_with_roll_up
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

suite("any_value_roll_up") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
    drop table if exists orders_2
    """

    sql """
    CREATE TABLE IF NOT EXISTS orders_2  (
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
    drop table if exists lineitem_2
    """

    sql"""
    CREATE TABLE IF NOT EXISTS lineitem_2 (
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
    drop table if exists partsupp_2
    """

    sql """
    CREATE TABLE IF NOT EXISTS partsupp_2 (
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

    sql """ insert into lineitem_2 values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-08', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (2, 4, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-09', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 4, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-10', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (4, 3, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-11', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (5, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-12', '2023-12-12', '2023-12-13', 'c', 'd', 'xxxxxxxxx');
    """

    sql """
    insert into orders_2 values
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
    insert into partsupp_2 values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    sql """analyze table partsupp_2 with sync"""
    sql """analyze table lineitem_2 with sync"""
    sql """analyze table orders_2 with sync"""


    // mv has any value, query also has any value
    def mv1_0 = """
            select
            o_orderdate,
            l_partkey,
            l_suppkey,
            any_value(o_orderstatus)
            from lineitem_2
            left join orders_2 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query1_0 = """
            select
            l_suppkey,
            any_value(o_orderstatus)
            from lineitem_2
            left join orders_2
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_suppkey;
    """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_success(db, mv1_0, query1_0, "any_mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS any_mv1_0"""


    // query has any value, mv doesn't have
    def mv2_0 = """
            select
            o_orderdate,
            l_partkey,
            l_suppkey,
            o_orderstatus
            from lineitem_2
            left join orders_2 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query2_0 = """
            select
            l_suppkey,
            any_value(o_orderstatus)
            from lineitem_2
            left join orders_2
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_suppkey;
    """
    order_qt_query2_0_before "${query2_0}"
    async_mv_rewrite_success(db, mv2_0, query2_0, "any_mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS any_mv2_0"""


    // mv has any value, query also has any value, with same filter
    def mv3_0 = """
            select
            l_suppkey,
            o_orderdate,
            l_partkey,
            o_orderstatus,
            any_value(l_suppkey),
            any_value(o_orderstatus)
            from lineitem_2
            left join orders_2 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where l_partkey = 3 and o_orderstatus = 'o'
            group by
            o_orderstatus,
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query3_0 = """
            select
            l_suppkey,
            any_value(l_suppkey),
            any_value(o_orderstatus)
            from lineitem_2
            left join orders_2
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where l_partkey = 3 and o_orderstatus = 'o'
            group by
            o_orderdate,
            l_suppkey;
    """
    order_qt_query3_0_before "${query3_0}"
    async_mv_rewrite_success(db, mv3_0, query3_0, "any_mv3_0")
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS any_mv3_0"""


    // query has any value, mv doesn't have, with same filter
    def mv4_0 = """
            select
            o_orderdate,
            l_partkey,
            l_suppkey,
            o_orderstatus
            from lineitem_2
            left join orders_2 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where l_partkey = 3 and o_orderstatus = 'o'
            group by
            o_orderstatus,
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query4_0 = """
            select
            any_value(l_suppkey),
            any_value(o_orderstatus)
            from lineitem_2
            left join orders_2
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where l_partkey = 3 and o_orderstatus = 'o'
            group by
            o_orderdate,
            l_suppkey;
    """
    order_qt_query4_0_before "${query4_0}"
    async_mv_rewrite_success(db, mv4_0, query4_0, "any_mv4_0")
    order_qt_query4_0_after "${query4_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS any_mv4_0"""


    // mv has any value, query also has any value, with different filter
    def mv5_0 = """
            select
            o_orderdate,
            any_value(l_partkey),
            o_orderstatus,
            l_suppkey,
            any_value(l_suppkey),
            any_value(o_orderstatus)
            from lineitem_2
            left join orders_2 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where l_partkey = 3
            group by
            o_orderstatus,
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query5_0 = """
            select
            any_value(l_partkey),
            any_value(l_suppkey),
            any_value(o_orderstatus)
            from lineitem_2
            left join orders_2
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where l_partkey = 3 and o_orderstatus = 'o'
            group by
            o_orderdate,
            l_suppkey;
    """
    order_qt_query5_0_before "${query5_0}"
    async_mv_rewrite_success(db, mv5_0, query5_0, "any_mv5_0")
    order_qt_query5_0_after "${query5_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS any_mv5_0"""


    def mv5_1 = """
            select
            o_orderdate,
            any_value(l_partkey),
            any_value(l_suppkey),
            any_value(o_orderstatus)
            from lineitem_2
            left join orders_2 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where l_partkey = 3 and o_orderstatus = 'o'
            group by
            o_orderstatus,
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query5_1 = """
            select
            l_suppkey,
            any_value(l_suppkey),
            any_value(o_orderstatus)
            from lineitem_2
            left join orders_2
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where l_partkey = 3
            group by
            o_orderdate,
            l_suppkey;
    """
    order_qt_query5_1_before "${query5_1}"
    async_mv_rewrite_fail(db, mv5_1, query5_1, "any_mv5_1")
    order_qt_query5_1_after "${query5_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS any_mv5_1"""


    // query has any value, mv doesn't have, with different filter
    def mv6_0 = """
            select
            o_orderdate,
            l_partkey,
            l_suppkey,
            o_orderstatus
            from lineitem_2
            left join orders_2 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderstatus = 'o'
            group by
            o_orderstatus,
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query6_0 = """
            select
            any_value(l_suppkey),
            any_value(o_orderstatus)
            from lineitem_2
            left join orders_2
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where l_partkey = 3 and o_orderstatus = 'o'
            group by
            o_orderdate,
            l_suppkey;
    """
    order_qt_query6_0_before "${query6_0}"
    async_mv_rewrite_success(db, mv6_0, query6_0, "any_mv6_0")
    order_qt_query6_0_after "${query6_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS any_mv6_0"""


    def mv6_1 = """
            select
            o_orderdate,
            l_partkey,
            l_suppkey,
            o_orderstatus
            from lineitem_2
            left join orders_2 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where l_partkey = 3 and o_orderstatus = 'o'
            group by
            o_orderstatus,
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query6_1 = """
            select
            l_suppkey,
            any_value(o_orderstatus)
            from lineitem_2
            left join orders_2
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            where o_orderstatus = 'o'
            group by
            o_orderdate,
            l_suppkey;
    """
    order_qt_query6_1_before "${query6_1}"
    async_mv_rewrite_fail(db, mv6_1, query6_1, "any_mv6_1")
    order_qt_query6_1_after "${query6_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS any_mv6_1"""

}
