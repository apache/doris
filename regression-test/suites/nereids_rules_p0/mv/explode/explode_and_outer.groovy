package mv.explode
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

suite("explode_and_outer") {
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
      O_COMMENT        VARCHAR(1024) NULL,
      o_array1 ARRAY<int(11)> NULL,
      o_array2 ARRAY<int(11)> NULL
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
    (1, 1, 'o', 9.5, '2023-12-08', 'a', 'b', 1, "1, 2", [], [2, 3]),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, "3, 4, 5", [4, 5], [6, 7, null]),
    (3, 1, 'o', 33.5, '2023-12-10', 'a', 'b', 1, null, [null], [8, 9]),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, "6, 7", [10, null], [null, 11, 12]),
    (5, 2, 'o', 1.2, '2023-12-12', 'c','d',2, "8", [13, 14, 15], []);  
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

    // single table
    // top filter(project) + explode + bottom filter(project) + scan
    // query with top filter(project) and view with top filter(project)
    def mv1_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query1_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""


    def mv1_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    def query1_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query1_1_before "${query1_1}"
    async_mv_rewrite_fail(db, mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""



    def mv1_2 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    def query1_2 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            ) t
            LATERAL VIEW explode(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query1_2_before "${query1_2}"
    // mv is explode_outer, query is explode, cannot rewrite
    async_mv_rewrite_fail(db, mv1_2, query1_2, "mv1_2")
    order_qt_query1_2_after "${query1_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_2"""


    def mv1_3 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders
            ) t
            LATERAL VIEW explode_split(t.o_comment, ',') t1 as c2
            where c2 > 2
            """
    def query1_3 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            ) t
            LATERAL VIEW explode_split(t.o_comment, ',') t1 as c2
            where c2 > 3
            """
    order_qt_query1_3_before "${query1_3}"
    async_mv_rewrite_success(db, mv1_3, query1_3, "mv1_3")
    order_qt_query1_3_after "${query1_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3"""


    // query with top filter(project) and view with bottom filter(project)
    def mv2_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    def query2_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    order_qt_query2_0_before "${query2_0}"
    async_mv_rewrite_fail(db, mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    def mv2_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    def query2_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where t.o_orderkey > 2
            """
    order_qt_query2_1_before "${query2_1}"
    async_mv_rewrite_success(db, mv2_1, query2_1, "mv2_1")
    order_qt_query2_1_after "${query2_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_1"""


    // query with top filter(project) and view with top and bottom filter(project)
    def mv3_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query3_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query3_0_before "${query3_0}"
    async_mv_rewrite_fail(db, mv3_0, query3_0, "mv3_0")
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""

    // query with bottom filter(project) and view with top filter(project)
    def mv4_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where t.o_orderkey > 1
            """
    def query4_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            where o_orderkey > 2
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    order_qt_query4_0_before "${query4_0}"
    async_mv_rewrite_success(db, mv4_0, query4_0, "mv4_0")
    order_qt_query4_0_after "${query4_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_0"""


    def mv4_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    def query4_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            where o_orderkey > 2
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    order_qt_query4_1_before "${query4_1}"
    async_mv_rewrite_fail(db, mv4_1, query4_1, "mv4_1")
    order_qt_query4_1_after "${query4_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_1"""


    // query with bottom filter(project) and view with bottom filter(project)
    def mv5_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    def query5_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 2
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    order_qt_query5_0_before "${query5_0}"
    async_mv_rewrite_success(db, mv5_0, query5_0, "mv5_0")
    order_qt_query5_0_after "${query5_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_0"""


    def mv5_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    def query5_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode(t.o_array2) t1 as c2
            """
    order_qt_query5_1_before "${query5_1}"
    async_mv_rewrite_fail(db, mv5_1, query5_1, "mv5_1")
    order_qt_query5_1_after "${query5_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_1"""


    // query with bottom filter(project) and view with top and bottom filter(project)
    def mv6_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query6_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    order_qt_query6_0_before "${query6_0}"
    async_mv_rewrite_fail(db, mv6_0, query6_0, "mv6_0")
    order_qt_query6_0_after "${query6_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_0"""


    def mv6_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where o_orderkey > 2
            """
    def query6_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 3
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    order_qt_query6_1_before "${query6_1}"
    async_mv_rewrite_success(db, mv6_1, query6_1, "mv6_1")
    order_qt_query6_1_after "${query6_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_1"""

    // query with top and bottom filter(project) and view with top filter(project)
    def mv7_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query7_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    order_qt_query7_0_before "${query7_0}"
    async_mv_rewrite_success(db, mv7_0, query7_0, "mv7_0")
    order_qt_query7_0_after "${query7_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_0"""


    def mv7_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2 and  t.o_orderkey > 1
            """
    def query7_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 2
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query7_1_before "${query7_1}"
    async_mv_rewrite_success(db, mv7_1, query7_1, "mv7_1")
    order_qt_query7_1_after "${query7_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_1"""


    def mv7_2 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    def query7_2 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query7_2_before "${query7_2}"
    // mv is explode_outer, query is explode, cannot rewrite
    async_mv_rewrite_fail(db, mv7_2, query7_2, "mv7_2")
    order_qt_query7_2_after "${query7_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_2"""

    // query with top and bottom filter(project) and view with bottom filter(project)
    def mv8_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    def query8_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    order_qt_query8_0_before "${query8_0}"
    async_mv_rewrite_success(db, mv8_0, query8_0, "mv8_0")
    order_qt_query8_0_after "${query8_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8_0"""


    def mv8_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query8_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 2
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    order_qt_query8_1_before "${query8_1}"
    async_mv_rewrite_success(db, mv8_1, query8_1, "mv8_1")
    order_qt_query8_1_after "${query8_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8_1"""



    def mv8_2 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    def query8_2 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query8_2_before "${query8_2}"
    // mv is explode_outer, query is explode, cannot rewrite
    async_mv_rewrite_fail(db, mv8_2, query8_2, "mv8_2")
    order_qt_query8_2_after "${query8_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8_2"""


    // query with top and bottom filter(project) and view with top and bottom filter(project)
    def mv9_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query9_0 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    order_qt_query9_0_before "${query9_0}"
    async_mv_rewrite_success(db, mv9_0, query9_0, "mv9_0")
    order_qt_query9_0_after "${query9_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_0"""


    def mv9_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query9_1 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 2
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query9_1_before "${query9_1}"
    async_mv_rewrite_success(db, mv9_1, query9_1, "mv9_1")
    order_qt_query9_1_after "${query9_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_1"""



    def mv9_2 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    def query9_2 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query9_2_before "${query9_2}"
    // predicate rewrite fail
    async_mv_rewrite_fail(db, mv9_2, query9_2, "mv9_2")
    order_qt_query9_2_after "${query9_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_2"""


    def mv9_3 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query9_3 =
            """
            select t.o_orderkey, c2
            from 
            (
            select * from 
            orders

             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query9_3_before "${query9_3}"
    // mv is explode without outer, query is explode_outer, cannot rewrite
    async_mv_rewrite_fail(db, mv9_3, query9_3, "mv9_3")
    order_qt_query9_3_after "${query9_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9_3"""


    // multi table
    // top filter(project) + explode + bottom filter(project) + join
    // query with top filter(project) and view with top filter(project)
    def mv10_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query10_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    order_qt_query10_0_before "${query10_0}"
    async_mv_rewrite_success(db, mv10_0, query10_0, "mv10_0")
    order_qt_query10_0_after "${query10_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv10_0"""


    def mv10_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    def query10_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query10_1_before "${query10_1}"
    async_mv_rewrite_fail(db, mv10_1, query10_1, "mv10_1")
    order_qt_query10_1_after "${query10_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv10_1"""



    def mv10_2 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    def query10_2 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            ) t
            LATERAL VIEW explode(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query10_2_before "${query10_2}"
    // mv is explode_outer, query is explode, cannot rewrite
    async_mv_rewrite_fail(db, mv10_2, query10_2, "mv10_2")
    order_qt_query10_2_after "${query10_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv10_2"""


    // query with top filter(project) and view with bottom filter(project)
    def mv11_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    def query11_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    order_qt_query11_0_before "${query11_0}"
    async_mv_rewrite_fail(db, mv11_0, query11_0, "mv11_0")
    order_qt_query11_0_after "${query11_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv11_0"""


    def mv11_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    def query11_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where t.o_orderkey > 2
            """
    order_qt_query11_1_before "${query11_1}"
    async_mv_rewrite_success(db, mv11_1, query11_1, "mv11_1")
    order_qt_query11_1_after "${query11_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv11_1"""


    // query with top filter(project) and view with top and bottom filter(project)
    def mv12_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query12_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query12_0_before "${query12_0}"
    async_mv_rewrite_fail(db, mv12_0, query12_0, "mv12_0")
    order_qt_query12_0_after "${query12_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv12_0"""

    // query with bottom filter(project) and view with top filter(project)
    def mv13_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where t.o_orderkey > 1
            """
    def query13_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            where o_orderkey > 2
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    order_qt_query13_0_before "${query13_0}"
    async_mv_rewrite_success(db, mv13_0, query13_0, "mv13_0")
    order_qt_query13_0_after "${query13_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv13_0"""


    def mv13_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    def query13_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            where o_orderkey > 2
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    order_qt_query13_1_before "${query13_1}"
    async_mv_rewrite_fail(db, mv13_1, query13_1, "mv13_1")
    order_qt_query13_1_after "${query13_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv13_1"""


    // query with bottom filter(project) and view with bottom filter(project)
    def mv14_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    def query14_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 2
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    order_qt_query14_0_before "${query14_0}"
    async_mv_rewrite_success(db, mv14_0, query14_0, "mv14_0")
    order_qt_query14_0_after "${query14_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv14_0"""


    def mv14_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    def query14_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode(t.o_array2) t1 as c2
            """
    order_qt_query14_1_before "${query14_1}"
    async_mv_rewrite_fail(db, mv14_1, query14_1, "mv14_1")
    order_qt_query14_1_after "${query14_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv14_1"""


    // query with bottom filter(project) and view with top and bottom filter(project)
    def mv15_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query15_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    order_qt_query15_0_before "${query15_0}"
    async_mv_rewrite_fail(db, mv15_0, query15_0, "mv15_0")
    order_qt_query15_0_after "${query15_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv15_0"""


    def mv15_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where o_orderkey > 2
            """
    def query15_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 3
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    order_qt_query15_1_before "${query15_1}"
    async_mv_rewrite_success(db, mv15_1, query15_1, "mv15_1")
    order_qt_query15_1_after "${query15_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv15_1"""

    // query with top and bottom filter(project) and view with top filter(project)
    def mv16_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query16_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    order_qt_query16_0_before "${query16_0}"
    async_mv_rewrite_success(db, mv16_0, query16_0, "mv16_0")
    order_qt_query16_0_after "${query16_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv16_0"""


    def mv16_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2 and  t.o_orderkey > 1
            """
    def query16_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 2
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query16_1_before "${query16_1}"
    async_mv_rewrite_success(db, mv16_1, query16_1, "mv16_1")
    order_qt_query16_1_after "${query16_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv16_1"""


    def mv16_2 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    def query16_2 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query16_2_before "${query16_2}"
    // mv is explode_outer, query is explode, cannot rewrite
    async_mv_rewrite_fail(db, mv16_2, query16_2, "mv16_2")
    order_qt_query16_2_after "${query16_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv16_2"""

    // query with top and bottom filter(project) and view with bottom filter(project)
    def mv17_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    def query17_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    order_qt_query17_0_before "${query17_0}"
    async_mv_rewrite_success(db, mv17_0, query17_0, "mv17_0")
    order_qt_query17_0_after "${query17_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv17_0"""


    def mv17_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query17_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 2
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    order_qt_query17_1_before "${query17_1}"
    async_mv_rewrite_success(db, mv17_1, query17_1, "mv17_1")
    order_qt_query17_1_after "${query17_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv17_1"""



    def mv17_2 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            """
    def query17_2 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query17_2_before "${query17_2}"
    // mv is explode_outer, query is explode, cannot rewrite
    async_mv_rewrite_fail(db, mv17_2, query17_2, "mv17_2")
    order_qt_query17_2_after "${query17_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv17_2"""


    // query with top and bottom filter(project) and view with top and bottom filter(project)
    def mv18_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query18_0 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    order_qt_query18_0_before "${query18_0}"
    async_mv_rewrite_success(db, mv18_0, query18_0, "mv18_0")
    order_qt_query18_0_after "${query18_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18_0"""


    def mv18_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query18_1 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 2
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query18_1_before "${query18_1}"
    async_mv_rewrite_success(db, mv18_1, query18_1, "mv18_1")
    order_qt_query18_1_after "${query18_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18_1"""



    def mv18_2 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 3
            """
    def query18_2 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query18_2_before "${query18_2}"
    // predicate rewrite fail
    async_mv_rewrite_fail(db, mv18_2, query18_2, "mv18_2")
    order_qt_query18_2_after "${query18_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18_2"""


    def mv18_3 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode(t.o_array2) t1 as c2
            where c2 > 2
            """
    def query18_3 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query18_3_before "${query18_3}"
    // mv is explode without outer, query is explode_outer, cannot rewrite
    async_mv_rewrite_fail(db, mv18_3, query18_3, "mv18_3")
    order_qt_query18_3_after "${query18_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18_3"""


    def mv18_4 =
            """
            select 
              t.o_orderkey, 
              t.l_orderkey, 
              c2 
            from 
              (
                select 
                  * 
                from 
                  (
                    select 
                      * 
                    from 
                      orders LATERAL VIEW explode_outer(o_array2) o1 as c1
                  ) o1 
                  left join lineitem on l_orderkey = o_orderkey 
                where 
                  o_orderkey > 1
              ) t LATERAL VIEW explode_outer(t.o_array2) t1 as c2 
            where 
              c2 > 2;
            """
    def query18_4 =
            """
            select t.o_orderkey, t.l_orderkey, c2
            from 
            (
            select * from 
            orders
            left join lineitem on l_orderkey = o_orderkey
             where o_orderkey > 1
            ) t
            LATERAL VIEW explode_outer(t.o_array2) t1 as c2
            where c2 > 2
            """
    order_qt_query18_4_before "${query18_4}"
    // mv is explode without outer, query is explode_outer, cannot rewrite
    async_mv_rewrite_fail(db, mv18_4, query18_4, "mv18_4")
    order_qt_query18_4_after "${query18_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18_4"""



    def mv18_5 =
            """
            select 
              t.o_orderkey, 
              t.l_orderkey, 
              c2 
            from 
              (
                select 
                  * 
                from 
                  (
                    select 
                      * 
                    from 
                      orders LATERAL VIEW explode_outer(o_array2) o1 as c1
                  ) o1 
                  left join lineitem on l_orderkey = o_orderkey 
                where 
                  o_orderkey > 1
              ) t LATERAL VIEW explode_outer(t.o_array2) t1 as c2 
            where 
              c2 > 2;
            """
    def query18_5 =
            """
            select 
              t.o_orderkey, 
              t.l_orderkey, 
              c2 
            from 
              (
                select 
                  * 
                from 
                  (
                    select 
                      * 
                    from 
                      orders LATERAL VIEW explode_outer(o_array2) o1 as c1
                  ) o1 
                  left join lineitem on l_orderkey = o_orderkey 
                where 
                  o_orderkey > 1
              ) t LATERAL VIEW explode_outer(t.o_array2) t1 as c2 
            where 
              c2 > 3;
            """
    order_qt_query18_5_before "${query18_5}"
    // mv is explode without outer, query is explode_outer, cannot rewrite
    async_mv_rewrite_success(db, mv18_5, query18_5, "mv18_5")
    order_qt_query18_5_after "${query18_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18_5"""
}
