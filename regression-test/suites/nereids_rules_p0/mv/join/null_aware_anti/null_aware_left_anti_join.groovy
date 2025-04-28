package mv.join.null_aware_anti
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

suite("null_aware_anti") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"

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
      o_comment        VARCHAR(79) NOT NULL
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
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

    sql """ insert into lineitem values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-08', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (2, 4, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-09', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 4, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-10', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (4, 3, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-11', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (5, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-12', '2023-12-12', '2023-12-13', 'c', 'd', 'xxxxxxxxx');
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

    sql """
    insert into partsupp values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    sql """
    drop table if exists partsupp_nullable
    """

    sql """
    CREATE TABLE IF NOT EXISTS partsupp_nullable (
      ps_partkey     INTEGER NULL,
      ps_suppkey     INTEGER NULL,
      ps_availqty    INTEGER NULL,
      ps_supplycost  DECIMALV3(15,2) NULL,
      ps_comment     VARCHAR(199) NULL 
    )
    DUPLICATE KEY(ps_partkey, ps_suppkey)
    DISTRIBUTED BY HASH(ps_partkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """
    insert into partsupp_nullable values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 4, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    sql """
    drop table if exists orders_nullable
    """

    sql """
    CREATE TABLE IF NOT EXISTS orders_nullable  (
      o_orderkey       INTEGER  NULL,
      o_custkey        INTEGER  NULL,
      o_orderstatus    CHAR(1)  NULL,
      o_totalprice     DECIMALV3(15,2)  NULL,
      o_orderdate      DATE NULL,
      o_orderpriority  CHAR(15) NULL,  
      o_clerk          CHAR(15) NULL, 
      o_shippriority   INTEGER NULL,
      o_comment        VARCHAR(79) NULL
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """
    insert into orders_nullable values
    (1, 1, 'o', 9.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 33.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 1.2, '2023-12-12', 'c','d',2, 'mi');  
    """


    sql """analyze table lineitem with sync;"""
    sql """analyze table orders with sync;"""
    sql """analyze table orders_nullable with sync;"""
    sql """analyze table partsupp with sync;"""

    sql """alter table lineitem modify column l_comment set stats ('row_count'='5');"""
    sql """alter table orders modify column o_comment set stats ('row_count'='8');"""
    sql """alter table orders_nullable modify column o_comment set stats ('row_count'='8');"""
    sql """alter table partsupp modify column ps_comment set stats ('row_count'='2');"""
    sql """alter table partsupp_nullable modify column ps_comment set stats ('row_count'='3');"""


    def mv1_0 =
            """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
            select o_custkey from orders
            );
            """
    def query1_0 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
            select o_custkey from orders
            );
            """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""


    def mv2_0 =
            """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
            select o_custkey from orders_nullable
            );
            """
    def query2_0 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
            select o_custkey from orders_nullable
            );
            """
    order_qt_query2_0_before "${query2_0}"
    // test NULL_AWARE_LEFT_ANTI_JOIN, should success
    async_mv_rewrite_success(db, mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    def mv2_1 =
            """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
            select o_custkey from orders_nullable
            );
            """
    def query2_1 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
            select o_custkey from orders_nullable
            ) and L_LINENUMBER = 4;
            """
    order_qt_query2_1_before "${query2_1}"
    // test NULL_AWARE_LEFT_ANTI_JOIN, should success
    async_mv_rewrite_success(db, mv2_1, query2_1, "mv2_1")
    order_qt_query2_1_after "${query2_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_1"""

    // left and right is complex expression
    def mv2_2 =
            """
            select  lineitem.L_LINENUMBER
            from lineitem
            where bin(L_ORDERKEY) not in (
            select bin(o_custkey) from orders_nullable
            );
            """
    def query2_2 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where bin(L_ORDERKEY) not in (
            select bin(o_custkey) from orders_nullable
            ) and L_LINENUMBER = 4;
            """
    order_qt_query2_2_before "${query2_2}"
    // test NULL_AWARE_LEFT_ANTI_JOIN, should success
    async_mv_rewrite_success(db, mv2_2, query2_2, "mv2_2")
    order_qt_query2_2_after "${query2_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_2"""


    def mv2_3 =
            """
            select lineitem.L_LINENUMBER
            from lineitem
            where bin(L_ORDERKEY) not in (
            select o_custkey from orders_nullable
            );
            """
    def query2_3 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
            select bin(o_custkey) from orders_nullable
            ) and L_LINENUMBER = 4;
            """
    order_qt_query2_3_before "${query2_3}"
    // left and right is not consistant
    async_mv_rewrite_fail(db, mv2_3, query2_3, "mv2_3")
    order_qt_query2_3_after "${query2_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_3"""



    // multi not in with 'or'
    def mv3_0 =
            """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
                select bin(o_custkey) from orders_nullable
            )
            or
            l_partkey not in (
                select ps_suppkey from partsupp_nullable
            );
            """
    def query3_0 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
                select bin(o_custkey) from orders_nullable
            )
            or
            l_partkey not in (
                select ps_suppkey from partsupp_nullable
            );
            """
    order_qt_query3_0_before "${query3_0}"
    // markJoinConjuncts should fail
    async_mv_rewrite_fail(db, mv3_0, query3_0, "mv3_0")
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""

    def mv3_1 =
            """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
                select o_custkey from orders_nullable
            )
            or
            l_partkey not in (
                select ps_suppkey from partsupp_nullable
            );
            """
    def query3_1 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
                select o_custkey from orders_nullable
            )
            or
            l_partkey not in (
                select ps_suppkey from partsupp_nullable
            );
            """
    order_qt_query3_1_before "${query3_1}"
    // markJoinConjuncts should fail
    async_mv_rewrite_fail(db, mv3_1, query3_1, "mv3_1")
    order_qt_query3_1_after "${query3_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_1"""


    // multi not in with 'and'
    def mv3_2 =
            """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
                select o_custkey from orders_nullable
            )
            and
            l_partkey not in (
                select ps_suppkey from partsupp_nullable
            );
            """
    def query3_2 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
                select o_custkey from orders_nullable
            )
            and
            l_partkey not in (
                select ps_suppkey from partsupp_nullable
            );
            """
    order_qt_query3_2_before "${query3_2}"
    async_mv_rewrite_success(db, mv3_2, query3_2, "mv3_2")
    order_qt_query3_2_after "${query3_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_2"""


    // not in [null literal]
    def mv4_0 =
            """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
                null, 1, 2
            );
            """
    def query4_0 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
                null, 1, 2
            );
            """
    order_qt_query4_0_before "${query4_0}"
    async_mv_rewrite_success_without_check_chosen(db, mv4_0, query4_0, "mv4_0")
    order_qt_query4_0_after "${query4_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_0"""


    // not in is complex plan, such as join + group by + limit + order by + filter + union all + with
    def mv5_0 =
            """
            select  lineitem.L_LINENUMBER
            from lineitem
            where bin(L_ORDERKEY) not in (
                select bin(o_custkey) from orders_nullable
                left join
                partsupp_nullable on o_orderkey = ps_partkey
                group by
                bin(o_custkey)
            )
            """
    def query5_0 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where bin(L_ORDERKEY) not in (
                select bin(o_custkey) from orders_nullable
                left join
                partsupp_nullable on o_orderkey = ps_partkey
                group by
                bin(o_custkey)
            ) and L_LINENUMBER = 4;
            """
    order_qt_query5_0_before "${query5_0}"
    // because agg under join
    async_mv_rewrite_fail(db, mv5_0, query5_0, "mv5_0")
    order_qt_query5_0_after "${query5_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_0"""


    def mv5_1 =
            """
            select bin(o_custkey) from orders_nullable
                left join
                partsupp_nullable on o_orderkey = ps_partkey
                group by
                bin(o_custkey)
            """
    def query5_1 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where bin(L_ORDERKEY) not in (
                select bin(o_custkey) from orders_nullable
                left join
                partsupp_nullable on o_orderkey = ps_partkey
                group by
                bin(o_custkey)
            ) and L_LINENUMBER = 4;
            """
    order_qt_query5_1_before "${query5_1}"
    async_mv_rewrite_success(db, mv5_1, query5_1, "mv5_1")
    order_qt_query5_1_after "${query5_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_1"""


    def mv5_2 =
            """
                select bin(o_custkey) from orders_nullable
                left join
                partsupp_nullable on o_orderkey = ps_partkey
            """
    def query5_2 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where bin(L_ORDERKEY) not in (
                select bin(o_custkey) from orders_nullable
                left join
                partsupp_nullable on o_orderkey = ps_partkey
            ) and L_LINENUMBER = 4;
            """
    order_qt_query5_2_before "${query5_2}"
    // because agg under join
    async_mv_rewrite_success(db, mv5_2, query5_2, "mv5_2")
    order_qt_query5_2_after "${query5_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_2"""


    def mv5_3 =
            """
            select  lineitem.L_LINENUMBER
            from lineitem
            where bin(L_ORDERKEY) not in (
                select bin(o_custkey) from orders_nullable
                left join
                partsupp_nullable on o_orderkey = ps_partkey
            );
            """
    def query5_3 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where bin(L_ORDERKEY) not in (
                select bin(o_custkey) from orders_nullable
                left join
                partsupp_nullable on o_orderkey = ps_partkey
            ) and L_LINENUMBER = 4;
            """
    order_qt_query5_3_before "${query5_3}"
    async_mv_rewrite_success(db, mv5_3, query5_3, "mv5_3")
    order_qt_query5_3_after "${query5_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_3"""


    def mv5_4 =
            """
            select  lineitem.L_LINENUMBER
            from lineitem
            where bin(L_ORDERKEY) not in (
                select bin(o_custkey) from orders_nullable
                left join 
                partsupp_nullable on o_orderkey = ps_partkey
                where o_custkey > 4
                order by o_custkey
                limit 2
            );
            """
    def query5_4 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where bin(L_ORDERKEY) not in (
                select bin(o_custkey) from orders_nullable
                left join 
                partsupp_nullable on o_orderkey = ps_partkey
                where o_custkey > 4
                order by o_custkey
                limit 2
            ) and L_LINENUMBER = 4;
            """
    order_qt_query5_4_before "${query5_4}"
    async_mv_rewrite_fail(db, mv5_4, query5_4, "mv5_4")
    order_qt_query5_4_after "${query5_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_4"""


    // nest not in
    def mv6_0 =
            """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
                select o_custkey from orders_nullable
                where o_custkey not in (
                    select ps_suppkey from partsupp_nullable
                )
            );
            """
    def query6_0 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            where L_ORDERKEY not in (
                select o_custkey from orders_nullable
                where o_custkey not in (
                    select ps_suppkey from partsupp_nullable
                )
            );
            """
    order_qt_query6_0_before "${query6_0}"
    async_mv_rewrite_success(db, mv6_0, query6_0, "mv6_0")
    order_qt_query6_0_after "${query6_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_0"""


    // with window function
    def mv7_0 =
            """
            select *
            from
            (
            select
            lineitem.L_LINENUMBER,
            sum(L_LINENUMBER) over
            (
            partition by L_ORDERKEY
            order by L_ORDERKEY
            rows between 1 preceding and 1 following
            ) as sum_window
            from lineitem
            ) t
            where 
            sum_window
            not in (
                select bin(o_custkey) from orders_nullable
                left join
                partsupp_nullable on o_orderkey = ps_partkey
            );
            """
    def query7_0 = """
            select *
            from
            (
            select
            lineitem.L_LINENUMBER,
            sum(L_LINENUMBER) over
            (
            partition by L_ORDERKEY
            order by L_ORDERKEY
            rows between 1 preceding and 1 following
            ) as sum_window
            from lineitem
            ) t
            where 
            sum_window
            not in (
                select bin(o_custkey) from orders_nullable
                left join
                partsupp_nullable on o_orderkey = ps_partkey
            );
            """
    order_qt_query7_0_before "${query7_0}"
    async_mv_rewrite_fail(db, mv7_0, query7_0, "mv7_0")
    order_qt_query7_0_after "${query7_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_0"""


    def mv7_1 =
            """
            select bin(o_custkey) from orders_nullable
            where
            o_custkey
            not in (
            select
            sum(L_LINENUMBER) over
            (
            partition by L_ORDERKEY
            order by L_ORDERKEY
            rows between 1 preceding and 1 following
            ) as sum_window
            from lineitem
            );
            """
    def query7_1 = """
            select bin(o_custkey) from orders_nullable
            where
            o_custkey
            not in (
            select
            sum(L_LINENUMBER) over
            (
            partition by L_ORDERKEY
            order by L_ORDERKEY
            rows between 1 preceding and 1 following
            ) as sum_window
            from lineitem
            );
            """
    order_qt_query7_1_before "${query7_1}"
    async_mv_rewrite_fail(db, mv7_1, query7_1, "mv7_1")
    order_qt_query7_1_after "${query7_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_1"""

}
