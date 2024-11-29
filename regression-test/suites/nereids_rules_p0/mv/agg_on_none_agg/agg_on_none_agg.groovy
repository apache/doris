package mv.agg_on_none_agg
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

suite("agg_on_none_agg") {
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

    sql """alter table orders modify column O_COMMENT set stats ('row_count'='8');"""

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

    sql """alter table lineitem modify column l_comment set stats ('row_count'='5');"""

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

    sql """alter table partsupp modify column ps_comment set stats ('row_count'='2');"""

    sql """analyze table orders with sync;"""
    sql """analyze table lineitem with sync;"""
    sql """analyze table partsupp with sync;"""

    // query used expression is in mv
    def mv1_0 = """
             select case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end,
             o_orderstatus,
             bin(o_orderkey)
             from orders;
             """
    def query1_0 = """
             select
             count(case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end),
             o_orderstatus,
             bin(o_orderkey)
             from orders
             group by
             o_orderstatus,
             bin(o_orderkey);
            """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_success_without_check_chosen(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""


    def mv1_1 = """
             select case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end,
             o_orderstatus,
             o_orderkey + 1
             from orders;
             """
    def query1_1 = """
             select
             count(case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end),
             o_orderstatus,
             exp(bin(o_orderkey + 1))
             from orders
             group by
             o_orderstatus,
             exp(bin(o_orderkey + 1));
            """
    order_qt_query1_1_before "${query1_1}"
    async_mv_rewrite_success(db, mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""

    def mv1_3 = """
             select case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end,
             o_orderstatus,
             o_orderkey + 1,
             l_linenumber
             from orders
             left join lineitem on o_orderkey =  l_orderkey;
             """
    def query1_3 = """
             select
             count(case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end),
             o_orderstatus,
             exp(bin(o_orderkey + 1)),
             l_linenumber
             from orders
             left join lineitem on o_orderkey =  l_orderkey
             group by
             l_linenumber,
             o_orderstatus,
             exp(bin(o_orderkey + 1));
            """
    order_qt_query1_3_before "${query1_3}"
    async_mv_rewrite_success(db, mv1_3, query1_3, "mv1_3")
    order_qt_query1_3_after "${query1_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3"""

    // query use expression is not in mv
    def mv2_0 = """
             select case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end,
             o_orderstatus,
             bin(o_orderkey),
             l_linenumber
             from orders
             left join lineitem on o_orderkey =  l_orderkey;
             """
    def query2_0 = """
             select
             count(case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end),
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey)
             from orders
             group by
             o_orderstatus,
             bin(o_orderkey),
             o_shippriority;
            """
    order_qt_query2_0_before "${query2_0}"
    async_mv_rewrite_fail(db, mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    def mv2_1 = """
             select case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end,
             o_orderstatus,
             bin(o_orderkey)
             from orders;
             """
    def query2_1 = """
             select
             count(case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end),
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey),
             l_suppkey
             from orders
             left join lineitem on o_orderkey =  l_orderkey
             group by
             o_orderstatus,
             bin(o_orderkey),
             o_shippriority,
             l_suppkey;
            """
    order_qt_query2_1_before "${query2_1}"
    async_mv_rewrite_fail(db, mv2_1, query2_1, "mv2_1")
    order_qt_query2_1_after "${query2_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_1"""


    // query use filter and view doesn't use filter
    def mv3_0 = """
             select case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end,
             o_orderstatus,
             bin(o_orderkey)
             from orders;
             """
    def query3_0 = """
             select
             count(case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end),
             o_orderstatus,
             bin(o_orderkey)
             from orders
             where o_orderstatus = 'o'
             group by
             o_orderstatus,
             bin(o_orderkey);
            """
    order_qt_query3_0_before "${query3_0}"
    async_mv_rewrite_success_without_check_chosen(db, mv3_0, query3_0, "mv3_0")
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""


    def mv3_1 = """
             select case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end,
             o_orderstatus,
             bin(o_orderkey)
             from orders;
             """
    def query3_1 = """
             select
             count(case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end),
             o_orderstatus,
             bin(o_orderkey)
             from orders
             where o_shippriority = 2
             group by
             o_orderstatus,
             bin(o_orderkey);
            """
    order_qt_query3_1_before "${query3_1}"
    // the filter slot used in query can not be found from mv
    async_mv_rewrite_fail(db, mv3_1, query3_1, "mv3_1")
    order_qt_query3_1_after "${query3_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_1"""


    def mv3_2 = """
             select case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end,
             o_orderstatus,
             bin(o_orderkey)
             from orders;
             """
    def query3_2 = """
             select
             count(case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end) as count_case,
             o_orderstatus,
             bin(o_orderkey)
             from orders
             where o_shippriority = 2
             group by
             o_orderstatus,
             bin(o_orderkey)
             having count_case > 0;
            """
    order_qt_query3_2_before "${query3_2}"
    // the filter slot used in query can not be found from mv
    async_mv_rewrite_fail(db, mv3_2, query3_2, "mv3_2")
    order_qt_query3_2_after "${query3_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_2"""


    def mv3_3 = """
             select case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end,
             o_orderstatus,
             bin(o_orderkey),
             l_suppkey,
             l_linenumber
             from orders
             left join lineitem on o_orderkey =  l_orderkey;
             """
    def query3_3 = """
             select
             count(case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end) as count_case,
             o_orderstatus,
             bin(o_orderkey)
             from orders
             left join lineitem on o_orderkey =  l_orderkey
             where l_linenumber = 4
             group by
             o_orderstatus,
             bin(o_orderkey);
            """
    order_qt_query3_3_before "${query3_3}"
    // the filter slot used in query can not be found from mv
    async_mv_rewrite_success(db, mv3_3, query3_3, "mv3_3")
    order_qt_query3_3_after "${query3_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_3"""



    // both query and view use filter
    def mv4_0 = """
             select case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end,
             o_orderstatus,
             bin(o_orderkey)
             from orders
             where o_shippriority = 2;
             """
    def query4_0 = """
             select
             count(case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end),
             o_orderstatus,
             bin(o_orderkey)
             from orders
             where o_shippriority = 2 and o_orderstatus = 'o'
             group by
             o_orderstatus,
             bin(o_orderkey);
            """
    order_qt_query4_0_before "${query4_0}"
    async_mv_rewrite_success(db, mv4_0, query4_0, "mv4_0")
    order_qt_query4_0_after "${query4_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_0"""


    def mv4_1 = """
             select case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end,
             o_orderstatus,
             bin(o_orderkey),
             l_partkey
             from orders
             left join lineitem on o_orderkey =  l_orderkey
             where o_shippriority = 2 and l_linenumber = 4;
             """
    def query4_1 = """
             select
             count(case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end),
             o_orderstatus,
             bin(o_orderkey),
             l_partkey
             from orders
             left join lineitem on o_orderkey =  l_orderkey
             where o_shippriority = 2 and o_orderstatus = 'o' and l_linenumber = 4
             group by
             o_orderstatus,
             bin(o_orderkey),
             l_partkey;
            """
    order_qt_query4_1_before "${query4_1}"
    async_mv_rewrite_success(db, mv4_1, query4_1, "mv4_1")
    order_qt_query4_1_after "${query4_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_1"""

    // query use less dimension in select then group by

    def mv5_0 = """
             select case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end,
             o_orderstatus,
             o_totalprice,
             bin(o_orderkey)
             from orders;
             """
    def query5_0 = """
             select
             count(case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end),
             sum(o_totalprice),
             bin(o_orderkey)
             from orders
             group by
             o_orderstatus,
             bin(o_orderkey);
            """
    order_qt_query5_0_before "${query5_0}"
    async_mv_rewrite_success_without_check_chosen(db, mv5_0, query5_0, "mv5_0")
    order_qt_query5_0_after "${query5_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_0"""



    def mv5_1 = """
             select case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end,
             o_orderstatus,
             o_totalprice,
             bin(o_orderkey),
             l_linenumber
             from orders
             left join lineitem on o_orderkey =  l_orderkey;
             """
    def query5_1 = """
             select
             count(case when o_shippriority > 1 and o_orderkey IN (4, 5) then o_custkey else o_shippriority end),
             sum(o_totalprice),
             bin(o_orderkey)
             from orders
             left join lineitem on o_orderkey =  l_orderkey
             group by
             o_orderstatus,
             bin(o_orderkey),
             l_linenumber;
            """
    order_qt_query5_1_before "${query5_1}"
    async_mv_rewrite_success(db, mv5_1, query5_1, "mv5_1")
    order_qt_query5_1_after "${query5_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_1"""
}
