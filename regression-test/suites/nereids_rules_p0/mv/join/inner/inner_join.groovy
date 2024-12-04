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

suite("inner_join") {
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
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 33.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 1.2, '2023-12-12', 'c','d',2, 'mi');  
    """

    sql """
    insert into partsupp values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    sql """analyze table lineitem with sync;"""
    sql """analyze table orders with sync;"""
    sql """analyze table partsupp with sync;"""

    sql """alter table lineitem modify column l_comment set stats ('row_count'='5');"""
    sql """alter table orders modify column o_comment set stats ('row_count'='8');"""
    sql """alter table partsupp modify column ps_comment set stats ('row_count'='2');"""

    // without filter
    def mv1_0 =
            """
            select  lineitem.L_LINENUMBER, orders.O_CUSTKEY
            from lineitem
            inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY
            """
    def query1_0 = """
            select lineitem.L_LINENUMBER
            from lineitem
            inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY
            """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""


    def mv1_1 = "select  lineitem.L_LINENUMBER, orders.O_CUSTKEY, partsupp.PS_AVAILQTY " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "inner join partsupp on lineitem.L_PARTKEY = partsupp.PS_PARTKEY " +
            "and lineitem.L_SUPPKEY = partsupp.PS_SUPPKEY"
    def query1_1 = "select  lineitem.L_LINENUMBER " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "inner join partsupp on lineitem.L_PARTKEY = partsupp.PS_PARTKEY " +
            "and lineitem.L_SUPPKEY = partsupp.PS_SUPPKEY"
    order_qt_query1_1_before "${query1_1}"
    async_mv_rewrite_success(db, mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""

    def mv1_2 = "select  lineitem.L_LINENUMBER, orders.O_CUSTKEY " +
            "from orders " +
            "inner join lineitem on lineitem.L_ORDERKEY = orders.O_ORDERKEY "
    def query1_2 = "select lineitem.L_LINENUMBER " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY "
    order_qt_query1_2_before "${query1_2}"
    // join direction is not same, should not match
    async_mv_rewrite_success(db, mv1_2, query1_2, "mv1_2")
    order_qt_query1_2_after "${query1_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_2"""

    // select with complex expression
    def mv1_3 = "select l_linenumber, o_custkey " +
            "from orders " +
            "inner join lineitem on lineitem.L_ORDERKEY = orders.O_ORDERKEY "
    def query1_3 = "select IFNULL(orders.O_CUSTKEY, 0) as custkey_not_null, " +
            "case when l_linenumber in (1,2,3) then l_linenumber else o_custkey end as case_when  " +
            "from orders " +
            "inner join lineitem on orders.O_ORDERKEY = lineitem.L_ORDERKEY"
    order_qt_query1_3_before "${query1_3}"
    async_mv_rewrite_success(db, mv1_3, query1_3, "mv1_3")
    order_qt_query1_3_after "${query1_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3"""

    def mv1_4 = """
        select  lineitem.L_LINENUMBER, orders.O_CUSTKEY, partsupp.PS_AVAILQTY
        from lineitem
        inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY
        inner join partsupp on lineitem.L_PARTKEY = partsupp.PS_PARTKEY
        and lineitem.L_SUPPKEY = partsupp.PS_SUPPKEY;
    """
    def query1_4 = """
        select  lineitem.L_LINENUMBER
        from lineitem
        inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY
        inner join partsupp on lineitem.L_PARTKEY = partsupp.PS_PARTKEY
        and lineitem.L_SUPPKEY = partsupp.PS_SUPPKEY;
    """
    order_qt_query1_4_before "${query1_4}"
    async_mv_rewrite_success(db, mv1_4, query1_4, "mv1_4")
    order_qt_query1_4_after "${query1_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_4"""

    def mv1_5 = """
            select  lineitem.L_LINENUMBER, orders.O_CUSTKEY, l_partkey, o_shippriority
            from lineitem
            inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY;
            """
    def query1_5 = """
            select  lineitem.L_LINENUMBER
            from lineitem
            inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY
            and o_shippriority = l_partkey;
            """
    order_qt_query1_5_before "${query1_5}"
    async_mv_rewrite_success(db, mv1_5, query1_5, "mv1_5")
    order_qt_query1_5_after "${query1_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_5"""

    // filter outside + left
    def mv2_0 = "select  lineitem.L_LINENUMBER, orders.O_CUSTKEY " +
            "from orders " +
            "inner join lineitem on lineitem.L_ORDERKEY = orders.O_ORDERKEY "
    def query2_0 = "select lineitem.L_LINENUMBER " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where lineitem.L_LINENUMBER > 0"
    order_qt_query2_0_before "${query2_0}"
    async_mv_rewrite_success(db, mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    def mv2_1 = "select  t1.L_LINENUMBER, orders.O_CUSTKEY " +
            "from (select * from lineitem where L_LINENUMBER > 1) t1 " +
            "inner join orders on t1.L_ORDERKEY = orders.O_ORDERKEY "
    def query2_1 = "select lineitem.L_LINENUMBER " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where lineitem.L_LINENUMBER > 1"
    order_qt_query2_1_before "${query2_1}"
    async_mv_rewrite_success(db, mv2_1, query2_1, "mv2_1")
    order_qt_query2_1_after "${query2_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_1"""


    def mv2_2 = "select  t1.L_LINENUMBER, orders.O_CUSTKEY, l_suppkey " +
            "from (select * from lineitem where L_LINENUMBER > 1) t1 " +
            "inner join orders on t1.L_ORDERKEY = orders.O_ORDERKEY "
    def query2_2 = "select lineitem.L_LINENUMBER " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where lineitem.L_LINENUMBER > 1 and l_suppkey = 3"
    order_qt_query2_2_before "${query2_2}"
    async_mv_rewrite_success(db, mv2_2, query2_2, "mv2_2")
    order_qt_query2_2_after "${query2_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_2"""


    def mv2_3 = "select  lineitem.L_LINENUMBER, orders.O_CUSTKEY, partsupp.PS_AVAILQTY, l_suppkey " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "inner join partsupp on lineitem.L_PARTKEY = partsupp.PS_PARTKEY " +
            "and lineitem.L_SUPPKEY = partsupp.PS_SUPPKEY"
    def query2_3= "select  lineitem.L_LINENUMBER " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "inner join partsupp on lineitem.L_PARTKEY = partsupp.PS_PARTKEY " +
            "and lineitem.L_SUPPKEY = partsupp.PS_SUPPKEY " +
            "where lineitem.L_LINENUMBER > 1 and l_suppkey = 3 "
    order_qt_query2_3_before "${query2_3}"
    async_mv_rewrite_success(db, mv2_3, query2_3, "mv2_3")
    order_qt_query2_3_after "${query2_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_3"""


    // filter outside + right
    def mv3_0 = "select  lineitem.L_LINENUMBER, orders.O_CUSTKEY " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY "
    def query3_0 = "select lineitem.L_LINENUMBER " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where orders.O_ORDERSTATUS = 'o'"
    order_qt_query3_0_before "${query3_0}"
    // use a filed not from mv, should not success
    async_mv_rewrite_fail(db, mv3_0, query3_0, "mv3_0")
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""


    def mv3_1 = "select lineitem.L_LINENUMBER, orders.O_CUSTKEY, orders.O_ORDERSTATUS " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY "
    def query3_1 = "select lineitem.L_LINENUMBER " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where orders.O_ORDERSTATUS = 'o'"
    order_qt_query3_1_before "${query3_1}"
    async_mv_rewrite_success(db, mv3_1, query3_1, "mv3_1")
    order_qt_query3_1_after "${query3_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_1"""


    def mv3_2 = "select lineitem.L_LINENUMBER, t2.O_CUSTKEY, t2.O_ORDERSTATUS " +
            "from lineitem " +
            "inner join " +
            "(select * from orders where O_ORDERSTATUS = 'o') t2 " +
            "on lineitem.L_ORDERKEY = t2.O_ORDERKEY "
    def query3_2 = "select lineitem.L_LINENUMBER " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where orders.O_ORDERSTATUS = 'o'"
    order_qt_query3_2_before "${query3_2}"
    async_mv_rewrite_success(db, mv3_2, query3_2, "mv3_2")
    order_qt_query3_2_after "${query3_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_2"""


    def mv3_3 = "select  lineitem.L_LINENUMBER, orders.O_CUSTKEY, partsupp.PS_AVAILQTY " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "inner join partsupp on lineitem.L_PARTKEY = partsupp.PS_PARTKEY " +
            "and lineitem.L_SUPPKEY = partsupp.PS_SUPPKEY"
    def query3_3= "select  lineitem.L_LINENUMBER " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "inner join partsupp on lineitem.L_PARTKEY = partsupp.PS_PARTKEY " +
            "and lineitem.L_SUPPKEY = partsupp.PS_SUPPKEY " +
            "where o_custkey in (1, 2, 3, 4) "
    order_qt_query3_3_before "${query3_3}"
    async_mv_rewrite_success(db, mv3_3, query3_3, "mv3_3")
    order_qt_query3_3_after "${query3_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_3"""

    // join derive, the mv is outer join with filter and query is inner join
    // the predicate should be ComparisonPredicate
    def mv3_4 = """
            select l_linenumber, o_custkey
            from orders
            left join lineitem on lineitem.L_ORDERKEY = orders.O_ORDERKEY
            where o_custkey = 1;
            """
    def query3_4 = """
            select IFNULL(orders.O_CUSTKEY, 0) as custkey_not_null,
            case when l_linenumber in (1,2,3) then l_linenumber else o_custkey end as case_when
            from orders
            inner join lineitem on orders.O_ORDERKEY = lineitem.L_ORDERKEY
            where o_custkey = 1 and l_linenumber > 0;
            """
    order_qt_query3_4_before "${query3_4}"
    async_mv_rewrite_success(db, mv3_4, query3_4, "mv3_4")
    order_qt_query3_4_after "${query3_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_4"""


    // filter outside + left + right
    def mv4_0 =
            """
            select l_linenumber, o_custkey, o_orderkey, o_orderstatus
            from lineitem
            inner join orders on lineitem.l_orderkey = orders.o_orderkey
            """
    def query4_0 =
            """
            select lineitem.l_linenumber
            from lineitem
            inner join orders on lineitem.l_orderkey = orders.o_orderkey
            where o_orderstatus = 'o' AND l_linenumber in (1, 2, 3, 4, 5)
            """
    order_qt_query4_0_before "${query4_0}"
    async_mv_rewrite_success(db, mv4_0, query4_0, "mv4_0")
    order_qt_query4_0_after "${query4_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_0"""


    // filter inside + left
    def mv5_0 = "select lineitem.l_linenumber, orders.o_custkey " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where lineitem.L_LINENUMBER > 1"
    def query5_0 = "select t1.L_LINENUMBER " +
            "from (select * from lineitem where l_linenumber > 1) t1 " +
            "inner join orders on t1.l_orderkey = orders.O_ORDERKEY "
    order_qt_query5_0_before "${query5_0}"
    async_mv_rewrite_success(db, mv5_0, query5_0, "mv5_0")
    order_qt_query5_0_after "${query5_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_0"""


    // filter inside + right
    def mv6_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey " +
            "from lineitem " +
            "inner join (select * from orders where o_orderdate = '2023-12-08') t2 " +
            "on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate "
    def query6_0 = "select l_partkey, l_suppkey, l_shipdate " +
            "from lineitem t1 " +
            "inner join (select * from orders where o_orderdate = '2023-12-08') t2 " +
            "on t1.l_orderkey = o_orderkey and t1.l_shipdate = o_orderdate "
    order_qt_query6_0_before "${query6_0}"
    async_mv_rewrite_success(db, mv6_0, query6_0, "mv6_0")
    order_qt_query6_0_after "${query6_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_0"""


    // filter inside + left + right
    def mv7_0 = "select l_shipdate, o_orderdate, l_partkey, l_suppkey " +
            "from lineitem " +
            "inner join (select * from orders where o_orderdate = '2023-12-08') t2 " +
            "on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate "
    def query7_0 = "select l_partkey, l_suppkey, l_shipdate " +
            "from (select l_shipdate, l_orderkey, l_partkey, l_suppkey " +
            "from lineitem where l_partkey in (2, 3, 4)) t1  " +
            "inner join (select * from orders where o_orderdate = '2023-12-08') t2 " +
            "on t1.l_orderkey = o_orderkey and t1.l_shipdate = o_orderdate " +
            "where l_partkey = 2"
    order_qt_query7_0_before "${query7_0}"
    async_mv_rewrite_success(db, mv7_0, query7_0, "mv7_0")
    order_qt_query7_0_after "${query7_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_0"""


    // check not match, because use a filed orders.O_SHIPPRIORITY which not in mv
    def mv10_0 = "select lineitem.L_LINENUMBER, orders.O_CUSTKEY " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY"
    def query10_0 = "select orders.O_CUSTKEY " +
            "from orders " +
            "inner join lineitem on orders.O_ORDERKEY = lineitem.L_ORDERKEY " +
            "WHERE lineitem.L_LINENUMBER > 0 AND orders.O_CUSTKEY = 1 AND " +
            "orders.O_SHIPPRIORITY = 2"
    order_qt_query10_0_before "${query10_0}"
    async_mv_rewrite_fail(db, mv10_0, query10_0, "mv10_0")
    order_qt_query10_0_after "${query10_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv10_0"""
}
