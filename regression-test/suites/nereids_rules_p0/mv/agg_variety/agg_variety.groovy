package mv.agg_variety
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

suite("agg_variety") {
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
    PARTITION BY RANGE(o_orderdate) (
    PARTITION `day_2` VALUES LESS THAN ('2023-12-9'),
    PARTITION `day_3` VALUES LESS THAN ("2023-12-11"),
    PARTITION `day_4` VALUES LESS THAN ("2023-12-30")
    )
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
    PARTITION BY RANGE(l_shipdate) (
    PARTITION `day_1` VALUES LESS THAN ('2023-12-9'),
    PARTITION `day_2` VALUES LESS THAN ("2023-12-11"),
    PARTITION `day_3` VALUES LESS THAN ("2023-12-30"))
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

    sql """analyze table orders with sync;"""
    sql """analyze table lineitem with sync;"""
    sql """analyze table partsupp with sync;"""

    def check_rewrite_but_not_chose = { mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """

        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        explain {
            sql("${query_sql}")
            check {result ->
                def splitResult = result.split("MaterializedViewRewriteFail")
                splitResult.length == 2 ? splitResult[0].contains(mv_name) : false
            }
        }
    }

    // query dimension is less then mv
    def mv1_0 = """
             select
             count(o_totalprice),
             o_shippriority,
             o_orderstatus,
             bin(o_orderkey)
             from orders
             group by
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
             """
    def query1_0 = """
             select 
             count(o_totalprice),
             max(distinct o_shippriority),
             min(distinct o_shippriority),
             avg(distinct o_shippriority),
             sum(distinct o_shippriority) / count(distinct o_shippriority)
             o_orderstatus,
             bin(o_orderkey)
             from orders
             group by
             o_orderstatus,
             bin(o_orderkey);
            """
    order_qt_query1_0_before "${query1_0}"
    check_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""

    def mv1_1 = """
             select
             o_shippriority,
             o_orderstatus,
             bin(o_orderkey)
             from orders
             group by
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
             """
    def query1_1 = """
             select 
             count(o_shippriority),
             max(distinct o_shippriority),
             min(distinct o_shippriority),
             avg(distinct o_shippriority),
             sum(distinct o_shippriority) / count(distinct o_shippriority),
             o_orderstatus,
             bin(o_orderkey)
             from orders
             group by
             o_orderstatus,
             bin(o_orderkey);
            """
    order_qt_query1_1_before "${query1_1}"
    // contains aggreagate function count with out distinct which is not supported, should fail
    check_mv_rewrite_fail(db, mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""


    def mv1_2 = """
             select
             count(o_totalprice),
             o_orderkey,
             o_custkey,
             o_shippriority,
             bin(o_orderkey)
             from orders
             group by
             o_orderkey,
             o_custkey,
             o_shippriority,
             bin(o_orderkey);
             """
    def query1_2 = """
             select 
             count(o_totalprice),
             max(distinct o_custkey + o_shippriority),
             min(distinct o_custkey + o_shippriority),
             avg(distinct o_custkey + o_shippriority),
             sum(distinct o_custkey + o_shippriority) / count(distinct o_custkey + o_shippriority)
             o_custkey,
             o_shippriority
             from orders
             group by
             o_custkey,
             o_shippriority,
             bin(o_orderkey);
            """
    order_qt_query1_2_before "${query1_2}"
    // test the arguments in aggregate function is complex, should success
    check_mv_rewrite_success(db, mv1_2, query1_2, "mv1_2")
    order_qt_query1_2_after "${query1_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_2"""



    def mv1_3 = """
             select
             count(o_totalprice),
             o_custkey,
             o_shippriority,
             bin(o_orderkey)
             from orders
             group by
             o_custkey,
             o_shippriority,
             bin(o_orderkey);
             """
    def query1_3 = """
             select 
             count(o_totalprice),
             max(distinct o_orderkey + o_shippriority),
             min(distinct o_orderkey + o_shippriority),
             avg(distinct o_custkey + o_shippriority),
             sum(distinct o_custkey + o_shippriority) / count(distinct o_custkey + o_shippriority)
             o_shippriority,
             bin(o_orderkey)
             from orders
             group by
             o_shippriority,
             bin(o_orderkey);
            """
    order_qt_query1_3_before "${query1_3}"
    // function use the dimension which is not in mv output, should fail
    check_mv_rewrite_fail(db, mv1_3, query1_3, "mv1_3")
    order_qt_query1_3_after "${query1_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3"""


    // query dimension is equals with mv
    def mv2_0 = """
             select
             count(o_totalprice),
             o_shippriority,
             o_orderstatus,
             bin(o_orderkey)
             from orders
             group by
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
             """
    def query2_0 = """
             select 
             count(o_totalprice),
             max(distinct o_shippriority),
             min(distinct o_shippriority),
             avg(distinct o_shippriority),
             sum(distinct o_shippriority) / count(distinct o_shippriority),
             o_shippriority,
             o_orderstatus,
             bin(o_orderkey)
             from orders
             group by
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
            """
    order_qt_query2_0_before "${query2_0}"
    check_mv_rewrite_success(db, mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    def mv2_1 = """
             select
             count(o_totalprice),
             o_shippriority,
             o_orderstatus,
             bin(o_orderkey)
             from orders
             group by
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
             """
    // query use less dimension then group by dimension
    def query2_1 = """
             select 
             count(o_totalprice),
             max(distinct o_shippriority),
             min(distinct o_shippriority),
             avg(distinct o_shippriority),
             sum(distinct o_shippriority) / count(distinct o_shippriority),
             bin(o_orderkey)
             from orders
             group by
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
            """
    order_qt_query2_1_before "${query2_1}"
    check_mv_rewrite_success(db, mv2_1, query2_1, "mv2_1")
    order_qt_query2_1_after "${query2_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_1"""


    def mv2_2 = """
             select
             count(o_totalprice),
             o_shippriority,
             o_orderstatus,
             bin(o_orderkey)
             from orders
             group by
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
             """
    def query2_2 = """
             select 
             count(o_shippriority),
             max(distinct o_shippriority),
             min(distinct o_shippriority),
             avg(distinct o_shippriority),
             sum(distinct o_shippriority) / count(distinct o_shippriority),
             bin(o_orderkey)
             from orders
             group by
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
            """
    order_qt_query2_2_before "${query2_2}"
    // contains aggreagate function count which is not supported, should fail
    check_mv_rewrite_fail(db, mv2_2, query2_2, "mv2_2")
    order_qt_query2_2_after "${query2_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_2"""


    def mv2_3 = """
             select
             count(o_totalprice),
             o_shippriority,
             o_orderstatus,
             bin(o_orderkey),
             o_orderkey
             from orders
             group by
             o_orderkey,
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
             """
    def query2_3 = """
             select 
             count(o_totalprice),
             max(distinct o_shippriority + o_orderkey),
             min(distinct o_shippriority + o_orderkey),
             avg(distinct o_shippriority),
             sum(distinct o_shippriority) / count(distinct o_shippriority),
             o_orderkey,
             o_orderstatus,
             bin(o_orderkey)
             from orders
             group by
             o_orderkey,
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
            """
    order_qt_query2_3_before "${query2_3}"
    // aggregate function use complex expression, should success
    check_mv_rewrite_success(db, mv2_3, query2_3, "mv2_3")
    order_qt_query2_3_after "${query2_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_3"""


    def mv2_4 = """
             select
             count(o_totalprice),
             o_shippriority,
             o_orderstatus,
             bin(o_orderkey)
             from orders
             group by
             o_orderkey,
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
             """
    // query use less dimension then group by dimension
    def query2_4 = """
             select 
             count(o_totalprice),
             max(distinct o_shippriority + o_orderkey),
             min(distinct o_shippriority + o_orderkey),
             avg(distinct o_shippriority),
             sum(distinct o_shippriority) / count(distinct o_shippriority),
             o_orderkey,
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey)
             from orders
             group by
             o_orderkey,
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
            """
    order_qt_query2_4_before "${query2_4}"
    // function use the dimension which is not in mv output, should fail
    check_mv_rewrite_fail(db, mv2_4, query2_4, "mv2_4")
    order_qt_query2_4_after "${query2_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_4"""


    def mv2_5 = """
             select
             count(o_totalprice),
             o_shippriority,
             o_orderstatus,
             bin(o_orderkey),
             o_orderkey
             from orders
             group by
             o_orderkey,
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
             """
    // query select use the same dimension with group by
    def query2_5 = """
             select 
             count(o_totalprice),
             max(distinct o_shippriority + o_orderkey),
             min(distinct o_shippriority + o_orderkey),
             avg(distinct o_shippriority),
             sum(distinct o_shippriority) / count(distinct o_shippriority),
             o_orderkey,
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey)
             from orders
             group by
             o_orderkey,
             o_orderstatus,
             o_shippriority,
             bin(o_orderkey);
            """
    order_qt_query2_5_before "${query2_5}"
    // aggregate function use complex expression, should success
    check_mv_rewrite_success(db, mv2_5, query2_5, "mv2_5")
    order_qt_query2_5_after "${query2_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_5"""
}
