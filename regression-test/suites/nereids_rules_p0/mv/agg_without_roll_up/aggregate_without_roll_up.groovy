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

suite("aggregate_without_roll_up") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET enable_agg_state = true"

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
    PARTITION `day_2` VALUES LESS THAN ('2023-12-9'),
    PARTITION `day_3` VALUES LESS THAN ("2023-12-11"),
    PARTITION `day_4` VALUES LESS THAN ("2023-12-30")
    )
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

    sql """
    insert into lineitem values
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

    // single table
    // with filter
    def mv1_0 = "select o_shippriority, o_comment, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end), " +
            "count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2 " +
            "from orders " +
            "group by " +
            "o_shippriority, " +
            "o_comment "
    def query1_0 = "select o_shippriority, o_comment, " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*) " +
            "from orders " +
            "where o_shippriority in (1, 2)" +
            "group by " +
            "o_shippriority, " +
            "o_comment "
     order_qt_query1_0_before "${query1_0}"
     check_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
     order_qt_query1_0_after "${query1_0}"
     sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""


    def mv1_1 = "select O_SHIPPRIORITY, O_COMMENT, O_ORDERDATE, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end) as filter_cnt_3, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (5, 6) then O_ORDERSTATUS else null end) as filter_cnt_4, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (7, 9) then O_ORDERSTATUS else null end) as filter_cnt_6, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (8, 10) then O_ORDERSTATUS else null end) as filter_cnt_7, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (11, 13) then O_ORDERSTATUS else null end) as filter_cnt_8, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (12, 11) then O_ORDERSTATUS else null end) as filter_cnt_9, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 15) then O_ORDERSTATUS else null end) as filter_cnt_10, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (11, 12) then O_ORDERSTATUS else null end) as filter_cnt_11, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (20, 3) then O_ORDERSTATUS else null end) as filter_cnt_14, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 22) then O_ORDERSTATUS else null end) as filter_cnt_17, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 25) then O_ORDERSTATUS else null end) as filter_cnt_18, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (19, 3) then O_ORDERSTATUS else null end) as filter_cnt_19, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 20) then O_ORDERSTATUS else null end) as filter_cnt_20 " +
            "from orders " +
            "where O_ORDERDATE < '2023-12-30'" +
            "group by " +
            "O_ORDERDATE, " +
            "O_SHIPPRIORITY, " +
            "O_COMMENT "
    def query1_1 = "select O_ORDERDATE, O_SHIPPRIORITY, O_COMMENT, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end) as filter_cnt_3, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (7, 9) then O_ORDERSTATUS else null end) as filter_cnt_6, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (11, 13) then O_ORDERSTATUS else null end) as filter_cnt_8, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (12, 11) then O_ORDERSTATUS else null end) as filter_cnt_9, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (11, 12) then O_ORDERSTATUS else null end) as filter_cnt_11, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 25) then O_ORDERSTATUS else null end) as filter_cnt_18, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (19, 3) then O_ORDERSTATUS else null end) as filter_cnt_19, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 20) then O_ORDERSTATUS else null end) as filter_cnt_20 " +
            "from orders " +
            "where O_ORDERDATE < '2023-12-30' and O_ORDERDATE > '2023-12-01'" +
            "group by " +
            "O_ORDERDATE, " +
            "O_SHIPPRIORITY, " +
            "O_COMMENT "
    order_qt_query1_1_before "${query1_1}"
    check_mv_rewrite_success(db, mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""


    def mv1_2 = "select O_SHIPPRIORITY, O_COMMENT, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end) as filter_cnt_3, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (5, 6) then O_ORDERSTATUS else null end) as filter_cnt_4, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (7, 9) then O_ORDERSTATUS else null end) as filter_cnt_6, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (8, 10) then O_ORDERSTATUS else null end) as filter_cnt_7, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (11, 13) then O_ORDERSTATUS else null end) as filter_cnt_8, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (12, 11) then O_ORDERSTATUS else null end) as filter_cnt_9, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 15) then O_ORDERSTATUS else null end) as filter_cnt_10, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (11, 12) then O_ORDERSTATUS else null end) as filter_cnt_11, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (20, 3) then O_ORDERSTATUS else null end) as filter_cnt_14, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 22) then O_ORDERSTATUS else null end) as filter_cnt_17, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 25) then O_ORDERSTATUS else null end) as filter_cnt_18, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (19, 3) then O_ORDERSTATUS else null end) as filter_cnt_19, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 20) then O_ORDERSTATUS else null end) as filter_cnt_20 " +
            "from orders " +
            "where O_ORDERDATE < '2023-12-30' and O_ORDERDATE > '2023-12-01'" +
            "group by " +
            "O_SHIPPRIORITY, " +
            "O_COMMENT "
    def query1_2 = "select O_SHIPPRIORITY, O_COMMENT, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end) as filter_cnt_3, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (7, 9) then O_ORDERSTATUS else null end) as filter_cnt_6, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (11, 13) then O_ORDERSTATUS else null end) as filter_cnt_8, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (12, 11) then O_ORDERSTATUS else null end) as filter_cnt_9, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (11, 12) then O_ORDERSTATUS else null end) as filter_cnt_11, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 25) then O_ORDERSTATUS else null end) as filter_cnt_18, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (19, 3) then O_ORDERSTATUS else null end) as filter_cnt_19, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 20) then O_ORDERSTATUS else null end) as filter_cnt_20 " +
            "from orders " +
            "where O_ORDERDATE < '2023-12-30' and O_ORDERDATE > '2023-12-01'" +
            "group by " +
            "O_SHIPPRIORITY, " +
            "O_COMMENT "
    order_qt_query1_2_before "${query1_2}"
    check_mv_rewrite_success(db, mv1_2, query1_2, "mv1_2")
    order_qt_query1_2_after "${query1_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_2"""

    // without filter
    def mv2_0 = "select O_SHIPPRIORITY, O_COMMENT, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end) as filter_cnt_3, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (5, 6) then O_ORDERSTATUS else null end) as filter_cnt_4, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (7, 9) then O_ORDERSTATUS else null end) as filter_cnt_6, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (8, 10) then O_ORDERSTATUS else null end) as filter_cnt_7, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (11, 13) then O_ORDERSTATUS else null end) as filter_cnt_8, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (12, 11) then O_ORDERSTATUS else null end) as filter_cnt_9, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 15) then O_ORDERSTATUS else null end) as filter_cnt_10, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (11, 12) then O_ORDERSTATUS else null end) as filter_cnt_11, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (20, 3) then O_ORDERSTATUS else null end) as filter_cnt_14, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 22) then O_ORDERSTATUS else null end) as filter_cnt_17, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 25) then O_ORDERSTATUS else null end) as filter_cnt_18, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (19, 3) then O_ORDERSTATUS else null end) as filter_cnt_19, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 20) then O_ORDERSTATUS else null end) as filter_cnt_20 " +
            "from orders " +
            "group by " +
            "O_SHIPPRIORITY, " +
            "O_COMMENT "
    def query2_0 = "select O_SHIPPRIORITY, O_COMMENT, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end) as filter_cnt_3, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (12, 11) then O_ORDERSTATUS else null end) as filter_cnt_9, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (11, 12) then O_ORDERSTATUS else null end) as filter_cnt_11, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (19, 3) then O_ORDERSTATUS else null end) as filter_cnt_19, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 20) then O_ORDERSTATUS else null end) as filter_cnt_20 " +
            "from orders " +
            "group by " +
            "O_SHIPPRIORITY, " +
            "O_COMMENT "
    order_qt_query2_0_before "${query2_0}"
    check_mv_rewrite_success(db, mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    // without group, scalar aggregate
    def mv3_0 = "select count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end), " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end), " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (5, 6) then O_ORDERSTATUS else null end) as filter_cnt_4, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end), " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (7, 9) then O_ORDERSTATUS else null end), " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (8, 10) then O_ORDERSTATUS else null end) as filter_cnt_7, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (11, 13) then O_ORDERSTATUS else null end) as filter_cnt_8, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (12, 11) then O_ORDERSTATUS else null end) as filter_cnt_9, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 15) then O_ORDERSTATUS else null end) as filter_cnt_10, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (11, 12) then O_ORDERSTATUS else null end) as filter_cnt_11, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (20, 3) then O_ORDERSTATUS else null end) as filter_cnt_14, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 22) then O_ORDERSTATUS else null end) as filter_cnt_17, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 25) then O_ORDERSTATUS else null end) as filter_cnt_18, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (19, 3) then O_ORDERSTATUS else null end) as filter_cnt_19, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 20) then O_ORDERSTATUS else null end) as filter_cnt_20 " +
            "from orders " +
            "where O_ORDERDATE < '2023-12-30' and O_ORDERDATE > '2023-12-01'"

    def query3_0 = "select count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end) as filter_cnt_3, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (7, 9) then O_ORDERSTATUS else null end) as filter_cnt_6, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (11, 13) then O_ORDERSTATUS else null end) as filter_cnt_8, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (12, 11) then O_ORDERSTATUS else null end) as filter_cnt_9, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (11, 12) then O_ORDERSTATUS else null end) as filter_cnt_11, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 25) then O_ORDERSTATUS else null end) as filter_cnt_18, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (19, 3) then O_ORDERSTATUS else null end) as filter_cnt_19, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 20) then O_ORDERSTATUS else null end) as filter_cnt_20 " +
            "from orders " +
            "where O_ORDERDATE < '2023-12-30' and O_ORDERDATE > '2023-12-01'"
    order_qt_query3_0_before "${query3_0}"
    check_mv_rewrite_success(db, mv3_0, query3_0, "mv3_0")
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""


    // multi table
    // filter inside + left
    def mv13_0 = "select o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as distinct_count " +
            "from lineitem " +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query13_0 = "select t1.l_partkey, t1.l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from (select * from lineitem where l_partkey = 2) t1 " +
            "left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query13_0_before "${query13_0}"
    check_mv_rewrite_success(db, mv13_0, query13_0, "mv13_0")
    order_qt_query13_0_after "${query13_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv13_0"""


    // filter inside + right
    def mv14_0 = "select o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as distinct_count " +
            "from lineitem " +
            "left join (select * from orders where o_orderdate = '2023-12-08') t2 " +
            "on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query14_0 = "select l_partkey, l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from lineitem t1 " +
            "left join (select * from orders where o_orderdate = '2023-12-08') t2 " +
            "on t1.l_orderkey = o_orderkey and t1.l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query14_0_before "${query14_0}"
    check_mv_rewrite_success(db, mv14_0, query14_0, "mv14_0")
    order_qt_query14_0_after "${query14_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv14_0"""


    // filter inside + right + left
    def mv15_0 = "select o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as distinct_count " +
            "from lineitem " +
            "left join (select * from orders where o_orderstatus = 'o') t2 " +
            "on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query15_0 = "select t1.l_partkey, t1.l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from (select * from lineitem where l_partkey in (2, 3)) t1 " +
            "left join (select * from orders where o_orderstatus = 'o') t2 " +
            "on t1.l_orderkey = o_orderkey and t1.l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query15_0_before "${query15_0}"
    check_mv_rewrite_success(db, mv15_0, query15_0, "mv15_0")
    order_qt_query15_0_after "${query15_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv15_0"""

    def mv15_1 = """
            select o_orderdate, l_partkey, l_suppkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as distinct_count
            from lineitem
            left join (select * from orders where o_orderstatus = 'o') t2
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query15_1 = """
            select t1.l_partkey, t1.l_suppkey + o_orderdate,
            sum(o_totalprice) + max(o_totalprice),
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) + count(*)
            from (select * from lineitem where l_partkey in (2, 3)) t1
            left join (select * from orders where o_orderstatus = 'o') t2
            on t1.l_orderkey = o_orderkey and t1.l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    order_qt_query15_1_before "${query15_1}"
    check_mv_rewrite_success(db, mv15_1, query15_1, "mv15_1")
    order_qt_query15_0_after "${query15_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv15_1"""

    // filter outside + left
    def mv16_0 = "select o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as distinct_count " +
            "from lineitem " +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query16_0 = "select t1.l_partkey, t1.l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from lineitem t1 " +
            "left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate " +
            "where l_partkey in (1, 2 ,3, 4) " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query16_0_before "${query16_0}"
    check_mv_rewrite_success(db, mv16_0, query16_0, "mv16_0")
    order_qt_query16_0_after "${query16_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv16_0"""

    // should not rewrite, because query has the dimension which is not in view
    def mv16_1 = """
            select o_orderdate, l_partkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as distinct_count
            from lineitem
            left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_partkey;
    """

    def query16_1 = """
            select t1.l_suppkey, o_orderdate,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*),
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)
            from lineitem t1
            left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate
            where l_partkey in (1, 2 ,3, 4)
            group by
            o_orderdate,
            l_suppkey;
    """
    order_qt_query16_1_before "${query16_1}"
    check_mv_rewrite_fail(db, mv16_1, query16_1, "mv16_1")
    order_qt_query16_1_after "${query16_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv16_1"""

    // filter outside + right
    def mv17_0 = "select o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as distinct_count " +
            "from lineitem " +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query17_0 = "select t1.l_partkey, t1.l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from lineitem t1 " +
            "left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate " +
            "where o_orderdate = '2023-12-11' " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query17_0_before "${query17_0}"
    check_mv_rewrite_success(db, mv17_0, query17_0, "mv17_0")
    order_qt_query17_0_after "${query17_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv17_0"""


    def mv17_1 = "select L_ORDERKEY, O_SHIPPRIORITY, O_COMMENT, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end), " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (5, 6) then O_ORDERSTATUS else null end) as filter_cnt_4, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (7, 9) then O_ORDERSTATUS else null end) as filter_cnt_6, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (8, 10) then O_ORDERSTATUS else null end) as filter_cnt_7, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (11, 13) then O_ORDERSTATUS else null end) as filter_cnt_8, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (12, 11) then O_ORDERSTATUS else null end), " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 15) then O_ORDERSTATUS else null end) as filter_cnt_10, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (11, 12) then O_ORDERSTATUS else null end) as filter_cnt_11, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end), " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (20, 3) then O_ORDERSTATUS else null end) as filter_cnt_14, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 22) then O_ORDERSTATUS else null end) as filter_cnt_17, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 25) then O_ORDERSTATUS else null end) as filter_cnt_18, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (19, 3) then O_ORDERSTATUS else null end) as filter_cnt_19, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 20) then O_ORDERSTATUS else null end) as filter_cnt_20 " +
            "from lineitem " +
            "left join " +
            "orders " +
            "on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where orders.O_ORDERDATE < '2023-12-30' and orders.O_ORDERDATE > '2023-12-01' " +
            "group by " +
            "lineitem.L_ORDERKEY, " +
            "orders.O_SHIPPRIORITY, " +
            "orders.O_COMMENT "
    def query17_1 =  "select L_ORDERKEY, O_SHIPPRIORITY, O_COMMENT, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end) as filter_cnt_3, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (5, 6) then O_ORDERSTATUS else null end) as filter_cnt_4, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (7, 9) then O_ORDERSTATUS else null end) as filter_cnt_6, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (8, 10) then O_ORDERSTATUS else null end) as filter_cnt_7, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (20, 3) then O_ORDERSTATUS else null end) as filter_cnt_14, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 22) then O_ORDERSTATUS else null end) as filter_cnt_17 " +
            "from lineitem " +
            "left join " +
            "orders " +
            "on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where orders.O_ORDERDATE < '2023-12-30' and orders.O_ORDERDATE > '2023-12-01' " +
            "group by " +
            "lineitem.L_ORDERKEY, " +
            "orders.O_SHIPPRIORITY, " +
            "orders.O_COMMENT "
    order_qt_query17_1_before "${query17_1}"
    check_mv_rewrite_success(db, mv17_1, query17_1, "mv17_1")
    order_qt_query17_1_after "${query17_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv17_1"""

    // filter outside + left + right
    def mv18_0 = "select l_shipdate, l_suppkey, " +
            "sum(o_totalprice) as sum_total, " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all, " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as distinct_count " +
            "from lineitem " +
            "left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "l_shipdate, " +
            "l_suppkey"
    def query18_0 = "select t1.l_suppkey, l_shipdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*), " +
            "count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) " +
            "from lineitem t1 " +
            "left join orders on t1.l_orderkey = orders.o_orderkey and t1.l_shipdate = o_orderdate " +
            "where l_shipdate = '2023-12-11' and l_suppkey = 3 " +
            "group by " +
            "l_shipdate, " +
            "l_suppkey"
    order_qt_query18_0_before "${query18_0}"
    check_mv_rewrite_success(db, mv18_0, query18_0, "mv18_0")
    order_qt_query18_0_after "${query18_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18_0"""


    def mv18_1 = "select l_linenumber, o_custkey, sum(o_totalprice) as sum_alias " +
            "from lineitem " +
            "inner join orders on l_orderkey = o_orderkey " +
            "group by l_linenumber, o_custkey "
    def query18_1 = "select l_linenumber, sum(o_totalprice) as sum_alias " +
            "from lineitem " +
            "inner join orders on l_orderkey = o_orderkey " +
            "where o_custkey = 2 and l_linenumber = 4 " +
            "group by l_linenumber, o_custkey "
    order_qt_query18_1_before "${query18_1}"
    check_mv_rewrite_success(db, mv18_1, query18_1, "mv18_1")
    order_qt_query18_1_after "${query18_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18_1"""


    def mv18_2 = "select lineitem.l_linenumber, orders.o_custkey, sum(o_totalprice) as sum_alias " +
            "from lineitem " +
            "inner join orders on l_orderkey = o_orderkey " +
            "group by lineitem.l_linenumber, orders.o_custkey "
    def query18_2 = "select lineitem.l_linenumber, sum(o_totalprice) as sum_alias " +
            "from lineitem " +
            "inner join orders on lineitem.l_orderkey = orders.o_orderkey " +
            "where o_custkey = 2 and l_suppkey= 3 " +
            "group by lineitem.l_linenumber, orders.o_custkey "
    order_qt_query18_2_before "${query18_2}"
    check_mv_rewrite_fail(db, mv18_2, query18_2, "mv18_2")
    order_qt_query18_2_after "${query18_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18_2"""


    // without filter
    def mv19_0 = "select o_orderdate, l_partkey, l_suppkey, " +
            "sum(o_totalprice), " +
            "max(o_totalprice) as max_total, " +
            "min(o_totalprice) as min_total, " +
            "count(*) as count_all " +
            "from lineitem " +
            "left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    def query19_0 = "select l_partkey, l_suppkey, o_orderdate, " +
            "sum(o_totalprice), " +
            "max(o_totalprice), " +
            "min(o_totalprice), " +
            "count(*) " +
            "from lineitem " +
            "left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate " +
            "group by " +
            "o_orderdate, " +
            "l_partkey, " +
            "l_suppkey"
    order_qt_query19_0_before "${query19_0}"
    check_mv_rewrite_success(db, mv19_0, query19_0, "mv19_0")
    order_qt_query19_0_after "${query19_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv19_0"""


    def mv19_1 = "select lineitem.l_linenumber, orders.o_custkey, sum(o_totalprice) as sum_alias " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "group by lineitem.L_LINENUMBER, orders.O_CUSTKEY "
    def query19_1 = "select lineitem.L_LINENUMBER, orders.O_CUSTKEY, sum(O_TOTALPRICE) as sum_alias " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "group by lineitem.L_LINENUMBER, orders.O_CUSTKEY "
    order_qt_query19_1_before "${query19_1}"
    check_mv_rewrite_success(db, mv19_1, query19_1, "mv19_1")
    order_qt_query19_1_after "${query19_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv19_1"""

    // with duplicated group by column
    def mv19_2 = """
            select
            o_orderdate as o_orderdate_1,
            l_partkey as l_partkey_1,
            l_suppkey as l_suppkey_1,
            l_suppkey as l_suppkey_2,
            l_partkey as l_partkey_2,
            o_orderdate as o_orderdate_2,
            sum(o_totalprice),
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_partkey,
            l_suppkey,
            l_suppkey,
            l_partkey,
            o_orderdate;
    """
    def query19_2 = """
            select
            o_orderdate as o_orderdate_1,
            l_partkey as l_partkey_1,
            l_suppkey as l_suppkey_1,
            l_suppkey as l_suppkey_2,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*)
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_suppkey,
            l_partkey,
            l_suppkey;
    """
    order_qt_query19_2_before "${query19_2}"
    check_mv_rewrite_success(db, mv19_2, query19_2, "mv19_2")
    order_qt_query19_2_after "${query19_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv19_2"""


    // aggregate function and group by expression is complex
    def mv19_3 = """
            select o_orderdate, l_partkey, l_suppkey + sum(o_totalprice),
            sum(o_totalprice),
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            min(o_totalprice) + sum(o_totalprice),
            count(*) as count_all
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_partkey,
            l_suppkey;
    """

    def query19_3 = """
            select l_partkey, l_suppkey + sum(o_totalprice), o_orderdate,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            min(o_totalprice) + sum(o_totalprice),
            count(*)
            from lineitem
            left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    order_qt_query19_3_before "${query19_3}"
    check_mv_rewrite_success(db, mv19_3, query19_3, "mv19_3")
    order_qt_query19_3_after "${query19_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv19_3"""


    // without group, scalar aggregate
    def mv20_0 = "select count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end) as filter_cnt_3, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (5, 6) then O_ORDERSTATUS else null end) as filter_cnt_4, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (7, 9) then O_ORDERSTATUS else null end) as filter_cnt_6, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (8, 10) then O_ORDERSTATUS else null end) as filter_cnt_7, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (11, 13) then O_ORDERSTATUS else null end) as filter_cnt_8, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (12, 11) then O_ORDERSTATUS else null end) as filter_cnt_9, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 15) then O_ORDERSTATUS else null end) as filter_cnt_10, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (11, 12) then O_ORDERSTATUS else null end) as filter_cnt_11, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (20, 3) then O_ORDERSTATUS else null end) as filter_cnt_14, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 22) then O_ORDERSTATUS else null end) as filter_cnt_17, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 25) then O_ORDERSTATUS else null end) as filter_cnt_18, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (19, 3) then O_ORDERSTATUS else null end) as filter_cnt_19, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 20) then O_ORDERSTATUS else null end) as filter_cnt_20 " +
            "from lineitem " +
            "left join " +
            "orders " +
            "on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where orders.O_ORDERDATE < '2023-12-30' and orders.O_ORDERDATE > '2023-12-01' "
    def query20_0 =  "select count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end) as filter_cnt_3, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (5, 6) then O_ORDERSTATUS else null end) as filter_cnt_4, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (8, 10) then O_ORDERSTATUS else null end) as filter_cnt_7, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (20, 3) then O_ORDERSTATUS else null end) as filter_cnt_14, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 22) then O_ORDERSTATUS else null end) as filter_cnt_17 " +
            "from lineitem " +
            "left join " +
            "orders " +
            "on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where orders.O_ORDERDATE < '2023-12-30' and orders.O_ORDERDATE > '2023-12-01' "
    order_qt_query20_0_before "${query20_0}"
    check_mv_rewrite_success(db, mv20_0, query20_0, "mv20_0")
    order_qt_query20_0_after "${query20_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv20_0"""

    // mv is not scalar aggregate bug query is, should not rewrite
    def mv20_1 = """
            select
            l_shipmode,
            l_shipinstruct,
            sum(l_extendedprice),
            count(*)
            from lineitem
            left join
            orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY
            group by
            l_shipmode,
            l_shipinstruct;
    """
    def query20_1 =
            """
            select
            sum(l_extendedprice),
            count(*)
            from lineitem
            left join
            orders
            on lineitem.L_ORDERKEY = orders.O_ORDERKEY
    """
    order_qt_query20_1_before "${query20_1}"
    check_mv_rewrite_success(db, mv20_1, query20_1, "mv20_1")
    order_qt_query20_1_after "${query20_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv20_1"""

    // mv is scalar aggregate bug query not, should not rewrite
    def mv20_2 = """
            select
            sum(l_extendedprice),
            count(*)
            from lineitem
            left join
            orders
            on lineitem.L_ORDERKEY = orders.O_ORDERKEY
    """
    def query20_2 = """
            select
            l_shipmode,
            l_shipinstruct,
            sum(l_extendedprice),
            count(*)
            from lineitem
            left join
            orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY
            group by
            l_shipmode,
            l_shipinstruct;
    """
    order_qt_query20_2_before "${query20_2}"
    check_mv_rewrite_fail(db, mv20_2, query20_2, "mv20_2")
    order_qt_query20_2_after "${query20_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv20_2"""

    // join input has simple agg, simple agg which can not contains rollup, cube
    def mv21_0 = """
            select
            l_linenumber,
            count(distinct l_orderkey),
            sum(case when l_orderkey in (1,2,3) then l_suppkey * l_linenumber else 0 end),
            max(case when l_orderkey in (4, 5) then (l_quantity *2 + part_supp_a.qty_max) * 0.88 else 100 end),
            avg(case when l_partkey in (2, 3, 4) then l_discount + o_totalprice + part_supp_a.qty_sum else 50 end)
            from lineitem
            left join orders on l_orderkey = o_orderkey
            left join
            (select ps_partkey, ps_suppkey, sum(ps_availqty) qty_sum, max(ps_availqty) qty_max,
                min(ps_availqty) qty_min,
                avg(ps_supplycost) cost_avg
                from partsupp
                group by ps_partkey,ps_suppkey) part_supp_a
            on l_partkey = part_supp_a.ps_partkey
            and l_suppkey = part_supp_a.ps_suppkey
            group by l_linenumber;
    """
    def query21_0 = """
                       select
            l_linenumber,
            count(distinct l_orderkey),
            sum(case when l_orderkey in (1,2,3) then l_suppkey * l_linenumber else 0 end),
            max(case when l_orderkey in (4, 5) then (l_quantity *2 + part_supp_a.qty_max) * 0.88 else 100 end),
            avg(case when l_partkey in (2, 3, 4) then l_discount + o_totalprice + part_supp_a.qty_sum else 50 end)
            from lineitem
            left join orders on l_orderkey = o_orderkey
            left join
            (select ps_partkey, ps_suppkey, sum(ps_availqty) qty_sum, max(ps_availqty) qty_max,
                min(ps_availqty) qty_min,
                avg(ps_supplycost) cost_avg
                from partsupp
                group by ps_partkey,ps_suppkey) part_supp_a
            on l_partkey = part_supp_a.ps_partkey
            and l_suppkey = part_supp_a.ps_suppkey
            group by l_linenumber;
    """
    order_qt_query21_0_before "${query21_0}"
    check_mv_rewrite_success(db, mv21_0, query21_0, "mv21_0")
    order_qt_query21_0_after "${query21_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv21_0"""

    // should rewrite success because query agg input the join has ps_availqty dimension which is not in mv
    def mv21_1 = """
            select
            l_linenumber,
            count(distinct l_orderkey),
            sum(case when l_orderkey in (1,2,3) then l_suppkey * l_linenumber else 0 end),
            max(case when l_orderkey in (4, 5) then (l_quantity *2 + part_supp_a.qty_max) * 0.88 else 100 end),
            avg(case when l_partkey in (2, 3, 4) then l_discount + o_totalprice + part_supp_a.qty_sum else 50 end)
            from lineitem
            left join orders on l_orderkey = o_orderkey
            left join
            (select ps_partkey, ps_suppkey, sum(ps_availqty) qty_sum, max(ps_availqty) qty_max,
                min(ps_availqty) qty_min,
                avg(ps_supplycost) cost_avg
                from partsupp
                group by ps_partkey,ps_suppkey) part_supp_a
            on l_partkey = part_supp_a.ps_partkey
            and l_suppkey = part_supp_a.ps_suppkey
            group by l_linenumber;
    """
    def query21_1 = """
            select
            l_linenumber,
            count(distinct l_orderkey),
            sum(case when l_orderkey in (1,2,3) then l_suppkey * l_linenumber else 0 end),
            max(case when l_orderkey in (4, 5) then (l_quantity *2 + part_supp_a.qty_max) * 0.88 else 100 end),
            avg(case when l_partkey in (2, 3, 4) then l_discount + o_totalprice + part_supp_a.qty_sum else 50 end)
            from lineitem
            left join orders on l_orderkey = o_orderkey
            left join
            (select ps_suppkey, ps_partkey, ps_availqty, sum(ps_availqty) qty_sum, max(ps_availqty) qty_max,
                min(ps_availqty) qty_min,
                avg(ps_supplycost) cost_avg
                from partsupp
                group by ps_suppkey, ps_partkey, ps_availqty) part_supp_a
            on l_partkey = part_supp_a.ps_partkey
            and l_suppkey = part_supp_a.ps_suppkey
            group by l_linenumber;
    """
    order_qt_query21_1_before "${query21_1}"
    check_mv_rewrite_fail(db, mv21_1, query21_1, "mv21_1")
    order_qt_query21_1_after "${query21_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv21_1"""

    // should not rewritten successfully, because query has more filter
    def mv21_2 = """
            select
            l_linenumber,
            count(distinct l_orderkey),
            sum(case when l_orderkey in (1,2,3) then l_suppkey * l_linenumber else 0 end),
            max(case when l_orderkey in (4, 5) then (l_quantity *2 + part_supp_a.qty_max) * 0.88 else 100 end),
            avg(case when l_partkey in (2, 3, 4) then l_discount + o_totalprice + part_supp_a.qty_sum else 50 end)
            from lineitem
            left join orders on l_orderkey = o_orderkey
            left join
            (select ps_partkey, ps_suppkey, sum(ps_availqty) qty_sum, max(ps_availqty) qty_max,
                min(ps_availqty) qty_min,
                avg(ps_supplycost) cost_avg
                from partsupp
                where ps_partkey in (1, 2)
                group by ps_partkey,ps_suppkey) part_supp_a
            on l_partkey = part_supp_a.ps_partkey
            and l_suppkey = part_supp_a.ps_suppkey
            group by l_linenumber;
    """
    def query21_2 = """
                       select
            l_linenumber,
            count(distinct l_orderkey),
            sum(case when l_orderkey in (1,2,3) then l_suppkey * l_linenumber else 0 end),
            max(case when l_orderkey in (4, 5) then (l_quantity *2 + part_supp_a.qty_max) * 0.88 else 100 end),
            avg(case when l_partkey in (2, 3, 4) then l_discount + o_totalprice + part_supp_a.qty_sum else 50 end)
            from lineitem
            left join orders on l_orderkey = o_orderkey
            left join
            (select ps_partkey, ps_suppkey, sum(ps_availqty) qty_sum, max(ps_availqty) qty_max,
                min(ps_availqty) qty_min,
                avg(ps_supplycost) cost_avg
                from partsupp
                group by ps_partkey,ps_suppkey) part_supp_a
            on l_partkey = part_supp_a.ps_partkey
            and l_suppkey = part_supp_a.ps_suppkey
            group by l_linenumber;
    """
    order_qt_query21_2_before "${query21_2}"
    check_mv_rewrite_fail(db, mv21_2, query21_2, "mv21_2")
    order_qt_query21_2_after "${query21_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv21_2"""


        def mv22_0 = """
            select
            o_orderdate,
            l_partkey,
            l_suppkey,
            max_union(max_state(o_shippriority))
            from lineitem
            left join orders  t2
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    def query22_0 = """
            select
            o_orderdate,
            l_partkey,
            l_suppkey,
            max(o_shippriority)
            from lineitem
            left join orders
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderdate,
            l_partkey,
            l_suppkey;
    """
    order_qt_query22_0_before "${query22_0}"
    check_mv_rewrite_success(db, mv22_0, query22_0, "mv22_0")
    order_qt_query22_0_after "${query22_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv22_0"""

   // test combinator aggregate function rewrite
    sql """set enable_agg_state=true"""
    // query has no combinator and mv has combinator
    // mv is union
    def mv23_0 = """
            select
            o_orderpriority,
            l_suppkey,
            sum_union(sum_state(o_shippriority)),
            group_concat_union(group_concat_state(o_orderpriority)),
            avg_union(avg_state(l_linenumber)),
            max_by_union(max_by_state(O_COMMENT,o_totalprice)),
            count_union(count_state(l_orderkey)),
            multi_distinct_count_union(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderpriority,
            l_suppkey;
    """
    def query23_0 = """
            select
            o_orderpriority,
            l_suppkey,
            sum(o_shippriority),
            group_concat(o_orderpriority),
            avg(l_linenumber),
            max_by(O_COMMENT,o_totalprice),
            count(l_orderkey),
            multi_distinct_count(l_shipmode)
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderpriority,
            l_suppkey
            order by o_orderpriority;
    """
    order_qt_query23_0_before "${query23_0}"
    check_mv_rewrite_success(db, mv23_0, query23_0, "mv23_0")
    order_qt_query23_0_after "${query23_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv23_0"""


    // mv is merge
    def mv23_1 = """
            select
            o_orderpriority,
            l_suppkey,
            sum_merge(sum_state(o_shippriority)),
            group_concat_merge(group_concat_state(o_orderpriority)),
            avg_merge(avg_state(l_linenumber)),
            max_by_merge(max_by_state(O_COMMENT,o_totalprice)),
            count_merge(count_state(l_orderkey)),
            multi_distinct_count_merge(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderpriority,
            l_suppkey;
    """
    def query23_1 = """
             select
            o_orderpriority,
            l_suppkey,
            sum(o_shippriority),
            group_concat(o_orderpriority),
            avg(l_linenumber),
            max_by(O_COMMENT,o_totalprice),
            count(l_orderkey),
            multi_distinct_count(l_shipmode)
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderpriority,
            l_suppkey
            order by o_orderpriority;
    """
    order_qt_query23_1_before "${query23_1}"
    // not supported, this usage is rare
    check_mv_rewrite_fail(db, mv23_1, query23_1, "mv23_1")
    order_qt_query23_1_after "${query23_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv23_1"""



    // both query and mv are combinator
    // mv is union, query is union
    def mv24_0 = """
            select
            o_orderpriority,
            l_suppkey,
            sum_union(sum_state(o_shippriority)),
            group_concat_union(group_concat_state(o_orderpriority)),
            avg_union(avg_state(l_linenumber)),
            max_by_union(max_by_state(O_COMMENT,o_totalprice)),
            count_union(count_state(l_orderkey)),
            multi_distinct_count_union(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderpriority,
            l_suppkey;
    """
    def query24_0 = """
            select
            o_orderpriority,
            l_suppkey,
            sum_union(sum_state(o_shippriority)),
            group_concat_union(group_concat_state(o_orderpriority)),
            avg_union(avg_state(l_linenumber)),
            max_by_union(max_by_state(O_COMMENT,o_totalprice)),
            count_union(count_state(l_orderkey)),
            multi_distinct_count_union(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderpriority,
            l_suppkey;
    """
    check_mv_rewrite_success(db, mv24_0, query24_0, "mv24_0")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv24_0"""


    // mv is union, query is merge
    def mv25_0 = """
            select
            o_orderpriority,
            l_suppkey,
            sum_union(sum_state(o_shippriority)),
            group_concat_union(group_concat_state(o_orderpriority)),
            avg_union(avg_state(l_linenumber)),
            max_by_union(max_by_state(O_COMMENT,o_totalprice)),
            count_union(count_state(l_orderkey)),
            multi_distinct_count_union(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderpriority,
            l_suppkey;
    """
    def query25_0 = """
            select
            o_orderpriority,
            l_suppkey,
            sum_merge(sum_state(o_shippriority)),
            group_concat_merge(group_concat_state(o_orderpriority)),
            avg_merge(avg_state(l_linenumber)),
            max_by_merge(max_by_state(O_COMMENT,o_totalprice)),
            count_merge(count_state(l_orderkey)),
            multi_distinct_count_merge(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderpriority,
            l_suppkey
            order by o_orderpriority;
    """
    order_qt_query25_0_before "${query25_0}"
    check_mv_rewrite_success(db, mv25_0, query25_0, "mv25_0")
    order_qt_query25_0_after "${query25_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv25_0"""


    // mv is merge, query is merge
    def mv26_0 = """
            select
            o_orderpriority,
            l_suppkey,
            sum_merge(sum_state(o_shippriority)),
            group_concat_merge(group_concat_state(o_orderpriority)),
            avg_merge(avg_state(l_linenumber)),
            max_by_merge(max_by_state(O_COMMENT,o_totalprice)),
            count_merge(count_state(l_orderkey)),
            multi_distinct_count_merge(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderpriority,
            l_suppkey;
    """
    def query26_0 = """
            select
            o_orderpriority,
            l_suppkey,
            sum_merge(sum_state(o_shippriority)),
            group_concat_merge(group_concat_state(o_orderpriority)),
            avg_merge(avg_state(l_linenumber)),
            max_by_merge(max_by_state(O_COMMENT,o_totalprice)),
            count_merge(count_state(l_orderkey)),
            multi_distinct_count_merge(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderpriority,
            l_suppkey
            order by o_orderpriority;
    """
    order_qt_query26_0_before "${query26_0}"
    check_mv_rewrite_success(db, mv26_0, query26_0, "mv26_0")
    order_qt_query26_0_after "${query26_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv26_0"""


    // mv is merge, query is union
    def mv27_0 = """
            select
            o_orderpriority,
            l_suppkey,
            sum_merge(sum_state(o_shippriority)),
            group_concat_merge(group_concat_state(o_orderpriority)),
            avg_merge(avg_state(l_linenumber)),
            max_by_merge(max_by_state(O_COMMENT,o_totalprice)),
            count_merge(count_state(l_orderkey)),
            multi_distinct_count_merge(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderpriority,
            l_suppkey;
    """
    def query27_0 = """
            select
            o_orderpriority,
            l_suppkey,
            sum_union(sum_state(o_shippriority)),
            group_concat_union(group_concat_state(o_orderpriority)),
            avg_union(avg_state(l_linenumber)),
            max_by_union(max_by_state(O_COMMENT,o_totalprice)),
            count_union(count_state(l_orderkey)),
            multi_distinct_count_union(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderpriority,
            l_suppkey;
    """
    check_mv_rewrite_fail(db, mv27_0, query27_0, "mv27_0")
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv27_0"""
}
