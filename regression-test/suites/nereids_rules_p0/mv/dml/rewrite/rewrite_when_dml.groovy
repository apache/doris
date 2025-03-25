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

suite("rewrite_when_dml") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "set enable_agg_state=true"

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

    // rewrite should success when table add column
    def mv1_0 = """
            select
            o_orderstatus,
            l_partkey,
            l_suppkey,
            sum_union(sum_state(o_shippriority)),
            group_concat_union(group_concat_state(o_orderstatus)),
            avg_union(avg_state(l_linenumber)),
            max_by_union(max_by_state(O_COMMENT,o_totalprice)),
            count_union(count_state(l_orderkey)),
            multi_distinct_count_union(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_partkey,
            l_suppkey;
    """
    def query1_0 = """
            select
            o_orderstatus,
            l_suppkey,
            sum_merge(sum_state(o_shippriority)),
            group_concat_merge(group_concat_state(o_orderstatus)),
            avg_merge(avg_state(l_linenumber)),
            max_by_merge(max_by_state(O_COMMENT,o_totalprice)),
            count_merge(count_state(l_orderkey)),
            multi_distinct_count_merge(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_suppkey
            order by o_orderstatus;
    """
    order_qt_query1_0_before "${query1_0}"
    sql """ALTER TABLE orders ADD COLUMN new_col INT KEY DEFAULT "0";"""
    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""


    // rewrite should fail when table drop column which is used by query
    def mv2_0 = """
            select
            o_orderstatus,
            l_partkey,
            l_suppkey,
            sum_union(sum_state(o_shippriority)),
            group_concat_union(group_concat_state(o_orderstatus)),
            avg_union(avg_state(l_linenumber)),
            max_by_union(max_by_state(O_COMMENT,o_totalprice)),
            count_union(count_state(l_orderkey)),
            multi_distinct_count_union(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_partkey,
            l_suppkey;
    """
    def query2_0 = """
            select
            o_orderstatus,
            l_suppkey,
            sum_merge(sum_state(o_shippriority)),
            group_concat_merge(group_concat_state(o_orderstatus)),
            avg_merge(avg_state(l_linenumber)),
            max_by_merge(max_by_state(O_COMMENT,o_totalprice)),
            count_merge(count_state(l_orderkey)),
            multi_distinct_count_merge(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_suppkey
            order by o_orderstatus;
    """
    order_qt_query2_0_before "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""
    sql """
        CREATE MATERIALIZED VIEW mv2_0
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv2_0}
    """
    waitingMTMVTaskFinishedByMvName("mv2_0")
    sql """ALTER TABLE lineitem DROP COLUMN l_suppkey;"""

    try {
        mv_not_part_in(query2_0, "mv2_0")
    } catch (Exception e) {
        // Unknown column because drop
    }
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    // rewrite should fail when table drop column which is not used by query
    // can be optimized in the future
    def mv3_0 = """
            select
            o_orderstatus,
            l_partkey,
            sum_union(sum_state(o_shippriority)),
            group_concat_union(group_concat_state(o_orderstatus)),
            avg_union(avg_state(l_linenumber)),
            count_union(count_state(l_orderkey)),
            multi_distinct_count_union(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders
            on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus,
            l_partkey;
    """
    def query3_0 = """
            select
            o_orderstatus,
            sum_merge(sum_state(o_shippriority)),
            group_concat_merge(group_concat_state(o_orderstatus)),
            avg_merge(avg_state(l_linenumber)),
            count_merge(count_state(l_orderkey)),
            multi_distinct_count_merge(multi_distinct_count_state(l_shipmode))
            from lineitem
            left join orders 
            on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            o_orderstatus
            order by o_orderstatus;
    """
    order_qt_query3_0_before "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""
    sql """
        CREATE MATERIALIZED VIEW mv3_0
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv3_0}
    """
    waitingMTMVTaskFinishedByMvName("mv3_0")
    sql """ALTER TABLE orders DROP COLUMN O_COMMENT;"""
    mv_not_part_in(query3_0, "mv3_0")

    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""
}
