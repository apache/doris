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

// Regression test for https://github.com/apache/doris/issues/59593
// When a partitioned mv misses some queried partitions, union compensation with the base table is required
// to produce a complete result. If enable_materialized_view_union_rewrite=false, the mv MUST NOT be used for
// such a query (otherwise it returns partial / wrong results). Before the fix the mv was still chosen, dropping
// the rows of the uncovered partition.
suite("union_rewrite_disabled_flag") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_materialized_view_rewrite=true"

    sql "drop table if exists orders_59593"
    sql """
    CREATE TABLE orders_59593 (
      o_orderkey   integer not null,
      o_custkey    integer not null,
      o_totalprice decimalv3(15,2) not null,
      o_orderdate  date not null
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    PARTITION BY RANGE(o_orderdate)(
      FROM ('2023-10-17') TO ('2023-10-20') INTERVAL 1 DAY
    )
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES ("replication_num" = "1");
    """

    sql """
    insert into orders_59593 values
    (1, 1, 99.50, '2023-10-17'),
    (2, 2, 109.20, '2023-10-18'),
    (3, 3, 119.30, '2023-10-19');
    """
    sql """alter table orders_59593 modify column o_orderkey set stats ('row_count'='3');"""

    def mv_name = "mv_59593"
    def query_sql = """
    select o_orderdate, o_custkey, sum(o_totalprice) as sum_total
    from orders_59593
    group by o_orderdate, o_custkey
    """

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
    sql """
        CREATE MATERIALIZED VIEW ${mv_name}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        partition by(o_orderdate)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        ${query_sql}
    """
    waitingMTMVTaskFinished(getJobName(db, mv_name))

    // Now insert a new partition into the base table that the mv does NOT cover yet, so the query needs
    // union compensation between the mv and the uncovered partition.
    sql """insert into orders_59593 values (4, 4, 129.40, '2023-10-19');"""
    // the 2023-10-19 mv partition is now stale/invalid w.r.t base data
    waitingPartitionIsExpected(mv_name, "p_20231019_20231020", false)

    def order_by = " order by 1,2,3"

    def compare_with_base = { ->
        sql "SET enable_materialized_view_rewrite=false"
        def base_res = sql (query_sql + order_by)
        sql "SET enable_materialized_view_rewrite=true"
        def mv_res = sql (query_sql + order_by)
        assertEquals(base_res, mv_res)
    }

    // Case 1: union rewrite ENABLED (default) -> mv can be used via union compensation, result correct.
    sql "SET enable_materialized_view_union_rewrite=true"
    mv_rewrite_success(query_sql, mv_name)
    compare_with_base()

    // Case 2 (#59593): union rewrite DISABLED -> mv requires union but union is off, so the mv MUST NOT be
    // chosen; the query falls back to the base table and still returns the complete, correct result.
    sql "SET enable_materialized_view_union_rewrite=false"
    mv_rewrite_fail(query_sql, mv_name)
    compare_with_base()

    sql "SET enable_materialized_view_union_rewrite=true"
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
    sql "drop table if exists orders_59593"
}
