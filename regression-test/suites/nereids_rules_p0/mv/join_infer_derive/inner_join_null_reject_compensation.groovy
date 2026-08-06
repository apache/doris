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

suite("inner_join_null_reject_compensation") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_materialized_view_rewrite=true"
    sql "set pre_materialized_view_rewrite_strategy=FORCE_IN_RBO"
    sql "set enable_nereids_timeout=false"

    sql """drop materialized view if exists mv_inner_join_null_reject_compensation"""
    sql """drop materialized view if exists mv_repro_left_join"""
    sql """drop materialized view if exists mv_repro_left_join_missing_right_output"""
    sql """drop materialized view if exists mv_repro_left_join_nullable_right_output"""
    sql """drop table if exists mv_repro_a"""
    sql """drop table if exists mv_repro_b"""
    sql """drop table if exists orders_inner_join_null_reject"""
    sql """drop table if exists lineitem_inner_join_null_reject"""

    sql """
        create table lineitem_inner_join_null_reject (
            l_orderkey int not null,
            l_shipdate date not null,
            l_suppkey int not null
        )
        duplicate key(l_orderkey)
        distributed by hash(l_orderkey) buckets 1
        properties (
            "replication_num" = "1"
        )
    """

    sql """
        create table orders_inner_join_null_reject (
            o_orderkey int not null,
            o_orderdate date not null
        )
        duplicate key(o_orderkey)
        distributed by hash(o_orderkey) buckets 1
        properties (
            "replication_num" = "1"
        )
    """

    sql """
        create table mv_repro_a (
            id int null,
            k int null
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties (
            "replication_num" = "1"
        )
    """

    sql """
        create table mv_repro_b (
            k int null,
            v int null
        )
        duplicate key(k)
        distributed by hash(k) buckets 1
        properties (
            "replication_num" = "1"
        )
    """

    sql """
        insert into lineitem_inner_join_null_reject values
            (1, '2023-10-17', 10),
            (999, '2023-10-17', 20)
    """

    sql """
        insert into orders_inner_join_null_reject values
            (1, '2023-10-17'),
            (888, '2023-10-17')
    """

    sql """
        insert into mv_repro_a values
            (1, 10),
            (2, 20)
    """

    sql """
        insert into mv_repro_b values
            (10, 100)
    """

    sql """analyze table lineitem_inner_join_null_reject with sync"""
    sql """analyze table orders_inner_join_null_reject with sync"""
    sql """analyze table mv_repro_a with sync"""
    sql """analyze table mv_repro_b with sync"""

    def withUseMvHint = { def stmt, def mvName ->
        stmt.replaceFirst("(?i)\\bselect\\b", "select /*+ use_mv(${mvName}) */")
    }

    def compare_res_with_forced_mv = { def stmt, def mvName ->
        def stmtWithUseMvHint = withUseMvHint(stmt, mvName)
        sql "set enable_materialized_view_rewrite=false"
        def origin_res = sql stmt
        logger.info("origin_res: " + origin_res)
        sql "set enable_materialized_view_rewrite=true"
        mv_rewrite_success(stmtWithUseMvHint, mvName)
        def mv_origin_res = sql stmtWithUseMvHint
        logger.info("mv_origin_res: " + mv_origin_res)
        assertTrue((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))
        for (int row = 0; row < mv_origin_res.size(); row++) {
            assertTrue(mv_origin_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_origin_res[row].size(); col++) {
                assertTrue(mv_origin_res[row][col] == origin_res[row][col])
            }
        }
    }

    def mvName = "mv_inner_join_null_reject_compensation"
    def mvSql = """
        select l.l_shipdate, l.l_suppkey, o.o_orderdate
        from lineitem_inner_join_null_reject l
        full outer join orders_inner_join_null_reject o
        on l.l_orderkey = o.o_orderkey
    """

    create_async_mv(db, mvName, mvSql)

    def queryNeedLeftSideCompensation = """
        select l.l_shipdate, l.l_suppkey, o.o_orderdate
        from lineitem_inner_join_null_reject l
        inner join orders_inner_join_null_reject o
        on l.l_orderkey = o.o_orderkey
        where o.o_orderdate = '2023-10-17'
        order by 1, 2, 3
    """

    def queryNeedRightSideCompensation = """
        select l.l_shipdate, l.l_suppkey, o.o_orderdate
        from lineitem_inner_join_null_reject l
        inner join orders_inner_join_null_reject o
        on l.l_orderkey = o.o_orderkey
        where l.l_shipdate = '2023-10-17'
        order by 1, 2, 3
    """

    compare_res_with_forced_mv(queryNeedLeftSideCompensation, mvName)
    compare_res_with_forced_mv(queryNeedRightSideCompensation, mvName)

    def leftJoinMvName = "mv_repro_left_join"
    def leftJoinMvSql = """
        select
            a.id as a_id,
            a.k as a_k,
            b.k as b_k,
            b.v as b_v
        from mv_repro_a a
        left join mv_repro_b b
        on a.k = b.k
    """

    create_async_mv(db, leftJoinMvName, leftJoinMvSql)

    def innerJoinQueryOnLeftJoinMv = """
        select a.id
        from mv_repro_a a
        inner join mv_repro_b b
        on a.k = b.k
        order by 1
    """

    compare_res_with_forced_mv(innerJoinQueryOnLeftJoinMv, leftJoinMvName)

    def leftJoinMvWithoutRightOutputName = "mv_repro_left_join_missing_right_output"
    def leftJoinMvWithoutRightOutputSql = """
        select
            a.id as a_id,
            a.k as a_k
        from mv_repro_a a
        left join mv_repro_b b
        on a.k = b.k
    """

    create_async_mv(db, leftJoinMvWithoutRightOutputName, leftJoinMvWithoutRightOutputSql)
    mv_rewrite_fail(innerJoinQueryOnLeftJoinMv, leftJoinMvWithoutRightOutputName)

    def leftJoinMvWithNullableRightOutputName = "mv_repro_left_join_nullable_right_output"
    def leftJoinMvWithNullableRightOutputSql = """
        select
            a.id as a_id,
            a.k as a_k,
            b.v as b_v
        from mv_repro_a a
        left join mv_repro_b b
        on a.k = b.k
    """

    create_async_mv(db, leftJoinMvWithNullableRightOutputName, leftJoinMvWithNullableRightOutputSql)
    mv_rewrite_fail(innerJoinQueryOnLeftJoinMv, leftJoinMvWithNullableRightOutputName)
}
