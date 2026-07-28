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

suite("scalar_agg_pushdown") {
    sql "set eager_aggregation_mode=1;"
    sql "set eager_aggregation_on_join=true;"
    sql "set disable_nereids_rules='SALT_JOIN';"
    sql "set runtime_filter_mode=OFF;"
    sql 'set ignore_shape_nodes="PhysicalProject, PhysicalDistribute";'

    // Test: scalar aggregation (no GROUP BY) must not be pushed below a join,
    // because scalar agg on empty input returns 1 NULL row (SQL standard),
    // which would be cross-joined with the other side to produce phantom rows.
    // Regression test for: scalar agg pushdown producing phantom rows when
    // constant propagation replaces GROUP BY key with a literal that doesn't
    // exist in the table.

    sql """drop table if exists t1;"""
    sql """create table t1 (a int, b int) distributed by hash(a) properties("replication_num" = "1");"""
    sql """insert into t1 values (1,10),(2,20);"""

    sql """drop table if exists t2;"""
    sql """create table t2 (c int) distributed by hash(c) properties("replication_num" = "1");"""
    sql """insert into t2 values (1),(2);"""

    // WHERE t1.a = 999 matches no rows in t1.
    // GROUP BY includes t1.a, t2.c (cross-table GROUP BY).
    // Without the fix, eager aggregation pushes a scalar partial aggregate
    // below the join, which on empty input returns 1 NULL row.
    // That NULL row cross-joins with t2's 2 rows, producing 2 phantom rows.
    // With the fix, pushdown is forbidden for scalar child aggregates.
    qt_scalar_agg_no_pushdown """
        SELECT t1.a, SUM(t1.b), t2.c
        FROM t1, t2
        WHERE t1.a = 999
        GROUP BY t1.a, t2.c
        ORDER BY t2.c;
    """

    // Test: reviewer's example — constant group key k survives ConstantPropagation
    // (retained when it is the only GROUP BY key, to keep group-by-with-literal
    // semantics), then goes through a Project where createContextFromProject
    // resolves k to empty input slots. sum(z) = sum(ta.b + tb.b) spans both
    // inner-join children, so visitLogicalJoin falls back to genAggregate with
    // groupKeys=[]. Plan after subquery unnesting (t1 self-joined as ta/tb,
    // t2 as the cross side):
    //
    //   Aggregate(group=[k], sum(z))
    //     CrossJoin
    //       Project(1 AS k, ta.b + tb.b AS z)
    //         InnerJoin(ta.a = tb.a)
    //           Scan t1 ta
    //           Scan t1 tb
    //       Scan t2
    //
    // WHERE ta.a = 999 empties the inner join. Without the guard at
    // genAggregate(), a scalar aggregate is created above the inner join;
    // its 0->1 row semantic produces a phantom row that cross-joins with
    // t2's rows, yielding (1, NULL) instead of an empty result.

    // no scalar aggregate is allowed between the inner join and the cross join
    qt_const_group_key_shape """
        explain shape plan
        SELECT k, SUM(z)
        FROM (
            SELECT /*+leading(ta broadcast tb)*/ 1 AS k, ta.b + tb.b AS z
            FROM t1 ta JOIN t1 tb ON ta.a = tb.a
            WHERE ta.a = 999
        ) sub, t2
        GROUP BY k;
    """

    order_qt_const_group_key_exe """
        SELECT k, SUM(z)
        FROM (
            SELECT /*+leading(ta broadcast tb)*/ 1 AS k, ta.b + tb.b AS z
            FROM t1 ta JOIN t1 tb ON ta.a = tb.a
            WHERE ta.a = 999
        ) sub, t2
        GROUP BY k;
    """

    // Test: valid pushdown must be preserved. Same shape as above, but
    // z = ta.b comes from one side only. The context reaching the Project
    // still ends up with groupKeys=[] (k is a constant), yet at the inner
    // join fillGroupByKeys adds the join key ta.a, so the aggregate finally
    // pushed onto scan(ta) is a grouped one: agg(group=[a], sum(b)).
    // This is why the empty-group-keys guard must sit at genAggregate (the
    // aggregate-creation boundary) instead of firing early when an
    // intermediate context has no group keys.

    // expect an aggregate pushed below the inner join, on top of scan(ta)
    qt_pushdown_preserved_shape """
        explain shape plan
        SELECT k, SUM(z)
        FROM (
            SELECT /*+leading(ta broadcast tb)*/ 1 AS k, ta.b AS z
            FROM t1 ta JOIN t1 tb ON ta.a = tb.a
        ) sub, t2
        GROUP BY k;
    """

    // (10 + 20) * 2 rows of t2 = 60
    order_qt_pushdown_preserved_exe """
        SELECT k, SUM(z)
        FROM (
            SELECT /*+leading(ta broadcast tb)*/ 1 AS k, ta.b AS z
            FROM t1 ta JOIN t1 tb ON ta.a = tb.a
        ) sub, t2
        GROUP BY k;
    """
}
