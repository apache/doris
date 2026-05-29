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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.util.MatchingUtils;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for banAggUnionAll fix in ChildrenPropertiesRegulator.
 */
public class AggregateUnionPlanTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_agg_union");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().parallelPipelineTaskNum = 2;

        // Tables with RANDOM distribution: no PhysicalDistribute needed in union inputs
        createTable("CREATE TABLE test_agg_union.t1_random ("
                + "  a INT NULL, b INT NULL"
                + ") ENGINE=OLAP DUPLICATE KEY(a, b)"
                + " DISTRIBUTED BY RANDOM BUCKETS AUTO"
                + " PROPERTIES ('replication_allocation' = 'tag.location.default: 1');");
        createTable("CREATE TABLE test_agg_union.t2_random ("
                + "  a INT NULL, b INT NULL"
                + ") ENGINE=OLAP DUPLICATE KEY(a, b)"
                + " DISTRIBUTED BY RANDOM BUCKETS AUTO"
                + " PROPERTIES ('replication_allocation' = 'tag.location.default: 1');");

        // Tables with HASH distribution: same key, no PhysicalDistribute needed when
        // group-by matches the distribution key
        createTable("CREATE TABLE test_agg_union.t1_hash ("
                + "  a INT NULL, b INT NULL"
                + ") ENGINE=OLAP DUPLICATE KEY(a, b)"
                + " DISTRIBUTED BY HASH(a) BUCKETS 3"
                + " PROPERTIES ('replication_allocation' = 'tag.location.default: 1');");
        createTable("CREATE TABLE test_agg_union.t2_hash ("
                + "  a INT NULL, b INT NULL"
                + ") ENGINE=OLAP DUPLICATE KEY(a, b)"
                + " DISTRIBUTED BY HASH(a) BUCKETS 3"
                + " PROPERTIES ('replication_allocation' = 'tag.location.default: 1');");
        createTable("CREATE TABLE test_agg_union.bitmap_tbl ("
                + "  id INT,"
                + "  tag INT,"
                + "  user_id BITMAP BITMAP_UNION"
                + ") ENGINE=OLAP AGGREGATE KEY(id, tag)"
                + " DISTRIBUTED BY HASH(id) BUCKETS 1"
                + " PROPERTIES ('replication_allocation' = 'tag.location.default: 1');");
        createTable("CREATE TABLE test_agg_union.decimal_tbl ("
                + "  f1 DECIMALV3(30, 5) NULL,"
                + "  f2 DECIMALV3(10, 6) NULL"
                + ") ENGINE=OLAP DUPLICATE KEY(f1)"
                + " DISTRIBUTED BY HASH(f1) BUCKETS 1"
                + " PROPERTIES ('replication_allocation' = 'tag.location.default: 1');");
    }

    @Test
    public void testAggUnionRandomDistributeUseOnePhase() {
        // Reproduces the exact bug scenario described in the issue
        String sql = "SELECT a, b FROM test_agg_union.t1_random GROUP BY a, b"
                + " UNION"
                + " SELECT a, b FROM test_agg_union.t2_random GROUP BY a, b";

        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            // The outer dedup agg should be one-phase (INPUT_TO_RESULT):
            //   - Before fix: BUFFER_TO_RESULT (two-phase, with redundant LOCAL agg)
            //   - After fix:  INPUT_TO_RESULT  (one-phase, no redundant LOCAL agg)
            MatchingUtils.assertMatches(planner.getOptimizedPlan(),
                    physicalResultSink(
                            physicalHashAggregate(any())
                                    .when(agg -> agg.getAggMode() == AggMode.INPUT_TO_RESULT)
                                    .when(agg -> agg.child(0) instanceof PhysicalUnion
                                            || hasUnionDescendant(agg.child(0)))
                    ));
        });
    }

    @Test
    public void testOuterAggOverUnionAllRandomUsesTwoPhase() {
        String sql = "SELECT a, b FROM"
                + " (SELECT a, b FROM test_agg_union.t1_random"
                + "  UNION ALL"
                + "  SELECT a, b FROM test_agg_union.t2_random) t"
                + " GROUP BY a, b";

        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            MatchingUtils.assertMatches(planner.getOptimizedPlan(),
                    physicalResultSink(
                            physicalHashAggregate(any())
                                    .when(agg -> agg.getAggMode() == AggMode.BUFFER_TO_RESULT)
                    ));
        });
    }

    @Test
    public void testNestedSetOperationDistinctUnionSingleInstance() {
        int beNumberForTest = connectContext.getSessionVariable().getBeNumberForTest();
        int parallelPipelineTaskNum = connectContext.getSessionVariable().parallelPipelineTaskNum;
        connectContext.getSessionVariable().setBeNumberForTest(1);
        connectContext.getSessionVariable().parallelPipelineTaskNum = 1;
        try {
            PlanChecker.from(connectContext).checkPlannerResult(
                    "SELECT * FROM (SELECT 1 a INTERSECT SELECT 1 a) t1"
                            + " UNION "
                            + "SELECT * FROM (SELECT 2 a EXCEPT SELECT 3 a) t2");
        } finally {
            connectContext.getSessionVariable().setBeNumberForTest(beNumberForTest);
            connectContext.getSessionVariable().parallelPipelineTaskNum = parallelPipelineTaskNum;
        }
    }

    @Test
    public void testTwoPhaseOnlyAggregateSingleInstance() {
        int beNumberForTest = connectContext.getSessionVariable().getBeNumberForTest();
        int parallelPipelineTaskNum = connectContext.getSessionVariable().parallelPipelineTaskNum;
        connectContext.getSessionVariable().setBeNumberForTest(1);
        connectContext.getSessionVariable().parallelPipelineTaskNum = 1;
        try {
            PlanChecker.from(connectContext).checkPlannerResult(
                    "SELECT orthogonal_bitmap_expr_calculate(user_id, tag, '(100&200)')"
                            + " FROM test_agg_union.bitmap_tbl");
            PlanChecker.from(connectContext).checkPlannerResult(
                    "SELECT orthogonal_bitmap_expr_calculate_count(user_id, tag, '(100&200)')"
                            + " FROM test_agg_union.bitmap_tbl");
        } finally {
            connectContext.getSessionVariable().setBeNumberForTest(beNumberForTest);
            connectContext.getSessionVariable().parallelPipelineTaskNum = parallelPipelineTaskNum;
        }
    }

    @Test
    public void testDecimal256GuardOnDistinctAggregateWithoutGroupBy() throws Exception {
        connectContext.getSessionVariable().enableDecimal256 = true;
        try {
            dropView("DROP VIEW IF EXISTS test_agg_union.v_decimal_distinct_sum");
            createView("CREATE VIEW test_agg_union.v_decimal_distinct_sum AS "
                    + "SELECT sum(DISTINCT f1 * f2) AS col_sum FROM test_agg_union.decimal_tbl");
        } finally {
            connectContext.getSessionVariable().enableDecimal256 = false;
        }

        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT * FROM test_agg_union.v_decimal_distinct_sum");
        Assertions.assertTrue(getSQLPlanOrErrorMsg(
                "EXPLAIN SELECT * FROM test_agg_union.v_decimal_distinct_sum").contains("PLAN FRAGMENT"));
    }

    /**
     * Walk up through PhysicalProject nodes to find if a PhysicalUnion sits below.
     * Used to handle optional project nodes that the optimizer may insert between
     * the dedup agg and the union.
     */
    private boolean hasUnionDescendant(org.apache.doris.nereids.trees.plans.Plan plan) {
        if (plan instanceof PhysicalUnion) {
            return true;
        }
        if (plan instanceof org.apache.doris.nereids.trees.plans.physical.PhysicalProject) {
            return hasUnionDescendant(plan.child(0));
        }
        return false;
    }
}
