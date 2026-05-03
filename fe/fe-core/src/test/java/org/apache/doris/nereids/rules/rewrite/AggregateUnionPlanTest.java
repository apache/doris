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

import org.junit.jupiter.api.Test;

/**
 * Tests for banAggUnionAll fix in ChildrenPropertiesRegulator.
 */
public class AggregateUnionPlanTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_agg_union");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");

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
