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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.util.Set;

class PushDownMinMaxThroughJoinTest implements MemoPatternMatchSupported {
    private static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private static final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private static final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
    private static final LogicalOlapScan scan4 = PlanConstructor.newLogicalOlapScan(3, "t4", 0);
    private MockUp<SessionVariable> mockUp = new MockUp<SessionVariable>() {
        @Mock
        public Set<Integer> getEnableNereidsRules() {
            return ImmutableSet.of(RuleType.PUSH_DOWN_MIN_MAX_THROUGH_JOIN.type());
        }
    };

    @Test
    void testSingleJoin() {
        Alias min = new Min(scan1.getOutput().get(0)).alias("min");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), min))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownMinMaxThroughJoin())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(),
                                        logicalOlapScan()
                                )
                        )
                );
    }

    @Test
    void testMultiJoin() {
        Alias min = new Min(scan1.getOutput().get(0)).alias("min");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .join(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                .join(scan4, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), min))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownMinMaxThroughJoin())
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(
                                                logicalJoin(
                                                        logicalAggregate(
                                                                logicalJoin(
                                                                        logicalAggregate(),
                                                                        logicalOlapScan()
                                                                )
                                                        ),
                                                        logicalOlapScan()
                                                )
                                        ),
                                        logicalOlapScan()
                                )
                        )
                );
    }

    @Test
    void testAggNotOutputGroupBy() {
        // agg don't output group by
        Alias min = new Min(scan1.getOutput().get(0)).alias("min");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .join(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(min))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownMinMaxThroughJoin())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(
                                                logicalJoin(
                                                        logicalAggregate(),
                                                        logicalOlapScan()
                                                )
                                        ),
                                        logicalOlapScan()
                                )
                        )
                );
    }

    @Test
    void testBothSideSingleJoin() {
        Alias min = new Min(scan1.getOutput().get(1)).alias("min");
        Alias max = new Max(scan2.getOutput().get(1)).alias("max");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), min, max))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .printlnTree()
                .applyTopDown(new PushDownMinMaxThroughJoin())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(),
                                        logicalAggregate()
                                )
                        )
                );
    }

    @Test
    void testBothSide() {
        Alias min = new Min(scan1.getOutput().get(1)).alias("min");
        Alias max = new Max(scan3.getOutput().get(1)).alias("max");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .join(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(min, max))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownMinMaxThroughJoin())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(
                                                logicalJoin(
                                                        logicalAggregate(),
                                                        logicalOlapScan()
                                                )
                                        ),
                                        logicalAggregate()
                                )
                        )
                );
    }
}
