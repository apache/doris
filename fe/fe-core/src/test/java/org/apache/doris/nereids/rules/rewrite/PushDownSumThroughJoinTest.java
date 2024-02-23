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
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
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

class PushDownSumThroughJoinTest implements MemoPatternMatchSupported {
    private static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private static final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private MockUp<SessionVariable> mockUp = new MockUp<SessionVariable>() {
        @Mock
        public Set<Integer> getEnableNereidsRules() {
            return ImmutableSet.of(RuleType.PUSH_DOWN_AGG_THROUGH_JOIN.type());
        }
    };

    @Test
    void testSingleJoinLeftSum() {
        Alias sum = new Sum(scan1.getOutput().get(1)).alias("sum");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), sum))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoin())
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
    void testSingleJoinRightSum() {
        Alias sum = new Sum(scan2.getOutput().get(1)).alias("sum");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), sum))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoin())
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
    void testSingleJoinBothSum() {
        Alias leftSum = new Sum(scan1.getOutput().get(1)).alias("leftSum");
        Alias rightSum = new Sum(scan2.getOutput().get(1)).alias("rightSum");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), leftSum, rightSum))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoin())
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
    void testAggNotOutputGroupBy() {
        // agg don't output group by
        Alias sum = new Sum(scan1.getOutput().get(1)).alias("sum");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(sum))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoin())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(),
                                        logicalAggregate()
                                )
                        )
                );
    }
}
