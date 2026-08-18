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

package org.apache.doris.nereids.rules.exploration;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TransposeAggSemiJoinTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);

    @Test
    void simple() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0),
                        ImmutableList.of(
                                scan1.getOutput().get(0),
                                new Alias(new Sum(scan1.getOutput().get(1)), "sum")
                        )
                )
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(TransposeAggSemiJoin.INSTANCE.build())
                .printlnExploration()
                .matchesExploration(
                        leftSemiLogicalJoin(
                                logicalAggregate(),
                                logicalOlapScan()
                        )
                );
    }

    @Test
    void markJoin() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .markJoin(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0),
                        ImmutableList.of(
                                scan1.getOutput().get(0),
                                new Alias(new Sum(scan1.getOutput().get(1)), "sum")
                        )
                )
                .build();
        int size = PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(TransposeAggSemiJoin.INSTANCE.build())
                .getAllPlan().size();
        Assertions.assertEquals(1, size);
    }

    @Test
    void testTransposeAggSemiJoinProjectRejectedWhenProjectContainsNoneMovableFunction() {
        /*
         * agg(project(assert_true(t1.id > 0))(t1 LEFT SEMI JOIN t2)): the transpose would
         * move the project below the aggregate and above the semi join's left input, so the
         * assertion would run on rows the semi join removes (t1 rows with no t2 match),
         * turning returned rows into errors. the transpose must be rejected.
         */
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .projectExprs(ImmutableList.of(
                        scan1.getOutput().get(0),
                        scan1.getOutput().get(1),
                        new Alias(new AssertTrue(
                                new GreaterThan(scan1.getOutput().get(0), Literal.of(0)),
                                new StringLiteral("msg")), "ok")
                ))
                .aggGroupUsingIndex(ImmutableList.of(0),
                        ImmutableList.of(
                                scan1.getOutput().get(0),
                                new Alias(new Sum(scan1.getOutput().get(1)), "sum")
                        )
                )
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(TransposeAggSemiJoinProject.INSTANCE.build())
                .checkMemo(memo -> Assertions.assertEquals(1, memo.getRoot().getLogicalExpressions().size()));
    }
}
