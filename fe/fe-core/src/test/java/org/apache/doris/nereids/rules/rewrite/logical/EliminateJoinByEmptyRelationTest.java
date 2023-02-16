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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Test;

public class EliminateJoinByEmptyRelationTest implements PatternMatchSupported {
    private final LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalEmptyRelation empty = PlanConstructor.newEmptyRelation();

    @Test
    public void testEliminateInnerJoin() {
        LogicalPlan innerJoin = new LogicalPlanBuilder(scan)
                .join(empty, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), innerJoin)
                .applyBottomUp(new EliminateJoinByEmptyRelation())
                .matchesFromRoot(
                        logicalEmptyRelation()
                );
    }

    @Test
    public void testEliminateCrossJoin() {
        LogicalPlan crossJoin = new LogicalPlanBuilder(scan)
                .join(empty, JoinType.CROSS_JOIN, Pair.of(0, 0))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), crossJoin)
                .applyBottomUp(new EliminateJoinByEmptyRelation())
                .matchesFromRoot(
                        logicalEmptyRelation()
                );
    }

    @Test
    public void testEliminateSemiJoin() {
        //left-semi with empty left child => empty
        LogicalPlan join = new LogicalPlanBuilder(scan)
                .join(empty, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyBottomUp(new EliminateJoinByEmptyRelation())
                .matchesFromRoot(
                        logicalEmptyRelation()
                );

        //left-semi with empty right child => empty
        join = new LogicalPlanBuilder(empty)
                .join(scan, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyBottomUp(new EliminateJoinByEmptyRelation())
                .matchesFromRoot(
                        logicalEmptyRelation()
                );
    }

    @Test
    public void testEliminateOuterJoin() {
        //left-outer with empty left child => empty
        LogicalPlan leftJoin = new LogicalPlanBuilder(empty)
                .join(scan, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), leftJoin)
                .applyBottomUp(new EliminateJoinByEmptyRelation())
                .matchesFromRoot(
                        logicalEmptyRelation().when(
                                resEmpty -> resEmpty.getOutput().size() == leftJoin.getOutput().size()
                        )
                );
        //right-outer with empty right child => empty
        LogicalPlan rightJoin = new LogicalPlanBuilder(scan)
                .join(empty, JoinType.RIGHT_OUTER_JOIN, Pair.of(0, 0))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), rightJoin)
                .applyBottomUp(new EliminateJoinByEmptyRelation())
                .matchesFromRoot(
                        logicalEmptyRelation().when(
                                resEmpty -> resEmpty.getOutput().size() == rightJoin.getOutput().size()
                        )
                );
    }

    @Test
    public void testEliminateAntiJoin() {
        //left-Anti with empty left child => empty
        LogicalPlan leftJoin = new LogicalPlanBuilder(empty)
                .join(scan, JoinType.LEFT_ANTI_JOIN, Pair.of(0, 0))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), leftJoin)
                .applyBottomUp(new EliminateJoinByEmptyRelation())
                .matchesFromRoot(
                        logicalEmptyRelation().when(
                                resEmpty -> resEmpty.getOutput().size() == leftJoin.getOutput().size()
                        )
                );
        //left-anti with empty right child => left
        LogicalPlan leftJoin2 = new LogicalPlanBuilder(scan)
                .join(empty, JoinType.LEFT_ANTI_JOIN, Pair.of(0, 0))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), leftJoin2)
                .applyBottomUp(new EliminateJoinByEmptyRelation())
                .matchesFromRoot(
                        logicalOlapScan()
                );
    }
}
