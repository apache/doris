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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class InnerJoinLeftAssociateTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    void testSimple() {
        /*
         * LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(id#4 = id#0)], otherJoinConjuncts=[] )
         * |--LogicalOlapScan ( qualified=db.t1, output=[id#4, name#5], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
         * +--LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(id#0 = id#2)], otherJoinConjuncts=[] )
         *    |--LogicalOlapScan ( qualified=db.t2, output=[id#0, name#1], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
         *    +--LogicalOlapScan ( qualified=db.t3, output=[id#2, name#3], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
         */
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(
                        new LogicalPlanBuilder(scan2)
                                .join(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                                .build(),
                        JoinType.INNER_JOIN, Pair.of(0, 0)
                )
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(InnerJoinLeftAssociate.INSTANCE.build())
                .printlnOrigin()
                .matchesExploration(
                        logicalJoin(
                                logicalJoin(
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2"))
                                ),
                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                        )
                );
    }

    @Test
    void testCheckReorderFailed() {
        JoinReorderContext joinReorderContext = new JoinReorderContext();

        joinReorderContext.setHasExchange(true);
        test(joinReorderContext);
        joinReorderContext.setHasExchange(false);

        joinReorderContext.setHasCommute(true);
        test(joinReorderContext);
        joinReorderContext.setHasCommute(false);

        joinReorderContext.setHasLeftAssociate(true);
        test(joinReorderContext);
        joinReorderContext.setHasLeftAssociate(false);

        joinReorderContext.setHasRightAssociate(true);
        test(joinReorderContext);
        joinReorderContext.setHasRightAssociate(false);
    }

    void test(JoinReorderContext joinReorderContext) {
        LogicalJoin topJoin = (LogicalJoin) new LogicalPlanBuilder(scan1)
                .join(
                        new LogicalPlanBuilder(scan2)
                                .join(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                                .build(),
                        JoinType.INNER_JOIN, Pair.of(0, 0)
                )
                .build();

        JoinReorderContext topJoinReorderContext = topJoin.getJoinReorderContext();
        topJoinReorderContext.copyFrom(joinReorderContext);

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(InnerJoinLeftAssociate.INSTANCE.build())
                .checkMemo(memo -> Assertions.assertEquals(1, memo.getRoot().getLogicalExpressions().size()));
    }
}
