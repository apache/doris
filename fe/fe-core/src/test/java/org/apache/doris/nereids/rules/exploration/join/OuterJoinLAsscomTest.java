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
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
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

import java.util.List;

public class OuterJoinLAsscomTest implements MemoPatternMatchSupported {

    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    public void testLAsscom() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .join(scan3, JoinType.LEFT_OUTER_JOIN, Pair.of(1, 1))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(OuterJoinLAsscom.INSTANCE.build())
                .matchesExploration(
                        leftOuterLogicalJoin(
                                leftOuterLogicalJoin(
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                ),
                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2"))
                        )
                );
    }

    @Test
    public void testOtherAndHash() {
        List<Expression> bottomHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan2.getOutput().get(0)));
        List<Expression> bottomOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan2.getOutput().get(1)));
        List<Expression> topHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan3.getOutput().get(0)));
        List<Expression> topOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan3.getOutput().get(1)));

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, bottomHashJoinConjunct, bottomOtherJoinConjunct)
                .join(scan3, JoinType.LEFT_OUTER_JOIN, topHashJoinConjunct, topOtherJoinConjunct)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(OuterJoinLAsscom.INSTANCE.build())
                .printlnExploration()
                .matchesExploration(
                        leftOuterLogicalJoin(
                                leftOuterLogicalJoin(
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                ).when(join -> join.getOtherJoinConjuncts().size() == 1
                                        && join.getHashJoinConjuncts().size() == 1),
                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2"))
                        ).when(join -> join.getOtherJoinConjuncts().size() == 1
                                && join.getHashJoinConjuncts().size() == 1)
                );
    }

    /* ****************** failed exchange ****************** */
    @Test
    public void testTopHashContainsB() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, ImmutableList.of(Pair.of(0, 0)))
                .join(scan3, JoinType.LEFT_OUTER_JOIN, ImmutableList.of(Pair.of(0, 0), Pair.of(2, 0)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .printlnTree()
                .applyExploration(OuterJoinLAsscom.INSTANCE.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(1, root.getLogicalExpressions().size());
                });
    }

    @Test
    public void testOtherHashContainsB() {
        List<Expression> topHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan3.getOutput().get(0)));
        List<Expression> topOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan3.getOutput().get(1)),
                new GreaterThan(scan2.getOutput().get(1), scan3.getOutput().get(1)));

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, ImmutableList.of(Pair.of(0, 0)))
                .join(scan3, JoinType.LEFT_OUTER_JOIN, topHashJoinConjunct, topOtherJoinConjunct)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .printlnTree()
                .applyExploration(InnerJoinLAsscom.INSTANCE.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(1, root.getLogicalExpressions().size());
                });
    }
}
