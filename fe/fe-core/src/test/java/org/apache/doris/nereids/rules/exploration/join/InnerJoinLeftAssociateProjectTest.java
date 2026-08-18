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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
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
import org.junit.jupiter.api.Test;

import java.util.List;

class InnerJoinLeftAssociateProjectTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    void testSimple() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(
                        new LogicalPlanBuilder(scan2)
                                .join(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                                .project(ImmutableList.of(0, 2))
                                .build(),
                        JoinType.INNER_JOIN, Pair.of(0, 0)
                )
                .projectAll()
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(InnerJoinLeftAssociateProject.INSTANCE.build())
                .printlnExploration()
                .matchesExploration(
                        logicalProject(logicalJoin(
                                logicalProject(
                                        logicalJoin(
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2"))
                                        )
                                ),
                                logicalProject(
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                )
                        ))
                );
    }

    @Test
    void testRejectedWhenSensitiveConjunct() {
        /*
         * the LeftAssociate reorder must be rejected when a conjunct contains a
         * NoneMovableFunction (assert_true): the reorder would move the conjunct referencing C
         * from A x (B join C)'s evaluation to (A join B) join C, where A rows pruned by B no
         * longer reach it and its required error is suppressed. the plan keeps the original
         * order.
         */
        List<Expression> bottomHashConjunct = ImmutableList.of(
                new EqualTo(scan2.getOutput().get(0), scan3.getOutput().get(0)));
        List<Expression> bottomOtherConjunct = ImmutableList.of(new AssertTrue(
                new GreaterThan(scan3.getOutput().get(0), new IntegerLiteral(0)), new StringLiteral("msg")));
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(
                        new LogicalPlanBuilder(scan2)
                                .join(scan3, JoinType.INNER_JOIN, bottomHashConjunct, bottomOtherConjunct)
                                .project(ImmutableList.of(0, 2))
                                .build(),
                        JoinType.INNER_JOIN, Pair.of(0, 0)
                )
                .projectAll()
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(InnerJoinLeftAssociateProject.INSTANCE.build())
                // the transposed shape (A join B) join C must not appear
                .nonMatch(
                        logicalProject(
                                innerLogicalJoin(
                                        logicalProject(innerLogicalJoin(
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2"))
                                        )),
                                        logicalProject(group())
                                )
                        )
                );
    }
}
