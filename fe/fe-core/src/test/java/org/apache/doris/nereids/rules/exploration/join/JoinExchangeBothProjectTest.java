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

class JoinExchangeBothProjectTest implements MemoPatternMatchSupported {
    @Test
    public void testSimple() {
        LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
        LogicalOlapScan scan4 = PlanConstructor.newLogicalOlapScan(3, "t4", 0);

        LogicalPlan plan = new LogicalPlanBuilder(
                new LogicalPlanBuilder(scan1)
                        .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                        .project(ImmutableList.of(0, 2))
                        .build())
                .join(
                        new LogicalPlanBuilder(scan3)
                                .join(scan4, JoinType.INNER_JOIN, Pair.of(0, 0))
                                .project(ImmutableList.of(0, 2))
                                .build(),
                        JoinType.INNER_JOIN, ImmutableList.of(Pair.of(0, 0), Pair.of(1, 1)))
                .projectAll()
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(JoinExchangeBothProject.INSTANCE.build())
                .matchesExploration(
                    logicalProject(
                        logicalJoin(
                            logicalProject(
                                logicalJoin(
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                )
                            ),
                            logicalProject(
                                logicalJoin(
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2")),
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t4"))
                                )
                            )
                        )
                    )
                );
    }

    @Test
    public void testRejectedWhenSensitiveConjunct() {
        /*
         * the exchange reorder must be rejected when a conjunct contains a NoneMovableFunction
         * (assert_true): the reorder would move the conjunct from (A join B) x (C join D)
         * evaluation to (A join C) join (B join D), where rows pruned by the new inner joins
         * no longer reach it and its required error is suppressed. the plan keeps the original
         * order.
         */
        LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
        LogicalOlapScan scan4 = PlanConstructor.newLogicalOlapScan(3, "t4", 0);
        List<Expression> leftHashConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan2.getOutput().get(0)));
        List<Expression> leftOtherConjunct = ImmutableList.of(new AssertTrue(
                new GreaterThan(scan2.getOutput().get(0), new IntegerLiteral(0)), new StringLiteral("msg")));
        LogicalPlan plan = new LogicalPlanBuilder(
                new LogicalPlanBuilder(scan1)
                        .join(scan2, JoinType.INNER_JOIN, leftHashConjunct, leftOtherConjunct)
                        .project(ImmutableList.of(0, 1, 2))
                        .build())
                .join(
                        new LogicalPlanBuilder(scan3)
                                .join(scan4, JoinType.INNER_JOIN, Pair.of(0, 0))
                                .project(ImmutableList.of(0, 2))
                                .build(),
                        JoinType.INNER_JOIN, ImmutableList.of(Pair.of(0, 0), Pair.of(2, 1)))
                .projectAll()
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(JoinExchangeBothProject.INSTANCE.build())
                // the transposed shape (A join C) join (B join D) must not appear
                .nonMatch(
                        logicalProject(
                                innerLogicalJoin(
                                        logicalProject(innerLogicalJoin(
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                        )),
                                        logicalProject(innerLogicalJoin(
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2")),
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t4"))
                                        ))
                                )
                        )
                );
    }
}
