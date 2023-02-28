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
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

public class InnerJoinLAsscomTest implements MemoPatternMatchSupported {

    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    public void testJoinLAsscom() {
        /*
         * Star-Join
         * t1 -- t2
         * |
         * t3
         * <p>
         *     t1.id=t3.id               t1.id=t2.id
         *       topJoin                  newTopJoin
         *       /     \                   /     \
         * t1.id=t2.id  t3          t1.id=t3.id   t2
         * bottomJoin       -->    newBottomJoin
         *   /    \                   /    \
         * t1      t2               t1      t3
         */

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .join(scan3, JoinType.INNER_JOIN, Pair.of(1, 1))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(InnerJoinLAsscom.INSTANCE.build())
                .matchesExploration(
                        logicalJoin(
                                logicalJoin(
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                ),
                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2"))
                        )
                )
                .printlnExploration();
    }

    @Test
    public void testHashAndOther() {
        List<Expression> bottomHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan2.getOutput().get(0)));
        List<Expression> bottomOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan2.getOutput().get(1)));
        List<Expression> topHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan3.getOutput().get(0)),
                new EqualTo(scan2.getOutput().get(0), scan3.getOutput().get(0)));
        List<Expression> topOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan3.getOutput().get(1)),
                new GreaterThan(scan2.getOutput().get(1), scan3.getOutput().get(1)));

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, bottomHashJoinConjunct, bottomOtherJoinConjunct)
                .join(scan3, JoinType.INNER_JOIN, topHashJoinConjunct, topOtherJoinConjunct)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(InnerJoinLAsscom.INSTANCE.build())
                .matchesExploration(
                        logicalJoin(
                                logicalJoin(
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                ),
                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2"))
                        )
                )
                .printlnExploration();
    }

    @Test
    public void testComplexConjuncts() {
        List<Expression> bottomHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan2.getOutput().get(0)));
        List<Expression> bottomOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan2.getOutput().get(1)));
        List<Expression> topHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan3.getOutput().get(0)),
                new EqualTo(scan2.getOutput().get(0), new Add(scan1.getOutput().get(0), scan3.getOutput().get(0))));
        List<Expression> topOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan3.getOutput().get(1)),
                new GreaterThan(scan2.getOutput().get(0), new Add(scan1.getOutput().get(0), scan3.getOutput().get(0))));

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, bottomHashJoinConjunct, bottomOtherJoinConjunct)
                .join(scan3, JoinType.INNER_JOIN, topHashJoinConjunct, topOtherJoinConjunct)
                .build();

        // test for no exception
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .printlnTree()
                .applyExploration(InnerJoinLAsscom.INSTANCE.build());
    }

    @Test
    public void testComplexConjunctsWithSubString() {
        List<Expression> bottomHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan2.getOutput().get(0)));
        List<Expression> bottomOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan2.getOutput().get(1)));
        List<Expression> topHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan3.getOutput().get(0)),
                new EqualTo(scan2.getOutput().get(0), new Add(scan1.getOutput().get(0), scan3.getOutput().get(0))));
        List<Expression> topOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan3.getOutput().get(1)),
                new Not(new EqualTo(new Substring(scan1.getOutput().get(1),
                        new Cast(new StringLiteral("1"), IntegerType.INSTANCE),
                        new Cast(new StringLiteral("3"), IntegerType.INSTANCE)),
                        Literal.of("abc"))));

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, bottomHashJoinConjunct, bottomOtherJoinConjunct)
                .join(scan3, JoinType.INNER_JOIN, topHashJoinConjunct, topOtherJoinConjunct)
                .build();

        // test for no exception
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .printlnTree()
                .applyExploration(InnerJoinLAsscom.INSTANCE.build());
    }
}
