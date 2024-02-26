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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class PushDownFilterThroughAggregationTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
            ImmutableList.of(""));

    /*-
     * origin plan:
     *                project
     *                  |
     *                filter gender=1
     *                  |
     *               aggregation group by gender
     *                  |
     *               scan(student)
     *
     *  transformed plan:
     *                project
     *                  |
     *               aggregation group by gender
     *                  |
     *               filter gender=1
     *                  |
     *               scan(student)
     */
    @Test
    public void pushDownPredicateOneFilterTest() {
        Slot gender = scan.getOutput().get(1);

        Expression filterPredicate = new GreaterThan(gender, Literal.of(1));
        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .aggAllUsingIndex(ImmutableList.of(3, 1), ImmutableList.of(1, 3))
                .filter(filterPredicate)
                .project(ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownFilterThroughAggregation())
                .matches(
                        logicalProject(
                                logicalAggregate(
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filter -> filter.getConjuncts().equals(ImmutableSet.of(filterPredicate)))
                                )
                        )
                );
    }

    @Test
    void scalarAgg() {
        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .agg(ImmutableList.of(), ImmutableList.of((new Sum(scan.getOutput().get(0))).alias("sum")))
                .filter(new If(Literal.of(false), Literal.of(false), Literal.of(false)))
                .project(ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownFilterThroughAggregation())
                .printlnTree()
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalAggregate(
                                                logicalOlapScan()
                                        )
                                )
                        )
                );
    }

    /*-
     * origin plan:
     *                project
     *                  |
     *                filter gender=1 and nameMax="abc" and (gender+10)<100
     *                  |
     *               aggregation group by gender
     *                  |
     *               scan(student)
     *
     *  transformed plan:
     *                project
     *                  |
     *                filter nameMax="abc"
     *                  |
     *               aggregation group by gender
     *                  |
     *               filter gender=1 and  and (gender+10)<100
     *                  |
     *               scan(student)
     */
    @Test
    public void pushDownPredicateTwoFilterTest() {
        Slot gender = scan.getOutput().get(1);
        NamedExpression nameMax = new Alias(new Max(scan.getOutput().get(2)), "nameMax");

        Expression filterPredicate = ExpressionUtils.and(
                new GreaterThan(gender, Literal.of(1)),
                new LessThanEqual(
                        new Add(gender, Literal.of(10)),
                        Literal.of(100)),
                new EqualTo(nameMax.toSlot(), Literal.of("abc")));

        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .aggGroupUsingIndex(ImmutableList.of(3, 1), ImmutableList.of(
                        scan.getOutput().get(1), scan.getOutput().get(3), nameMax))
                .filter(filterPredicate)
                .project(ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownFilterThroughAggregation())
                .printlnTree()
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalAggregate(
                                                logicalFilter(
                                                        logicalOlapScan()
                                                ).when(filter -> ImmutableList.copyOf(filter.getConjuncts()).get(0) instanceof GreaterThan
                                                        && ImmutableList.copyOf(filter.getConjuncts()).get(1) instanceof LessThanEqual)
                                        )
                                ).when(filter -> ImmutableList.copyOf(filter.getConjuncts()).get(0) instanceof EqualTo)
                        )
                );
    }

    @Test
    public void pushDownPredicateGroupWithRepeatTest() {
        Slot id = scan.getOutput().get(0);
        Slot gender = scan.getOutput().get(1);
        Slot name = scan.getOutput().get(2);
        LogicalRepeat repeatPlan = new LogicalRepeat<>(
                ImmutableList.of(ImmutableList.of(id, gender), ImmutableList.of(id)),
                ImmutableList.of(id, gender, name), scan);
        NamedExpression nameMax = new Alias(new Max(name), "nameMax");

        final Expression filterPredicateId = new GreaterThan(id, Literal.of(1));
        LogicalPlan plan = new LogicalPlanBuilder(repeatPlan)
                .aggGroupUsingIndexAndSourceRepeat(ImmutableList.of(0, 1), ImmutableList.of(
                        id, gender, nameMax), Optional.of(repeatPlan))
                .filter(filterPredicateId)
                .project(ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownFilterThroughAggregation())
                .printlnTree()
                .matches(
                        logicalProject(
                                logicalAggregate(
                                        logicalFilter(
                                                logicalRepeat()
                                        ).when(filter -> filter.getConjuncts()
                                                .equals(ImmutableSet.of(filterPredicateId)))
                                )
                        )
                );

        repeatPlan = new LogicalRepeat<>(
                ImmutableList.of(ImmutableList.of(id, gender), ImmutableList.of(gender)),
                ImmutableList.of(id, gender, name), scan);
        plan = new LogicalPlanBuilder(repeatPlan)
                .aggGroupUsingIndexAndSourceRepeat(ImmutableList.of(0, 1), ImmutableList.of(
                        id, gender, nameMax), Optional.of(repeatPlan))
                .filter(filterPredicateId)
                .project(ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownFilterThroughAggregation())
                .printlnTree()
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalAggregate(
                                                logicalRepeat()
                                        )
                                ).when(filter -> filter.getConjuncts().equals(ImmutableSet.of(filterPredicateId)))
                        )
                );
    }

}
