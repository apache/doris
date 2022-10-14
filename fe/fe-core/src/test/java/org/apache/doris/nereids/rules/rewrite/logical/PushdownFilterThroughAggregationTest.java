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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class PushdownFilterThroughAggregationTest implements PatternMatchSupported {

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
        LogicalPlan scan = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student,
                ImmutableList.of(""));
        Slot gender = scan.getOutput().get(1);

        Expression filterPredicate = new GreaterThan(gender, Literal.of(1));
        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .aggAllUsingIndex(ImmutableList.of(3, 1), ImmutableList.of(1, 3))
                .filter(filterPredicate)
                .project(ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushdownFilterThroughAggregation())
                .matches(
                        logicalProject(
                                logicalAggregate(
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filter -> filter.getPredicates().equals(filterPredicate))
                                )
                        )
                );
    }

    /*-
     * origin plan:
     *                project
     *                  |
     *                filter gender=1 and name="abc" and (gender+10)<100
     *                  |
     *               aggregation group by gender
     *                  |
     *               scan(student)
     *
     *  transformed plan:
     *                project
     *                  |
     *                filter name="abc"
     *                  |
     *               aggregation group by gender
     *                  |
     *               filter gender=1 and  and (gender+10)<100
     *                  |
     *               scan(student)
     */
    @Test
    public void pushDownPredicateTwoFilterTest() {
        LogicalPlan scan = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student,
                ImmutableList.of(""));
        Slot gender = scan.getOutput().get(1);
        Slot name = scan.getOutput().get(2);

        Expression filterPredicate = ExpressionUtils.and(
                new GreaterThan(gender, Literal.of(1)),
                new LessThanEqual(
                        new Add(gender, Literal.of(10)),
                        Literal.of(100)),
                new EqualTo(name, Literal.of("abc")));

        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .aggAllUsingIndex(ImmutableList.of(3, 1), ImmutableList.of(1, 3))
                .filter(filterPredicate)
                .project(ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushdownFilterThroughAggregation())
                .printlnTree()
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalAggregate(
                                                logicalFilter(
                                                        logicalOlapScan()
                                                ).when(filter -> filter.getPredicates().child(0) instanceof GreaterThan)
                                                        .when(filter -> filter.getPredicates()
                                                                .child(1) instanceof LessThanEqual)
                                        )
                                ).when(filter -> filter.getPredicates() instanceof EqualTo)
                        )
                );
    }

}
