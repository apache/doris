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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class InferFilterNotNullTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void testInfer() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .filter(new EqualTo(new Add(scan1.getOutput().get(0), scan1.getOutput().get(1)), Literal.of(1)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .matches(
                        // LogicalFilter ( predicates=((((id#0 + name#1) = 1) AND ( not id IS NULL)) AND ( not name IS NULL)) )
                        logicalFilter().when(filter -> filter.getConjuncts().size() == 3)
                );
    }

    @Test
    void testInferFail() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .filter(new IsNull(scan1.getOutput().get(0)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .matches(
                        // LogicalFilter ( predicates=id IS NULL )
                        logicalFilter().when(filter -> filter.getConjuncts().size() == 1)
                );
    }

    @Test
    void testInferFailOr() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .filter(new Or(new IsNull(scan1.getOutput().get(0)),
                        new EqualTo(scan1.getOutput().get(0), Literal.of(1))))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .matches(
                        // LogicalFilter ( predicates=(id IS NULL OR (id#0 = 1)) )
                        logicalFilter().when(filter -> filter.getConjuncts().size() == 1)
                );
    }

    @Test
    void testSkipWidePredicateButKeepSimplePredicate() {
        Expression widePredicate = new EqualTo(repeatAdd(scan1.getOutput().get(0), 257), Literal.of(1));
        Expression simplePredicate = new EqualTo(scan1.getOutput().get(1), Literal.of(1));
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .filter(ImmutableSet.of(widePredicate, simplePredicate))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .matches(
                        logicalFilter().when(filter -> filter.getConjuncts().stream()
                                .filter(e -> e instanceof Not && ((Not) e).isGeneratedIsNotNull())
                                .count() == 1)
                );
    }

    private Expression repeatAdd(Expression expression, int width) {
        if (width == 1) {
            return expression;
        }
        int leftWidth = width / 2;
        return new Add(repeatAdd(expression, leftWidth), repeatAdd(expression, width - leftWidth));
    }
}
