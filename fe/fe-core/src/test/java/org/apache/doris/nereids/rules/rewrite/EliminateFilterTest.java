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

import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link EliminateFilter}.
 */
class EliminateFilterTest implements MemoPatternMatchSupported {
    private static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void testEliminateFilterFalse() {
        LogicalPlan filterFalse = new LogicalPlanBuilder(scan1)
                .filter(BooleanLiteral.FALSE)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), filterFalse)
                .applyTopDown(new EliminateFilter())
                .matches(logicalEmptyRelation());
    }

    @Test
    void testEliminateFilterNull() {
        LogicalPlan filterNull = new LogicalPlanBuilder(scan1)
                .filter(NullLiteral.INSTANCE)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), filterNull)
                .applyTopDown(new EliminateFilter())
                .matches(logicalEmptyRelation());
    }

    @Test
    void testEliminateFilterTrue() {
        LogicalPlan filterTrue = new LogicalPlanBuilder(scan1)
                .filter(BooleanLiteral.TRUE)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), filterTrue)
                .applyTopDown(new EliminateFilter())
                .matches(logicalOlapScan());
    }

    @Test
    void testEliminateOneFilterTrue() {
        And expr = new And(BooleanLiteral.TRUE, new GreaterThan(scan1.getOutput().get(0), Literal.of("1")));
        LogicalPlan filter = new LogicalPlanBuilder(scan1)
                .filter(expr)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new EliminateFilter())
                .applyBottomUp(new ExpressionNormalization())
                .matches(
                        logicalFilter(logicalOlapScan()).when(f -> f.getPredicate() instanceof GreaterThan)
                );
    }

    @Test
    void testEliminateOneFilterFalse() {
        Or expr = new Or(BooleanLiteral.FALSE, new GreaterThan(scan1.getOutput().get(0), Literal.of("1")));
        LogicalPlan filter = new LogicalPlanBuilder(scan1)
                .filter(expr)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new EliminateFilter())
                .applyBottomUp(new ExpressionNormalization())
                .matches(
                        logicalFilter(logicalOlapScan()).when(f -> f.getPredicate() instanceof GreaterThan)
                );
    }
}
