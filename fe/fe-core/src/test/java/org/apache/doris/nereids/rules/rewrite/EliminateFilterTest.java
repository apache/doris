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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

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
    void testEliminateFilterReduceNull() {
        List<Expression> exprList = Arrays.asList(
                new EqualTo(scan1.getOutput().get(0), Literal.of(1)),
                new GreaterThan(scan1.getOutput().get(1), Literal.of(1)),
                NullLiteral.INSTANCE,
                NullLiteral.INSTANCE);
        Expression expr = new Or(ExpressionUtils.falseOrNull(scan1.getOutput().get(1)),
                new GreaterThan(scan1.getOutput().get(0), Literal.of(1)));
        LogicalPlan filter = new LogicalPlanBuilder(scan1)
                .filter(expr)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new EliminateFilter())
                .matches(
                    logicalFilter().when(f -> f.getPredicate().toSql().equals("(id > 1)"))
                );

        expr = new Or(ExpressionUtils.falseOrNull(scan1.getOutput().get(0)),
                ExpressionUtils.falseOrNull(scan1.getOutput().get(1)));
        filter = new LogicalPlanBuilder(scan1)
                .filter(expr)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new EliminateFilter())
                .matches(logicalEmptyRelation());

        filter = new LogicalPlanBuilder(scan1)
                .filter(new And(exprList))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new EliminateFilter())
                .matches(logicalEmptyRelation());

        filter = new LogicalPlanBuilder(scan1)
                .filter(new And(NullLiteral.INSTANCE, new Or(exprList)))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new EliminateFilter())
                .matches(logicalEmptyRelation());

        filter = new LogicalPlanBuilder(scan1)
                .filter(new Or(NullLiteral.INSTANCE, NullLiteral.INSTANCE))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new EliminateFilter())
                .matches(logicalEmptyRelation());

        filter = new LogicalPlanBuilder(scan1)
                .filter(NullLiteral.INSTANCE)
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new EliminateFilter())
                .matches(logicalEmptyRelation());

        filter = new LogicalPlanBuilder(scan1)
                .filter(new Not(NullLiteral.INSTANCE))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new EliminateFilter())
                .matches(logicalEmptyRelation());

        filter = new LogicalPlanBuilder(scan1)
                .filter(new Not(new And(exprList)))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new EliminateFilter())
                .matches(logicalFilter().when(
                        f -> f.getPredicate().toSql().equals("( not AND[(id = 1),(name > 1),NULL,NULL])"))
                );
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

    @Test
    void testEliminateNullLiteral() {
        Expression a = new SlotReference("a", IntegerType.INSTANCE);
        Expression b = new SlotReference("b", IntegerType.INSTANCE);
        Expression one = Literal.of(1);
        Expression two = Literal.of(2);
        Expression expression = new And(Arrays.asList(
               new And(new GreaterThan(a, one), new NullLiteral(IntegerType.INSTANCE)),
               new Or(Arrays.asList(new GreaterThan(b, two), new NullLiteral(IntegerType.INSTANCE),
                       new EqualTo(a, new NullLiteral(IntegerType.INSTANCE)))),
               new Not(new And(new GreaterThan(a, one), new NullLiteral(IntegerType.INSTANCE)))
        ));
        Expression expectExpression = new And(Arrays.asList(
                new And(new GreaterThan(a, one), BooleanLiteral.FALSE),
                new Or(Arrays.asList(new GreaterThan(b, two), BooleanLiteral.FALSE,
                        new EqualTo(a, new NullLiteral(IntegerType.INSTANCE)))),
                new Not(new And(new GreaterThan(a, one), new NullLiteral(IntegerType.INSTANCE)))
        ));
        Assertions.assertEquals(expectExpression, new EliminateFilter().eliminateNullLiteral(expression));
    }
}
