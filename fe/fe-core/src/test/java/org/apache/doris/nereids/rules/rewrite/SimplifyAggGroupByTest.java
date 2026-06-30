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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

class SimplifyAggGroupByTest implements MemoPatternMatchSupported {
    private static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void test() {
        Slot id = scan1.getOutput().get(0);
        List<NamedExpression> output = ImmutableList.of(
                id,
                new Add(id, Literal.of(1)).alias("id1"),
                new Add(id, Literal.of(2)).alias("id2"),
                new Add(id, Literal.of(3)).alias("id3"),
                new Count().alias("count")
        );
        List<Expression> groupBy = ImmutableList.of(
                id,
                new Add(id, Literal.of(1)),
                new Add(id, Literal.of(2)),
                new Add(id, Literal.of(3))
        );
        LogicalPlan agg = new LogicalPlanBuilder(scan1)
                .agg(groupBy, output)
                .build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        PlanChecker.from(connectContext, agg)
                .applyTopDown(new SimplifyAggGroupBy())
                .matchesFromRoot(
                        logicalAggregate().when(a -> a.getGroupByExpressions().size() == 1)
                );
        PlanChecker.from(connectContext, agg)
                .analyze()
                .matchesFromRoot(
                        logicalProject(logicalAggregate().when(a -> a.getGroupByExpressions().size() == 1))
                );
    }

    @Test
    void testSqrt() {
        Slot id = scan1.getOutput().get(0);
        List<NamedExpression> output = ImmutableList.of(
                id,
                new Multiply(id, id).alias("sqrt"),
                new Count().alias("count")
        );
        List<Expression> groupBy = ImmutableList.of(
                id,
                new Multiply(id, id)
        );
        LogicalPlan agg = new LogicalPlanBuilder(scan1)
                .agg(groupBy, output)
                .build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        PlanChecker.from(connectContext, agg)
                .applyTopDown(new SimplifyAggGroupBy())
                .matchesFromRoot(
                        logicalAggregate().when(a -> a.equals(agg))
                );
        PlanChecker.from(connectContext, agg)
                .analyze()
                .matchesFromRoot(
                        logicalProject(logicalAggregate().when(a -> a.getGroupByExpressions().size() == 2))
                );
    }

    @Test
    void testAbs() {
        Slot id = scan1.getOutput().get(0);
        List<NamedExpression> output = ImmutableList.of(
                id,
                new Abs(id).alias("abs"),
                new Count().alias("count")
        );
        List<Expression> groupBy = ImmutableList.of(
                id,
                new Abs(id)
        );
        LogicalPlan agg = new LogicalPlanBuilder(scan1)
                .agg(groupBy, output)
                .build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        PlanChecker.from(connectContext, agg)
                .applyTopDown(new SimplifyAggGroupBy())
                .matchesFromRoot(
                        logicalAggregate().when(a -> a.equals(agg))
                );
        PlanChecker.from(connectContext, agg)
                .analyze()
                .matchesFromRoot(
                        logicalProject(logicalAggregate().when(a -> a.getGroupByExpressions().size() == 2))
                );
    }

    @Test
    void testisBinaryArithmeticSlot() {
        Slot id = scan1.getOutput().get(0);

        Mod mod = new Mod(id, Literal.of(2));
        Assertions.assertFalse(SimplifyAggGroupBy.isBinaryArithmeticSlot(mod));

        Add add = new Add(id, Literal.of(2));
        Assertions.assertTrue(SimplifyAggGroupBy.isBinaryArithmeticSlot(add));

        Subtract subtract = new Subtract(id, Literal.of(2));
        Assertions.assertTrue(SimplifyAggGroupBy.isBinaryArithmeticSlot(subtract));

        Multiply multiply = new Multiply(id, Literal.of(2));
        Assertions.assertTrue(SimplifyAggGroupBy.isBinaryArithmeticSlot(multiply));

        Divide divide = new Divide(id, Literal.of(2));
        Assertions.assertTrue(SimplifyAggGroupBy.isBinaryArithmeticSlot(divide));
    }

    // ========== new tests for injectivity checks ==========

    @Test
    void testMultiplyByZero() {
        Slot id = scan1.getOutput().get(0);
        Assertions.assertFalse(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Multiply(id, Literal.of(0))));
        Assertions.assertFalse(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Multiply(Literal.of(0), id)));
    }

    @Test
    void testDivideZeroNumerator() {
        Slot id = scan1.getOutput().get(0);
        Assertions.assertFalse(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Divide(Literal.of(0), id)));
    }

    @Test
    void testDivideByZero() {
        Slot id = scan1.getOutput().get(0);
        Assertions.assertFalse(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Divide(id, Literal.of(0))));
    }

    @Test
    void testNullLiteral() {
        Slot id = scan1.getOutput().get(0);
        Assertions.assertFalse(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Add(id, NullLiteral.INSTANCE)));
        Assertions.assertFalse(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Multiply(id, NullLiteral.INSTANCE)));
    }

    @Test
    void testMultiplyWithDoubleLiteral() {
        Slot id = scan1.getOutput().get(0);
        Assertions.assertFalse(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Multiply(id, new DoubleLiteral(0.1))));
    }

    @Test
    void testDivideWithDoubleLiteral() {
        Slot id = scan1.getOutput().get(0);
        Assertions.assertFalse(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Divide(id, new DoubleLiteral(2.0))));
    }

    @Test
    void testMultiplyWithFloatSlot() {
        Slot floatSlot = new SlotReference("f", FloatType.INSTANCE);
        Assertions.assertFalse(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Multiply(floatSlot, Literal.of(2))));
    }

    @Test
    void testMultiplyDoubleSlotWithIntLiteral() {
        Slot doubleSlot = new SlotReference("d", DoubleType.INSTANCE);
        Assertions.assertFalse(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Multiply(doubleSlot, Literal.of(2))));
    }

    @Test
    void testAddWithDoubleLiteral() {
        // Float/double arithmetic may be imprecise, reject for all ops
        Slot id = scan1.getOutput().get(0);
        Assertions.assertFalse(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Add(id, new DoubleLiteral(1.0))));
    }

    @Test
    void testAddWithFloatLiteral() {
        Slot id = scan1.getOutput().get(0);
        Assertions.assertFalse(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Add(id, new FloatLiteral(1.0f))));
    }

    @Test
    void testSubtractWithDoubleLiteral() {
        Slot id = scan1.getOutput().get(0);
        Assertions.assertFalse(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Subtract(id, new DoubleLiteral(1.0))));
    }

    @Test
    void testMultiplyWithDecimalLiteral() {
        // Small decimal multiply should pass (precision fits)
        Slot id = scan1.getOutput().get(0);
        Assertions.assertTrue(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Multiply(id, new DecimalLiteral(new BigDecimal("2.0")))));
    }

    @Test
    void testDivideWithDecimalLiteral() {
        // Divide with decimal: precision overflow too extreme to worry about
        Slot id = scan1.getOutput().get(0);
        Assertions.assertTrue(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Divide(id, new DecimalLiteral(new BigDecimal("2.0")))));
    }

    @Test
    void testAddWithDecimalLiteral() {
        // Add/Subtract with decimal are exact, should pass
        Slot id = scan1.getOutput().get(0);
        Assertions.assertTrue(SimplifyAggGroupBy.isBinaryArithmeticSlot(
                new Add(id, new DecimalLiteral(new BigDecimal("1.0")))));
    }

    // ========== tests for isInjectiveCastTo ==========

    @Test
    void testIntegerWidening() {
        Assertions.assertTrue(TinyIntType.INSTANCE.isInjectiveCastTo(IntegerType.INSTANCE));
        Assertions.assertTrue(IntegerType.INSTANCE.isInjectiveCastTo(BigIntType.INSTANCE));
        Assertions.assertFalse(IntegerType.INSTANCE.isInjectiveCastTo(TinyIntType.INSTANCE));
        Assertions.assertFalse(BigIntType.INSTANCE.isInjectiveCastTo(IntegerType.INSTANCE));
    }

    @Test
    void testDecimalWidening() {
        Assertions.assertTrue(DecimalV3Type.createDecimalV3Type(5, 2)
                .isInjectiveCastTo(DecimalV3Type.createDecimalV3Type(10, 4)));
        Assertions.assertFalse(DecimalV3Type.createDecimalV3Type(10, 4)
                .isInjectiveCastTo(DecimalV3Type.createDecimalV3Type(5, 2)));
    }

    @Test
    void testIntegralToDecimalWidening() {
        Assertions.assertTrue(TinyIntType.INSTANCE
                .isInjectiveCastTo(DecimalV3Type.createDecimalV3Type(10, 0)));
        // BigInt has 19 digits, DECIMAL(5,0) only has 5 integer digits
        Assertions.assertFalse(BigIntType.INSTANCE
                .isInjectiveCastTo(DecimalV3Type.createDecimalV3Type(5, 0)));
    }

    @Test
    void testCrossFamilyRejected() {
        Assertions.assertFalse(IntegerType.INSTANCE.isInjectiveCastTo(FloatType.INSTANCE));
        Assertions.assertFalse(FloatType.INSTANCE.isInjectiveCastTo(IntegerType.INSTANCE));
        Assertions.assertFalse(IntegerType.INSTANCE.isInjectiveCastTo(DoubleType.INSTANCE));
    }

    // ========== tests for canExtractSlot ==========

    @Test
    void testCanExtractSlotBare() {
        Slot id = scan1.getOutput().get(0);
        Assertions.assertTrue(SimplifyAggGroupBy.canExtractSlot(id));
    }

    @Test
    void testCanExtractSlotWidening() {
        Slot id = scan1.getOutput().get(0);
        // INT->BIGINT is lossless widening
        Expression cast = new Cast(id, BigIntType.INSTANCE);
        Assertions.assertTrue(SimplifyAggGroupBy.canExtractSlot(cast));
    }

    @Test
    void testCanExtractSlotExplicitCast() {
        Slot id = scan1.getOutput().get(0);
        // explicit cast should also be acceptable if lossless
        Expression cast = new Cast(id, BigIntType.INSTANCE, true);
        Assertions.assertTrue(SimplifyAggGroupBy.canExtractSlot(cast));
    }

    @Test
    void testCanExtractSlotNarrowing() {
        Slot id = scan1.getOutput().get(0);
        // INT -> TINYINT is narrowing, should be rejected
        Expression cast = new Cast(id, TinyIntType.INSTANCE);
        Assertions.assertFalse(SimplifyAggGroupBy.canExtractSlot(cast));
    }

    // ========== integration tests via PlanChecker ==========

    @Test
    void testMultiplyByZeroNotSimplified() {
        Slot id = scan1.getOutput().get(0);
        List<NamedExpression> output = ImmutableList.of(id, new Count().alias("cnt"));
        List<Expression> groupBy = ImmutableList.of(id, new Multiply(id, Literal.of(0)));
        LogicalPlan agg = new LogicalPlanBuilder(scan1)
                .agg(groupBy, output)
                .build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        PlanChecker.from(connectContext, agg)
                .applyTopDown(new SimplifyAggGroupBy())
                .matchesFromRoot(
                        logicalAggregate().when(a -> a.equals(agg))
                );
    }

    @Test
    void testNullLiteralNotSimplified() {
        Slot id = scan1.getOutput().get(0);
        List<NamedExpression> output = ImmutableList.of(id, new Count().alias("cnt"));
        List<Expression> groupBy = ImmutableList.of(id, new Add(id, NullLiteral.INSTANCE));
        LogicalPlan agg = new LogicalPlanBuilder(scan1)
                .agg(groupBy, output)
                .build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        PlanChecker.from(connectContext, agg)
                .applyTopDown(new SimplifyAggGroupBy())
                .matchesFromRoot(
                        logicalAggregate().when(a -> a.equals(agg))
                );
    }

    @Test
    void testMultiplyDoubleLiteralNotSimplified() {
        Slot id = scan1.getOutput().get(0);
        List<NamedExpression> output = ImmutableList.of(id, new Count().alias("cnt"));
        List<Expression> groupBy = ImmutableList.of(id, new Multiply(id, new DoubleLiteral(0.1)));
        LogicalPlan agg = new LogicalPlanBuilder(scan1)
                .agg(groupBy, output)
                .build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        PlanChecker.from(connectContext, agg)
                .applyTopDown(new SimplifyAggGroupBy())
                .matchesFromRoot(
                        logicalAggregate().when(a -> a.equals(agg))
                );
    }
}
