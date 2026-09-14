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
import org.apache.doris.nereids.trees.expressions.TryCast;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
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
    private static final LogicalOlapScan scan = PlanConstructor.newDpHyperLogicalOlapScan(0, "t1", 0);
    private static final Slot x = scan.getOutput().get(0);
    private static final Slot y = scan.getOutput().get(1);

    @Test
    void testBareSlotDeterminantSimplifiesDependencies() {
        Expression plusOne = new Add(x, Literal.of(1));
        Expression plusTwo = new Add(x, Literal.of(2));

        assertSimplified(ImmutableList.of(x, plusOne, plusTwo), ImmutableList.of(x));
    }

    @Test
    void testMissingDeterminantKeepsDependencies() {
        Expression plusOne = new Add(x, Literal.of(1));
        Expression plusTwo = new Add(x, Literal.of(2));

        assertUnchanged(ImmutableList.of(plusOne, plusTwo));
    }

    @Test
    void testDecimalCollisionWithoutDeterminantIsKept() {
        Expression first = new Divide(x, new DecimalLiteral(new BigDecimal("1000000.0")));
        Expression second = new Divide(x, new DecimalLiteral(new BigDecimal("2000000.0")));

        assertUnchanged(ImmutableList.of(first, second));
    }

    @Test
    void testDecimalOverflowWithDeterminantIsKept() {
        Slot decimal = new SlotReference("d", DecimalV3Type.createDecimalV3Type(38, 0));
        Expression mayOverflow = new Multiply(decimal, Literal.of(10));

        assertUnchanged(ImmutableList.of(decimal, mayOverflow));
    }

    @Test
    void testMultipleDeterminantsSimplifyIndependently() {
        Expression xDependent = new Add(x, Literal.of(1));
        Expression yDependent = new Subtract(y, Literal.of(2));

        assertSimplified(
                ImmutableList.of(x, xDependent, y, yDependent),
                ImmutableList.of(x, y));
    }

    @Test
    void testMixedSupportedAndUnsupportedKeysSimplifyLocally() {
        Expression xDependent = new Add(x, Literal.of(1));
        Expression xUnsupported = new Abs(x);
        Expression yDependent = new Multiply(y, Literal.of(2));
        Expression yUnsupported = new Mod(y, Literal.of(3));

        assertSimplified(
                ImmutableList.of(x, xDependent, xUnsupported, y, yDependent, yUnsupported),
                ImmutableList.of(x, xUnsupported, y, yUnsupported));
    }

    @Test
    void testExactDuplicatesAreRemovedStably() {
        Expression absX = new Abs(x);
        Expression absY = new Abs(y);

        assertSimplified(
                ImmutableList.of(absX, y, absX, absY, y),
                ImmutableList.of(absX, y, absY));
    }

    @Test
    void testInjectiveCastCannotReplaceBareSlotDeterminant() {
        Expression determinant = new Cast(x, BigIntType.INSTANCE);

        assertUnchanged(ImmutableList.of(determinant, new Add(x, Literal.of(1))));
    }

    @Test
    void testNestedInjectiveCastCannotReplaceBareSlotDeterminant() {
        Expression determinant = new Cast(
                new Cast(x, BigIntType.INSTANCE), DecimalV3Type.createDecimalV3Type(20, 0));

        assertUnchanged(ImmutableList.of(determinant, new Subtract(x, Literal.of(1))));
    }

    @Test
    void testNonInjectiveCastIsNotADeterminant() {
        Expression narrowingCast = new Cast(x, TinyIntType.INSTANCE);

        assertUnchanged(ImmutableList.of(x, narrowingCast));
        assertUnchanged(ImmutableList.of(narrowingCast, new Add(x, Literal.of(1))));
    }

    @Test
    void testCastChainWithNonInjectiveLayerIsNotADeterminant() {
        Expression castChain = new Cast(new Cast(x, TinyIntType.INSTANCE), BigIntType.INSTANCE);

        assertUnchanged(ImmutableList.of(castChain, new Add(x, Literal.of(1))));
    }

    @Test
    void testOnlyAuditedCastFamiliesCanProveInjectivity() {
        Assertions.assertTrue(SimplifyAggGroupBy.isProvenInjectiveCast(
                IntegerType.INSTANCE, BigIntType.INSTANCE));
        Assertions.assertTrue(SimplifyAggGroupBy.isProvenInjectiveCast(
                DecimalV3Type.createDecimalV3Type(5, 2),
                DecimalV3Type.createDecimalV3Type(10, 4)));
        Assertions.assertTrue(SimplifyAggGroupBy.isProvenInjectiveCast(
                DateTimeV2Type.of(3), DateTimeV2Type.of(6)));
        Assertions.assertFalse(SimplifyAggGroupBy.isProvenInjectiveCast(
                DateTimeV2Type.of(6), DateTimeV2Type.of(3)));

        VarcharType varchar20 = VarcharType.createVarcharType(20);
        Assertions.assertTrue(SimplifyAggGroupBy.isProvenInjectiveCast(varchar20, varchar20));
        Assertions.assertFalse(SimplifyAggGroupBy.isProvenInjectiveCast(
                varchar20, VarcharType.createVarcharType(5)));
        Assertions.assertFalse(SimplifyAggGroupBy.isProvenInjectiveCast(
                VarcharType.createVarcharType(5), varchar20));
        Assertions.assertFalse(SimplifyAggGroupBy.isProvenInjectiveCast(
                ArrayType.of(IntegerType.INSTANCE), ArrayType.of(BigIntType.INSTANCE)));
    }

    @Test
    void testVarcharNarrowingCastIsNotADeterminant() {
        Slot varcharSlot = new SlotReference("v", VarcharType.createVarcharType(20));
        Expression narrowingCast = new Cast(varcharSlot, VarcharType.createVarcharType(5));

        Assertions.assertFalse(SimplifyAggGroupBy.isProvenInjectiveCast(
                narrowingCast.child(0).getDataType(), narrowingCast.getDataType()));
    }

    @Test
    void testBareSlotReplacesOtherDeterminantsForTheSameBaseSlot() {
        Expression cast = new Cast(x, BigIntType.INSTANCE);

        assertSimplified(
                ImmutableList.of(cast, x, new Add(x, Literal.of(1))),
                ImmutableList.of(x));
    }

    @Test
    void testInjectiveCastDeterminantsDoNotEliminateEachOtherWithoutBareSlot() {
        Expression first = new Cast(x, BigIntType.INSTANCE);
        Expression second = new Cast(x, DecimalV3Type.createDecimalV3Type(20, 0));

        assertUnchanged(ImmutableList.of(first, second, new Add(x, Literal.of(1))));
    }

    @Test
    void testDependentOrdinaryCastMustBeLossless() {
        Expression narrowingDependent = new Add(new Cast(x, TinyIntType.INSTANCE), Literal.of(1));
        Expression strictNarrowingDependent = new Add(
                new Cast(x, TinyIntType.INSTANCE, true, true), Literal.of(1));

        assertUnchanged(ImmutableList.of(x, narrowingDependent));
        assertUnchanged(ImmutableList.of(x, strictNarrowingDependent));

        Expression wideningDependent = new Add(new Cast(x, BigIntType.INSTANCE), Literal.of(1));
        Expression strictWideningDependent = new Add(
                new Cast(x, BigIntType.INSTANCE, true, true), Literal.of(1));
        assertSimplified(ImmutableList.of(x, wideningDependent), ImmutableList.of(x));
        assertSimplified(ImmutableList.of(x, strictWideningDependent), ImmutableList.of(x));

        Expression determinant = new Cast(x, BigIntType.INSTANCE);
        assertUnchanged(ImmutableList.of(determinant, wideningDependent));
    }

    @Test
    void testDependentTryCastCanSafelyReturnNull() {
        Expression dependent = new Add(new TryCast(x, TinyIntType.INSTANCE), Literal.of(1));

        assertSimplified(ImmutableList.of(x, dependent), ImmutableList.of(x));
    }

    @Test
    void testRiskyOrUnsupportedExpressionsAreKept() {
        Expression variableDivisor = new Divide(Literal.of(1), x);
        Expression zeroDivisor = new Divide(x, Literal.of(0));
        Expression nullLiteral = new Add(x, NullLiteral.INSTANCE);
        Expression floatingArithmetic = new Add(x, new DoubleLiteral(1.0));
        Expression unsupported = new Abs(x);

        assertUnchanged(ImmutableList.of(
                x, variableDivisor, zeroDivisor, nullLiteral, floatingArithmetic, unsupported));
    }

    @Test
    void testDivisionIsConservativelyRetained() {
        assertUnchanged(ImmutableList.of(x, new Divide(x, Literal.of(2))));
    }

    @Test
    void testMultiplyByZeroIsADeterministicDependency() {
        assertSimplified(
                ImmutableList.of(x, new Multiply(x, Literal.of(0))),
                ImmutableList.of(x));
    }

    @Test
    void testOutputExpressionsRemainUnchanged() {
        Expression plusOne = new Add(x, Literal.of(1));
        List<Expression> groupBy = ImmutableList.of(x, plusOne);
        List<NamedExpression> output = ImmutableList.of(
                x, plusOne.alias("plus_one"), new Count().alias("count"));
        LogicalPlan aggregate = new LogicalPlanBuilder(scan).agg(groupBy, output).build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);

        PlanChecker.from(connectContext, aggregate)
                .applyTopDown(new SimplifyAggGroupBy())
                .matchesFromRoot(logicalAggregate().when(rewritten ->
                        rewritten.getGroupByExpressions().equals(ImmutableList.of(x))
                                && rewritten.getOutputExpressions().equals(output)));
    }

    @Test
    void testCastGroupKeyCannotReplaceSlotNeededByDependentOutput() {
        Expression castX = new Cast(x, BigIntType.INSTANCE);
        Expression plusOne = new Add(x, Literal.of(1));
        List<Expression> groupBy = ImmutableList.of(castX, plusOne);
        List<NamedExpression> output = ImmutableList.of(
                castX.alias("cast_x"), plusOne.alias("plus_one"), new Count().alias("count"));
        LogicalPlan aggregate = new LogicalPlanBuilder(scan).agg(groupBy, output).build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);

        PlanChecker.from(connectContext, aggregate)
                .applyTopDown(new SimplifyAggGroupBy())
                .matchesFromRoot(logicalAggregate().when(rewritten ->
                        rewritten.getGroupByExpressions().equals(groupBy)
                                && rewritten.getOutputExpressions().equals(output)));
    }

    private void assertSimplified(List<Expression> original, List<Expression> expected) {
        Assertions.assertEquals(expected, SimplifyAggGroupBy.simplifyGroupBy(original));
    }

    private void assertUnchanged(List<Expression> original) {
        Assertions.assertNull(SimplifyAggGroupBy.simplifyGroupBy(original));
    }
}
