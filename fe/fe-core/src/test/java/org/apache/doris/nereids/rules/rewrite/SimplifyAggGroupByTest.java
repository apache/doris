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
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.TryCast;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Atan2;
import org.apache.doris.nereids.trees.expressions.functions.scalar.IsInf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.IsNan;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Pow;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SignBit;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.StringType;
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
    private static final LogicalOlapScan scan = PlanConstructor.newDpHyperLogicalOlapScan(0, "t1", 0);
    private static final Slot x = scan.getOutput().get(0);
    private static final Slot y = scan.getOutput().get(1);

    @Test
    void testBareSlotDeterminantSimplifiesDependencies() {
        assertSimplified(ImmutableList.of(x, new Add(x, Literal.of(1)), new Add(x, Literal.of(2))),
                ImmutableList.of(x));
    }

    @Test
    void testMissingDeterminantKeepsDependencies() {
        assertUnchanged(ImmutableList.of(new Add(x, Literal.of(1)), new Add(x, Literal.of(2))));
    }

    @Test
    void testDecimalCollisionWithoutDeterminantIsKept() {
        Expression first = new Divide(x, new DecimalLiteral(new BigDecimal("1000000.0")));
        Expression second = new Divide(x, new DecimalLiteral(new BigDecimal("2000000.0")));

        assertUnchanged(ImmutableList.of(first, second));
    }

    @Test
    void testDecimalArithmeticWithDeterminantIsRedundant() {
        Slot decimal = new SlotReference("d", DecimalV3Type.createDecimalV3Type(38, 0));

        assertSimplified(ImmutableList.of(decimal, new Multiply(decimal, Literal.of(10))),
                ImmutableList.of(decimal));
    }

    @Test
    void testMultipleDeterminantsSimplifyDependencies() {
        assertSimplified(ImmutableList.of(x, new Add(x, Literal.of(1)), y,
                new Subtract(y, Literal.of(2)), new Add(x, y)), ImmutableList.of(x, y));
    }

    @Test
    void testAllInputsMustBeExistingGroupingSlots() {
        Expression bothInputs = new Add(x, y);
        Expression missingInput = new Abs(y);

        assertSimplified(ImmutableList.of(x, new Abs(x), bothInputs, missingInput),
                ImmutableList.of(x, bothInputs, missingInput));
    }

    @Test
    void testExactDuplicatesAreRemovedStably() {
        Expression absX = new Abs(x);
        Expression absY = new Abs(y);

        assertSimplified(ImmutableList.of(absX, absY, absX), ImmutableList.of(absX, absY));
        assertSimplified(ImmutableList.of(y, x, y, x), ImmutableList.of(y, x));
    }

    @Test
    void testCastCannotReplaceBareSlotDeterminant() {
        assertUnchanged(ImmutableList.of(new Cast(x, BigIntType.INSTANCE), new Add(x, Literal.of(1))));
        assertUnchanged(ImmutableList.of(new Cast(x, TinyIntType.INSTANCE), new Add(x, Literal.of(1))));
        assertUnchanged(ImmutableList.of(new Cast(new Cast(x, BigIntType.INSTANCE),
                DecimalV3Type.createDecimalV3Type(20, 0)), new Subtract(x, Literal.of(1))));
    }

    @Test
    void testBareSlotEliminatesCastDependencies() {
        assertSimplified(ImmutableList.of(new Cast(x, BigIntType.INSTANCE), x,
                new Cast(x, TinyIntType.INSTANCE), new Add(x, Literal.of(1))), ImmutableList.of(x));
        assertSimplified(ImmutableList.of(x,
                new Add(new Cast(x, TinyIntType.INSTANCE, true, true), Literal.of(1))), ImmutableList.of(x));
        assertSimplified(ImmutableList.of(x,
                new TryCast(new Cast(x, TinyIntType.INSTANCE), BigIntType.INSTANCE)), ImmutableList.of(x));
    }

    @Test
    void testCastDeterminantsDoNotEliminateEachOtherWithoutBareSlot() {
        assertUnchanged(ImmutableList.of(new Cast(x, BigIntType.INSTANCE),
                new Cast(x, DecimalV3Type.createDecimalV3Type(20, 0)), new Add(x, Literal.of(1))));
    }

    @Test
    void testDeterministicDependenciesDoNotRequireInjectivityOrErrorChecks() {
        assertSimplified(ImmutableList.of(x, new Divide(Literal.of(1), x), new Divide(x, Literal.of(0)),
                new Add(x, NullLiteral.INSTANCE), new Add(x, new DoubleLiteral(1.0)),
                new Abs(x), new Mod(x, Literal.of(3)), new Multiply(x, Literal.of(0))), ImmutableList.of(x));
    }

    @Test
    void testVolatileExpressionsAreKept() {
        Expression random = new Random();
        Expression dependent = new Add(x, new Random());

        assertSimplified(ImmutableList.of(x, random, dependent, new Abs(x)),
                ImmutableList.of(x, random, dependent));
    }

    @Test
    void testNonMovableAndNondeterministicGroupKeysAreKept() {
        assertUnchanged(ImmutableList.of(x,
                new AssertTrue(new GreaterThan(x, Literal.of(0)), new StringLiteral("bad"))));
        assertUnchanged(ImmutableList.of(x, new Score()));
        assertUnchanged(ImmutableList.of(x, new Abs(new Score())));
    }

    @Test
    void testFloatingPointGroupingKeepsRepresentationSensitiveExpressions() {
        Slot floating = new SlotReference("floating", DoubleType.INSTANCE);
        assertUnchanged(ImmutableList.of(floating, new SignBit(floating)));
        assertUnchanged(ImmutableList.of(floating, new Atan2(floating, new DoubleLiteral(-1.0))));
        assertUnchanged(ImmutableList.of(floating, new Pow(floating, new DoubleLiteral(-1.0))));
        assertUnchanged(ImmutableList.of(floating, new Cast(floating, StringType.INSTANCE)));
    }

    @Test
    void testFloatingPointGroupingSimplifiesCongruentExpressions() {
        Slot floating = new SlotReference("floating", DoubleType.INSTANCE);
        assertSimplified(ImmutableList.of(floating, new Abs(floating),
                new Add(floating, new DoubleLiteral(1.0)),
                new Multiply(floating, new DoubleLiteral(1.0)),
                new Cast(floating, FloatType.INSTANCE),
                new IsNan(floating), new IsInf(floating)), ImmutableList.of(floating));
    }

    @Test
    void testConstantsCannotRemoveAllGroupingKeys() {
        assertUnchanged(ImmutableList.of(Literal.of(1), Literal.of(2)));
        assertSimplified(ImmutableList.of(x, Literal.of(1), NullLiteral.INSTANCE), ImmutableList.of(x));
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
