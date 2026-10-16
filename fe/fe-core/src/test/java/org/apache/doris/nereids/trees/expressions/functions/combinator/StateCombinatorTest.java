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

package org.apache.doris.nereids.trees.expressions.functions.combinator;

import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.rules.expression.rules.ConvertAggStateCast;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.PercentileReservoir;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.MoreFieldsThread;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StateCombinatorTest {
    @BeforeEach
    void setUp() {
        MemoTestUtils.createConnectContext();
    }

    @Test
    void testConstantFoldingPreservesStateLayout() {
        StateCombinator state = StateCombinator.create(new PercentileReservoir(
                new Cast(new VarcharLiteral("7"), DoubleType.INSTANCE), new DoubleLiteral(0.25)));
        AggStateType analyzedType = (AggStateType) state.getDataType();
        Assertions.assertEquals(ImmutableList.of(true, false), analyzedType.getSubTypeNullables());

        StateCombinator folded = (StateCombinator) MoreFieldsThread.keepFunctionSignature(
                () -> FoldConstantRuleOnFE.evaluateWithoutContext(state));
        Assertions.assertEquals(new DoubleLiteral(7), folded.child(0));
        Assertions.assertFalse(folded.child(0).nullable());
        Assertions.assertEquals(analyzedType, folded.getDataType());

        FunctionCallExpr translated = (FunctionCallExpr) ExpressionTranslator.translate(
                folded, new PlanTranslatorContext());
        Assertions.assertEquals(analyzedType.toCatalogDataType(), translated.getFn().getReturnType());
    }

    @Test
    void testCombineConstantFoldingPreservesInputLayout() {
        PercentileReservoir nested = new PercentileReservoir(
                new Cast(new VarcharLiteral("7"), DoubleType.INSTANCE), new DoubleLiteral(0.25));
        CombineCombinator combine = new CombineCombinator(nested.children(), nested);
        AggStateType analyzedType = (AggStateType) combine.getDataType();
        CombineCombinator folded = (CombineCombinator) MoreFieldsThread.keepFunctionSignature(
                () -> FoldConstantRuleOnFE.evaluateWithoutContext(combine));
        Assertions.assertFalse(folded.child(0).nullable());
        Assertions.assertEquals(new DoubleLiteral(7), folded.child(0));
        Assertions.assertEquals(analyzedType, folded.getDataType());
        Assertions.assertEquals(folded, MoreFieldsThread.keepFunctionSignature(
                () -> folded.withDistinctAndChildren(false, folded.children())));
        FunctionCallExpr translated = (FunctionCallExpr) ExpressionTranslator.translate(
                new AggregateExpression(folded, AggregateParam.LOCAL_RESULT), new PlanTranslatorContext());
        Assertions.assertEquals(analyzedType.toCatalogDataType(), translated.getFn().getReturnType());
        Assertions.assertTrue(translated.getChild(0).isNullable());
    }

    @Test
    void testAnalysisCanRecomputeStateType() {
        StateCombinator state = StateCombinator.create(new PercentileReservoir(
                new Cast(new VarcharLiteral("7"), DoubleType.INSTANCE), new DoubleLiteral(0.25)));
        state.getSignature();
        StateCombinator rebound = MoreFieldsThread.keepFunctionSignature(false,
                () -> state.withChildren(ImmutableList.of(new DoubleLiteral(7), new DoubleLiteral(0.25))));
        Assertions.assertEquals(ImmutableList.of(false, false),
                ((AggStateType) rebound.getDataType()).getSubTypeNullables());
    }

    @Test
    void testStateLayoutsHaveDistinctExpressionIdentity() {
        PercentileReservoir nullable = new PercentileReservoir(
                new Cast(new VarcharLiteral("7"), DoubleType.INSTANCE), new DoubleLiteral(0.25));
        PercentileReservoir nonnullable = new PercentileReservoir(new DoubleLiteral(7), new DoubleLiteral(0.25));
        for (boolean combine : ImmutableList.of(false, true)) {
            Expression beforeFold = combine ? new CombineCombinator(nullable.children(), nullable)
                    : StateCombinator.create(nullable);
            Expression direct = combine ? new CombineCombinator(nonnullable.children(), nonnullable)
                    : StateCombinator.create(nonnullable);
            Expression folded = MoreFieldsThread.keepFunctionSignature(
                    () -> FoldConstantRuleOnFE.evaluateWithoutContext(beforeFold));
            Assertions.assertEquals(direct.children(), folded.children());
            Assertions.assertNotEquals(direct.getDataType(), folded.getDataType());
            Assertions.assertNotEquals(direct, folded);
            Assertions.assertEquals(2, ImmutableSet.of(direct, folded).size());
            Assertions.assertEquals(folded, MoreFieldsThread.keepFunctionSignature(
                    () -> folded.withChildren(folded.children())));
        }
    }

    @Test
    void testExplicitStateCastAfterConstantFolding() {
        PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze("select cast(percentile_reservoir_state(cast('7' as double), 0.25) "
                        + "as agg_state<percentile_reservoir(double not null, double not null)>)")
                .rewrite();
    }

    @Test
    void testExplicitStateCastRebindsCustomSignature() {
        for (String nullable : ImmutableList.of("null", "not null")) {
            PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select cast(max_state(null) as agg_state<max(int " + nullable + ")>)")
                    .rewrite();
        }
    }

    @Test
    void testExplicitStateCastUpdatesStateLayout() {
        for (boolean nullable : ImmutableList.of(false, true)) {
            Expression argument = nullable ? new DoubleLiteral(7)
                    : new Cast(new VarcharLiteral("7"), DoubleType.INSTANCE);
            StateCombinator state = StateCombinator.create(
                    new PercentileReservoir(argument, new DoubleLiteral(0.25)));
            AggStateType target = new AggStateType("percentile_reservoir",
                    ImmutableList.of(DoubleType.INSTANCE, DoubleType.INSTANCE),
                    ImmutableList.of(nullable, false), true);
            Assertions.assertNotEquals(target, state.getDataType());

            Cast converted = (Cast) MoreFieldsThread.keepFunctionSignature(
                    () -> ConvertAggStateCast.convert(new Cast(state, target)));
            Assertions.assertEquals(nullable, converted.child().child(0).nullable());
            Assertions.assertEquals(target, converted.child().getDataType());
            FunctionCallExpr translated = (FunctionCallExpr) ExpressionTranslator.translate(
                    converted.child(), new PlanTranslatorContext());
            Assertions.assertEquals(target.toCatalogDataType(), translated.getFn().getReturnType());
        }
    }
}
