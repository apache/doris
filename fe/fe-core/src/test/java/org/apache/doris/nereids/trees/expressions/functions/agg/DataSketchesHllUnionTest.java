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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.combinator.MergeCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.UnionCombinator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Concat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Crc32;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

class DataSketchesHllUnionTest {
    private static final SlotReference SKETCH = SlotReference.of("sketch", StringType.INSTANCE);

    private DataSketchesHllUnion analyze(Expression sketch, Expression lgMaxK, Expression tgtType) {
        DataSketchesHllUnion call = lgMaxK == null
                ? new DataSketchesHllUnion(sketch)
                : (tgtType == null
                        ? new DataSketchesHllUnion(sketch, lgMaxK)
                        : new DataSketchesHllUnion(sketch, lgMaxK, tgtType));
        return (DataSketchesHllUnion) ExpressionAnalyzer.analyzeFunction(null, null, call);
    }

    @Test
    void testSignaturesFollowFirstArgumentType() {
        Assertions.assertEquals(9, DataSketchesHllUnion.SIGNATURES.size());
        for (DataType type : ImmutableList.of(
                StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, VarBinaryType.INSTANCE)) {
            Expression input = SlotReference.of("sk", type);
            DataSketchesHllUnion oneArg = analyze(input, null, null);
            DataSketchesHllUnion twoArgs = analyze(input, new IntegerLiteral(12), null);
            DataSketchesHllUnion threeArgs = analyze(input, new IntegerLiteral(12), new StringLiteral("HLL_8"));

            for (DataSketchesHllUnion fn : ImmutableList.of(oneArg, twoArgs, threeArgs)) {
                Assertions.assertEquals(type, fn.getDataType());
                List<FunctionSignature> signatures = fn.getSignatures();
                Assertions.assertEquals(3, signatures.size());
                signatures.forEach(sig -> Assertions.assertEquals(type, sig.returnType));
            }
        }
        Assertions.assertThrows(AnalysisException.class,
                () -> analyze(new IntegerLiteral(1), null, null));
    }

    @Test
    void testDistinctIgnoredAndNullableBehavior() {
        DataSketchesHllUnion oneArg = new DataSketchesHllUnion(true, SKETCH);
        DataSketchesHllUnion twoArgs = new DataSketchesHllUnion(true, SKETCH, new IntegerLiteral(8));
        DataSketchesHllUnion threeArgs = new DataSketchesHllUnion(
                true, SKETCH, new IntegerLiteral(8), new StringLiteral("HLL_6"));

        for (DataSketchesHllUnion fn : ImmutableList.of(oneArg, twoArgs, threeArgs)) {
            DataSketchesHllUnion rewritten = fn.withDistinctAndChildren(true, fn.children());
            Assertions.assertInstanceOf(NullIgnoringAggregateFunction.class, fn);
            Assertions.assertInstanceOf(NullableAggregateFunction.class, fn);
            Assertions.assertFalse(fn.isDistinct());
            Assertions.assertFalse(rewritten.isDistinct());
            Assertions.assertTrue(fn.nullable());
            Assertions.assertFalse(fn.isAlwaysNullable());
            Assertions.assertTrue(fn.withAlwaysNullable(true).isAlwaysNullable());
            Assertions.assertTrue(fn.withAlwaysNullable(true).nullable());
            SlotReference nonNullSketch = new SlotReference("sk_not_null", StringType.INSTANCE, false);
            DataSketchesHllUnion nonNullFn = new DataSketchesHllUnion(nonNullSketch);
            Assertions.assertFalse(nonNullFn.nullable());
            Assertions.assertTrue(nonNullFn.withAlwaysNullable(true).nullable());
        }
    }

    @Test
    void testLgMaxKAndTgtTypeValidation() {
        for (int k : new int[] {7, 12, 21}) {
            for (String type : ImmutableList.of("HLL_4", "hll_6", "HlL_8")) {
                Assertions.assertDoesNotThrow(
                        () -> analyze(SKETCH, new IntegerLiteral(k), new StringLiteral(type)));
            }
        }
        for (Expression invalidK : ImmutableList.of(
                new IntegerLiteral(6),
                new IntegerLiteral(22),
                new DecimalV3Literal(new BigDecimal("8.5")),
                new StringLiteral("12"),
                new NullLiteral(IntegerType.INSTANCE),
                SlotReference.of("lg_max_k", IntegerType.INSTANCE))) {
            Assertions.assertThrows(AnalysisException.class, () -> analyze(SKETCH, invalidK, null));
        }
        for (Expression invalidType : ImmutableList.of(
                new StringLiteral("HLL_5"),
                new IntegerLiteral(4),
                new NullLiteral(StringType.INSTANCE),
                SlotReference.of("tgt_type", StringType.INSTANCE))) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> analyze(SKETCH, new IntegerLiteral(12), invalidType));
        }

        DataSketchesHllUnion foldable = analyze(SKETCH,
                new Add(new IntegerLiteral(10), new IntegerLiteral(2)),
                new Concat(new StringLiteral("HLL_"), new StringLiteral("6")));
        Expression folded = FoldConstantRuleOnFE.evaluateWithoutContext(foldable);
        Assertions.assertDoesNotThrow(folded::checkLegalityAfterRewrite);
        Assertions.assertEquals(new IntegerLiteral(12), folded.child(1));
        Assertions.assertEquals(new VarcharLiteral("HLL_6"), folded.child(2));

        Expression onlyBe = new Add(new IntegerLiteral(10), new Crc32(new StringLiteral("")));
        Assertions.assertDoesNotThrow(() -> analyze(SKETCH, onlyBe, null).checkLegalityAfterRewrite());
    }

    @Test
    void testConstructRollUpPreservesParameters() {
        SlotReference mvSlot = SlotReference.of("mv_sk", StringType.INSTANCE);

        DataSketchesHllUnion oneArg = new DataSketchesHllUnion(SKETCH);
        Assertions.assertTrue(oneArg.canRollUp());
        Function r1 = oneArg.constructRollUp(mvSlot);
        Assertions.assertEquals(ImmutableList.of(mvSlot), r1.getArguments());

        DataSketchesHllUnion twoArgs = new DataSketchesHllUnion(SKETCH, new IntegerLiteral(10));
        Function r2 = twoArgs.constructRollUp(mvSlot);
        Assertions.assertEquals(ImmutableList.of(mvSlot, new IntegerLiteral(10)), r2.getArguments());

        DataSketchesHllUnion threeArgs = new DataSketchesHllUnion(
                SKETCH, new IntegerLiteral(10), new StringLiteral("HLL_8"));
        Function r3 = threeArgs.constructRollUp(mvSlot);
        Assertions.assertEquals(
                ImmutableList.of(mvSlot, new IntegerLiteral(10), new StringLiteral("HLL_8")),
                r3.getArguments());
    }

    @Test
    void testAliasAndCombinators() {
        FunctionRegistry registry = new FunctionRegistry();
        for (String name : ImmutableList.of("datasketches_hll_union", "ds_hll_union")) {
            FunctionBuilder directBuilder = registry.findFunctionBuilder(name, SKETCH);
            Assertions.assertInstanceOf(DataSketchesHllUnion.class, directBuilder.build(name, SKETCH).first);

            String stateName = name + "_state";
            List<Expression> args = ImmutableList.of(SKETCH, new IntegerLiteral(8), new StringLiteral("HLL_8"));
            FunctionBuilder stateBuilder = registry.findFunctionBuilder(stateName, args);
            StateCombinator state = (StateCombinator) stateBuilder.build(stateName, args).first;
            Assertions.assertDoesNotThrow(state::checkLegalityAfterRewrite);

            SlotReference stateSlot = new SlotReference("state", state.getDataType(), false);
            MergeCombinator merge = (MergeCombinator) registry
                    .findFunctionBuilder(name + "_merge", stateSlot)
                    .build(name + "_merge", stateSlot).first;
            Assertions.assertDoesNotThrow(merge::checkLegalityBeforeTypeCoercion);

            UnionCombinator union = (UnionCombinator) registry
                    .findFunctionBuilder(name + "_union", stateSlot)
                    .build(name + "_union", stateSlot).first;
            Assertions.assertDoesNotThrow(union::checkLegalityBeforeTypeCoercion);
        }
    }
}
