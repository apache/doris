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

import org.apache.doris.catalog.BuiltinAggregateFunctions;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.analysis.WindowFunctionChecker;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.AggCombinerFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Concat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Crc32;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
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
    void testSignaturesAndReturnTypes() {
        Assertions.assertEquals(9, DataSketchesHllUnion.SIGNATURES.size());
        for (DataType type : ImmutableList.of(
                StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, VarBinaryType.INSTANCE)) {
            DataType expectedRet = type.isVarBinaryType() ? VarBinaryType.INSTANCE : StringType.INSTANCE;
            Expression input = SlotReference.of("sk", type);
            DataSketchesHllUnion oneArg = analyze(input, null, null);
            DataSketchesHllUnion twoArgs = analyze(input, new IntegerLiteral(12), null);
            DataSketchesHllUnion threeArgs = analyze(input, new IntegerLiteral(12), new StringLiteral("HLL_8"));

            for (DataSketchesHllUnion fn : ImmutableList.of(oneArg, twoArgs, threeArgs)) {
                // VARCHAR keeps its exact arg0 type without implicit cast while returning STRING (2GB max)
                Assertions.assertEquals(input, fn.child(0));
                Assertions.assertEquals(expectedRet, fn.getDataType());
                List<FunctionSignature> signatures = fn.getSignatures();
                Assertions.assertEquals(3, signatures.size());
                signatures.forEach(sig -> {
                    Assertions.assertEquals(type, sig.argumentsTypes.get(0));
                    Assertions.assertEquals(expectedRet, sig.returnType);
                });
            }
        }
        Assertions.assertThrows(AnalysisException.class,
                () -> analyze(new IntegerLiteral(1), null, null));
    }

    @Test
    void testDistinctRejectedAndNullableBehavior() {
        DataSketchesHllUnion oneArg = new DataSketchesHllUnion(true, SKETCH);
        DataSketchesHllUnion twoArgs = new DataSketchesHllUnion(true, SKETCH, new IntegerLiteral(8));
        DataSketchesHllUnion threeArgs = new DataSketchesHllUnion(
                true, SKETCH, new IntegerLiteral(8), new StringLiteral("HLL_6"));

        for (DataSketchesHllUnion fn : ImmutableList.of(oneArg, twoArgs, threeArgs)) {
            DataSketchesHllUnion rewritten = fn.withDistinctAndChildren(true, fn.children());
            Assertions.assertInstanceOf(NullIgnoringAggregateFunction.class, fn);
            Assertions.assertInstanceOf(NullableAggregateFunction.class, fn);
            Assertions.assertInstanceOf(NotSupportAggState.class, fn);
            Assertions.assertTrue(fn.isDistinct());
            Assertions.assertTrue(rewritten.isDistinct());
            AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                    () -> ExpressionAnalyzer.analyzeFunction(null, null, fn));
            Assertions.assertTrue(ex.getMessage().contains("DISTINCT"));

            DataSketchesHllUnion nonDistinct = fn.withDistinctAndChildren(false, fn.children());
            Assertions.assertFalse(nonDistinct.isDistinct());
            Assertions.assertTrue(nonDistinct.nullable());
            Assertions.assertFalse(nonDistinct.isAlwaysNullable());
            Assertions.assertTrue(nonDistinct.withAlwaysNullable(true).isAlwaysNullable());
            Assertions.assertTrue(nonDistinct.withAlwaysNullable(true).nullable());
        }

        SlotReference nonNullSketch = new SlotReference("sk_not_null", StringType.INSTANCE, false);
        DataSketchesHllUnion nonNullFn = new DataSketchesHllUnion(nonNullSketch);
        Assertions.assertFalse(nonNullFn.nullable());
        Assertions.assertTrue(nonNullFn.withAlwaysNullable(true).nullable());
    }

    @Test
    void testLgMaxKAndTgtTypeValidation() {
        for (int k : new int[] {7, 12, 21}) {
            for (String type : ImmutableList.of("HLL_4", "hll_6", "HlL_8")) {
                Assertions.assertDoesNotThrow(
                        () -> analyze(SKETCH, new IntegerLiteral(k), new StringLiteral(type)));
            }
        }
        Expression onlyBeK = new Add(new IntegerLiteral(10), new Crc32(new StringLiteral("")));
        for (Expression invalidK : ImmutableList.of(
                new IntegerLiteral(6),
                new IntegerLiteral(22),
                new BigIntLiteral(4294967308L),
                new DecimalV3Literal(new BigDecimal("8.5")),
                new StringLiteral("12"),
                new NullLiteral(IntegerType.INSTANCE),
                new Cast(new NullLiteral(StringType.INSTANCE), IntegerType.INSTANCE),
                onlyBeK,
                SlotReference.of("lg_max_k", IntegerType.INSTANCE))) {
            Assertions.assertThrows(AnalysisException.class, () -> analyze(SKETCH, invalidK, null));
        }
        Expression onlyBeType = new Concat(
                new StringLiteral("HLL_"), new Cast(new Crc32(new StringLiteral("")), StringType.INSTANCE));
        for (Expression invalidType : ImmutableList.of(
                new StringLiteral("HLL_5"),
                new IntegerLiteral(4),
                new NullLiteral(StringType.INSTANCE),
                new Cast(new NullLiteral(IntegerType.INSTANCE), StringType.INSTANCE),
                onlyBeType,
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

        // Direct construction with BE-only constant is also rejected by checkLegalityAfterRewrite
        Assertions.assertThrows(AnalysisException.class,
                () -> new DataSketchesHllUnion(SKETCH, onlyBeK).checkLegalityAfterRewrite());
    }

    @Test
    void testConstructRollUpPreservesParametersAndRebindsSignature() {
        SlotReference varcharSketch = SlotReference.of("varchar_sk", VarcharType.SYSTEM_DEFAULT);
        SlotReference mvSlot = SlotReference.of("mv_sk", StringType.INSTANCE);

        DataSketchesHllUnion oneArg = new DataSketchesHllUnion(SKETCH);
        Assertions.assertTrue(oneArg.canRollUp());
        Function r1 = oneArg.constructRollUp(mvSlot);
        Assertions.assertEquals(ImmutableList.of(mvSlot), r1.getArguments());

        DataSketchesHllUnion twoArgs = new DataSketchesHllUnion(SKETCH, new IntegerLiteral(10));
        Function r2 = twoArgs.constructRollUp(mvSlot);
        Assertions.assertEquals(ImmutableList.of(mvSlot, new IntegerLiteral(10)), r2.getArguments());

        DataSketchesHllUnion threeArgs = new DataSketchesHllUnion(
                varcharSketch, new IntegerLiteral(10), new StringLiteral("HLL_8"));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, threeArgs.getSignature().argumentsTypes.get(0));
        DataSketchesHllUnion r3 = (DataSketchesHllUnion) threeArgs.constructRollUp(mvSlot);
        Assertions.assertEquals(
                ImmutableList.of(mvSlot, new IntegerLiteral(10), new StringLiteral("HLL_8")),
                r3.getArguments());
        Assertions.assertEquals(StringType.INSTANCE, r3.getSignature().argumentsTypes.get(0));
        Assertions.assertEquals(StringType.INSTANCE, r3.getDataType());

        DataSketchesHllUnionAgg aggFromVarchar = new DataSketchesHllUnionAgg(varcharSketch, new IntegerLiteral(8));
        DataSketchesHllUnionAgg rolledAgg = (DataSketchesHllUnionAgg) aggFromVarchar.constructRollUp(mvSlot);
        Assertions.assertEquals(ImmutableList.of(mvSlot, new IntegerLiteral(8)), rolledAgg.getArguments());
        Assertions.assertEquals(StringType.INSTANCE, rolledAgg.getSignature().argumentsTypes.get(0));
    }

    @Test
    void testAliasAndAggStateRejected() {
        FunctionRegistry registry = new FunctionRegistry();
        for (String name : ImmutableList.of("datasketches_hll_union", "ds_hll_union")) {
            FunctionBuilder directBuilder = registry.findFunctionBuilder(name, SKETCH);
            Assertions.assertInstanceOf(DataSketchesHllUnion.class, directBuilder.build(name, SKETCH).first);
            Assertions.assertTrue(BuiltinAggregateFunctions.INSTANCE.notSupportAggStateDdlFuncNames.contains(name));

            for (String suffix : ImmutableList.of(
                    AggCombinerFunctionBuilder.STATE_SUFFIX,
                    AggCombinerFunctionBuilder.MERGE_SUFFIX,
                    AggCombinerFunctionBuilder.UNION_SUFFIX,
                    AggCombinerFunctionBuilder.COMBINE_SUFFIX,
                    AggCombinerFunctionBuilder.FINALIZE_SUFFIX)) {
                String combinerName = name + suffix;
                Assertions.assertThrows(AnalysisException.class,
                        () -> registry.findFunctionBuilder(combinerName, ImmutableList.of(SKETCH)));
            }
        }

        for (String typeStr : ImmutableList.of(
                "AGG_STATE<datasketches_hll_union(STRING)>",
                "AGG_STATE<ds_hll_union(STRING)>",
                "AGG_STATE<Ds_Hll_Union(STRING, INT, VARCHAR)>")) {
            ParseException ex = Assertions.assertThrows(ParseException.class,
                    () -> DataType.convertFromString(typeStr));
            Assertions.assertTrue(ex.getMessage().contains("does not support AGG_STATE"), ex.getMessage());
        }

        // Legacy NotSupportAggState functions keep their existing AGG_STATE DDL parser behavior.
        Assertions.assertFalse(BuiltinAggregateFunctions.INSTANCE.notSupportAggStateDdlFuncNames
                .contains("orthogonal_bitmap_union_count"));
        Assertions.assertFalse(BuiltinAggregateFunctions.INSTANCE.notSupportAggStateDdlFuncNames
                .contains("ai_agg"));
        Assertions.assertDoesNotThrow(
                () -> DataType.convertFromString("AGG_STATE<orthogonal_bitmap_union_count(BITMAP)>"));
        Assertions.assertDoesNotThrow(
                () -> DataType.convertFromString("AGG_STATE<ai_agg(STRING, STRING, STRING)>"));
    }

    @Test
    void testWindowRejected() {
        for (DataSketchesHllUnion fn : ImmutableList.of(
                new DataSketchesHllUnion(SKETCH),
                new DataSketchesHllUnion(SKETCH, new IntegerLiteral(12), new VarcharLiteral("HLL_8")))) {
            WindowExpression window = new WindowExpression(fn, ImmutableList.of(), ImmutableList.of());
            AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                    () -> new WindowFunctionChecker(window).checkWindowFunction());
            Assertions.assertEquals(
                    "Window function does not support aggregate function: datasketches_hll_union",
                    ex.getMessage());
        }
    }

    @Test
    void testRewriteWhenAnalyzeBindsOptionLiterals() {
        DataSketchesHllUnion oneArg = new DataSketchesHllUnion(SKETCH);
        Assertions.assertSame(oneArg, oneArg.rewriteWhenAnalyze());

        DataSketchesHllUnion alreadyBound = new DataSketchesHllUnion(
                SKETCH, new IntegerLiteral(12), new VarcharLiteral("HLL_8"));
        Assertions.assertSame(alreadyBound, alreadyBound.rewriteWhenAnalyze());

        DataSketchesHllUnion twoArgs = new DataSketchesHllUnion(
                SKETCH, new Add(new IntegerLiteral(10), new IntegerLiteral(2)));
        Expression rewrittenTwoArgs = twoArgs.rewriteWhenAnalyze();
        Assertions.assertInstanceOf(DataSketchesHllUnion.class, rewrittenTwoArgs);
        Assertions.assertEquals(new IntegerLiteral(12), rewrittenTwoArgs.child(1));
        Assertions.assertSame(rewrittenTwoArgs, ((DataSketchesHllUnion) rewrittenTwoArgs).rewriteWhenAnalyze());

        Expression conditionalK = new If(
                new LessThan(new IntegerLiteral(1), new IntegerLiteral(2)),
                new Add(new IntegerLiteral(4), new IntegerLiteral(4)),
                new IntegerLiteral(12));
        Expression computedTgtType = new Concat(new StringLiteral("hll_"), new StringLiteral("8"));
        DataSketchesHllUnion threeArgs = new DataSketchesHllUnion(SKETCH, conditionalK, computedTgtType);
        Expression rewrittenThreeArgs = threeArgs.rewriteWhenAnalyze();
        Assertions.assertInstanceOf(DataSketchesHllUnion.class, rewrittenThreeArgs);
        Assertions.assertEquals(new IntegerLiteral(8), rewrittenThreeArgs.child(1));
        Assertions.assertEquals(new VarcharLiteral("HLL_8"), rewrittenThreeArgs.child(2));
        Assertions.assertSame(rewrittenThreeArgs, ((DataSketchesHllUnion) rewrittenThreeArgs).rewriteWhenAnalyze());
    }
}
