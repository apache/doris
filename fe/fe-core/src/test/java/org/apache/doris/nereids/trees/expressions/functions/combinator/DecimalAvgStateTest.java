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
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ColumnToProtobuf;
import org.apache.doris.catalog.ColumnToThrift;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DecimalAvgStateTest {
    private AggStateType analyzeState(String sql) {
        return ((AggStateType) new NereidsParser().parseDataType(sql).conversion()).resolveDeclaration();
    }

    @Test
    void testProducerSignatureSurvivesSessionChange() throws Exception {
        ConnectContext context = MemoTestUtils.createConnectContext();
        context.getSessionVariable().enableDecimal256 = true;
        SlotReference input = new SlotReference("v", DecimalV3Type.createDecimalV3Type(18, 2), true);
        Avg avg = new Avg(input);
        DataType result = DecimalV3Type.createDecimalV3Type(76, 4);
        AggStateType state = (AggStateType) StateCombinator.create(avg).getDataType();
        Assertions.assertEquals(result, state.getResultType());
        Assertions.assertEquals(state, new CombineCombinator(avg.children(), avg).getDataType());
        Assertions.assertEquals(state, DataType.fromCatalogType(state.toCatalogDataType()));
        Assertions.assertEquals(state, analyzeState(state.toSql()));
        Type catalogType = state.toCatalogDataType();
        Assertions.assertEquals(catalogType,
                GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(catalogType, Type.class), Type.class));
        Column column = new Column("s", catalogType, false, AggregateType.GENERIC, false, null, "");
        Assertions.assertTrue(ColumnToThrift.toThrift(column).isSetAggStateResultType());
        Assertions.assertTrue(ColumnToProtobuf.toPb(column, ImmutableSet.of(), ImmutableList.of())
                .hasAggStateResultType());
        Assertions.assertTrue(state.toCatalogDataType().toThrift().isSetAggStateResultType());

        context.getSessionVariable().enableDecimal256 = false;
        SlotReference stateSlot = new SlotReference("s", state, false);
        MergeCombinator merge = new MergeCombinator(ImmutableList.of(stateSlot), new Avg(input));
        Assertions.assertEquals(result, merge.getDataType());
        Assertions.assertEquals(result,
                new FinalizeCombinator(ImmutableList.of(stateSlot), new Avg(input)).getDataType());
        FunctionCallExpr translated = (FunctionCallExpr) ExpressionTranslator.translate(
                merge, new PlanTranslatorContext());
        Assertions.assertEquals(result.toCatalogDataType(), translated.getFn().getReturnType());
        Assertions.assertEquals(result.toCatalogDataType(), translated.getType());

        AggStateType decimal128 = (AggStateType) StateCombinator.create(new Avg(input)).getDataType();
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(38, 4), decimal128.getResultType());
        Assertions.assertNotEquals(decimal128, state);
        AggStateType nonNullableResult = new AggStateType("avg", state.getSubTypes(), state.getSubTypeNullables(),
                false, result);
        Assertions.assertNotEquals(state, nonNullableResult);
        Assertions.assertFalse(CheckCast.check(state, nonNullableResult, false));
        Assertions.assertTrue(CheckCast.checkWithLooseAggState(state, decimal128, false));
        Assertions.assertTrue(CheckCast.checkWithLooseAggState(decimal128, state, false));
        Assertions.assertFalse(CheckCast.check(state, decimal128, false));
        Assertions.assertFalse(CheckCast.check(decimal128, state, false));
        context.getSessionVariable().enableDecimal256 = true;
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(38, 4),
                decimal128.getResultType());
    }

    @Test
    void testHighScaleStateTranslationAfterDisablingDecimal256() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        context.getSessionVariable().enableDecimal256 = true;
        SlotReference input = new SlotReference("v", DecimalV3Type.createDecimalV3Type(76, 40), true);
        StateCombinator state = StateCombinator.create(new Avg(input));
        CombineCombinator combine = new CombineCombinator(ImmutableList.of(input), new Avg(input));
        AggStateType produced = (AggStateType) state.getDataType();
        PlanTranslatorContext translatorContext = new PlanTranslatorContext();
        translatorContext.addExprIdSlotRefPair(input.getExprId(),
                new SlotRef(input.getDataType().toCatalogDataType(), input.nullable()));
        for (Expression producer : ImmutableList.of(state, combine)) {
            FunctionCallExpr translated = (FunctionCallExpr) ExpressionTranslator.translate(producer, translatorContext);
            Assertions.assertEquals(produced.toCatalogDataType(), translated.getType());
            Assertions.assertEquals(Function.BinaryType.AGG_STATE, translated.getFn().getBinaryType());
            Assertions.assertEquals(Function.NullableMode.ALWAYS_NOT_NULLABLE, translated.getFn().getNullableMode());
        }
        for (boolean legacy : new boolean[] {false, true}) {
            org.apache.doris.catalog.AggStateType catalog =
                    (org.apache.doris.catalog.AggStateType) produced.toCatalogDataType();
            if (legacy) {
                catalog = new org.apache.doris.catalog.AggStateType(catalog.getFunctionName(),
                        catalog.getResultIsNullable(), catalog.getSubTypes(), catalog.getSubTypeNullables());
            }
            context.getSessionVariable().enableDecimal256 = false;
            AggStateType stored = (AggStateType) DataType.fromCatalogType(catalog);
            SlotReference stateSlot = new SlotReference("s", stored, false);
            MergeCombinator merge = new MergeCombinator(ImmutableList.of(stateSlot), new Avg(input));
            UnionCombinator union = new UnionCombinator(ImmutableList.of(stateSlot), new Avg(input));
            FunctionCallExpr translatedMerge = (FunctionCallExpr) ExpressionTranslator.translate(
                    merge, new PlanTranslatorContext());
            FunctionCallExpr translatedUnion = (FunctionCallExpr) ExpressionTranslator.translate(
                    union, new PlanTranslatorContext());
            Assertions.assertEquals(produced.getResultType().toCatalogDataType(),
                    translatedMerge.getFn().getReturnType());
            Assertions.assertEquals(catalog, translatedUnion.getFn().getReturnType());
            Assertions.assertEquals(catalog, translatedMerge.getFn().getArgs()[0]);
            Assertions.assertEquals(catalog, translatedUnion.getFn().getArgs()[0]);
            Assertions.assertFalse(translatedMerge.getFn().hasVarArgs());
            Assertions.assertFalse(translatedUnion.getFn().hasVarArgs());
            translatorContext.addExprIdSlotRefPair(stateSlot.getExprId(), new SlotRef(catalog, false));
            FinalizeCombinator finalize = new FinalizeCombinator(ImmutableList.of(stateSlot), new Avg(input));
            FunctionCallExpr translatedFinalize = (FunctionCallExpr) ExpressionTranslator.translate(
                    finalize, translatorContext);
            Assertions.assertEquals(produced.getResultType().toCatalogDataType(), translatedFinalize.getType());
            Assertions.assertEquals(catalog, translatedFinalize.getFn().getArgs()[0]);
            Assertions.assertEquals(Function.BinaryType.AGG_STATE, translatedFinalize.getFn().getBinaryType());
        }
    }

    @Test
    void testCountArgumentMetadataAndNullability() {
        SlotReference input = new SlotReference("v", IntegerType.INSTANCE, true);
        Count count = new Count(input);
        StateCombinator state = StateCombinator.create(count);
        CombineCombinator combine = new CombineCombinator(ImmutableList.of(input), count);
        SlotReference stored = new SlotReference("s", state.getDataType(), false);
        PlanTranslatorContext context = new PlanTranslatorContext();
        context.addExprIdSlotRefPair(input.getExprId(),
                new SlotRef(input.getDataType().toCatalogDataType(), true));
        context.addExprIdSlotRefPair(stored.getExprId(),
                new SlotRef(stored.getDataType().toCatalogDataType(), false));
        for (Expression producer : ImmutableList.of(state, combine)) {
            FunctionCallExpr translated = (FunctionCallExpr) ExpressionTranslator.translate(producer, context);
            Assertions.assertEquals(producer == state, translated.getFn().hasVarArgs());
            Assertions.assertEquals(Function.NullableMode.ALWAYS_NOT_NULLABLE, translated.getFn().getNullableMode());
        }
        for (Expression consumer : ImmutableList.of(
                new MergeCombinator(ImmutableList.of(stored), count),
                new UnionCombinator(ImmutableList.of(stored), count),
                new FinalizeCombinator(ImmutableList.of(stored), count))) {
            FunctionCallExpr translated = (FunctionCallExpr) ExpressionTranslator.translate(consumer, context);
            Assertions.assertFalse(translated.getFn().hasVarArgs());
            Assertions.assertEquals(1, translated.getFn().getArgs().length);
            Assertions.assertEquals(Function.NullableMode.ALWAYS_NOT_NULLABLE, translated.getFn().getNullableMode());
        }
    }

    @Test
    void testDecimalWidthsAndScales() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        for (int precision : new int[] {9, 18, 38, 76}) {
            for (int scale : new int[] {2, 6}) {
                context.getSessionVariable().enableDecimal256 = true;
                SlotReference input = new SlotReference("v", DecimalV3Type.createDecimalV3Type(precision, scale), true);
                AggStateType state = (AggStateType) StateCombinator.create(new Avg(input)).getDataType();
                Assertions.assertEquals(state, analyzeState(state.toSql()));
                context.getSessionVariable().enableDecimal256 = false;
                Assertions.assertEquals(DecimalV3Type.createDecimalV3TypeNotCheck256(76, Math.max(scale, 4)),
                        state.getResultType());
            }
        }
    }

    @Test
    void testDeclarationInfersSessionResultType() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        for (boolean enableDecimal256 : new boolean[] {false, true}) {
            context.getSessionVariable().enableDecimal256 = enableDecimal256;
            for (int scale : new int[] {2, 6}) {
                AggStateType declared = (AggStateType) analyzeState(
                        "AGG_STATE<AVG(DECIMAL(18," + scale + "))>").conversion();
                SlotReference input = new SlotReference("v", DecimalV3Type.createDecimalV3Type(18, scale), true);
                Assertions.assertEquals(StateCombinator.create(new Avg(input)).getDataType(), declared);
                Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(
                        enableDecimal256 ? 76 : 38, Math.max(4, scale)), declared.getResultType());
            }
        }
        context.getSessionVariable().enableDecimal256 = false;
        AggStateType decimal128 = (AggStateType) analyzeState("agg_state<avg(decimal(18,2))>")
                .conversion();
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(38, 4), decimal128.getResultType());
        String savedSql = decimal128.toSql();
        context.getSessionVariable().enableDecimal256 = true;
        AggStateType recreated = analyzeState(savedSql);
        Assertions.assertNotEquals(decimal128, recreated);
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(76, 4), recreated.getResultType());
        Assertions.assertEquals(decimal128, DataType.fromCatalogType(GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(decimal128.toCatalogDataType(), Type.class), Type.class)));
    }

    @Test
    void testRejectNewStateColumnWithOldExecVersion() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        context.getSessionVariable().enableDecimal256 = true;
        int oldVersion = Config.be_exec_version;
        try {
            Config.be_exec_version = Config.AGG_STATE_RESULT_TYPE_MIN_BE_EXEC_VERSION - 1;
            DataType declaration = new NereidsParser().parseDataType("agg_state<avg(decimal(18,2))>").conversion();
            ColumnDefinition column = new ColumnDefinition("s", declaration, true);
            AnalysisException error = Assertions.assertThrows(AnalysisException.class, column::analyzeType);
            Assertions.assertTrue(error.getMessage().contains("require be_exec_version >= "));
        } finally {
            Config.be_exec_version = oldVersion;
        }
    }
}
