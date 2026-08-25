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
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.rules.analysis.WindowFunctionChecker;
import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.AIAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregatePhase;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.NotSupportAggState;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapExprCalculate;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapExprCalculateCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapIntersect;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapIntersectCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CombineCombinatorTest {

    @Test
    void testAvgCombineReturnsCompatibleAggState() {
        SlotReference argument = new SlotReference("value", IntegerType.INSTANCE, true);
        FunctionBuilder builder = new FunctionRegistry().findFunctionBuilder("avg_combine", argument);
        CombineCombinator combine = (CombineCombinator) builder.build("avg_combine", argument).first;
        Avg avg = (Avg) combine.getNestedFunction();

        Assertions.assertEquals("avg_combine", combine.getName());
        Assertions.assertFalse(combine.nullable());
        Assertions.assertEquals(avg.getIntermediateTypes(), combine.getIntermediateTypes());

        AggStateType stateType = (AggStateType) combine.getDataType();
        Assertions.assertEquals("avg", stateType.getFunctionName());
        Assertions.assertEquals(ImmutableList.of(IntegerType.INSTANCE), stateType.getSubTypes());
        Assertions.assertEquals(ImmutableList.of(true), stateType.getSubTypeNullables());

        UnionCombinator union = new UnionCombinator(ImmutableList.of(combine), avg);
        Assertions.assertEquals(stateType, union.getDataType());

        FunctionCallExpr translated = (FunctionCallExpr) ExpressionTranslator.translate(
                combine, new PlanTranslatorContext());
        Assertions.assertEquals("avg_combine", translated.getFn().getFunctionName().getFunction());
        Assertions.assertEquals(Function.BinaryType.AGG_STATE, translated.getFn().getBinaryType());
        Assertions.assertEquals(Function.NullableMode.ALWAYS_NOT_NULLABLE,
                translated.getFn().getNullableMode());
        Assertions.assertEquals(stateType.toCatalogDataType(), translated.getFn().getReturnType());
        Assertions.assertEquals(avg.getIntermediateTypes().toCatalogDataType(),
                ((org.apache.doris.catalog.AggregateFunction) translated.getFn()).getIntermediateType());
    }

    @Test
    void testDistinctIsRejected() {
        SlotReference argument = new SlotReference("value", IntegerType.INSTANCE, false);
        CombineCombinator combine =
                new CombineCombinator(ImmutableList.of(argument), new Avg(argument));

        Assertions.assertThrows(AnalysisException.class,
                () -> combine.withDistinctAndChildren(true, ImmutableList.of(argument)));
        Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                        .analyze("select avg_combine(distinct 1)"));
    }

    @Test
    void testZeroArgumentAggregateIsRejected() {
        FunctionRegistry functionRegistry = new FunctionRegistry();
        Assertions.assertTrue(functionRegistry
                .findBuiltinFunctionBuilder("count_combine", ImmutableList.of()).isEmpty());
        Assertions.assertThrows(AnalysisException.class,
                () -> new CombineCombinator(ImmutableList.of(), new Count()));
    }

    @Test
    void testScalarFunctionDoesNotSupportCombine() {
        SlotReference argument = new SlotReference("value", IntegerType.INSTANCE, false);
        FunctionRegistry functionRegistry = new FunctionRegistry();
        Assertions.assertTrue(functionRegistry
                .findBuiltinFunctionBuilder("abs_combine", ImmutableList.of(argument)).isEmpty());
        Assertions.assertFalse(functionRegistry.isAggregateFunction(null, "abs_combine"));
        Assertions.assertTrue(functionRegistry.isAggregateFunction(null, "avg_combine"));
    }

    @Test
    void testCombineDoesNotSupportAggStateCast() {
        Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                        .analyze("select cast(topn_combine('x', 10, 100) "
                                + "as agg_state<topn(varchar, int)>)"));
    }

    @Test
    void testCombineRequiresExactAggStateMatch() {
        SlotReference argument = new SlotReference("value", IntegerType.INSTANCE, false);
        CombineCombinator combine = new CombineCombinator(
                ImmutableList.of(argument), new Avg(argument));
        AggStateType exactType = (AggStateType) combine.getDataType();
        AggStateType differentSubtype = new AggStateType("avg",
                ImmutableList.of(BigIntType.INSTANCE), ImmutableList.of(false), true);
        AggStateType differentNullability = new AggStateType("avg",
                ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true);

        Assertions.assertTrue(CheckCast.checkWithLooseAggState(exactType, exactType, false));
        Assertions.assertFalse(CheckCast.checkWithLooseAggState(exactType, differentSubtype, false));
        Assertions.assertFalse(CheckCast.checkWithLooseAggState(exactType, differentNullability, false));
        Assertions.assertTrue(CheckCast.checkWithLooseAggState(
                StateCombinator.create(new Avg(argument)).getDataType(), differentSubtype, false));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                        .analyze("select cast(avg_combine(cast(1 as int)) "
                                + "as agg_state<avg(bigint not null)>)"));
        Assertions.assertTrue(exception.getMessage().contains(
                "Aggregate combine state requires an exact AggState type match"));
    }

    @Test
    void testAiAggDoesNotSupportAggState() {
        Assertions.assertTrue(NotSupportAggState.class.isAssignableFrom(AIAgg.class));

        ImmutableList<VarcharLiteral> arguments = ImmutableList.of(
                new VarcharLiteral("value"), new VarcharLiteral("task"));
        FunctionRegistry functionRegistry = new FunctionRegistry();
        Assertions.assertTrue(functionRegistry
                .findBuiltinFunctionBuilder("ai_agg_state", arguments).isEmpty());
        Assertions.assertTrue(functionRegistry
                .findBuiltinFunctionBuilder("ai_agg_combine", arguments).isEmpty());

        AggStateType stateType = new AggStateType("ai_agg",
                ImmutableList.of(StringType.INSTANCE, StringType.INSTANCE, StringType.INSTANCE),
                ImmutableList.of(false, false, false), true);
        ImmutableList<SlotReference> stateArgument = ImmutableList.of(
                new SlotReference("state", stateType, false));
        Assertions.assertTrue(functionRegistry
                .findBuiltinFunctionBuilder("ai_agg_merge", stateArgument).isEmpty());
        Assertions.assertTrue(functionRegistry
                .findBuiltinFunctionBuilder("ai_agg_union", stateArgument).isEmpty());

        AIAgg aiAgg = new AIAgg(new VarcharLiteral("resource"),
                new VarcharLiteral("value"), new VarcharLiteral("task"));
        Assertions.assertThrows(AnalysisException.class, () -> StateCombinator.create(aiAgg));
    }

    @Test
    void testCombineDoesNotSupportWindow() {
        SlotReference argument = new SlotReference("value", IntegerType.INSTANCE, true);
        CombineCombinator combine = new CombineCombinator(ImmutableList.of(argument), new Avg(argument));
        WindowExpression window = new WindowExpression(combine, ImmutableList.of(), ImmutableList.of());

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> new WindowFunctionChecker(window).checkWindowFunction());
        Assertions.assertEquals("Window function does not support aggregate combine function: avg_combine",
                exception.getMessage());
    }

    @Test
    void testOrthogonalBitmapFunctionsDoNotSupportAggState() {
        Assertions.assertTrue(NotSupportAggState.class.isAssignableFrom(
                OrthogonalBitmapExprCalculate.class));
        Assertions.assertTrue(NotSupportAggState.class.isAssignableFrom(
                OrthogonalBitmapExprCalculateCount.class));
        Assertions.assertTrue(NotSupportAggState.class.isAssignableFrom(
                OrthogonalBitmapIntersect.class));
        Assertions.assertTrue(NotSupportAggState.class.isAssignableFrom(
                OrthogonalBitmapIntersectCount.class));
        Assertions.assertTrue(NotSupportAggState.class.isAssignableFrom(
                OrthogonalBitmapUnionCount.class));

        SlotReference bitmap = new SlotReference("bitmap", BitmapType.INSTANCE, false);
        VarcharLiteral filterColumn = new VarcharLiteral("filter");
        VarcharLiteral expression = new VarcharLiteral("filter");
        OrthogonalBitmapExprCalculate nested =
                new OrthogonalBitmapExprCalculate(bitmap, filterColumn, expression);
        FunctionRegistry functionRegistry = new FunctionRegistry();
        Assertions.assertTrue(functionRegistry.findBuiltinFunctionBuilder(
                "orthogonal_bitmap_expr_calculate_state", nested.children()).isEmpty());
        Assertions.assertTrue(functionRegistry.findBuiltinFunctionBuilder(
                "orthogonal_bitmap_expr_calculate_combine", nested.children()).isEmpty());

        SlotReference bitmaps = new SlotReference(
                "bitmaps", ArrayType.of(BitmapType.INSTANCE), false);
        Assertions.assertFalse(functionRegistry.findBuiltinFunctionBuilder(
                "orthogonal_bitmap_union_count_foreach", ImmutableList.of(bitmaps)).isEmpty());
        Assertions.assertDoesNotThrow(() -> functionRegistry.findFunctionBuilder(
                "orthogonal_bitmap_union_count_foreach", bitmaps)
                .build("orthogonal_bitmap_union_count_foreach", bitmaps));

        Assertions.assertThrows(AnalysisException.class, () -> StateCombinator.create(nested));
    }

    @Test
    void testCombineDelegatesAggregatePhaseSupport() {
        SlotReference bitmap = new SlotReference("bitmap", BitmapType.INSTANCE, false);
        VarcharLiteral filterColumn = new VarcharLiteral("filter");
        VarcharLiteral expression = new VarcharLiteral("filter");
        OrthogonalBitmapExprCalculate nested =
                new OrthogonalBitmapExprCalculate(bitmap, filterColumn, expression);
        CombineCombinator combine = new CombineCombinator(nested.children(), nested);

        Assertions.assertFalse(combine.supportAggregatePhase(AggregatePhase.ONE));
        Assertions.assertTrue(combine.supportAggregatePhase(AggregatePhase.TWO));
    }
}
