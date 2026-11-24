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
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.rules.analysis.WindowFunctionChecker;
import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.rules.expression.rules.ConvertAggStateCast;
import org.apache.doris.nereids.rules.rewrite.PushDownAggStateCast;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.AIAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregatePhase;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.NotSupportAggState;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapExprCalculate;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapExprCalculateCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapIntersect;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapIntersectCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.MoreFieldsThread;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class CombineCombinatorTest {

    @Test
    void testConditionalCodecCastPreservesNullsAndConditions() {
        int version = Config.be_exec_version;
        try {
            MemoTestUtils.createConnectContext();
            Config.be_exec_version = 20;
            SlotReference value = new SlotReference("v", IntegerType.INSTANCE, true);
            SlotReference condition = new SlotReference("condition", BooleanType.INSTANCE, true);
            CombineCombinator combine = new CombineCombinator(ImmutableList.of(value), new Avg(value));
            AggStateType source = (AggStateType) combine.getDataType();
            AggStateType target = source.withBeExecVersion(19);
            If conditional = new If(condition, combine, new NullLiteral(source));
            // CASE without ELSE has an implicit outer NULL. COALESCE must preserve
            // explicit NULL alternatives as well, not replace them with empty states.
            List<Expression> wrappers = ImmutableList.of(conditional,
                    new CaseWhen(ImmutableList.of(new WhenClause(condition, combine))),
                    new Coalesce(conditional, new NullLiteral(source)));
            for (Expression wrapper : wrappers) {
                Assertions.assertEquals(source, wrapper.getDataType()); // Populate cached signatures.
                Cast converted = MoreFieldsThread.keepFunctionSignature(true,
                        () -> ConvertAggStateCast.convert(new Cast(wrapper, target)));
                Expression rebuilt = converted.child();
                Assertions.assertEquals(wrapper.getClass(), rebuilt.getClass());
                Assertions.assertEquals(target, rebuilt.getDataType());
                Assertions.assertTrue(rebuilt.nullable());
                List<CombineCombinator> producers = rebuilt.collectToList(CombineCombinator.class::isInstance);
                Assertions.assertEquals(1, producers.size());
                Assertions.assertEquals(target, producers.get(0).getDataType());
                Assertions.assertEquals(wrapper.collectToList(NullLiteral.class::isInstance).size(),
                        rebuilt.collectToList(NullLiteral.class::isInstance).size());
                for (NullLiteral literal : rebuilt.<NullLiteral>collectToList(NullLiteral.class::isInstance)) {
                    Assertions.assertEquals(target, literal.getDataType());
                }
                for (If expr : rebuilt.<If>collectToList(If.class::isInstance)) {
                    Assertions.assertSame(condition, expr.getCondition());
                }
                for (WhenClause clause : rebuilt.<WhenClause>collectToList(WhenClause.class::isInstance)) {
                    Assertions.assertSame(condition, clause.getOperand());
                }
            }
        } finally {
            Config.be_exec_version = version;
        }
    }

    @Test
    void testConditionalCastDoesNotRelabelStoredState() {
        MemoTestUtils.createConnectContext();
        AggStateType source = new AggStateType("avg", ImmutableList.of(IntegerType.INSTANCE),
                ImmutableList.of(true), true, 20);
        AggStateType target = source.withBeExecVersion(19);
        SlotReference stored = new SlotReference("stored", source, true);
        SlotReference condition = new SlotReference("condition", BooleanType.INSTANCE, false);
        // A stored value has no raw-input producer to rebuild. Moving its cast into
        // IF must not hide the unsupported 20-to-19 conversion from validation.
        Cast converted = ConvertAggStateCast.convert(
                new Cast(new If(condition, stored, new NullLiteral(source)), target));
        List<Cast> storedCasts = converted.collectToList(
                expr -> expr instanceof Cast && expr.child(0).equals(stored));
        Assertions.assertEquals(1, storedCasts.size());
        Assertions.assertThrows(AnalysisException.class, () -> CheckCast.validate(storedCasts.get(0)));
        Assertions.assertEquals(source, storedCasts.get(0).child().getDataType());
    }

    @Test
    void testCombineCastUsesSinkCodec() {
        int version = Config.be_exec_version;
        try {
            MemoTestUtils.createConnectContext();
            Config.be_exec_version = 20;
            Expression argument = new Nullable(new IntegerLiteral(2));
            CombineCombinator combine = new CombineCombinator(ImmutableList.of(argument), new Avg(argument));
            // Read the signature first. The sink rewrite must replace this cached version 20.
            Assertions.assertEquals(20, ((AggStateType) combine.getDataType()).getBeExecVersion());
            AggStateType target = new AggStateType("avg", ImmutableList.of(IntegerType.INSTANCE),
                    ImmutableList.of(true), true, 19);
            // Also model constant folding: the child is now non-null, but the analyzed
            // input layout stays nullable. Changing the codec must preserve that layout.
            CombineCombinator folded = MoreFieldsThread.keepFunctionSignature(true,
                    () -> combine.withChildren(ImmutableList.of(new IntegerLiteral(2))));
            for (CombineCombinator source : ImmutableList.of(combine, folded)) {
                Cast cast = MoreFieldsThread.keepFunctionSignature(true,
                        () -> ConvertAggStateCast.convert(new Cast(source, target)));
                CombineCombinator producer = (CombineCombinator) cast.child();
                Assertions.assertEquals(target, producer.getDataType());
                Assertions.assertNotEquals(source, producer);

                FunctionCallExpr translated = (FunctionCallExpr) ExpressionTranslator.translate(
                        new AggregateExpression(producer, AggregateParam.LOCAL_RESULT), new PlanTranslatorContext());
                Assertions.assertEquals(target.toCatalogDataType(), translated.getFn().getReturnType());
                Assertions.assertTrue(translated.getChild(0).isNullable());
            }
        } finally {
            Config.be_exec_version = version;
        }
    }

    @Test
    void testCodecCastKeepsOtherConsumers() {
        int version = Config.be_exec_version;
        try {
            ConnectContext context = MemoTestUtils.createConnectContext();
            Config.be_exec_version = 20;
            Alias value = new Alias(new Nullable(new IntegerLiteral(2)), "v");
            LogicalOneRowRelation input = new LogicalOneRowRelation(
                    StatementScopeIdGenerator.newRelationId(), ImmutableList.of(value));
            CombineCombinator combine = new CombineCombinator(ImmutableList.of(value.toSlot()), new Avg(value.toSlot()));
            Alias original = new Alias(combine, "new_state");
            AggStateType oldType = new AggStateType("avg", ImmutableList.of(IntegerType.INSTANCE),
                    ImmutableList.of(true), true, 19);
            LogicalAggregate<LogicalOneRowRelation> aggregate = new LogicalAggregate<>(
                    ImmutableList.of(), ImmutableList.of(original), input);
            // One consumer still needs version 20. Two other consumers need version 19.
            // They must share a new output without changing the original slot or its producer.
            LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(original.toSlot(),
                    new Alias(new Cast(original.toSlot(), oldType), "old_state"),
                    new Alias(new Cast(original.toSlot(), oldType), "old_again")), aggregate);
            LogicalProject<?> rewritten = (LogicalProject<?>) PlanChecker
                    .from(context, project)
                    .applyTopDown(new PushDownAggStateCast()).getPlan();
            Assertions.assertEquals(original.toSlot(), rewritten.getProjects().get(0));
            Assertions.assertEquals(oldType, rewritten.getProjects().get(1).getDataType());
            Assertions.assertEquals(rewritten.getProjects().get(1).child(0), rewritten.getProjects().get(2).child(0));
            LogicalAggregate<?> newAggregate = (LogicalAggregate<?>) rewritten.child();
            Assertions.assertEquals(2, newAggregate.getOutputExpressions().size());
            Assertions.assertEquals(original, newAggregate.getOutputExpressions().get(0));
            Assertions.assertEquals(oldType, newAggregate.getOutputExpressions().get(1).getDataType());
        } finally {
            Config.be_exec_version = version;
        }
    }

    @Test
    void testCombineRebuildKeepsCodecVersion() {
        int version = Config.be_exec_version;
        try {
            Config.be_exec_version = 19;
            SlotReference argument = new SlotReference("value", IntegerType.INSTANCE, true);
            CombineCombinator combine = new CombineCombinator(ImmutableList.of(argument), new Avg(argument));
            AggStateType original = (AggStateType) combine.getDataType();
            Config.be_exec_version = 20;
            // A rewrite can preserve or recompute the signature. Neither path may replace
            // the producer's chosen codec with the current FE version.
            for (boolean keepSignature : new boolean[] {false, true}) {
                MoreFieldsThread.keepFunctionSignature(keepSignature, () -> {
                    CombineCombinator rebuilt = combine.withChildren(ImmutableList.of(argument));
                    Assertions.assertEquals(original, rebuilt.getDataType());
                    Assertions.assertEquals(original,
                            rebuilt.withDistinctAndChildren(false, ImmutableList.of(argument)).getDataType());
                    return null;
                });
            }
        } finally {
            Config.be_exec_version = version;
        }
    }

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
    void testAggStateCombinatorClassification() {
        SlotReference argument = new SlotReference("value", IntegerType.INSTANCE, false);
        FunctionRegistry functionRegistry = new FunctionRegistry();
        Assertions.assertTrue(functionRegistry
                .findBuiltinFunctionBuilder("abs_combine", ImmutableList.of(argument)).isEmpty());
        Assertions.assertFalse(functionRegistry.isBuiltinAggStateCombinator("abs_combine"));
        Assertions.assertFalse(functionRegistry.isAggregateFunction(null, "abs_combine"));
        Assertions.assertTrue(functionRegistry.isBuiltinAggStateCombinator("avg_state"));
        Assertions.assertTrue(functionRegistry.isBuiltinAggStateCombinator("avg_merge"));
        Assertions.assertTrue(functionRegistry.isBuiltinAggStateCombinator("avg_union"));
        Assertions.assertTrue(functionRegistry.isBuiltinAggStateCombinator("avg_combine"));
        Assertions.assertTrue(functionRegistry.isBuiltinAggStateCombinator("avg_foreach"));
        Assertions.assertFalse(functionRegistry.isAggregateFunction(null, "avg_state"));
        Assertions.assertTrue(functionRegistry.isAggregateFunction(null, "avg_merge"));
        Assertions.assertTrue(functionRegistry.isAggregateFunction(null, "avg_union"));
        Assertions.assertTrue(functionRegistry.isAggregateFunction(null, "avg_combine"));
        Assertions.assertTrue(functionRegistry.isAggregateFunction(null, "avg_foreach"));
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
        Assertions.assertTrue(CheckCast.checkWithLooseAggState(exactType, differentSubtype, false));
        Assertions.assertTrue(CheckCast.checkWithLooseAggState(exactType, differentNullability, false));
        Assertions.assertTrue(CheckCast.check(exactType, exactType, false));
        Assertions.assertFalse(CheckCast.check(exactType, differentSubtype, false));
        Assertions.assertFalse(CheckCast.check(exactType, differentNullability, false));
        Assertions.assertSame(combine, ConvertAggStateCast.convert(new Cast(combine, differentSubtype)).child());
        Assertions.assertSame(combine, ConvertAggStateCast.convert(new Cast(combine, differentNullability)).child());

        PlanChecker checker = PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze("select cast(avg_combine(cast(1 as int)) as agg_state<avg(bigint not null)>)");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, checker::rewrite);
        Assertions.assertTrue(exception.getMessage().contains(
                "cannot cast agg_state<avg("));
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
