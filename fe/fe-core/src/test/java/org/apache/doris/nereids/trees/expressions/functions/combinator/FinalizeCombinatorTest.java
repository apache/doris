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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class FinalizeCombinatorTest {
    private FinalizeCombinator build(String name, Expression argument) {
        FunctionBuilder builder = new FunctionRegistry().findFunctionBuilder(name, argument);
        return (FinalizeCombinator) builder.build(name, argument).first;
    }

    @Test
    void testScalarClassificationAndTranslation() {
        FunctionRegistry registry = new FunctionRegistry();
        Assertions.assertTrue(registry.isBuiltinAggStateCombinator("avg_finalize"));
        Assertions.assertFalse(registry.isAggregateFunction(null, "avg_finalize"));
        Assertions.assertFalse(registry.isBuiltinAggStateCombinator("abs_finalize"));
        SlotReference value = new SlotReference("v", IntegerType.INSTANCE, true);
        StateCombinator state = StateCombinator.create(new Avg(value));
        FinalizeCombinator finalize = build("avg_finalize", state);
        Assertions.assertEquals(DoubleType.INSTANCE, finalize.getDataType());
        Assertions.assertTrue(finalize.nullable());
        FunctionCallExpr translated = (FunctionCallExpr) ExpressionTranslator.translate(
                finalize, new PlanTranslatorContext());
        Assertions.assertEquals("avg_finalize", translated.getFn().getFunctionName().getFunction());
        Assertions.assertEquals(Function.BinaryType.AGG_STATE, translated.getFn().getBinaryType());
        Assertions.assertEquals(Function.NullableMode.ALWAYS_NULLABLE, translated.getFn().getNullableMode());
        Assertions.assertEquals(state.getDataType().toCatalogDataType(), translated.getFn().getArgs()[0]);
    }

    @Test
    void testCountAndOuterNullability() {
        SlotReference value = new SlotReference("v", IntegerType.INSTANCE, true);
        StateCombinator state = StateCombinator.create(new Count(value));
        Assertions.assertFalse(build("count_finalize", state).nullable());
        SlotReference nullableState = new SlotReference("s", state.getDataType(), true);
        Assertions.assertTrue(build("count_finalize", nullableState).nullable());
    }

    @Test
    void testNonNullableStateUsesNestedResultType() {
        SlotReference value = new SlotReference("v", IntegerType.INSTANCE, false);
        StateCombinator state = StateCombinator.create(new Avg(value));
        Assertions.assertFalse(build("avg_finalize", state).nullable());
    }

    @Test
    void testDecimalResultPrecision() {
        MemoTestUtils.createConnectContext();
        SlotReference value = new SlotReference("v", DecimalV3Type.createDecimalV3Type(12, 2), true);
        FinalizeCombinator finalize = build("avg_finalize", StateCombinator.create(new Avg(value)));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(38, 4), finalize.getDataType());
    }

    @Test
    void testStateFunctionMustMatch() {
        for (String sql : ImmutableList.of("select sum_finalize(avg_state(1))",
                "select avg_finalize(sum_state(1))")) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql));
            Assertions.assertTrue(exception.getMessage().contains("requires a state of"));
        }
    }

    @Test
    void testAliasCanonicalization() {
        AggStateType state = new AggStateType("var_pop", ImmutableList.of(DoubleType.INSTANCE),
                ImmutableList.of(true), true);
        FinalizeCombinator finalize = build("variance_finalize", new SlotReference("s", state, false));
        Assertions.assertEquals("variance_finalize", finalize.getName());
        Assertions.assertEquals(DoubleType.INSTANCE, finalize.getDataType());
    }

    @Test
    void testInvalidArguments() {
        for (String sql : ImmutableList.of("select avg_finalize(1)", "select avg_finalize()",
                "select avg_finalize(avg_state(1), avg_state(2))",
                "select avg_finalize(distinct avg_state(1))")) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql));
        }
    }

    @Test
    void testStoredParameterizedStateAndCombine() {
        PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                "select topn_finalize(s) from (select topn_combine('x', 2) s) t");
        PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                "select avg_finalize(avg_combine(cast(1 as int)))");
    }
}
