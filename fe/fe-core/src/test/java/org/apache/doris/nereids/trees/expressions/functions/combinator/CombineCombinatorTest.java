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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.IntegerType;

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
    }

    @Test
    void testZeroArgumentAggregateIsRejected() {
        FunctionRegistry functionRegistry = new FunctionRegistry();
        Assertions.assertTrue(functionRegistry
                .findBuiltinFunctionBuilder("count_combine", ImmutableList.of()).isEmpty());
        Assertions.assertThrows(AnalysisException.class,
                () -> new CombineCombinator(ImmutableList.of(), new Count()));
    }
}
