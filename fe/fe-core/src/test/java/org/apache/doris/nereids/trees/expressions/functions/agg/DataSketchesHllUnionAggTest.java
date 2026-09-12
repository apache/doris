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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.combinator.MergeCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.UnionCombinator;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

class DataSketchesHllUnionAggTest {
    private static final SlotReference SKETCH = SlotReference.of("sketch", StringType.INSTANCE);

    @Test
    void testSignatures() {
        List<FunctionSignature> signatures = new DataSketchesHllUnionAgg(SKETCH).getSignatures();

        Assertions.assertEquals(6, signatures.size());
        Assertions.assertEquals(3, signatures.stream().filter(signature -> signature.arity == 1).count());
        Assertions.assertEquals(3, signatures.stream().filter(signature -> signature.arity == 2).count());
        signatures.stream().filter(signature -> signature.arity == 2)
                .forEach(signature -> Assertions.assertEquals(IntegerType.INSTANCE, signature.getArgType(1)));
    }

    @Test
    void testDistinctIsIgnored() {
        DataSketchesHllUnionAgg oneArgument = new DataSketchesHllUnionAgg(true, SKETCH);
        DataSketchesHllUnionAgg twoArguments =
                new DataSketchesHllUnionAgg(true, SKETCH, new IntegerLiteral(8));

        for (DataSketchesHllUnionAgg function : ImmutableList.of(oneArgument, twoArguments)) {
            DataSketchesHllUnionAgg rewritten = function.withDistinctAndChildren(true, function.children());
            Assertions.assertFalse(function.isDistinct());
            Assertions.assertFalse(rewritten.isDistinct());
            Assertions.assertTrue(function.getDistinctArguments().isEmpty());
            Assertions.assertTrue(rewritten.getDistinctArguments().isEmpty());
        }
    }

    @Test
    void testLgMaxKBoundaries() {
        for (int value : new int[] {7, 21}) {
            DataSketchesHllUnionAgg function =
                    new DataSketchesHllUnionAgg(SKETCH, new IntegerLiteral(value));
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
            Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
        }
    }

    @Test
    void testLgMaxKMustBeAConstantInteger() {
        DataSketchesHllUnionAgg nonConstant = new DataSketchesHllUnionAgg(
                SKETCH, SlotReference.of("lg_max_k", IntegerType.INSTANCE));
        DataSketchesHllUnionAgg nonInteger = new DataSketchesHllUnionAgg(
                SKETCH, new DecimalV3Literal(new BigDecimal("8.5")));
        DataSketchesHllUnionAgg nullValue = new DataSketchesHllUnionAgg(SKETCH, new NullLiteral());

        Assertions.assertDoesNotThrow(nonConstant::checkLegalityBeforeTypeCoercion);
        Assertions.assertThrows(AnalysisException.class, nonConstant::checkLegalityAfterRewrite);
        Assertions.assertThrows(AnalysisException.class, nonInteger::checkLegalityBeforeTypeCoercion);
        Assertions.assertThrows(AnalysisException.class, nullValue::checkLegalityBeforeTypeCoercion);
    }

    @Test
    void testLgMaxKRange() {
        for (int value : new int[] {6, 22}) {
            DataSketchesHllUnionAgg function =
                    new DataSketchesHllUnionAgg(SKETCH, new IntegerLiteral(value));
            Assertions.assertThrows(AnalysisException.class, function::checkLegalityAfterRewrite);
        }
    }

    @Test
    void testTypedAggStateCanBuildMergeAndUnion() {
        FunctionRegistry functionRegistry = new FunctionRegistry();
        for (String functionName : ImmutableList.of(
                "datasketches_hll_union_agg", "ds_hll_estimate", "datasketches_hll_estimate")) {
            for (StateCombinator state : ImmutableList.of(
                    buildState(functionRegistry, functionName + "_state", SKETCH),
                    buildState(functionRegistry, functionName + "_state", SKETCH, new IntegerLiteral(8)))) {
                Assertions.assertEquals(StateCombinator.class, state.getClass());
                Assertions.assertDoesNotThrow(state::checkLegalityAfterRewrite);
                SlotReference stateSlot = new SlotReference("state", state.getDataType(), false);

                String mergeName = functionName + "_merge";
                FunctionBuilder mergeBuilder = functionRegistry.findFunctionBuilder(mergeName, stateSlot);
                MergeCombinator merge = (MergeCombinator) mergeBuilder.build(mergeName, stateSlot).first;
                Assertions.assertDoesNotThrow(merge::checkLegalityBeforeTypeCoercion);

                String unionName = functionName + "_union";
                FunctionBuilder unionBuilder = functionRegistry.findFunctionBuilder(unionName, stateSlot);
                UnionCombinator union = (UnionCombinator) unionBuilder.build(unionName, stateSlot).first;
                Assertions.assertDoesNotThrow(union::checkLegalityBeforeTypeCoercion);
            }
        }
    }

    @Test
    void testStateCombinatorValidatesLgMaxKAfterRewrite() {
        FunctionRegistry functionRegistry = new FunctionRegistry();
        for (String functionName : ImmutableList.of(
                "datasketches_hll_union_agg", "ds_hll_estimate", "datasketches_hll_estimate")) {
            String stateName = functionName + "_state";
            for (int value : new int[] {7, 21}) {
                StateCombinator state = buildState(functionRegistry, stateName, SKETCH, new IntegerLiteral(value));
                Assertions.assertDoesNotThrow(state::checkLegalityAfterRewrite);
            }
            for (Expression invalidLgMaxK : ImmutableList.of(
                    new IntegerLiteral(6),
                    new IntegerLiteral(22),
                    SlotReference.of("lg_max_k", IntegerType.INSTANCE))) {
                StateCombinator state = buildState(functionRegistry, stateName, SKETCH, invalidLgMaxK);
                Assertions.assertThrows(AnalysisException.class, state::checkLegalityAfterRewrite);
            }
        }
    }

    @Test
    void testStateCombinatorRewritesAllChildren() {
        FunctionRegistry functionRegistry = new FunctionRegistry();
        StateCombinator state = buildState(
                functionRegistry, "datasketches_hll_union_agg_state", SKETCH, new IntegerLiteral(8));
        StateCombinator rewritten = (StateCombinator) state.accept(new DefaultExpressionRewriter<Void>() {
            @Override
            public Expression visitIntegerLiteral(IntegerLiteral integerLiteral, Void context) {
                return new IntegerLiteral(22);
            }
        }, null);

        Assertions.assertEquals(2, state.arity());
        Assertions.assertThrows(AnalysisException.class, rewritten::checkLegalityAfterRewrite);
    }

    private static StateCombinator buildState(
            FunctionRegistry functionRegistry, String stateName, Expression... stateArguments) {
        List<Expression> arguments = ImmutableList.copyOf(stateArguments);
        FunctionBuilder stateBuilder = functionRegistry.findFunctionBuilder(stateName, arguments);
        return (StateCombinator) stateBuilder.build(stateName, arguments).first;
    }
}
