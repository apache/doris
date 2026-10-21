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

package org.apache.doris.nereids.rules.exploration.mv.rollup;

import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CombinatorRollupHandlerTest {
    private final FunctionRegistry registry = new FunctionRegistry();
    private final SlotReference value = new SlotReference("v", DoubleType.INSTANCE, true);

    @BeforeAll
    static void setUp() {
        MemoTestUtils.createConnectContext();
    }

    private Expression build(String name, Expression argument) {
        return registry.findFunctionBuilder(name, argument).build(name, argument).first;
    }

    private Expression finalizeValue(boolean stored) {
        Expression state = build("avg_state", value);
        return build("avg_finalize", stored ? new SlotReference("s", state.getDataType(), true) : state);
    }

    private boolean canRollup(AggFunctionRollUpHandler handler, Expression query, Expression view) {
        SlotReference mvSlot = SlotReference.of("mv_state", view.getDataType());
        return handler.canRollup((AggregateFunction) query, query, Pair.of(view, mvSlot),
                ImmutableMap.of(view, mvSlot));
    }

    private void assertRollup(AggFunctionRollUpHandler handler, Expression query, Expression view, String name) {
        Assertions.assertTrue(canRollup(handler, query, view));
        SlotReference mvSlot = SlotReference.of("mv_state", view.getDataType());
        Function result = handler.doRollup((AggregateFunction) query, query, Pair.of(view, mvSlot),
                ImmutableMap.of(view, mvSlot));
        Assertions.assertEquals(name, result.getName());
        Assertions.assertEquals(ImmutableList.of(mvSlot), result.getArguments());
    }

    @Test
    void testDifferentOuterAggregatesOverFinalize() {
        for (boolean stored : ImmutableList.of(false, true)) {
            Expression finalized = finalizeValue(stored);
            Assertions.assertFalse(canRollup(BothCombinatorRollupHandler.INSTANCE,
                    build("sum_combine", finalized), build("max_combine", finalized)));
        }
    }

    @Test
    void testScalarArgumentMustMatch() {
        Expression finalized = finalizeValue(false);
        Assertions.assertFalse(canRollup(BothCombinatorRollupHandler.INSTANCE,
                build("sum_combine", finalized), build("sum_combine", new Abs(finalized))));
    }

    @Test
    void testSameOuterAggregateOverFinalize() {
        Expression finalized = finalizeValue(false);
        assertRollup(BothCombinatorRollupHandler.INSTANCE,
                build("sum_combine", finalized), build("sum_combine", finalized), "sum_union");
    }

    @Test
    void testMergeAndUnionStateChain() {
        Expression state = build("sum_state", value);
        assertRollup(BothCombinatorRollupHandler.INSTANCE,
                build("sum_merge", state), build("sum_union", state), "sum_merge");
        assertRollup(BothCombinatorRollupHandler.INSTANCE,
                build("sum_merge", build("sum_union", state)), build("sum_union", state), "sum_merge");
    }

    @Test
    void testUnionStopsAtCombineValueArguments() {
        Expression finalized = finalizeValue(false);
        assertRollup(BothCombinatorRollupHandler.INSTANCE,
                build("sum_merge", build("sum_combine", finalized)),
                build("sum_union", build("sum_combine", finalized)), "sum_merge");
        Assertions.assertFalse(canRollup(BothCombinatorRollupHandler.INSTANCE,
                build("sum_merge", build("sum_combine", finalized)),
                build("max_union", build("max_combine", finalized))));
    }

    @Test
    void testStateRetainsFinalizeArgument() {
        Expression finalized = finalizeValue(false);
        Assertions.assertFalse(canRollup(BothCombinatorRollupHandler.INSTANCE,
                build("sum_merge", build("sum_state", finalized)),
                build("max_union", build("max_state", finalized))));
    }

    @Test
    void testStoredStateMustMatch() {
        Expression state = build("sum_state", value);
        SlotReference first = SlotReference.of("first_state", state.getDataType());
        SlotReference second = SlotReference.of("second_state", state.getDataType());
        assertRollup(BothCombinatorRollupHandler.INSTANCE,
                build("sum_merge", first), build("sum_union", first), "sum_merge");
        Assertions.assertFalse(canRollup(BothCombinatorRollupHandler.INSTANCE,
                build("sum_merge", first), build("sum_union", second)));
    }

    @Test
    void testSingleCombinatorOverFinalize() {
        for (boolean stored : ImmutableList.of(false, true)) {
            Expression finalized = finalizeValue(stored);
            Sum query = new Sum(finalized);
            assertRollup(SingleCombinatorRollupHandler.INSTANCE,
                    query, build("sum_combine", finalized), "sum_merge");
            Assertions.assertFalse(canRollup(SingleCombinatorRollupHandler.INSTANCE,
                    query, build("max_combine", finalized)));
            Assertions.assertFalse(canRollup(SingleCombinatorRollupHandler.INSTANCE,
                    query, build("sum_combine", new Abs(finalized))));
        }
    }

    @Test
    void testSingleCombinatorStateChain() {
        Expression finalized = finalizeValue(false);
        Sum query = new Sum(finalized);
        assertRollup(SingleCombinatorRollupHandler.INSTANCE, query,
                build("sum_union", StateCombinator.create(query)), "sum_merge");
        assertRollup(SingleCombinatorRollupHandler.INSTANCE, query,
                build("sum_union", build("sum_combine", finalized)), "sum_merge");
    }
}
