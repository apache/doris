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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.exploration.mv.rollup.SingleCombinatorRollupHandler;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.UnionCombinator;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CountTest {
    @Test
    void testCountDistinctStillAllowsOrdinaryType() {
        Count count = new Count(true, SlotReference.of("k", IntegerType.INSTANCE));

        Assertions.assertDoesNotThrow(count::checkLegalityAfterRewrite);
    }

    @Test
    void testCountDistinctAllowsVariantWithDedicatedState() {
        Count count = new Count(true, SlotReference.of("v", VariantType.INSTANCE));

        Assertions.assertDoesNotThrow(count::checkLegalityAfterRewrite);
    }

    @Test
    void testMultiDistinctCountAllowsVariantWithDedicatedState() {
        MultiDistinctCount count = new MultiDistinctCount(SlotReference.of("v", VariantType.INSTANCE));

        Assertions.assertDoesNotThrow(count::checkLegalityAfterRewrite);
    }

    @Test
    void testCountDistinctStillRejectsJson() {
        Count count = new Count(true, SlotReference.of("j", JsonType.INSTANCE));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, count::checkLegalityAfterRewrite);
        Assertions.assertTrue(exception.getMessage().contains("COUNT DISTINCT could not process type"));
        Assertions.assertTrue(exception.getMessage().contains("count(DISTINCT j)"));
    }

    @Test
    void testCountDistinctStillRejectsHll() {
        Count count = new Count(true, SlotReference.of("h", HllType.INSTANCE));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, count::checkLegalityAfterRewrite);
        Assertions.assertTrue(exception.getMessage().contains("COUNT DISTINCT could not process type"));
        Assertions.assertTrue(exception.getMessage().contains("count(DISTINCT h)"));
    }

    @Test
    void testMultiDistinctCountAllowsArray() {
        MultiDistinctCount count = new MultiDistinctCount(
                SlotReference.of("arr", ArrayType.of(IntegerType.INSTANCE)));

        Assertions.assertDoesNotThrow(count::checkLegalityAfterRewrite);
    }

    @Test
    void testMultiDistinctCountAllowsUnboundArgumentBeforeLegalityCheck() {
        Assertions.assertDoesNotThrow(() -> new MultiDistinctCount(new UnboundSlot("kint")));
    }

    @Test
    void testMultiDistinctCountCanRollupFromUnionState() {
        SlotReference kint = SlotReference.of("kint", IntegerType.INSTANCE);
        MultiDistinctCount queryFunction = new MultiDistinctCount(kint);
        StateCombinator stateCombinator = StateCombinator.create(queryFunction);
        UnionCombinator unionCombinator = new UnionCombinator(ImmutableList.of(stateCombinator), queryFunction);
        SlotReference mvSlot = SlotReference.of("__multi_distinct_count_1", unionCombinator.getDataType());
        Pair<Expression, Expression> mvExprToMvScanExprPair = Pair.of(unionCombinator, mvSlot);

        Assertions.assertTrue(SingleCombinatorRollupHandler.INSTANCE.canRollup(
                queryFunction, queryFunction, mvExprToMvScanExprPair,
                ImmutableMap.of(unionCombinator, mvSlot)));
    }
}
