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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.DataSketchesHllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.DataSketchesHllUnionAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnionAgg;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Crc32;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DataSketchesHllRollupHandlerTest {
    private static final SlotReference SKETCH = SlotReference.of("sk", StringType.INSTANCE);
    private static final SlotReference OTHER_SKETCH = SlotReference.of("other_sk", StringType.INSTANCE);
    private static final SlotReference MV_SLOT = SlotReference.of("mv_sk", StringType.INSTANCE);

    private static boolean canRollup(AggregateFunction query, Expression view) {
        return DataSketchesHllRollupHandler.INSTANCE.canRollup(
                query, query, Pair.of(view, MV_SLOT), ImmutableMap.of(view, MV_SLOT));
    }

    private static Function doRollup(AggregateFunction query, Expression view) {
        return DataSketchesHllRollupHandler.INSTANCE.doRollup(
                query, query, Pair.of(view, MV_SLOT), ImmutableMap.of(view, MV_SLOT));
    }

    @Test
    void testRollupMatrix() {
        DataSketchesHllUnion mvNoArg = new DataSketchesHllUnion(SKETCH);
        DataSketchesHllUnion mvK12 = new DataSketchesHllUnion(SKETCH, new IntegerLiteral(12));
        DataSketchesHllUnion mvK12Hll8 = new DataSketchesHllUnion(
                SKETCH, new IntegerLiteral(12), new StringLiteral("HLL_8"));
        DataSketchesHllUnion mvK21 = new DataSketchesHllUnion(SKETCH, new IntegerLiteral(21));

        // 1) No-arg query vs no-arg MV -> allowed
        Assertions.assertTrue(canRollup(new DataSketchesHllUnionAgg(SKETCH), mvNoArg));
        Assertions.assertTrue(canRollup(new DataSketchesHllUnion(SKETCH), mvNoArg));

        // 2) Kq query vs no-arg MV -> allowed
        Assertions.assertTrue(canRollup(new DataSketchesHllUnionAgg(SKETCH, new IntegerLiteral(8)), mvNoArg));
        Assertions.assertTrue(canRollup(
                new DataSketchesHllUnion(SKETCH, new IntegerLiteral(8), new StringLiteral("HLL_6")), mvNoArg));

        // 3) Kq query vs Kmv MV -> allowed iff Kq <= Kmv (tgt_type ignored)
        Assertions.assertTrue(canRollup(new DataSketchesHllUnionAgg(SKETCH, new IntegerLiteral(8)), mvK12Hll8));
        Assertions.assertTrue(canRollup(new DataSketchesHllUnionAgg(SKETCH, new IntegerLiteral(12)), mvK12));
        Assertions.assertTrue(canRollup(
                new DataSketchesHllUnion(SKETCH, new IntegerLiteral(8), new StringLiteral("HLL_6")), mvK12Hll8));
        Assertions.assertFalse(canRollup(new DataSketchesHllUnionAgg(SKETCH, new IntegerLiteral(16)), mvK12));
        Assertions.assertFalse(canRollup(
                new DataSketchesHllUnion(SKETCH, new IntegerLiteral(16), new StringLiteral("HLL_8")), mvK12Hll8));

        // 4) No-arg query vs Kmv < 21 -> rejected; vs Kmv == 21 -> allowed
        Assertions.assertFalse(canRollup(new DataSketchesHllUnionAgg(SKETCH), mvK12));
        Assertions.assertFalse(canRollup(new DataSketchesHllUnion(SKETCH), mvK12));
        Assertions.assertTrue(canRollup(new DataSketchesHllUnionAgg(SKETCH), mvK21));
        Assertions.assertTrue(canRollup(new DataSketchesHllUnion(SKETCH), mvK21));
    }

    @Test
    void testRejectsMismatchedArgsAndUnsupportedFunctions() {
        DataSketchesHllUnion mvNoArg = new DataSketchesHllUnion(SKETCH);
        Assertions.assertFalse(canRollup(new DataSketchesHllUnionAgg(OTHER_SKETCH), mvNoArg));
        Assertions.assertFalse(canRollup(new DataSketchesHllUnion(OTHER_SKETCH), mvNoArg));

        // View must be DataSketchesHllUnion, not DataSketchesHllUnionAgg or native HllUnion
        Assertions.assertFalse(canRollup(
                new DataSketchesHllUnionAgg(SKETCH), new DataSketchesHllUnionAgg(SKETCH)));
        SlotReference hllSlot = SlotReference.of("hll_col", HllType.INSTANCE);
        Assertions.assertFalse(canRollup(
                new DataSketchesHllUnionAgg(SKETCH), new HllUnion(hllSlot)));
        Assertions.assertFalse(canRollup(new HllUnionAgg(hllSlot), mvNoArg));

        // Non-literal lg_max_k is conservatively rejected
        Expression nonLiteralK = new Add(new IntegerLiteral(10), new Crc32(new StringLiteral("")));
        Assertions.assertFalse(canRollup(
                new DataSketchesHllUnionAgg(SKETCH, nonLiteralK), mvNoArg));
        Assertions.assertFalse(canRollup(
                new DataSketchesHllUnionAgg(SKETCH, new IntegerLiteral(8)),
                new DataSketchesHllUnion(SKETCH, nonLiteralK)));
    }

    @Test
    void testDoRollupPreservesQueryParameters() {
        DataSketchesHllUnion mvK12Hll4 = new DataSketchesHllUnion(
                SKETCH, new IntegerLiteral(12), new StringLiteral("HLL_4"));

        DataSketchesHllUnionAgg queryAgg = new DataSketchesHllUnionAgg(SKETCH, new IntegerLiteral(8));
        Assertions.assertTrue(canRollup(queryAgg, mvK12Hll4));
        Function rolledUpAgg = doRollup(queryAgg, mvK12Hll4);
        Assertions.assertInstanceOf(DataSketchesHllUnionAgg.class, rolledUpAgg);
        Assertions.assertEquals(ImmutableList.of(MV_SLOT, new IntegerLiteral(8)), rolledUpAgg.getArguments());

        DataSketchesHllUnion queryUnion = new DataSketchesHllUnion(
                SKETCH, new IntegerLiteral(10), new StringLiteral("HLL_8"));
        Assertions.assertTrue(canRollup(queryUnion, mvK12Hll4));
        Function rolledUpUnion = doRollup(queryUnion, mvK12Hll4);
        Assertions.assertInstanceOf(DataSketchesHllUnion.class, rolledUpUnion);
        Assertions.assertEquals(
                ImmutableList.of(MV_SLOT, new IntegerLiteral(10), new StringLiteral("HLL_8")),
                rolledUpUnion.getArguments());
    }
}
