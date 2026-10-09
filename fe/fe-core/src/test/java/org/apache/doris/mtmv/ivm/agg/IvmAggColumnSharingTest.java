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

package org.apache.doris.mtmv.ivm.agg;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Verifies the unified column pool sharing introduced for IVM aggregate normalize:
 * visible aggregate columns are reused as hidden state columns, and delta outputs
 * are deduplicated by column name.
 */
class IvmAggColumnSharingTest extends IvmAggProcessorTestBase {
    private final IvmAggFunctionRegistry registry = IvmAggFunctionRegistry.INSTANCE;

    // ---------------------------------------------------------
    // Normalize: hidden state columns are shared from the pool.
    // ---------------------------------------------------------

    @Test
    void testVisibleCountColumnReusedAsSumHiddenState() {
        // SUM(x) needs hidden COUNT(x); a visible COUNT(x) column is already in the pool.
        Map<IvmAggColumnKey, Slot> pool = new LinkedHashMap<>();
        pool.put(IvmAggColumnKey.of(IvmAggFunctionKind.COUNT, value),
                new Alias(new Count(value), "cnt_v").toSlot());
        List<NamedExpression> hiddenOutputs = new ArrayList<>();

        IvmAggTargetSpec spec = new IvmAggSumProcessor().buildTargetSpec(
                0, new Sum(value), new Alias(new Sum(value), "sum_v"),
                pool, hiddenOutputs);

        Assertions.assertTrue(hiddenOutputs.isEmpty(), "visible COUNT(x) should be reused, no new hidden column");
        // The hidden COUNT state now points at the visible cnt_v slot.
        Slot hiddenCount = spec.toPlaceholderTarget().getHiddenStateSlot(IvmAggStateKey.COUNT);
        Assertions.assertEquals("cnt_v", hiddenCount.getName());
    }

    @Test
    void testAvgReusesVisibleSumAndCountColumns() {
        // AVG(x) needs hidden SUM(x) + COUNT(x); both visible columns exist in the pool.
        Map<IvmAggColumnKey, Slot> pool = new LinkedHashMap<>();
        pool.put(IvmAggColumnKey.of(IvmAggFunctionKind.SUM, value),
                new Alias(new Sum(value), "sum_v").toSlot());
        pool.put(IvmAggColumnKey.of(IvmAggFunctionKind.COUNT, value),
                new Alias(new Count(value), "cnt_v").toSlot());
        List<NamedExpression> hiddenOutputs = new ArrayList<>();

        IvmAggTargetSpec spec = new IvmAggAvgProcessor().buildTargetSpec(
                0, new Avg(value), new Alias(new Avg(value), "avg_v"),
                pool, hiddenOutputs);

        Assertions.assertTrue(hiddenOutputs.isEmpty(), "AVG should reuse visible SUM and COUNT columns");
        IvmAggTarget target = spec.toPlaceholderTarget();
        Assertions.assertEquals("sum_v", target.getHiddenStateSlot(IvmAggStateKey.SUM).getName());
        Assertions.assertEquals("cnt_v", target.getHiddenStateSlot(IvmAggStateKey.COUNT).getName());
    }

    @Test
    void testAvgWithoutVisibleColumnsCreatesHiddenSumAndCount() {
        // AVG(x) alone: no visible SUM/COUNT column exists, so hidden columns are created.
        Map<IvmAggColumnKey, Slot> pool = new LinkedHashMap<>();
        List<NamedExpression> hiddenOutputs = new ArrayList<>();

        IvmAggTargetSpec spec = new IvmAggAvgProcessor().buildTargetSpec(
                0, new Avg(value), new Alias(new Avg(value), "avg_v"),
                pool, hiddenOutputs);

        Assertions.assertEquals(2, hiddenOutputs.size(), "AVG alone needs hidden SUM and COUNT");
        Assertions.assertTrue(hiddenOutputs.get(0).getName().startsWith("__DORIS_IVM_AGG_0_"));
        Assertions.assertTrue(hiddenOutputs.get(1).getName().startsWith("__DORIS_IVM_AGG_0_"));
        IvmAggTarget target = spec.toPlaceholderTarget();
        Assertions.assertEquals(2, target.getHiddenStateSlots().size());
    }

    @Test
    void testSumCreatesHiddenCountThenAvgReusesIt() {
        // SUM(x) processed first creates hidden COUNT(x); AVG(x) later reuses it.
        Map<IvmAggColumnKey, Slot> pool = new LinkedHashMap<>();
        List<NamedExpression> hiddenOutputs = new ArrayList<>();

        IvmAggTargetSpec sumSpec = new IvmAggSumProcessor().buildTargetSpec(
                0, new Sum(value), new Alias(new Sum(value), "sum_v"),
                pool, hiddenOutputs);
        String hiddenCountName = sumSpec.toPlaceholderTarget()
                .getHiddenStateSlot(IvmAggStateKey.COUNT).getName();
        Assertions.assertEquals(1, hiddenOutputs.size());

        // AVG reuses the SUM's hidden COUNT; only a hidden SUM is created.
        IvmAggTargetSpec avgSpec = new IvmAggAvgProcessor().buildTargetSpec(
                1, new Avg(value), new Alias(new Avg(value), "avg_v"),
                pool, hiddenOutputs);
        IvmAggTarget avgTarget = avgSpec.toPlaceholderTarget();
        Assertions.assertEquals(hiddenCountName,
                avgTarget.getHiddenStateSlot(IvmAggStateKey.COUNT).getName());
        Assertions.assertEquals(2, hiddenOutputs.size(), "SUM hidden COUNT + AVG hidden SUM only");
        Assertions.assertTrue(hiddenOutputs.get(1).getName().startsWith("__DORIS_IVM_AGG_1_SUM"));
    }

    @Test
    void testKindOfMapsVisibleFunctionToKind() {
        Assertions.assertEquals(IvmAggFunctionKind.SUM, registry.kindOf(new Sum(value)));
        Assertions.assertEquals(IvmAggFunctionKind.COUNT, registry.kindOf(new Count(value)));
        Assertions.assertEquals(IvmAggFunctionKind.AVG, registry.kindOf(new Avg(value)));
    }

    // ---------------------------------------------------------
    // Delta: shared state columns produce a single delta output.
    // ---------------------------------------------------------

    @Test
    void testDeltaOutputsDeduplicatedByColumnName() {
        // Two targets (SUM and AVG) both reference the same hidden COUNT column.
        Slot sharedCount = slot("cnt_v", IntegerType.INSTANCE);
        IvmAggTarget sumTarget = targetWithHidden(
                target(0, IvmAggFunctionKind.SUM, "sum_v", BigIntType.INSTANCE,
                        new LinkedHashMap<>(), ImmutableList.of(value)),
                IvmAggStateKey.COUNT, sharedCount);
        IvmAggTarget avgTarget = targetWithHidden(
                target(1, IvmAggFunctionKind.AVG, "avg_v", BigIntType.INSTANCE,
                        new LinkedHashMap<>(), ImmutableList.of(value)),
                IvmAggStateKey.COUNT, sharedCount);

        List<NamedExpression> outputs = new ArrayList<>();
        Set<String> emitted = new HashSet<>();
        registry.appendDeltaAggregateOutputs(sumTarget, dmlFactor, outputs,
                IvmAggExpressionBuilder.INSTANCE, emitted);
        registry.appendDeltaAggregateOutputs(avgTarget, dmlFactor, outputs,
                IvmAggExpressionBuilder.INSTANCE, emitted);

        // SUM-like processors emit 2 outputs each (SUM + COUNT); the shared COUNT delta
        // output must appear only once.
        Assertions.assertEquals(3, outputs.size(), "shared COUNT delta should be deduplicated");
        long countOutputs = outputs.stream()
                .filter(output -> "cnt_v".equals(output.getName())).count();
        Assertions.assertEquals(1, countOutputs, "shared COUNT delta output must appear once");
    }

    private IvmAggTarget targetWithHidden(IvmAggTarget target, IvmAggStateKey key, Slot slot) {
        Map<IvmAggStateKey, Slot> hidden = new LinkedHashMap<>(target.getHiddenStateSlots());
        hidden.put(key, slot);
        return new IvmAggTarget(target.getOrdinal(), target.getFunctionKind(),
                target.getVisibleSlot(), hidden, target.getExprArgs());
    }
}
