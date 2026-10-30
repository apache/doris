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

import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.CountAtMin;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.MinTopK;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopKFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraySortedMerge;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cardinality;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class IvmAggMinProcessorTest extends IvmAggProcessorTestBase {
    private static final long TOP_K = 8;

    @Test
    void testMinTopKDeltaAndApply() {
        IvmAggMinProcessor processor = new IvmAggMinProcessor();
        Assertions.assertTrue(processor.supportsOriginalFunction(new Min(value)));
        Assertions.assertEquals(IvmAggFunctionKind.MIN, processor.handledFunctionKind());
        Assertions.assertEquals(ImmutableList.of(IvmAggStateKey.COUNT, IvmAggStateKey.MIN_TOPK),
                processor.hiddenStateKeys(new Min(value), TOP_K));

        IvmAggTarget target = newMinTarget(TOP_K);
        List<NamedExpression> outputs = deltaOutputs(processor, target);
        // Insert-side top-K, delete-side top-K, and the signed non-NULL row count.
        Assertions.assertEquals(3, outputs.size());
        Assertions.assertTrue(outputs.get(0).child(0) instanceof MinTopK);
        Assertions.assertTrue(outputs.get(1).child(0) instanceof MinTopK);
        Assertions.assertTrue(outputs.get(1).getName().contains("DELMIN"));
        Assertions.assertTrue(outputs.get(2).child(0) instanceof Sum);

        Map<String, Expression> finalByName = apply(processor, target,
                ImmutableList.of(slot("min_v", IntegerType.INSTANCE),
                        slot("__ivm_count", BigIntType.INSTANCE),
                        target.getHiddenStateSlot(IvmAggStateKey.MIN_TOPK),
                        target.getHiddenStateSlot(IvmAggStateKey.MIN_TOPK_BOUND)),
                mappedDeltaSlots(processor, target, outputs), slot("delta_group_count", BigIntType.INSTANCE));
        Expression state = finalByName.get(target.getHiddenStateSlot(IvmAggStateKey.MIN_TOPK).getName());
        Assertions.assertTrue(state.anyMatch(node -> node instanceof ArraySortedMerge));
        Assertions.assertTrue(state.anyMatch(node -> node instanceof AssertTrue));
        Assertions.assertTrue(state.anyMatch(node -> node instanceof Cardinality),
                "the merge guard must check whether any value survived");
        Assertions.assertTrue(state.anyMatch(node -> node instanceof ElementAt));
        Expression visible = finalByName.get("min_v");
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof ElementAt));
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof GreaterThan));
        Expression bound = finalByName.get(
                target.getHiddenStateSlot(IvmAggStateKey.MIN_TOPK_BOUND).getName());
        Assertions.assertTrue(bound.anyMatch(node -> node instanceof CaseWhen),
                "the bound column is maintained by the CASE rule");
        Assertions.assertTrue(bound.anyMatch(node -> node instanceof Cardinality),
                "the bound rule compares the pre-truncation candidate count with K");
        Assertions.assertTrue(finalByName.containsKey("__ivm_count"));
    }

    @Test
    void testMinExtremeOnlyMode() {
        IvmAggMinProcessor processor = new IvmAggMinProcessor();
        IvmAggTarget target = newMinTarget(0);
        List<NamedExpression> outputs = deltaOutputs(processor, target);
        // K <= 1 also takes the MIN/COUNT_AT_MIN path: two aggregates per side plus the count sum.
        Assertions.assertEquals(5, outputs.size());
        Assertions.assertTrue(outputs.get(0).child(0) instanceof Min);
        Assertions.assertTrue(outputs.get(1).child(0) instanceof CountAtMin);
        Map<String, Slot> deltaOutputsByName = new HashMap<>();
        for (NamedExpression output : outputs) {
            deltaOutputsByName.put(output.getName(), output.toSlot());
        }
        List<NamedExpression> topOutputs = new ArrayList<>();
        processor.appendDeltaTopProjectOutputs(target, deltaOutputsByName, topOutputs,
                IvmAggExpressionBuilder.INSTANCE);
        for (NamedExpression output : topOutputs) {
            deltaOutputsByName.put(output.getName(), output.toSlot());
        }

        Map<String, Expression> finalByName = apply(processor, target,
                ImmutableList.of(slot("min_v", IntegerType.INSTANCE),
                        slot("__ivm_count", BigIntType.INSTANCE),
                        target.getHiddenStateSlot(IvmAggStateKey.MIN_TOPK),
                        target.getHiddenStateSlot(IvmAggStateKey.MIN_TOPK_BOUND)),
                mappedDeltaSlots(processor, target, deltaOutputsByName),
                slot("delta_group_count", BigIntType.INSTANCE));
        Expression state = finalByName.get(target.getHiddenStateSlot(IvmAggStateKey.MIN_TOPK).getName());
        // The extreme-only state is a one-element array built from the better pair.
        Assertions.assertTrue(state.anyMatch(node -> node instanceof Array));
        Assertions.assertTrue(state.anyMatch(node -> node instanceof CreateNamedStruct));
        Assertions.assertTrue(state.anyMatch(node -> node instanceof AssertTrue));
        Assertions.assertFalse(state.anyMatch(node -> node instanceof ArraySortedMerge),
                "K = 0 does not merge lists");
        Assertions.assertNotNull(finalByName.get(
                target.getHiddenStateSlot(IvmAggStateKey.MIN_TOPK_BOUND).getName()));
        Expression visible = finalByName.get("min_v");
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof CaseWhen));
        Assertions.assertTrue(visible.anyMatch(node -> node instanceof ElementAt));
    }

    @Test
    void testMinTopKOfOneUsesExtremeAndCountAtMin() {
        // K = 1 keeps at most one pair, so no top-K state is materialized: the aggregate emits the
        // visible MIN column plus COUNT_AT_MIN, and the pair array is derived above it. Everything
        // downstream still merges the same array.
        IvmAggMinProcessor processor = new IvmAggMinProcessor();
        List<NamedExpression> hiddenOutputs = new ArrayList<>();
        IvmAggTargetSpec spec = processor.buildTargetSpec(2, new Min(value),
                new Alias(new Min(value), "min_v"), new HashMap<>(), hiddenOutputs, 1);
        Assertions.assertEquals(2, hiddenOutputs.size(), "COUNT plus the internal COUNT_AT_MIN");
        Assertions.assertTrue(hiddenOutputs.get(1).child(0) instanceof CountAtMin);
        Assertions.assertFalse(hiddenOutputs.stream()
                .anyMatch(output -> output.child(0) instanceof MinTopK));
        Assertions.assertEquals(ImmutableList.of(
                IvmUtil.ivmAggHiddenColumnName(2, IvmAggStateKey.MIN_COUNT.name())),
                spec.getInternalAggregateOutputNames().stream().collect(Collectors.toList()));
        Assertions.assertNull(spec.toPlaceholderTarget().getHiddenStateSlot(IvmAggStateKey.MIN_COUNT),
                "the internal count is not a persisted state");

        Expression stateExpr = derivedOutput(spec, IvmAggStateKey.MIN_TOPK);
        Assertions.assertTrue(stateExpr instanceof If);
        Assertions.assertTrue(stateExpr.anyMatch(node -> node instanceof Array));
        Assertions.assertTrue(stateExpr.anyMatch(node -> node instanceof CreateNamedStruct));
        Assertions.assertEquals(TopKFunction.resultType(IntegerType.INSTANCE), stateExpr.getDataType());
        Expression boundExpr = derivedOutput(spec, IvmAggStateKey.MIN_TOPK_BOUND);
        Assertions.assertTrue(boundExpr instanceof If, "K = 1's bound is the retained value itself");

        IvmAggTarget target = spec.toPlaceholderTarget();
        List<NamedExpression> outputs = deltaOutputs(processor, target);
        Assertions.assertEquals(5, outputs.size(), "insert/delete MIN and COUNT_AT_MIN, plus the count sum");
        Assertions.assertTrue(outputs.get(0).child(0) instanceof Min);
        Assertions.assertTrue(outputs.get(1).child(0) instanceof CountAtMin);
        Assertions.assertTrue(outputs.get(2).child(0) instanceof Min);
        Assertions.assertTrue(outputs.get(3).child(0) instanceof CountAtMin);
        Assertions.assertTrue(outputs.get(4).child(0) instanceof Sum);

        Map<String, Slot> deltaOutputsByName = new HashMap<>();
        for (NamedExpression output : outputs) {
            deltaOutputsByName.put(output.getName(), output.toSlot());
        }
        List<NamedExpression> topOutputs = new ArrayList<>();
        processor.appendDeltaTopProjectOutputs(target, deltaOutputsByName, topOutputs,
                IvmAggExpressionBuilder.INSTANCE);
        Assertions.assertEquals(2, topOutputs.size(), "insert-side and delete-side arrays");
        Assertions.assertTrue(topOutputs.get(1).getName().contains("DELMIN"));
        for (NamedExpression output : topOutputs) {
            deltaOutputsByName.put(output.getName(), output.toSlot());
        }

        Map<String, Expression> finalByName = apply(processor, target,
                ImmutableList.of(slot("min_v", IntegerType.INSTANCE),
                        target.getHiddenStateSlot(IvmAggStateKey.COUNT),
                        target.getHiddenStateSlot(IvmAggStateKey.MIN_TOPK),
                        target.getHiddenStateSlot(IvmAggStateKey.MIN_TOPK_BOUND)),
                mappedDeltaSlots(processor, target, deltaOutputsByName),
                slot("delta_group_count", BigIntType.INSTANCE));
        Expression state = finalByName.get(target.getHiddenStateSlot(IvmAggStateKey.MIN_TOPK).getName());
        Assertions.assertTrue(state.anyMatch(node -> node instanceof ArraySortedMerge),
                "K = 1 keeps the top-K merge logic");
    }

    private Expression derivedOutput(IvmAggTargetSpec spec, IvmAggStateKey stateKey) {
        String name = IvmUtil.ivmAggHiddenColumnName(2, stateKey.name());
        for (NamedExpression output : spec.getDerivedStateOutputs()) {
            if (name.equals(output.getName())) {
                return output.child(0);
            }
        }
        throw new AssertionError("missing derived state " + name);
    }

    private IvmAggTarget newMinTarget(long topK) {
        ArrayType stateType = TopKFunction.resultType(IntegerType.INSTANCE);
        return target(2, IvmAggFunctionKind.MIN, "min_v", IntegerType.INSTANCE,
                ImmutableMap.of(IvmAggStateKey.COUNT, slot("__ivm_count", BigIntType.INSTANCE),
                        IvmAggStateKey.MIN_TOPK,
                        slot(IvmUtil.ivmAggHiddenColumnName(2, "MIN_TOPK"), stateType),
                        IvmAggStateKey.MIN_TOPK_BOUND,
                        slot(IvmUtil.ivmAggHiddenColumnName(2, "MIN_TOPK_BOUND"), IntegerType.INSTANCE)),
                valueArg(), topK);
    }
}
