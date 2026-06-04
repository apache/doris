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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapEmpty;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapOr;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract class IvmAggProcessorTestBase {
    protected final Slot value = new SlotReference("v", IntegerType.INSTANCE, true);
    protected final Slot bitmap = new SlotReference("b", BitmapType.INSTANCE, true);
    protected final Slot dmlFactor = new SlotReference("dml_factor", TinyIntType.INSTANCE, false);

    protected List<NamedExpression> deltaOutputs(IvmAggFunctionProcessor processor, IvmAggTarget target) {
        List<NamedExpression> outputs = new ArrayList<>();
        processor.appendDeltaAggregateOutputs(target, dmlFactor, outputs, IvmAggExpressionBuilder.INSTANCE);
        return outputs;
    }

    protected Map<String, Slot> mappedDeltaSlots(IvmAggFunctionProcessor processor, IvmAggTarget target,
            List<NamedExpression> outputs) {
        Map<String, Slot> outputByName = new HashMap<>();
        for (NamedExpression output : outputs) {
            outputByName.put(output.getName(), output.toSlot());
        }
        Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots = new HashMap<>();
        processor.mapApplyDeltaSlots(target, outputByName, applyDeltaSlots,
                slot("delta_group_count", BigIntType.INSTANCE), IvmAggExpressionBuilder.INSTANCE);
        Map<String, Slot> byRefName = new HashMap<>();
        for (Map.Entry<IvmAggDeltaSlotRef, Slot> entry : applyDeltaSlots.entrySet()) {
            byRefName.put(entry.getKey().toString(), entry.getValue());
        }
        return byRefName;
    }

    protected Map<String, Expression> apply(IvmAggFunctionProcessor processor, IvmAggTarget target,
            List<Slot> oldSlots, Map<String, Slot> deltaSlotsByRefName, Expression newGroupCount) {
        LogicalOlapScan rawMvScan = Mockito.mock(LogicalOlapScan.class);
        Mockito.when(rawMvScan.getOutput()).thenReturn(oldSlots);
        Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots = new HashMap<>();
        for (Map.Entry<String, Slot> entry : deltaSlotsByRefName.entrySet()) {
            String[] parts = entry.getKey().split(":", 2);
            applyDeltaSlots.put(new IvmAggDeltaSlotRef(Integer.parseInt(parts[0]), parts[1]), entry.getValue());
        }
        Map<String, Expression> finalByName = new HashMap<>();
        processor.appendApplyExpressions(target, new IvmAggApplyContext(finalByName, rawMvScan,
                applyDeltaSlots, newGroupCount, IvmAggExpressionBuilder.INSTANCE));
        return finalByName;
    }

    protected IvmAggTarget target(int ordinal, IvmAggFunctionKind kind, String visibleName,
            DataType visibleType, Map<IvmAggStateKey, Slot> hiddenSlots, List<Expression> args) {
        return new IvmAggTarget(ordinal, kind, slot(visibleName, visibleType), hiddenSlots, args);
    }

    protected Slot slot(String name, DataType dataType) {
        return new SlotReference(name, dataType, true);
    }

    protected String slotName(Expression expression) {
        return ((Slot) expression).getName();
    }

    protected void assertTwoBitmapDeltaOutputs(List<NamedExpression> outputs) {
        Assertions.assertEquals(2, outputs.size());
        Assertions.assertTrue(outputs.get(0).child(0) instanceof BitmapUnion);
        Assertions.assertTrue(outputs.get(0).child(0).anyMatch(node -> node instanceof If));
        Assertions.assertTrue(outputs.get(1).child(0) instanceof BitmapUnion);
        Assertions.assertTrue(outputs.get(1).getName().contains("BITMAP_DELETE_UNION"));
    }

    protected void assertTwoSumDeltaOutputs(List<NamedExpression> outputs) {
        Assertions.assertEquals(2, outputs.size());
        Assertions.assertTrue(outputs.stream().allMatch(output -> output.child(0) instanceof Sum));
        Assertions.assertTrue(outputs.get(0).child(0).anyMatch(node -> node instanceof If));
    }

    protected void assertBitmapApplyGuard(Expression expression) {
        Assertions.assertTrue(expression.anyMatch(node -> node instanceof BitmapCount
                && ((BitmapCount) node).child(0) instanceof Slot
                && ((Slot) ((BitmapCount) node).child(0)).getName().contains("BITMAP_DELETE_UNION")));
        Assertions.assertTrue(expression.anyMatch(node -> node instanceof BitmapEmpty));
        Assertions.assertTrue(expression.anyMatch(node -> node instanceof BitmapOr));
    }

    protected List<Expression> valueArg() {
        return ImmutableList.of(value);
    }

    protected List<Expression> bitmapArg() {
        return ImmutableList.of(bitmap);
    }
}
