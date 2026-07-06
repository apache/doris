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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Monotonic;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.thrift.TRuntimeFilterType;
import org.apache.doris.thrift.TTargetExprMonotonicity;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;

class RuntimeFilterPartitionPruneClassifierTest {
    @Test
    void testRejectInputSlotOutsideMonotonicChild() {
        SlotReference slot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
        DateTrunc dateTrunc = new DateTrunc(slot, slot);
        Monotonic monotonic = dateTrunc;

        Assertions.assertEquals(1, dateTrunc.getInputSlots().size());
        Assertions.assertFalse(RuntimeFilterPartitionPruneClassifier.hasInputSlotOnlyInMonotonicChild(
                dateTrunc, monotonic.getMonotonicFunctionChildIndex()));
    }

    @Test
    void testAcceptInputSlotOnlyInMonotonicChild() {
        SlotReference slot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
        DateTrunc dateTrunc = new DateTrunc(slot, new VarcharLiteral("day"));
        Monotonic monotonic = dateTrunc;

        Assertions.assertTrue(RuntimeFilterPartitionPruneClassifier.hasInputSlotOnlyInMonotonicChild(
                dateTrunc, monotonic.getMonotonicFunctionChildIndex()));
    }

    @Test
    void testBloomRangePartitionUnsupported() {
        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.BLOOM, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM);

        Assertions.assertFalse(classification.canPrunePartitions());
        Assertions.assertTrue(classification.getUnsupportedReason().contains("BLOOM"));
        Assertions.assertTrue(classification.getPartitionMonotonicity().isEmpty());
    }

    @Test
    void testBloomListPartitionSupported() {
        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.BLOOM, PartitionType.LIST, ListPartitionItem.DUMMY_ITEM);

        assertSupportedIncreasingPartitions(classification);
    }

    @Test
    void testInOrBloomRangePartitionStillSupported() {
        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.IN_OR_BLOOM, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM);

        assertSupportedIncreasingPartitions(classification);
    }

    private RuntimeFilterPartitionPruneClassifier.Classification classify(
            TRuntimeFilterType filterType, PartitionType partitionType, PartitionItem partitionItem) {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(1), new TupleId(1));
        slotDescriptor.setColumn(partitionColumn);
        slotDescriptor.setType(partitionColumn.getType());
        SlotRef targetSlot = new SlotRef(slotDescriptor);
        SlotReference nereidsTarget = new SlotReference("part_col", IntegerType.INSTANCE);

        OlapTable table = Mockito.mock(OlapTable.class);
        PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getSelectedPartitionIds()).thenReturn(ImmutableList.of(1L, 2L));
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(partitionInfo.getType()).thenReturn(partitionType);
        Mockito.when(partitionInfo.getPartitionColumns()).thenReturn(ImmutableList.of(partitionColumn));
        Mockito.when(partitionInfo.getItem(1L)).thenReturn(partitionItem);
        Mockito.when(partitionInfo.getItem(2L)).thenReturn(partitionItem);

        return RuntimeFilterPartitionPruneClassifier.classify(filterType, targetSlot, nereidsTarget, scanNode);
    }

    private void assertSupportedIncreasingPartitions(
            RuntimeFilterPartitionPruneClassifier.Classification classification) {
        Assertions.assertTrue(classification.canPrunePartitions());
        Map<Long, TTargetExprMonotonicity> monotonicity = classification.getPartitionMonotonicity();
        Assertions.assertEquals(2, monotonicity.size());
        Assertions.assertEquals(TTargetExprMonotonicity.MONOTONIC_INCREASING, monotonicity.get(1L));
        Assertions.assertEquals(TTargetExprMonotonicity.MONOTONIC_INCREASING, monotonicity.get(2L));
    }
}
