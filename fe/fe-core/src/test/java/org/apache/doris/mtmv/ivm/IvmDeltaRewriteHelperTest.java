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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

class IvmDeltaRewriteHelperTest {

    private final IvmDeltaRewriteHelper helper = IvmDeltaRewriteHelper.INSTANCE;

    // ==================== isIncrementalDeltaScan ====================

    @Test
    void testIsIncrementalDeltaScan_regularScan_returnsFalse() {
        LogicalOlapScan scan = Mockito.mock(LogicalOlapScan.class);
        Assertions.assertFalse(helper.isIncrementalDeltaScan(scan));
    }

    @Test
    void testIsIncrementalDeltaScan_streamScanNotIncremental_returnsFalse() {
        LogicalOlapTableStreamScan scan = Mockito.mock(LogicalOlapTableStreamScan.class);
        Mockito.when(scan.isIncrementalScan()).thenReturn(false);
        Assertions.assertFalse(helper.isIncrementalDeltaScan(scan));
    }

    @Test
    void testIsIncrementalDeltaScan_streamScanIncremental_returnsTrue() {
        LogicalOlapTableStreamScan scan = Mockito.mock(LogicalOlapTableStreamScan.class);
        Mockito.when(scan.isIncrementalScan()).thenReturn(true);
        Assertions.assertTrue(helper.isIncrementalDeltaScan(scan));
    }

    // ==================== findSlotByName ====================

    @Test
    void testFindSlotByName_found() {
        List<Slot> slots = new ArrayList<>();
        Slot slot = Mockito.mock(Slot.class);
        Mockito.when(slot.getName()).thenReturn("foundSlot");
        slots.add(slot);

        Slot result = helper.findSlotByName(slots, "foundSlot");
        Assertions.assertSame(slot, result);
    }

    @Test
    void testFindSlotByName_notFound_throws() {
        List<Slot> slots = new ArrayList<>();
        Slot slot = Mockito.mock(Slot.class);
        Mockito.when(slot.getName()).thenReturn("otherSlot");
        slots.add(slot);

        Assertions.assertThrows(AnalysisException.class,
                () -> helper.findSlotByName(slots, "missingSlot"));
    }

    @Test
    void testFindSlotByName_findsColumnBinlogOperationCol() {
        List<Slot> slots = new ArrayList<>();
        Slot opSlot = Mockito.mock(Slot.class);
        Mockito.when(opSlot.getName()).thenReturn(Column.BINLOG_OPERATION_COL);
        slots.add(opSlot);

        Slot result = helper.findSlotByName(slots, Column.BINLOG_OPERATION_COL);
        Assertions.assertSame(opSlot, result);
    }

    // ==================== toSnapshotScan ====================

    @Test
    void testToSnapshotScanReturnsRegularScanWithTso() {
        LogicalOlapTableStreamScan deltaScan = Mockito.mock(LogicalOlapTableStreamScan.class);
        OlapTableStreamWrapper wrapper = Mockito.mock(OlapTableStreamWrapper.class);
        OlapTable originTable = Mockito.mock(OlapTable.class);
        Mockito.when(wrapper.getBaseTable()).thenReturn(originTable);
        Mockito.when(deltaScan.getTable()).thenReturn(wrapper);
        Mockito.when(deltaScan.getRelationId()).thenReturn(org.apache.doris.nereids.trees.plans.RelationId.createGenerator().getNextId());
        Mockito.when(deltaScan.getQualifier()).thenReturn(java.util.Collections.emptyList());
        Mockito.when(deltaScan.getManuallySpecifiedPartitions()).thenReturn(java.util.Collections.emptyList());
        Mockito.when(deltaScan.getSelectedTabletIds()).thenReturn(java.util.Collections.emptyList());
        Mockito.when(deltaScan.getHints()).thenReturn(java.util.Collections.emptyList());
        Mockito.when(deltaScan.getTableSample()).thenReturn(java.util.Optional.empty());
        Mockito.when(deltaScan.getOperativeSlots()).thenReturn(java.util.Collections.emptyList());

        LogicalOlapScan snapshot = helper.toSnapshotScan(deltaScan, 100L);

        Assertions.assertNotNull(snapshot);
        Assertions.assertEquals(100L, snapshot.getTso());
        Assertions.assertFalse(snapshot instanceof LogicalOlapTableStreamScan,
                "Snapshot scan should be LogicalOlapScan, not stream scan");
    }

    @Test
    void testToSnapshotScanDifferentTsoValues() {
        LogicalOlapTableStreamScan deltaScan = Mockito.mock(LogicalOlapTableStreamScan.class);
        OlapTableStreamWrapper wrapper = Mockito.mock(OlapTableStreamWrapper.class);
        OlapTable originTable = Mockito.mock(OlapTable.class);
        Mockito.when(wrapper.getBaseTable()).thenReturn(originTable);
        Mockito.when(deltaScan.getTable()).thenReturn(wrapper);
        Mockito.when(deltaScan.getRelationId()).thenReturn(org.apache.doris.nereids.trees.plans.RelationId.createGenerator().getNextId());
        Mockito.when(deltaScan.getQualifier()).thenReturn(java.util.Collections.emptyList());
        Mockito.when(deltaScan.getManuallySpecifiedPartitions()).thenReturn(java.util.Collections.emptyList());
        Mockito.when(deltaScan.getSelectedTabletIds()).thenReturn(java.util.Collections.emptyList());
        Mockito.when(deltaScan.getHints()).thenReturn(java.util.Collections.emptyList());
        Mockito.when(deltaScan.getTableSample()).thenReturn(java.util.Optional.empty());
        Mockito.when(deltaScan.getOperativeSlots()).thenReturn(java.util.Collections.emptyList());

        LogicalOlapScan preSnapshot = helper.toSnapshotScan(deltaScan, 50L);
        Assertions.assertEquals(50L, preSnapshot.getTso());

        LogicalOlapScan postSnapshot = helper.toSnapshotScan(deltaScan, 200L);
        Assertions.assertEquals(200L, postSnapshot.getTso());
    }
}
