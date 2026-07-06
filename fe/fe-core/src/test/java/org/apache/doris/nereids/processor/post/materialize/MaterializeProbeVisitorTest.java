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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TAccessPathType;
import org.apache.doris.thrift.TColumnAccessPath;
import org.apache.doris.thrift.TDataAccessPath;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

public class MaterializeProbeVisitorTest {

    @Test
    public void testOlapScanUsesRelationSlotWithAccessPaths() {
        SlotReference contextSlot = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference relationSlot = contextSlot.withAccessPaths(
                ImmutableList.of(dataPath("nested")), ImmutableList.of());
        contextSlot = (SlotReference) contextSlot.withNullable(false);
        PhysicalOlapScan scan = mockBaseOlapScan(relationSlot);

        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(contextSlot);
        Optional<MaterializeSource> source = new MaterializeProbeVisitor().visitPhysicalOlapScan(scan, context);

        Assertions.assertTrue(source.isPresent());
        Assertions.assertSame(relationSlot, source.get().baseSlot);
        Assertions.assertEquals(relationSlot.getAllAccessPaths(), source.get().baseSlot.getAllAccessPaths());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFilterUsingIndexUsesRelationSlotWithAccessPaths() {
        ConnectContext oldContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().topNLazyMaterializationUsingIndex = true;
        context.setThreadLocalInfo();
        try {
            SlotReference contextSlot = new SlotReference("a", IntegerType.INSTANCE);
            SlotReference relationSlot = contextSlot.withAccessPaths(
                    ImmutableList.of(dataPath("nested")), ImmutableList.of());
            contextSlot = (SlotReference) contextSlot.withNullable(false);
            PhysicalOlapScan scan = mockBaseOlapScan(relationSlot);

            PhysicalFilter<PhysicalOlapScan> filter = Mockito.mock(PhysicalFilter.class);
            Mockito.when(filter.child()).thenReturn(scan);
            Mockito.when(filter.getInputSlots()).thenReturn(ImmutableSet.of(contextSlot));

            MaterializeProbeVisitor.ProbeContext probeContext = new MaterializeProbeVisitor.ProbeContext(contextSlot);
            Optional<MaterializeSource> source =
                    new MaterializeProbeVisitor().visitPhysicalFilter(filter, probeContext);

            Assertions.assertTrue(source.isPresent());
            Assertions.assertSame(relationSlot, source.get().baseSlot);
            Assertions.assertEquals(relationSlot.getAllAccessPaths(), source.get().baseSlot.getAllAccessPaths());
        } finally {
            if (oldContext == null) {
                ConnectContext.remove();
            } else {
                oldContext.setThreadLocalInfo();
            }
        }
    }

    private TColumnAccessPath dataPath(String... path) {
        TColumnAccessPath accessPath = new TColumnAccessPath(TAccessPathType.DATA);
        accessPath.data_access_path = new TDataAccessPath(ImmutableList.copyOf(path));
        return accessPath;
    }

    private PhysicalOlapScan mockBaseOlapScan(SlotReference outputSlot) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getBaseIndexId()).thenReturn(1L);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        PhysicalOlapScan scan = Mockito.mock(PhysicalOlapScan.class);
        Mockito.when(scan.getSelectedIndexId()).thenReturn(1L);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(outputSlot));
        Mockito.when(scan.getOperativeSlots()).thenReturn(ImmutableList.of());
        return scan;
    }
}
