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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

class SingleTabletGatherTest {
    private final SessionVariable session = Mockito.spy(new SessionVariable());
    private final LogicalOlapScan scan = Mockito.mock(LogicalOlapScan.class);
    private final OlapTable table = Mockito.mock(OlapTable.class);

    private void prepare() {
        Mockito.doReturn(1).when(session).getParallelExecInstanceNum("");
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getTableSample()).thenReturn(Optional.empty());
        Mockito.when(scan.getScanParams()).thenReturn(Optional.empty());
        Mockito.when(scan.getSelectedTabletIds()).thenReturn(ImmutableList.of(10L));
    }

    @Test
    void testPrunedTabletAndGates() {
        prepare();
        Assertions.assertTrue(eligible());
        session.enableSingleTabletGather = false;
        Assertions.assertFalse(eligible());
        session.enableSingleTabletGather = true;
        session.setEnableNereidsDistributePlanner(false);
        Assertions.assertFalse(eligible());
        session.setEnableNereidsDistributePlanner(true);
        session.enableQueryCache = true;
        Assertions.assertFalse(eligible());
        session.enableQueryCache = false;
        Mockito.doReturn(2).when(session).getParallelExecInstanceNum("");
        Assertions.assertFalse(eligible());
        Mockito.doReturn(1).when(session).getParallelExecInstanceNum("");
        Mockito.when(scan.getSelectedTabletIds()).thenReturn(ImmutableList.of(10L, 11L));
        Assertions.assertFalse(eligible());
    }

    @Test
    void testUnrestrictedTabletsAcrossPartitionsAndSelectedIndex() {
        prepare();
        Mockito.when(scan.getSelectedTabletIds()).thenReturn(ImmutableList.of());
        Mockito.when(scan.getSelectedPartitionIds()).thenReturn(ImmutableList.of(1L));
        Mockito.when(scan.getSelectedIndexId()).thenReturn(100L);
        Partition partition = Mockito.mock(Partition.class);
        MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
        Mockito.when(table.getPartition(1L)).thenReturn(partition);
        Mockito.when(table.getPartitionIndex(partition, 100L)).thenReturn(index);
        Tablet tablet = Mockito.mock(Tablet.class);
        Mockito.when(index.getTablets()).thenReturn(ImmutableList.of(tablet));
        Assertions.assertTrue(eligible());
        Mockito.when(index.getTablets()).thenReturn(ImmutableList.of(tablet, tablet));
        Assertions.assertFalse(eligible());
        Mockito.when(index.getTablets()).thenReturn(ImmutableList.of(tablet));
        Partition secondPartition = Mockito.mock(Partition.class);
        Mockito.when(table.getPartition(2L)).thenReturn(secondPartition);
        Mockito.when(table.getPartitionIndex(secondPartition, 100L)).thenReturn(index);
        Mockito.when(scan.getSelectedPartitionIds()).thenReturn(ImmutableList.of(1L, 2L));
        Assertions.assertFalse(eligible());
        Mockito.when(scan.getSelectedPartitionIds()).thenReturn(ImmutableList.of());
        Assertions.assertFalse(eligible());
    }

    @Test
    void testSamplingAndSpecialScanAreExcluded() {
        prepare();
        Mockito.when(scan.getTableSample()).thenReturn(Optional.of(Mockito.mock(TableSample.class)));
        Assertions.assertFalse(eligible());
        Mockito.when(scan.getTableSample()).thenReturn(Optional.empty());
        Mockito.when(scan.getScanParams()).thenReturn(Optional.of(Mockito.mock(TableScanParams.class)));
        Assertions.assertFalse(eligible());
    }

    @Test
    void testSessionSwitchIsForwarded() throws Exception {
        SessionVariable original = new SessionVariable();
        Assertions.assertTrue(original.enableSingleTabletGather);
        original.enableSingleTabletGather = false;
        SessionVariable forwarded = new SessionVariable();
        forwarded.setForwardedSessionVariables(original.getForwardVariables());
        Assertions.assertFalse(forwarded.enableSingleTabletGather);
    }

    private boolean eligible() {
        return LogicalOlapScanToPhysicalOlapScan.canUseSingleTabletGather(scan, session, "");
    }
}
