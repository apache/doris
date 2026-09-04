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

import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class IvmDeltaRewriteStateTest extends IvmDeltaTestBase {

    @Test
    void testSequenceEncodesRefreshVersionAndDeltaIndex() {
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(
                ImmutableMap.of(), false, 7L, BigIntType.INSTANCE, ImmutableMap.of());

        for (int i = 0; i < 5; i++) {
            state.nextDeltaIndex();
        }
        Assertions.assertEquals((7L << 11) | (5L << 1) | 1,
                ((BigIntLiteral) state.toSequence(state.nextDeltaIndex())).getValue());
    }

    @Test
    void testCreateDeltaScanRestrictsToWindowPartitions() {
        OlapTable table = buildMultiPartitionTable(3);
        OlapTableStream stream = registerTestStream(table, 1L);
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(
                ImmutableMap.of(table, stream), false, 1L, BigIntType.INSTANCE,
                ImmutableMap.of(table, ImmutableList.of(partitionId(table, 2), partitionId(table, 3))));

        Optional<LogicalOlapTableStreamScan> deltaScan = state.createDeltaScan(
                new LogicalOlapScan(PlanConstructor.getNextRelationId(), table, ImmutableList.of("test_db")));
        Assertions.assertTrue(deltaScan.isPresent());
        Assertions.assertEquals(ImmutableList.of(partitionId(table, 2), partitionId(table, 3)),
                deltaScan.get().getSelectedPartitionIds());
    }

    @Test
    void testRestrictWindowLeavesUnlimitedScanUnchanged() {
        OlapTable table = buildMultiPartitionTable(3);
        OlapTableStream stream = registerTestStream(table, 1L);
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(
                ImmutableMap.of(table, stream), false, 1L, BigIntType.INSTANCE, ImmutableMap.of());

        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(), table,
                ImmutableList.of("test_db"));
        Assertions.assertSame(scan, state.restrictWindow(scan));
    }

    @Test
    void testRestrictWindowAppliesToExcludedTable() {
        // An excluded table (not in the streams map) still gets its snapshot side windowed.
        OlapTable table = buildMultiPartitionTable(3);
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(
                ImmutableMap.of(), false, 1L, BigIntType.INSTANCE,
                ImmutableMap.of(table, ImmutableList.of(partitionId(table, 3))));

        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(), table,
                ImmutableList.of("test_db"));
        Assertions.assertTrue(state.isExcluded(scan));
        LogicalOlapScan restricted = state.restrictWindow(scan);
        Assertions.assertEquals(ImmutableList.of(partitionId(table, 3)), restricted.getSelectedPartitionIds());
    }

    @Test
    void testCreateDeltaScanNarrowsTabletsToWindowPartitions() {
        OlapTable table = buildMultiPartitionTable(3);
        addTablet(table, 1, 1001L);
        addTablet(table, 2, 1002L);
        addTablet(table, 3, 1003L);
        OlapTableStream stream = registerTestStream(table, 1L);
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(
                ImmutableMap.of(table, stream), false, 1L, BigIntType.INSTANCE,
                ImmutableMap.of(table, ImmutableList.of(partitionId(table, 2), partitionId(table, 3))));

        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(), table,
                ImmutableList.of("test_db"))
                .withSelectedTabletIds(ImmutableList.of(1001L, 1002L, 1003L));
        Optional<LogicalOlapTableStreamScan> deltaScan = state.createDeltaScan(scan);
        Assertions.assertTrue(deltaScan.isPresent());
        // Tablet 1001 belongs to partition 1 (outside the window) and is dropped.
        Assertions.assertEquals(ImmutableList.of(1002L, 1003L), deltaScan.get().getSelectedTabletIds());
    }

    @Test
    void testRestrictWindowIntersectsExistingSelection() {
        OlapTable table = buildMultiPartitionTable(3);
        OlapTableStream stream = registerTestStream(table, 1L);
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(
                ImmutableMap.of(table, stream), false, 1L, BigIntType.INSTANCE,
                ImmutableMap.of(table, ImmutableList.of(partitionId(table, 2), partitionId(table, 3))));

        // Pre-existing selection p1+p2 intersects the window p2+p3 -> p2 only.
        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(), table,
                ImmutableList.of("test_db"))
                .withSelectedPartitionIds(ImmutableList.of(partitionId(table, 1), partitionId(table, 2)));
        LogicalOlapScan restricted = state.restrictWindow(scan);
        Assertions.assertEquals(ImmutableList.of(partitionId(table, 2)), restricted.getSelectedPartitionIds());
    }

    @Test
    void testRestrictWindowRestrictsAllPartitionsSelection() {
        OlapTable table = buildMultiPartitionTable(3);
        OlapTableStream stream = registerTestStream(table, 1L);
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(
                ImmutableMap.of(table, stream), false, 1L, BigIntType.INSTANCE,
                ImmutableMap.of(table, ImmutableList.of(partitionId(table, 3))));

        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(), table,
                ImmutableList.of("test_db"));
        LogicalOlapScan restricted = state.restrictWindow(scan);
        Assertions.assertEquals(ImmutableList.of(partitionId(table, 3)), restricted.getSelectedPartitionIds());
    }

    private OlapTable buildMultiPartitionTable(int partitionCount) {
        OlapTable table = PlanConstructor.newOlapTable(0, "t1", 0);
        for (int i = 1; i <= partitionCount; i++) {
            long partitionId = table.getId() * 100 + i;
            Partition partition = new Partition(partitionId, "p" + i,
                    new MaterializedIndex(table.getBaseIndexId(), MaterializedIndex.IndexState.NORMAL),
                    new RandomDistributionInfo(1));
            table.addPartition(partition);
        }
        enableRowBinlog(table);
        table.setQualifiedDbName("test_db");
        return table;
    }

    private long partitionId(OlapTable table, int partitionIndex) {
        return table.getPartition("p" + partitionIndex).getId();
    }

    private void addTablet(OlapTable table, int partitionIndex, long tabletId) {
        Partition partition = table.getPartition("p" + partitionIndex);
        MaterializedIndex index = partition.getBaseIndex();
        index.addTablet(new LocalTablet(tabletId),
                new TabletMeta(1L, table.getId(), partition.getId(), index.getId(), 0, TStorageMedium.HDD, true));
    }

    @Test
    void testLargeIntSequenceEncodesRefreshVersionAndDeltaIndex() {
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(
                ImmutableMap.of(), false, 7L, LargeIntType.INSTANCE, ImmutableMap.of());

        for (int i = 0; i < 3; i++) {
            state.nextDeltaIndex();
        }
        LargeIntLiteral sequence = (LargeIntLiteral) state.toSequence(state.nextDeltaIndex());
        Assertions.assertEquals(java.math.BigInteger.valueOf(7).shiftLeft(75)
                .or(java.math.BigInteger.valueOf(3).shiftLeft(65)).or(java.math.BigInteger.ONE), sequence.getValue());
    }

    @Test
    void testSequenceRejectsTooManyDeltaScans() {
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(
                ImmutableMap.of(), false, 1L, BigIntType.INSTANCE, ImmutableMap.of());

        for (int i = 0; i < 1024; i++) {
            state.nextDeltaIndex();
        }
        IvmException exception = Assertions.assertThrows(IvmException.class, state::nextDeltaIndex);
        Assertions.assertTrue(exception.getMessage().contains("too many delta scans"));
    }

    @Test
    void testExcludedTableDoesNotCreateDeltaScan() {
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(
                ImmutableMap.of(), false, 1L, BigIntType.INSTANCE, ImmutableMap.of());

        Assertions.assertTrue(state.isExcluded(buildScan()));
        Assertions.assertFalse(state.createDeltaScan(buildScan()).isPresent());
    }
}
