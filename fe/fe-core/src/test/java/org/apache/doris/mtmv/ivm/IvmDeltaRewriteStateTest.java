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
import org.apache.doris.catalog.stream.CloudOlapTableStreamUpdate;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.CloudTableStreamReadStateHelper;
import org.apache.doris.common.Config;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.rules.analysis.CloudTableStreamReadStateHook;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.commands.insert.StreamConsumptionInfoExtractor;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

class IvmDeltaRewriteStateTest extends IvmDeltaTestBase {

    @Test
    void testCloudJoinSnapshotsAndDeltaUseSameReadState() throws Exception {
        OlapTable table = buildMultiPartitionTable(4);
        OlapTableStream stream = registerTestStream(table, 1L);
        long caughtUp = partitionId(table, 1);
        long pending = partitionId(table, 2);
        long empty = partitionId(table, 3);
        long outsideScope = partitionId(table, 4);
        LogicalOlapScan left = new LogicalOlapScan(PlanConstructor.getNextRelationId(), table, List.of("test_db"))
                .withSelectedPartitionIds(List.of(caughtUp));
        LogicalOlapScan right = new LogicalOlapScan(PlanConstructor.getNextRelationId(), table, List.of("test_db"))
                .withSelectedPartitionIds(List.of(pending, empty, outsideScope));
        Plan original = new LogicalUnion(Qualifier.ALL, List.of(left, right));
        IvmIncrRefreshContext context = new IvmIncrRefreshContext(
                buildDupMtmvFromPlan(left.getOutput()), MemoTestUtils.createConnectContext(), null, false);
        Cloud.TableStreamIdentityPB identity = stream.getCloudIdentity();
        Map<Cloud.TableStreamIdentityPB, Set<Long>> request = Map.of(identity, Set.of(caughtUp, pending, empty));
        Map<Long, Cloud.TableStreamPartitionReadStatePB> states = Map.of(
                caughtUp, consumedState(caughtUp, 5, 10, 10),
                pending, consumedState(pending, 7, 20, 25),
                empty, consumedState(empty, Partition.PARTITION_INIT_VERSION, -1, -1));

        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<CloudTableStreamReadStateHelper> metaService =
                        Mockito.mockStatic(CloudTableStreamReadStateHelper.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            config.when(Config::isNotCloudMode).thenReturn(false);
            metaService.when(() -> CloudTableStreamReadStateHelper.getReadStates(request))
                    .thenAnswer(invocation -> {
                        Assertions.assertTrue(table.tryWriteLock(0, TimeUnit.MILLISECONDS),
                                "read-state RPC must run after releasing the table read lock");
                        table.writeUnlock();
                        return Map.of(identity, states);
                    });
            IvmRewriteContext rewriteContext = IvmRewriteContext.incremental(context.getMtmv(),
                    Map.of(new BaseTableInfo(table), Set.of(caughtUp, pending, empty)));
            rewriteContext.preloadCloudReadStates(List.of(table));
            IvmDeltaRewriteState state = IvmDeltaRewriteState.forAppendOnlyDup(original, context, rewriteContext);

            // The caught-up partition advances after the RPC. The pending partition still has an older local version.
            table.getPartition(caughtUp).updateVisibleVersionAndTime(6, 1, 11);
            Assertions.assertTrue(state.createDeltaScan(left).isEmpty());
            LogicalOlapTableStreamScan delta = state.createDeltaScan(right).orElseThrow();
            Assertions.assertEquals(List.of(pending, empty), delta.getSelectedPartitionIds());
            Assertions.assertEquals(Map.of(pending, states.get(pending), empty, states.get(empty)),
                    delta.getTable().getCloudReadStates());

            Plan before = IvmDeltaRewriteHelper.INSTANCE.preSnapshot(right, state).first;
            Plan leftAfter = IvmDeltaRewriteHelper.INSTANCE.postSnapshot(left, state).first;
            Plan rightAfter = IvmDeltaRewriteHelper.INSTANCE.postSnapshot(right, state).first;
            LogicalOlapTableStreamScan preScan = before.<LogicalOlapTableStreamScan>collectToList(
                    LogicalOlapTableStreamScan.class::isInstance).get(0);
            Assertions.assertEquals(delta.getTable().getCloudReadStates(), preScan.getTable().getCloudReadStates());
            Assertions.assertTrue(preScan.isSnapshot());
            LogicalOlapScan leftPost = postSnapshotScan(leftAfter);
            LogicalOlapScan rightPost = postSnapshotScan(rightAfter);
            // The fixed version is a scan option. The table still owns all schema and distribution metadata.
            Assertions.assertSame(table, leftPost.getTable());
            Assertions.assertSame(table, rightPost.getTable());
            Assertions.assertEquals(Map.of(caughtUp, 5L),
                    leftPost.getScanParams().orElseThrow().getFixedPartitionVersions());
            Assertions.assertEquals(Map.of(pending, 7L, empty, Partition.PARTITION_INIT_VERSION),
                    rightPost.getScanParams().orElseThrow().getFixedPartitionVersions());
            Assertions.assertEquals(List.of(pending), rightPost.selectNonEmptyPartitionIds());
            LogicalOlapScan prunedPost = rightPost.withSelectedPartitionIds(List.of(pending));
            Assertions.assertSame(rightPost.getScanParams().orElseThrow(), prunedPost.getScanParams().orElseThrow());
            OlapScanNode postNode = Mockito.mock(OlapScanNode.class);
            Mockito.when(postNode.getOlapTable()).thenReturn(prunedPost.getTable());
            Mockito.when(postNode.getFixedPartitionVersions())
                    .thenReturn(prunedPost.getScanParams().orElseThrow().getFixedPartitionVersions());
            ScanNode.setVisibleVersionForOlapScanNodes(List.of(postNode));
            Mockito.verify(postNode).updateScanRangeVersions(
                    Map.of(pending, 7L, empty, Partition.PARTITION_INIT_VERSION));

            Plan rewritten = new LogicalUnion(Qualifier.ALL, List.of(delta, before, leftAfter, rightAfter));
            CloudTableStreamReadStateHook.installReadStates(rewritten);
            metaService.verify(() -> CloudTableStreamReadStateHelper.getReadStates(request));
            metaService.verifyNoMoreInteractions();
            Assertions.assertTrue(StreamConsumptionInfoExtractor.extract(leftAfter).isEmpty());
            CloudOlapTableStreamUpdate update =
                    (CloudOlapTableStreamUpdate) StreamConsumptionInfoExtractor.extract(rewritten).get(0).getUpdate();
            Assertions.assertEquals(Set.of(pending), update.getPartitionUpdates().keySet());
            Assertions.assertEquals(25, update.getPartitionUpdates().get(pending).getNextOffsetTso());
        }
    }

    @Test
    void testRejectsPartitionAddedAfterCloudReadStatesWereCaptured() {
        OlapTable table = buildMultiPartitionTable(1);
        OlapTableStream stream = registerTestStream(table, 1L);
        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(), table, List.of("test_db"));
        IvmIncrRefreshContext context = new IvmIncrRefreshContext(
                buildDupMtmvFromPlan(scan.getOutput()), MemoTestUtils.createConnectContext(), null, false);
        IvmRewriteContext rewriteContext = IvmRewriteContext.incremental(context.getMtmv());
        Cloud.TableStreamIdentityPB identity = stream.getCloudIdentity();
        long initialPartition = partitionId(table, 1);
        Map<Cloud.TableStreamIdentityPB, Set<Long>> request = Map.of(identity, Set.of(initialPartition));

        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<CloudTableStreamReadStateHelper> metaService =
                        Mockito.mockStatic(CloudTableStreamReadStateHelper.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            metaService.when(() -> CloudTableStreamReadStateHelper.getReadStates(request))
                    .thenReturn(Map.of(identity, Map.of(initialPartition, consumedState(initialPartition, 5, 10, 11))));
            rewriteContext.preloadCloudReadStates(List.of(table));
            addPartition(table, 2);
            LogicalOlapScan laterScan = scan.withSelectedPartitionIds(table.getPartitionIds());

            table.readLock();
            try {
                IvmException error = Assertions.assertThrows(IvmException.class,
                        () -> IvmDeltaRewriteState.forAppendOnlyDup(laterScan, context, rewriteContext));
                Assertions.assertEquals(IvmFailureReason.PLAN_REWRITE_FAILED, error.getFailureReason());
            } finally {
                table.readUnlock();
            }
            metaService.verify(() -> CloudTableStreamReadStateHelper.getReadStates(request));
            metaService.verifyNoMoreInteractions();
        }
    }

    private static Cloud.TableStreamPartitionReadStatePB consumedState(
            long id, long version, long offset, long end) {
        return Cloud.TableStreamPartitionReadStatePB.newBuilder()
                .setPartitionId(id)
                .setVisibleVersion(version)
                .setOffsetState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED)
                .setOffsetTso(offset)
                .setEndTso(end)
                .build();
    }

    private static LogicalOlapScan postSnapshotScan(Plan plan) {
        return plan.<LogicalOlapScan>collectToList(LogicalOlapScan.class::isInstance).get(0);
    }

    @Test
    void testSequenceEncodesSequencePrefixAndDeltaIndex() {
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
            addPartition(table, i);
        }
        enableChangelog(table);
        table.setQualifiedDbName("test_db");
        return table;
    }

    private void addPartition(OlapTable table, int partitionIndex) {
        long partitionId = table.getId() * 100 + partitionIndex;
        table.addPartition(new Partition(partitionId, "p" + partitionIndex,
                new MaterializedIndex(table.getBaseIndexId(), MaterializedIndex.IndexState.NORMAL),
                new RandomDistributionInfo(1)));
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
    void testLargeIntSequenceEncodesSequencePrefixAndDeltaIndex() {
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
