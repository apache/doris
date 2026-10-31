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

package org.apache.doris.datasource.scan;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccSnapshot;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenSchemaCacheValue;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * FIX-BATCH-MODE-SPLIT (P4-T06e / NG-7) — guards {@link PluginDrivenScanNode#shouldUseBatchMode},
 * the pure four-input gate deciding whether a plugin-driven partitioned scan uses batched/streaming
 * split generation instead of synchronous enumeration.
 *
 * <p><b>Why this matters:</b> batch mode mirrors legacy {@code MaxComputeScanNode.isBatchMode()}.
 * Getting any gate wrong has real consequences: enabling batch when it should not (e.g. dropping the
 * "not the NOT_PRUNED sentinel" or "must have files" guard) spins up async read sessions for the wrong tables;
 * disabling it when it should fire (e.g. an off-by-one on the partition-count threshold) silently
 * regresses large-partition scans back to slow synchronous planning + large single sessions (the
 * exact OOM/latency risk this fix removes). The connector {@code fileNum > 0} check is folded into
 * the {@code supportsBatchScan} input.</p>
 *
 * <p><b>Coverage scope:</b> the original tests pin the PURE static {@code shouldUseBatchMode} gate. The
 * FIX-M3 tests additionally drive {@code computeBatchMode}'s streaming-flavor dispatch + {@code
 * numApproximateSplits} on a {@code CALLS_REAL_METHODS} mock (connector/desc fields injected). The MTMV
 * tests drive real snapshot warmup, injection into the refresh statement, a no-filter logical scan, the
 * pre-lock scan-view preload, physical deferred finalization, and the partition-flavor batch gate. Still NOT
 * exercised here: the partition-flavor {@code computeBatchMode} branch's {@code scanProvider != null}
 * null-guard, and BOTH async {@code startSplit} pumps (partition + streaming) — these need a live harness
 * this module lacks (DV-019 gaps), covered by flip-gated e2e.</p>
 */
public class PluginDrivenScanNodeBatchModeTest {

    private static final int THRESHOLD = 1024; // Explicit threshold, independent of session defaults.

    private static SelectedPartitions pruned(int count) {
        Map<String, PartitionItem> items = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            items.put("pt=" + i, Mockito.mock(PartitionItem.class));
        }
        return new SelectedPartitions(count, items, true);
    }

    @Test
    public void testNotPrunedNeverBatches() {
        // NOT_PRUNED = non-partitioned / pruning not applicable -> never batch. NOTE: NOT_PRUNED carries
        // an EMPTY map, so this case is non-discriminating for the == NOT_PRUNED guard alone (0 >= THRESHOLD
        // is false regardless); the guard's discriminating counterpart is testNoPredicatePartitionedTableBatches
        // (a full, non-sentinel isPruned=false map, which MUST batch). This test documents the NOT_PRUNED
        // singleton path (non-partitioned / unpruned).
        Assertions.assertFalse(
                PluginDrivenScanNode.shouldUseBatchMode(SelectedPartitions.NOT_PRUNED, true, true, THRESHOLD));
    }

    @Test
    public void testNullSelectionNeverBatches() {
        Assertions.assertFalse(PluginDrivenScanNode.shouldUseBatchMode(null, true, true, THRESHOLD));
    }

    @Test
    public void testNoPredicatePartitionedTableBatches() {
        // A partitioned table with NO partition predicate: ExternalTable.initSelectedPartitions returns a
        // FULL, non-NOT_PRUNED map with isPruned=false (PruneFileScanPartition only runs under a
        // LogicalFilter). Scanning every partition is EXACTLY the case that most needs async/streaming split
        // generation, and legacy MaxComputeScanNode.isBatchMode (its != NOT_PRUNED gate) batched it. The gate
        // is == NOT_PRUNED, NOT !isPruned: this object is not the sentinel, so it MUST batch once the
        // partition count reaches the threshold. The old !isPruned gate wrongly returned false here (forcing
        // slow synchronous planning on the largest scans) — the regression this now pins against. NOTE this
        // inverts the former testUnprocessedPruningNeverBatches assertion: a prior review's "!isPruned is
        // equivalent to != NOT_PRUNED and slightly stronger" note was mistaken (they diverge for exactly this
        // case); that note and the test pinning it are superseded (decisions-log D-035 / deviations-log DV-019).
        Map<String, PartitionItem> items = new LinkedHashMap<>();
        for (int i = 0; i < THRESHOLD; i++) {
            items.put("pt=" + i, Mockito.mock(PartitionItem.class));
        }
        SelectedPartitions noPredicateFullScan = new SelectedPartitions(THRESHOLD, items, false);
        Assertions.assertTrue(
                PluginDrivenScanNode.shouldUseBatchMode(noPredicateFullScan, true, true, THRESHOLD));
    }

    @Test
    public void testDeferredPartitionViewMaterializationRestoresFullScanBatchEligibility() {
        Map<String, PartitionItem> items = new LinkedHashMap<>();
        for (int index = 0; index < THRESHOLD; index++) {
            items.put("pt=" + index, Mockito.mock(PartitionItem.class));
        }

        SelectedPartitions materialized = PluginDrivenScanNode.materializeDeferredSelectedPartitions(
                SelectedPartitions.DEFERRED_PARTITION_PRUNING, Optional.of(items));

        Assertions.assertNotSame(SelectedPartitions.DEFERRED_PARTITION_PRUNING, materialized);
        Assertions.assertTrue(
                PluginDrivenScanNode.shouldUseBatchMode(materialized, true, true, THRESHOLD));
    }

    @Test
    public void testUnavailableDeferredPartitionViewDegradesToScanAll() {
        // An unrepresentable connector partition makes the whole deferred view unavailable. That must degrade
        // to NOT_PRUNED (read every partition) instead of an empty selection, which would read none.
        SelectedPartitions degraded = PluginDrivenScanNode.materializeDeferredSelectedPartitions(
                SelectedPartitions.DEFERRED_PARTITION_PRUNING, Optional.empty());

        Assertions.assertSame(SelectedPartitions.NOT_PRUNED, degraded);
        Assertions.assertNull(PluginDrivenScanNode.resolveRequiredPartitions(degraded));
    }

    @Test
    public void testMtmvWarmupInvalidMaterializedPinPreservesScanAllBeforeBatchDispatch() {
        assertMtmvWarmupBatchMode(true);
    }

    @Test
    public void testMtmvWarmupValidMaterializedPinRetainsBatchMode() {
        assertMtmvWarmupBatchMode(false);
    }

    @SuppressWarnings("unchecked")
    private static void assertMtmvWarmupBatchMode(boolean hasMalformedPartition) {
        // SELF_MANAGE SELECT id FROM hive_table: enough valid DATE partitions to open the partition batch
        // gate, plus an HMS-created dt=not-a-date whose id rows still need to be scanned.
        List<ConnectorPartitionInfo> partitions = new ArrayList<>();
        for (int index = 0; index < THRESHOLD; index++) {
            String value = LocalDate.of(2024, 1, 1).plusDays(index).toString();
            partitions.add(new ConnectorPartitionInfo("dt=" + value, Collections.singletonMap("dt", value),
                    Collections.emptyMap(), Collections.singletonList(value), Collections.emptyList()));
        }
        if (hasMalformedPartition) {
            partitions.add(new ConnectorPartitionInfo("dt=not-a-date",
                    Collections.singletonMap("dt", "not-a-date"), Collections.emptyMap(),
                    Collections.singletonList("not-a-date"), Collections.emptyList()));
        }

        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);
        Mockito.when(connector.getCapabilities())
                .thenReturn(Collections.singleton(ConnectorCapability.SUPPORTS_METADATA_PRELOAD));
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("hive");
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        ExternalDatabase<PluginDrivenExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(db.getRemoteName()).thenReturn("db");
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(metadata.getTableHandle(session, "db", "hive_table")).thenReturn(Optional.of(handle));
        // Plain, nontransactional Hive has a lightweight fence rather than a data snapshot.
        ConnectorMvccSnapshot fence = ConnectorMvccSnapshot.builder().snapshotId(-1L).build();
        Mockito.when(metadata.beginQuerySnapshot(session, handle)).thenReturn(Optional.of(fence));
        Mockito.when(metadata.applySnapshot(session, handle, fence)).thenReturn(handle);
        Mockito.when(metadata.listPartitions(session, handle, Optional.empty())).thenReturn(partitions);
        Column partitionColumn = new Column("dt", Type.DATEV2);
        SchemaCacheValue schema = new PluginDrivenSchemaCacheValue(
                List.of(new Column("id", Type.INT), partitionColumn), List.of(partitionColumn), List.of("dt"));
        PluginDrivenMvccExternalTable table = new PluginDrivenMvccExternalTable(
                31L, "hive_table", "hive_table", catalog, db) {
            @Override
            protected synchronized void makeSureInitialized() {
                // No Env-backed catalog initialization in this connector unit test.
            }

            @Override
            protected Optional<SchemaCacheValue> getLatestSchemaCacheValue() {
                return Optional.of(schema);
            }

            @Override
            public boolean supportsConnectorPartitionPruning() {
                return true;
            }
        };

        // Same warmup and snapshot injection as MTMVTask.beforeMTMVRefresh, without starting a live job.
        PluginDrivenMvccSnapshot lightweight = (PluginDrivenMvccSnapshot) table.loadSnapshot(
                Optional.empty(), Optional.empty());
        PluginDrivenMvccSnapshot materialized = (PluginDrivenMvccSnapshot)
                table.materializePartitionViewForMtmv(lightweight);
        Assertions.assertTrue(materialized.isPartitionViewMaterialized());
        Assertions.assertSame(fence, materialized.getConnectorSnapshot());
        Assertions.assertEquals(THRESHOLD, materialized.getNameToPartitionItem().size());
        Assertions.assertEquals(partitions.size(), materialized.getNameToLastModifiedMillis().size());
        Assertions.assertEquals(hasMalformedPartition, materialized.isPartitionInvalid());
        Assertions.assertEquals(hasMalformedPartition ? PartitionType.UNPARTITIONED : PartitionType.LIST,
                table.getPartitionType(Optional.of(materialized)));

        ConnectContext connectContext = new ConnectContext();
        StatementContext statementContext = new StatementContext(connectContext, null);
        connectContext.setStatementContext(statementContext);
        connectContext.setThreadLocalInfo();
        try {
            statementContext.setSnapshot(new MvccTableInfo(table), materialized);
            LogicalFileScan scan = new LogicalFileScan(new RelationId(1), table, List.of("hive", "db"),
                    Collections.emptyList(), Optional.empty(), Optional.empty(), Optional.empty(),
                    Optional.of(List.of(new SlotReference("id", IntegerType.INSTANCE))));
            Assertions.assertTrue(scan.getSelectedPartitions().isDeferredPartitionPruning());
            Assertions.assertFalse(scan.hasPartitionPredicate());

            TableIf internalTable = Mockito.mock(TableIf.class);
            Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
            statementContext.getTables().put(List.of("internal", "db", "mv"), internalTable);
            statementContext.registerExternalTableForPreload(table, Optional.empty(), Optional.empty());
            statementContext.preloadDeferredScanPartitionViewsBeforeLock(true);
            Optional<Map<String, PartitionItem>> scanView = statementContext.resolveScanPartitionView(
                    table, Optional.empty(), Optional.empty(), () -> {
                        throw new AssertionError("finalization must consume the warmed scan view");
                    });
            Assertions.assertEquals(!hasMalformedPartition, scanView.isPresent());

            ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
            Mockito.when(provider.streamingSplitEstimate(Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.anyBoolean())).thenReturn(-1L);
            Mockito.when(provider.supportsBatchScan(session, handle)).thenReturn(true);
            PluginDrivenScanNode node = streamingNode(provider);
            Deencapsulation.setField(node, "connectorSession", session);
            Deencapsulation.setField(node, "currentHandle", handle);
            TupleDescriptor desc = Deencapsulation.getField(node, "desc");
            Mockito.when(desc.getTable()).thenReturn(table);
            node.setSelectedPartitions(scan.getSelectedPartitions());
            // Drive the real no-arg finalization resolver, not just the pure SelectedPartitions helper.
            Deencapsulation.invoke(node, "materializeDeferredSelectedPartitions");
            SelectedPartitions finalized = Deencapsulation.getField(node, "selectedPartitions");
            Assertions.assertEquals(!hasMalformedPartition, node.isBatchMode());
            Assertions.assertFalse((Boolean) Deencapsulation.getField(node, "streamingBatch"));
            Assertions.assertNull(PluginDrivenScanNode.resolveRequiredPartitions(finalized));
            if (hasMalformedPartition) {
                Assertions.assertSame(SelectedPartitions.NOT_PRUNED, finalized,
                        "the malformed partition must not be excluded by startSplit's map-key batches");
            } else {
                Assertions.assertFalse(finalized.isNotPruned());
                Assertions.assertEquals(materialized.getNameToPartitionItem(), finalized.selectedPartitions);
                Assertions.assertFalse(finalized.isPruned);
            }
            Mockito.verify(provider).supportsBatchScan(session, handle);
            Mockito.verify(metadata, Mockito.times(1)).listPartitions(session, handle, Optional.empty());
            Mockito.verify(metadata, Mockito.times(1)).beginQuerySnapshot(session, handle);
            Mockito.verify(metadata, Mockito.times(1)).applySnapshot(session, handle, fence);
        } finally {
            try {
                statementContext.close();
            } finally {
                ConnectContext.remove();
            }
        }
    }

    @Test
    public void testDisabledMvPhysicalFinalizationConsumesThePrelockedView() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableMaterializedViewRewrite(false);
        sessionVariable.setEnableDmlMaterializedViewRewrite(false);
        Map<String, PartitionItem> items = new LinkedHashMap<>();
        for (int index = 0; index < THRESHOLD; index++) {
            items.put("pt=" + index, Mockito.mock(PartitionItem.class));
        }
        Optional<Map<String, PartitionItem>> scanView = Optional.of(items);

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hiveExternalTable.getId()).thenReturn(31L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);
        Mockito.when(hiveExternalTable.getNameToPartitionItemsForScan(Mockito.any())).thenReturn(scanView);

        StatementContext statementContext = new StatementContext(
                connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(
                    hiveExternalTable, Optional.empty(), Optional.empty());
            statementContext.preloadDeferredScanPartitionViewsBeforeLock(true);

            Optional<Map<String, PartitionItem>> consumed =
                    statementContext.resolveScanPartitionView(hiveExternalTable, Optional.empty(),
                            Optional.empty(), () -> {
                                throw new AssertionError("physical finalization must reuse the prelocked view");
                            });
            SelectedPartitions materialized = PluginDrivenScanNode.materializeDeferredSelectedPartitions(
                    SelectedPartitions.DEFERRED_PARTITION_PRUNING, consumed);

            Assertions.assertEquals(scanView.get().keySet(), materialized.selectedPartitions.keySet());
            Assertions.assertTrue(PluginDrivenScanNode.shouldUseBatchMode(
                    materialized, true, true, THRESHOLD));
            Mockito.verify(hiveExternalTable, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testNoSlotsNeverBatches() {
        // No required slots (e.g. count-only) -> not batch. Pins the hasSlots guard.
        Assertions.assertFalse(PluginDrivenScanNode.shouldUseBatchMode(pruned(THRESHOLD), false, true, THRESHOLD));
    }

    @Test
    public void testConnectorWithoutBatchSupportNeverBatches() {
        // supportsBatchScan=false -> not batch. Pins the supportsBatchScan guard. (A null scan provider
        // also resolves to supportsBatchScan=false, but that mapping lives in computeBatchMode's
        // null-guard and is NOT exercised by this static-helper test — see DV-019.)
        Assertions.assertFalse(PluginDrivenScanNode.shouldUseBatchMode(pruned(THRESHOLD), true, false, THRESHOLD));
    }

    @Test
    public void testZeroThresholdDisablesBatch() {
        // num_partitions_in_batch_mode == 0 disables batch mode entirely (legacy contract). Pins the
        // `numPartitionsInBatchMode > 0` guard: with `>= 0` a zero threshold would wrongly batch.
        Assertions.assertFalse(PluginDrivenScanNode.shouldUseBatchMode(pruned(THRESHOLD), true, true, 0));
    }

    @Test
    public void testBelowThresholdDoesNotBatch() {
        // Fewer pruned partitions than the threshold -> synchronous path (small scans need no batching).
        Assertions.assertFalse(
                PluginDrivenScanNode.shouldUseBatchMode(pruned(THRESHOLD - 1), true, true, THRESHOLD));
    }

    @Test
    public void testAtThresholdBatches() {
        // size == threshold is INCLUSIVE (legacy uses >=). Pins the boundary: a `>` mutant fails here.
        Assertions.assertTrue(
                PluginDrivenScanNode.shouldUseBatchMode(pruned(THRESHOLD), true, true, THRESHOLD));
    }

    @Test
    public void testAboveThresholdBatches() {
        // The main success case: a large pruned partition set on a file-bearing, sloted, pruned table.
        Assertions.assertTrue(
                PluginDrivenScanNode.shouldUseBatchMode(pruned(THRESHOLD + 5), true, true, THRESHOLD));
    }

    // --- FIX-M3 streaming (file-count) batch flavor: computeBatchMode dispatch + numApproximateSplits ---
    // Driven on a CALLS_REAL_METHODS mock (no constructor — see class note), with the connector/desc fields
    // injected and getPushDownAggNoGroupingOp stubbed, so the real computeBatchMode wiring runs.

    /** A node whose connector exposes the given provider, with a single-slot desc + non-count agg. */
    private static PluginDrivenScanNode streamingNode(ConnectorScanPlanProvider provider) {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        Connector connector = Mockito.mock(Connector.class);
        // The node resolves the provider PER TABLE via getScanPlanProvider(currentHandle); a real connector
        // delegates that overload to the no-arg getter, so the mock answers the arg form (currentHandle is
        // null in this partial node, hence the null-tolerant any() matcher).
        Mockito.when(connector.getScanPlanProvider(Mockito.any())).thenReturn(provider);
        Deencapsulation.setField(node, "connector", connector);
        // hasSlots = true (the streaming gate requires output slots). getSlots() returns ArrayList (concrete).
        TupleDescriptor desc = Mockito.mock(TupleDescriptor.class);
        ArrayList<SlotDescriptor> slots = new ArrayList<>();
        slots.add(Mockito.mock(SlotDescriptor.class));
        Mockito.when(desc.getSlots()).thenReturn(slots);
        Deencapsulation.setField(node, "desc", desc);
        // Non-count agg so countPushdown=false; sessionVariable needed by the partition fallback arg eval.
        Mockito.doReturn(TPushAggOp.NONE).when(node).getPushDownAggNoGroupingOp();
        SessionVariable sv = Mockito.mock(SessionVariable.class);
        Mockito.when(sv.getEnableExternalTableBatchMode()).thenReturn(true);
        Mockito.when(sv.getNumPartitionsInBatchMode()).thenReturn(THRESHOLD);
        Mockito.when(sv.getEnableExternalTableBatchMode()).thenReturn(true);
        Deencapsulation.setField(node, "sessionVariable", sv);
        return node;
    }

    @Test
    void disabledBatchModeSkipsBothPartitionAndStreamingProviders() {
        for (SelectedPartitions partitions : new SelectedPartitions[] {pruned(1), SelectedPartitions.NOT_PRUNED}) {
            ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
            PluginDrivenScanNode node = streamingNode(provider);
            SessionVariable defaults = new SessionVariable();
            defaults.enableExternalTableBatchMode = false;
            defaults.numPartitionsInBatchMode = 1;
            Deencapsulation.setField(node, "sessionVariable", defaults);
            Deencapsulation.setField(node, "selectedPartitions", partitions);
            Assertions.assertFalse(node.isBatchMode());
            Mockito.verifyNoInteractions(provider);
        }
    }

    @Test
    void enabledBatchModeAcceptsOnePartitionThroughTheEngineGate() {
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        Mockito.when(provider.supportsBatchScan(Mockito.any(), Mockito.any())).thenReturn(true);
        PluginDrivenScanNode node = streamingNode(provider);
        SessionVariable enabled = new SessionVariable();
        enabled.numPartitionsInBatchMode = 1;
        Deencapsulation.setField(node, "sessionVariable", enabled);
        Deencapsulation.setField(node, "selectedPartitions", pruned(1));
        Assertions.assertTrue(node.isBatchMode());
        Mockito.verify(provider, Mockito.never()).streamingSplitEstimate(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    }

    @Test
    public void testComputeBatchModePrefersStreamingWhenEstimateNonNegative() {
        // A connector that returns a non-negative streamingSplitEstimate enters the streaming flavor of batch
        // mode (before the partition-count flavor). MUTATION: dropping the `estimate >= 0` block -> falls through
        // to the partition path (false here) -> red.
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        Mockito.when(provider.streamingSplitEstimate(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(50L);
        PluginDrivenScanNode node = streamingNode(provider);

        Assertions.assertTrue(node.isBatchMode());
        Assertions.assertTrue((Boolean) Deencapsulation.getField(node, "streamingBatch"));
        Assertions.assertEquals(50L, (long) (Long) Deencapsulation.getField(node, "streamingSplitEstimate"));
        // numApproximateSplits then reports the streamed estimate (not the partition count).
        Assertions.assertEquals(50, node.numApproximateSplits());
    }

    @Test
    public void testComputeBatchModeFallsBackToPartitionPathWhenNoStreaming() {
        // A connector that declines streaming (estimate < 0) must NOT set the streaming flag; the node falls
        // back to the partition-count gate (false here: no pruned partitions, supportsBatchScan false). MUTATION:
        // setting streamingBatch unconditionally -> the flag would be true -> red.
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        Mockito.when(provider.streamingSplitEstimate(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(-1L);
        PluginDrivenScanNode node = streamingNode(provider);

        Assertions.assertFalse(node.isBatchMode());
        Assertions.assertFalse((Boolean) Deencapsulation.getField(node, "streamingBatch"));
    }

    @Test
    public void testNumApproximateSplitsStreamingCapsAtIntMax() {
        // A pathologically large matched-file count must clamp to Integer.MAX_VALUE, never overflow to a
        // negative split count (FileQueryScanNode rejects negative). MUTATION: dropping the cap -> negative -> red.
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(node, "streamingBatch", true);
        Deencapsulation.setField(node, "streamingSplitEstimate", (long) Integer.MAX_VALUE + 100L);
        Assertions.assertEquals(Integer.MAX_VALUE, node.numApproximateSplits());
    }
}
