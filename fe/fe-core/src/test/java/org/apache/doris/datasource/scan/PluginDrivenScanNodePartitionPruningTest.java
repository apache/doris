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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.FilterApplicationResult;
import org.apache.doris.datasource.connector.converter.ExprToConnectorExpressionConverter;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * FIX-PRUNE-PUSHDOWN (P4-T06e / DG-1) — guards {@link PluginDrivenScanNode#resolveRequiredPartitions},
 * the three-state mapping from the Nereids {@code SelectedPartitions} to the partition list pushed
 * down to the connector SPI.
 *
 * <p><b>Why this matters:</b> before this fix the plugin-driven MaxCompute read path dropped the
 * pruned partition set entirely, so the ODPS read session spanned ALL partitions (full-scan
 * perf/memory regression). The fix threads the pruned set through, but the null-vs-empty distinction
 * is load-bearing and easy to get wrong:</p>
 * <ul>
 *   <li>{@code null} = "not pruned, scan all" — must NOT be confused with the short-circuit case,
 *       or every row would be silently dropped;</li>
 *   <li>non-empty list = "scan only these" — must be forwarded, or large tables regress to a full
 *       scan;</li>
 *   <li>empty list = "pruned to zero partitions" — must be distinguishable (non-null) so
 *       {@code getSplits()} can short-circuit with no splits, mirroring legacy
 *       {@code MaxComputeScanNode.getSplits():724-727}.</li>
 * </ul>
 */
public class PluginDrivenScanNodePartitionPruningTest {

    @Test
    public void testNotPrunedScansAllPartitions() {
        // NOT_PRUNED -> null (scan all). Returning [] here would be read as "pruned to zero" and
        // silently drop all rows.
        Assertions.assertNull(
                PluginDrivenScanNode.resolveRequiredPartitions(SelectedPartitions.NOT_PRUNED));
    }

    @Test
    public void testNullSelectionScansAllPartitions() {
        Assertions.assertNull(PluginDrivenScanNode.resolveRequiredPartitions(null));
    }

    @Test
    public void testUnprocessedPruningScansAllPartitions() {
        // isPruned=false with a populated map is still "pruning not processed" -> scan all.
        Map<String, PartitionItem> items = new LinkedHashMap<>();
        items.put("pt=1", Mockito.mock(PartitionItem.class));
        SelectedPartitions notProcessed = new SelectedPartitions(3, items, false);
        Assertions.assertNull(PluginDrivenScanNode.resolveRequiredPartitions(notProcessed));
    }

    @Test
    public void testPrunedSubsetForwardsPartitionNames() {
        // Pruned non-empty set must be forwarded; otherwise the connector reads all partitions.
        Map<String, PartitionItem> items = new LinkedHashMap<>();
        items.put("pt=1", Mockito.mock(PartitionItem.class));
        items.put("pt=2,region=cn", Mockito.mock(PartitionItem.class));
        SelectedPartitions pruned = new SelectedPartitions(5, items, true);

        List<String> result = PluginDrivenScanNode.resolveRequiredPartitions(pruned);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.containsAll(Arrays.asList("pt=1", "pt=2,region=cn")));
    }

    @Test
    public void testPrunedToZeroReturnsEmptyNonNullForShortCircuit() {
        // Pruned to zero partitions -> non-null empty list, distinct from the null "scan all"
        // case, so getSplits() can short-circuit and read nothing.
        // NOTE (FIX-NONPART-PRUNE-DATALOSS): this isPruned=true+empty state is correct ONLY when it
        // comes from a genuinely PARTITIONED table whose predicates pruned away every partition
        // (e.g. WHERE pt='nonexistent'). A NON-partitioned table must never reach this state, or the
        // short-circuit silently drops all rows; PluginDrivenExternalTable.supportInternalPartitionPruned()
        // returns unconditional true precisely so PruneFileScanPartition leaves non-partitioned tables
        // NOT_PRUNED (see PluginDrivenExternalTablePartitionTest
        // #testNonPartitionedTableReportsNoPartitionsButStillOptsIntoPruning).
        // totalPartitionNum=5 (> 0): a GENUINE prune-to-zero over a non-empty universe.
        SelectedPartitions emptyPruned = new SelectedPartitions(5, Collections.emptyMap(), true);

        List<String> result = PluginDrivenScanNode.resolveRequiredPartitions(emptyPruned);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testPrunedToZeroWithoutIgnoreStillShortCircuits() {
        // Default (non-predicate-driven connector, ignoreShortCircuit=false): a genuine prune-to-zero still
        // maps to the non-null empty list so getSplits() short-circuits — the existing MaxCompute parity.
        SelectedPartitions emptyPruned = new SelectedPartitions(5, Collections.emptyMap(), true);
        List<String> result = PluginDrivenScanNode.resolveRequiredPartitions(emptyPruned, false);
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testPrunedToZeroWithUnknownTotalStillShortCircuits() {
        SelectedPartitions emptyPruned = new SelectedPartitions(
                SelectedPartitions.UNKNOWN_TOTAL_PARTITION_NUM, Collections.emptyMap(), true, true);

        List<String> result = PluginDrivenScanNode.resolveRequiredPartitions(emptyPruned);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testPrunedToZeroWithIgnoreShortCircuitScansAll() {
        // A predicate-driven connector (paimon: ConnectorScanPlanProvider.ignorePartitionPruneShortCircuit()
        // == true) must NOT short-circuit a genuine prune-to-zero. With the master-parity isNull=false a
        // genuine-null paimon partition renders as a NON-null sentinel, so `region IS NULL` prunes EVERY
        // partition away (empty set over a 5-partition universe); short-circuiting here would drop the
        // genuine-null row. Returning null (scan-all) lets getSplits() call planScan, which the paimon SDK
        // answers from the pushed `region IS NULL` predicate -> the genuine-null row is returned (master
        // PaimonScanNode parity; regression test_paimon_runtime_filter_partition_pruning qt_null_partition_4).
        // MUTATION: ignoring the flag -> returns the empty short-circuit list -> red.
        SelectedPartitions emptyPruned = new SelectedPartitions(5, Collections.emptyMap(), true);
        Assertions.assertNull(
                PluginDrivenScanNode.resolveRequiredPartitions(emptyPruned, true),
                "a predicate-driven connector must scan-all (null) on a prune-to-zero, not short-circuit");
        Assertions.assertTrue(PluginDrivenScanNode.partitionsPrunedToEmpty(emptyPruned, true),
                "the request must retain that scan-all came from an empty live selection");
        Assertions.assertFalse(PluginDrivenScanNode.partitionsPrunedToEmpty(emptyPruned, false),
                "a connector that short-circuits receives no request to annotate");
    }

    @Test
    public void testPrunedSubsetWithIgnoreShortCircuitStillForwards() {
        // The flag only governs the empty/short-circuit case. A non-empty pruned set is still forwarded
        // unchanged (the connector may ignore it, but the non-empty contract is preserved).
        Map<String, PartitionItem> items = new LinkedHashMap<>();
        items.put("pt=1", Mockito.mock(PartitionItem.class));
        SelectedPartitions pruned = new SelectedPartitions(5, items, true);
        List<String> result = PluginDrivenScanNode.resolveRequiredPartitions(pruned, true);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
    }

    @Test
    public void testPrunedEmptyOverEmptyUniverseScansAll() {
        // RD-1 (B5b-4): a pruned-but-empty selection whose partition UNIVERSE was ALSO empty
        // (totalPartitionNum == 0) is NOT a genuine prune-to-zero. It is an MVCC time-travel pin
        // (FOR VERSION/TIME AS OF, @tag, @branch) whose snapshot deliberately carries an empty
        // partition-item map and defers partition pruning to the connector's predicate pushdown.
        // It must map to null (scan-all) so getSplits() does NOT short-circuit and planScan runs,
        // mirroring legacy PaimonScanNode (which ignores selectedPartitions and re-plans via the SDK).
        // MUTATION: returning the empty list (like the totalPartitionNum>0 case above) short-circuits
        // to zero splits -> silent data loss for partitioned time-travel + a partition predicate, and
        // this assertion goes red.
        SelectedPartitions emptyUniverse = new SelectedPartitions(0, Collections.emptyMap(), true);

        Assertions.assertNull(PluginDrivenScanNode.resolveRequiredPartitions(emptyUniverse),
                "a pruned-empty selection over an empty partition universe (time-travel pin) must scan all");
        Assertions.assertFalse(PluginDrivenScanNode.partitionsPrunedToEmpty(emptyUniverse, true),
                "an empty metadata universe is not a predicate exclusion of live partitions");
    }

    @Test
    public void testPhysicalScanReusesThePruningHandleInsteadOfReapplyingTheFilter() {
        // The logical pruning rule applied the connector predicate and materialized the selection from that
        // handle ("generation A"). The physical scan must reuse it: applying the predicate a second time could
        // observe a different remote generation ("generation B") and leave the batched split path resolving
        // generation A's partition names through generation B's pruned-partition metadata.
        ConnectorTableHandle generationA = Mockito.mock(ConnectorTableHandle.class);
        FilterApplicationResult<ConnectorTableHandle> pruned =
                new FilterApplicationResult<>(generationA, null, false);
        ConnectorExpression coveredPredicate = in("p", 1, 2);
        SelectedPartitions selection = SelectedPartitions.connectorFiltered(
                SelectedPartitions.UNKNOWN_TOTAL_PARTITION_NUM,
                Collections.singletonMap("p=1", Mockito.mock(PartitionItem.class)), true, pruned,
                coveredPredicate);

        Optional<FilterApplicationResult<ConnectorTableHandle>> reused =
                PluginDrivenScanNode.reusedConnectorFilterResult(selection);

        Assertions.assertTrue(reused.isPresent(), "a connector-pruned selection carries its filter result");
        Assertions.assertSame(generationA, reused.get().getHandle(),
                "the physical scan must reuse the handle the selection was materialized from");
        Assertions.assertFalse(PluginDrivenScanNode.reusedConnectorFilterResult(
                SelectedPartitions.NOT_PRUNED).isPresent(), "a plain selection has nothing to reuse");
    }

    @Test
    public void testPredicatesAddedAfterConnectorFilteringRemainForBackendEvaluation() {
        // Async-MV union compensation can copy a scan that was pruned with p IN (1, 2) and later attach
        // another conjunct, p = 2. The physical scan must reuse the connector handle from the earlier
        // filter, but must not clear the larger current predicate set for backend evaluation.
        ConnectorExpression firstOriginalPredicate = in("p", 1, 2);
        ConnectorExpression secondOriginalPredicate = equal("q", 3);
        ConnectorExpression originalPredicate = new ConnectorAnd(java.util.Arrays.asList(
                firstOriginalPredicate, secondOriginalPredicate));
        ConnectorExpression compensationPredicate = equal("p", 2);
        ConnectorExpression currentPredicate = new ConnectorAnd(
                java.util.Arrays.asList(originalPredicate, compensationPredicate));
        FilterApplicationResult<ConnectorTableHandle> pruned =
                new FilterApplicationResult<>(Mockito.mock(ConnectorTableHandle.class), null, false);
        SelectedPartitions selection = SelectedPartitions.connectorFiltered(
                SelectedPartitions.UNKNOWN_TOTAL_PARTITION_NUM,
                Collections.singletonMap("p=1", Mockito.mock(PartitionItem.class)), true, pruned,
                originalPredicate);

        Assertions.assertTrue(PluginDrivenScanNode.connectorFilterCoversCurrentPredicate(
                selection, originalPredicate));
        Assertions.assertTrue(PluginDrivenScanNode.connectorFilterCoversCurrentPredicate(
                selection, new ConnectorAnd(java.util.Arrays.asList(
                        secondOriginalPredicate, firstOriginalPredicate))));
        Assertions.assertFalse(PluginDrivenScanNode.connectorFilterCoversCurrentPredicate(
                selection, currentPredicate), "a later compensation predicate is not covered by the old result");

        ConnectorTableHandle prunedHandle = Mockito.mock(ConnectorTableHandle.class);
        FilterApplicationResult<ConnectorTableHandle> result =
                new FilterApplicationResult<>(prunedHandle, null, false);
        List<Expr> compensationConjuncts = new ArrayList<>(java.util.Arrays.asList(
                Mockito.mock(Expr.class), Mockito.mock(Expr.class)));

        ConnectorTableHandle reusedHandle = PluginDrivenScanNode.applyConnectorFilterResult(
                result, false, compensationConjuncts);

        Assertions.assertSame(prunedHandle, reusedHandle);
        Assertions.assertEquals(2, compensationConjuncts.size(),
                "predicates added after logical connector filtering must remain for backend evaluation");

        List<Expr> originalConjuncts = new ArrayList<>(Collections.singletonList(Mockito.mock(Expr.class)));
        Assertions.assertSame(prunedHandle, PluginDrivenScanNode.applyConnectorFilterResult(
                result, true, originalConjuncts));
        Assertions.assertTrue(originalConjuncts.isEmpty(),
                "the exact predicate accepted by the connector may be consumed");
    }

    @Test
    public void testConvertPredicateReusesCoveredLogicalHandleWithoutConnectorRpc() throws AnalysisException {
        try (PredicateFixture fixture = new PredicateFixture()) {
            Expr accepted = legacyIn(1, 2);
            ConnectorExpression covered = ExprToConnectorExpressionConverter.convert(accepted);
            ConnectorTableHandle filteredHandle = Mockito.mock(ConnectorTableHandle.class);
            FilterApplicationResult<ConnectorTableHandle> result =
                    new FilterApplicationResult<>(filteredHandle, null, false);
            SelectedPartitions selected = SelectedPartitions.connectorFiltered(
                    SelectedPartitions.UNKNOWN_TOTAL_PARTITION_NUM, fixture.items, true, result, covered);
            fixture.setSelectionAndConjuncts(selected, List.of(accepted));
            fixture.lock();

            fixture.node.convertPredicate();

            Assertions.assertSame(filteredHandle, Deencapsulation.getField(fixture.node, "currentHandle"));
            Assertions.assertTrue(fixture.node.getConjuncts().isEmpty(),
                    "only the exact predicate already accepted by the connector may be consumed");
            Assertions.assertSame(selected, Deencapsulation.getField(fixture.node, "selectedPartitions"));
            Mockito.verifyNoInteractions(fixture.metadata, fixture.connector);
        }
    }

    @Test
    public void testConvertPredicateReusesLogicalHandleAndRetainsStrongerResidualWithoutRpc()
            throws AnalysisException {
        try (PredicateFixture fixture = new PredicateFixture()) {
            Expr accepted = legacyIn(1, 2);
            Expr compensation = legacyEqual(2);
            ConnectorExpression covered = ExprToConnectorExpressionConverter.convert(accepted);
            ConnectorTableHandle filteredHandle = Mockito.mock(ConnectorTableHandle.class);
            FilterApplicationResult<ConnectorTableHandle> result =
                    new FilterApplicationResult<>(filteredHandle, null, false);
            SelectedPartitions selected = SelectedPartitions.connectorFiltered(
                    SelectedPartitions.UNKNOWN_TOTAL_PARTITION_NUM, fixture.items, true, result, covered);
            fixture.setSelectionAndConjuncts(selected, List.of(accepted, compensation));
            fixture.lock();

            fixture.node.convertPredicate();

            Assertions.assertSame(filteredHandle, Deencapsulation.getField(fixture.node, "currentHandle"));
            Assertions.assertEquals(List.of(accepted, compensation), fixture.node.getConjuncts(),
                    "the compensation conjunct must remain for BE even when the old filter was fully accepted");
            Assertions.assertSame(covered, selected.getCoveredConnectorFilter().get());
            Assertions.assertSame(selected, Deencapsulation.getField(fixture.node, "selectedPartitions"));
            Mockito.verifyNoInteractions(fixture.metadata, fixture.connector);
        }
    }

    @Test
    public void testConvertPredicateWarmedFallbackKeepsResidualWithoutConnectorRpc() throws AnalysisException {
        try (PredicateFixture fixture = new PredicateFixture()) {
            fixture.warmFullView();
            Expr residual = legacyEqual(2);
            SelectedPartitions selected = new SelectedPartitions(fixture.fullView.size(), fixture.items, true);
            fixture.setSelectionAndConjuncts(selected, List.of(residual));
            fixture.lock();

            fixture.node.convertPredicate();

            Assertions.assertSame(fixture.initialHandle, Deencapsulation.getField(fixture.node, "currentHandle"));
            Assertions.assertEquals(List.of(residual), fixture.node.getConjuncts());
            Assertions.assertSame(selected, Deencapsulation.getField(fixture.node, "selectedPartitions"));
            Mockito.verify(fixture.table, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
            Mockito.verify(fixture.table, Mockito.never()).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            Mockito.verifyNoInteractions(fixture.metadata, fixture.connector);
        }
    }

    private static Expr legacyIn(int... values) throws AnalysisException {
        List<Expr> literals = new ArrayList<>();
        for (int value : values) {
            literals.add(new IntLiteral(value, Type.INT));
        }
        return new InPredicate(legacyPartitionSlot(), literals, false);
    }

    private static Expr legacyEqual(int value) throws AnalysisException {
        return new BinaryPredicate(BinaryPredicate.Operator.EQ,
                legacyPartitionSlot(), new IntLiteral(value, Type.INT));
    }

    private static SlotRef legacyPartitionSlot() {
        SlotRef slot = Mockito.mock(SlotRef.class);
        Mockito.when(slot.getColumnName()).thenReturn("p");
        Mockito.when(slot.getType()).thenReturn(Type.INT);
        return slot;
    }

    private static final class PredicateFixture implements AutoCloseable {
        private final ConnectContext context = new ConnectContext();
        private final StatementContext statement = new StatementContext(context, null);
        private final PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        private final TableIf internalTable = Mockito.mock(TableIf.class);
        private final AtomicBoolean internalLockHeld = new AtomicBoolean();
        private final Connector connector = Mockito.mock(Connector.class);
        private final ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        private final ConnectorTableHandle initialHandle = Mockito.mock(ConnectorTableHandle.class);
        private final PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        private final Map<String, PartitionItem> items =
                Map.of("p=2", Mockito.mock(PartitionItem.class));

        private final Map<String, PartitionItem> fullView = Map.of(
                "p=1", Mockito.mock(PartitionItem.class), "p=2", items.get("p=2"),
                "p=3", Mockito.mock(PartitionItem.class));

        private PredicateFixture() {
            context.setStatementContext(statement);
            context.getSessionVariable().setEnableMaterializedViewRewrite(false);
            context.getSessionVariable().setEnableDmlMaterializedViewRewrite(false);
            context.setThreadLocalInfo();
            Mockito.when(table.getId()).thenReturn(32L);
            Mockito.when(table.supportsExternalMetadataPreload()).thenReturn(true);
            Mockito.when(table.supportsConnectorPartitionPruning()).thenReturn(true);
            Mockito.doAnswer(invocation -> {
                Assertions.assertFalse(internalLockHeld.get(), "enumeration belongs before lock()");
                return Optional.of(fullView);
            }).when(table).getNameToPartitionItemsForScan(Mockito.any());
            Mockito.when(internalTable.getId()).thenReturn(31L);
            Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
            Mockito.when(internalTable.tryReadLock(1, TimeUnit.MINUTES)).thenAnswer(invocation -> {
                internalLockHeld.set(true);
                return true;
            });
            Mockito.doAnswer(invocation -> {
                internalLockHeld.set(false);
                return null;
            }).when(internalTable).readUnlock();
            statement.getTables().put(List.of("internal", "db", "t"), internalTable);
            TupleDescriptor desc = Mockito.mock(TupleDescriptor.class);
            Mockito.when(desc.getTable()).thenReturn(table);
            Deencapsulation.setField(node, "desc", desc);
            Deencapsulation.setField(node, "connector", connector);
            Deencapsulation.setField(node, "cachedMetadata", metadata);
            Deencapsulation.setField(node, "connectorSession", Mockito.mock(ConnectorSession.class));
            Deencapsulation.setField(node, "currentHandle", initialHandle);
        }

        private void warmFullView() {
            statement.registerExternalTableForPreload(table, Optional.empty(), Optional.empty());
            statement.preloadDeferredScanPartitionViewsBeforeLock(true);
            Assertions.assertSame(fullView, statement.getExternalTablePreloadInfo(table.getId()).get()
                    .getScanPartitionView().get());
        }

        private void setSelectionAndConjuncts(SelectedPartitions selected, List<Expr> conjuncts) {
            Deencapsulation.setField(node, "selectedPartitions", selected);
            Deencapsulation.setField(node, "conjuncts", new ArrayList<>(conjuncts));
        }

        private void lock() {
            statement.lock();
            Assertions.assertTrue(internalLockHeld.get(), "exercise convertPredicate inside the lock window");
            Mockito.verify(internalTable).tryReadLock(1, TimeUnit.MINUTES);
        }

        @Override
        public void close() {
            try {
                statement.close();
            } finally {
                ConnectContext.remove();
            }
        }
    }

    private static ConnectorIn in(String column, int... values) {
        List<ConnectorExpression> literals = new ArrayList<>(values.length);
        for (int value : values) {
            literals.add(ConnectorLiteral.ofInt(value));
        }
        return new ConnectorIn(columnRef(column), literals, false);
    }

    private static ConnectorComparison equal(String column, int value) {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ,
                columnRef(column), ConnectorLiteral.ofInt(value));
    }

    private static ConnectorColumnRef columnRef(String column) {
        return new ConnectorColumnRef(column, ConnectorType.of("INT"));
    }
}
