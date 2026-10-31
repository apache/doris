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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.FilterApplicationResult;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.connector.converter.NereidsToConnectorExpressionConverter;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable.ConnectorFilteredPartitionView;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.ExternalTablePreloadInfo;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** Checks for recorded-view range fallback and pre-lock connector pruning in {@link PruneFileScanPartition}. */
public class PruneFileScanPartitionTest {
    @Test
    public void testFallbackRangesComeOnlyFromTheRecordedPartitionView() throws AnalysisException {
        Map<String, PartitionItem> recordedView = new LinkedHashMap<>();
        recordedView.put("p=1", listItem(1));
        recordedView.put("p=2", listItem(2));
        ExternalTable externalTable = Mockito.mock(ExternalTable.class);
        LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);

        Optional<SortedPartitionRanges<String>> ranges = PruneFileScanPartition.fallbackSortedPartitionRanges(
                externalTable, scan, SelectedPartitions.NOT_PRUNED, recordedView);

        Assertions.assertTrue(ranges.isPresent());
        List<String> rangeNames = rangeNames(ranges);
        Assertions.assertEquals(List.of("p=1", "p=2"), rangeNames);
        // A view whose generation is not a materialized connector snapshot must not consult the cross-query
        // range cache: those ranges could have been built from another enumeration of the same table.
        Mockito.verify(externalTable, Mockito.never()).getSortedPartitionRanges(Mockito.any());
    }

    @Test
    public void testMaterializedConnectorViewReusesCachedRanges() throws AnalysisException {
        Map<String, PartitionItem> recordedView = new LinkedHashMap<>();
        recordedView.put("p=1", listItem(1));
        recordedView.put("p=2", listItem(2));
        ExternalTable externalTable = Mockito.mock(ExternalTable.class);
        LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
        // The ranges of an immutable MVCC pin: same version token, so the cross-query cache entry describes
        // exactly the partition set the recorded map came from.
        SortedPartitionRanges<String> cached = SortedPartitionRanges.build(recordedView);
        Mockito.when(externalTable.getSortedPartitionRanges(scan)).thenReturn(Optional.of(cached));
        SelectedPartitions materializedPin = new SelectedPartitions(2, recordedView, true);

        Optional<SortedPartitionRanges<String>> ranges = PruneFileScanPartition.fallbackSortedPartitionRanges(
                externalTable, scan, materializedPin, recordedView);

        Assertions.assertTrue(ranges.isPresent());
        Assertions.assertSame(cached, ranges.get());
        Assertions.assertEquals(List.of("p=1", "p=2"), rangeNames(ranges));
    }

    @Test
    public void testMaterializedConnectorViewWithoutCacheEntryRebuildsRanges() throws AnalysisException {
        Map<String, PartitionItem> recordedView = new LinkedHashMap<>();
        recordedView.put("p=1", listItem(1));
        ExternalTable externalTable = Mockito.mock(ExternalTable.class);
        LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
        Mockito.when(externalTable.getSortedPartitionRanges(scan)).thenReturn(Optional.empty());
        SelectedPartitions materializedPin = new SelectedPartitions(1, recordedView, true);

        Optional<SortedPartitionRanges<String>> ranges = PruneFileScanPartition.fallbackSortedPartitionRanges(
                externalTable, scan, materializedPin, recordedView);

        Assertions.assertTrue(ranges.isPresent());
        Assertions.assertEquals(List.of("p=1"), rangeNames(ranges));
    }

    @Test
    public void testRecordedRangesWinOverTheCacheLookup() throws AnalysisException {
        Map<String, PartitionItem> recordedView = new LinkedHashMap<>();
        recordedView.put("p=1", listItem(1));
        ExternalTable externalTable = Mockito.mock(ExternalTable.class);
        LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
        SortedPartitionRanges<String> recorded = SortedPartitionRanges.build(recordedView);
        SelectedPartitions withRecordedRanges = new SelectedPartitions(1, recordedView, true, false,
                Optional.of(recorded));

        Optional<SortedPartitionRanges<String>> ranges = PruneFileScanPartition.fallbackSortedPartitionRanges(
                externalTable, scan, withRecordedRanges, recordedView);

        Assertions.assertTrue(ranges.isPresent());
        Assertions.assertSame(recorded, ranges.get());
        Mockito.verify(externalTable, Mockito.never()).getSortedPartitionRanges(Mockito.any());
    }

    @Test
    public void testPreloadedRelationFiltersAreAppliedOnceAndReusedByLogicalPruning() throws AnalysisException {
        assertPreloadedRelationFilters(false);
    }

    @Test
    public void testCopiedAliasesKeepDistinctPreloadedFiltersAndStrongerResiduals() throws AnalysisException {
        assertPreloadedRelationFilters(true);
    }

    private static void assertPreloadedRelationFilters(boolean copyRelations) throws AnalysisException {
        try (PruningFixture fixture = new PruningFixture()) {
            RelationId firstRelation = fixture.statement.getNextRelationId();
            RelationId secondRelation = fixture.statement.getNextRelationId();
            Expression firstPredicate = new InPredicate(fixture.partitionSlot,
                    List.of(new IntegerLiteral(1), new IntegerLiteral(2)));
            Expression secondPredicate = new EqualTo(fixture.partitionSlot, new IntegerLiteral(3));
            ConnectorExpression firstCovered = NereidsToConnectorExpressionConverter.convert(firstPredicate);
            ConnectorExpression secondCovered = NereidsToConnectorExpressionConverter.convert(secondPredicate);
            ConnectorTableHandle firstHandle = Mockito.mock(ConnectorTableHandle.class);
            ConnectorTableHandle secondHandle = Mockito.mock(ConnectorTableHandle.class);
            FilterApplicationResult<ConnectorTableHandle> firstResult =
                    new FilterApplicationResult<>(firstHandle, null, false);
            FilterApplicationResult<ConnectorTableHandle> secondResult =
                    new FilterApplicationResult<>(secondHandle, null, false);
            ConnectorFilteredPartitionView firstView = filteredView(
                    Map.of("p=1", fixture.fullView.get("p=1"), "p=2", fixture.fullView.get("p=2")), firstResult);
            ConnectorFilteredPartitionView secondView = filteredView(
                    Map.of("p=3", fixture.fullView.get("p=3")), secondResult);
            fixture.register(true);
            ExternalTablePreloadInfo preload = fixture.preloadInfo();
            // Match collect's unbound predicates; the actual pre-lock pass binds the partition column.
            preload.registerScanPredicate(firstRelation, new InPredicate(new UnboundSlot("p"),
                    List.of(new IntegerLiteral(1), new IntegerLiteral(2))));
            preload.registerScanPredicate(secondRelation, new EqualTo(new UnboundSlot("p"), new IntegerLiteral(3)));
            Mockito.doAnswer(invocation -> {
                Assertions.assertFalse(fixture.internalLockHeld.get(), "selective request must precede lock()");
                ConnectorExpression predicate = invocation.getArgument(1);
                if (firstCovered.equals(predicate)) {
                    return Optional.of(firstView);
                }
                Assertions.assertEquals(secondCovered, predicate);
                return Optional.of(secondView);
            }).when(fixture.table).applyPartitionFilterForScan(Mockito.any(), Mockito.any());

            fixture.statement.preloadDeferredScanPartitionViewsBeforeLock(true);
            fixture.statement.preloadDeferredScanPartitionViewsBeforeLock(true);
            Assertions.assertSame(firstView, preload.getFilteredScanView(firstRelation).get());
            Assertions.assertSame(secondView, preload.getFilteredScanView(secondRelation).get());
            Assertions.assertFalse(preload.hasScanPartitionView(), "selective aliases must not enumerate all");
            fixture.lock();

            // Rewrite adds p=2 after preloading p IN (1, 2). Prune further locally, but carry only the
            // predicate actually accepted by the connector, not the stronger current predicate.
            Expression addedPredicate = new EqualTo(fixture.partitionSlot, new IntegerLiteral(2));
            LogicalFileScan firstScan = fixture.scan(firstRelation);
            LogicalFileScan secondScan = fixture.scan(secondRelation);
            if (copyRelations) {
                firstScan = firstScan.withRelationId(fixture.statement.getNextRelationId());
                secondScan = secondScan.withRelationId(fixture.statement.getNextRelationId());
            }
            LogicalFilter<LogicalFileScan> strongerFilter = new LogicalFilter<>(
                    Set.of(firstPredicate, addedPredicate), new And(firstPredicate, addedPredicate),
                    firstScan);
            SelectedPartitions first = fixture.prune(strongerFilter);
            SelectedPartitions second = fixture.prune(new LogicalFilter<>(
                    Set.of(secondPredicate), secondScan));

            Assertions.assertEquals(Set.of("p=2"), first.selectedPartitions.keySet());
            Assertions.assertSame(fixture.fullView.get("p=2"), first.selectedPartitions.get("p=2"));
            Assertions.assertSame(firstResult, first.getConnectorFilterResult().get());
            Assertions.assertSame(firstHandle, first.getConnectorFilterResult().get().getHandle());
            Assertions.assertSame(preload.getCoveredScanPredicate(firstRelation),
                    first.getCoveredConnectorFilter().get());
            Assertions.assertEquals(firstCovered, first.getCoveredConnectorFilter().get());
            Assertions.assertNotEquals(NereidsToConnectorExpressionConverter.convert(strongerFilter.getPredicate()),
                    first.getCoveredConnectorFilter().get());
            Assertions.assertEquals(Set.of(firstPredicate, addedPredicate), strongerFilter.getConjuncts());
            Assertions.assertEquals(SelectedPartitions.UNKNOWN_TOTAL_PARTITION_NUM, first.totalPartitionNum);
            Assertions.assertEquals(Set.of("p=3"), second.selectedPartitions.keySet());
            Assertions.assertSame(secondHandle, second.getConnectorFilterResult().get().getHandle());
            Assertions.assertEquals(secondCovered, second.getCoveredConnectorFilter().get());
            Mockito.verify(fixture.table, Mockito.times(1))
                    .applyPartitionFilterForScan(Mockito.any(), Mockito.eq(firstCovered));
            Mockito.verify(fixture.table, Mockito.times(1))
                    .applyPartitionFilterForScan(Mockito.any(), Mockito.eq(secondCovered));
            Mockito.verify(fixture.table, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        }
    }

    @Test
    public void testDeclinedPreloadedFilterUsesWarmedFullViewUnderInternalLock() throws AnalysisException {
        try (PruningFixture fixture = new PruningFixture()) {
            RelationId relation = fixture.statement.getNextRelationId();
            fixture.register(true);
            fixture.preloadInfo().registerScanPredicate(relation,
                    new EqualTo(new UnboundSlot("p"), new IntegerLiteral(2)));
            fixture.statement.preloadDeferredScanPartitionViewsBeforeLock(true);
            Assertions.assertTrue(fixture.preloadInfo().hasFilteredScanView(relation));
            Assertions.assertTrue(fixture.preloadInfo().getFilteredScanView(relation).isEmpty());
            Assertions.assertSame(fixture.fullView, fixture.preloadInfo().getScanPartitionView().get());
            fixture.lock();

            Expression predicate = new EqualTo(fixture.partitionSlot, new IntegerLiteral(2));
            SelectedPartitions selected = fixture.prune(new LogicalFilter<>(Set.of(predicate), fixture.scan(relation)));

            fixture.assertLocalSelection(selected, "p=2");
            Mockito.verify(fixture.table, Mockito.times(1)).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            Mockito.verify(fixture.table, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
        }
    }

    @Test
    public void testLateLogicalFilterUsesWarmedFullViewWithoutSelectiveRequest() throws AnalysisException {
        try (PruningFixture fixture = new PruningFixture()) {
            RelationId relation = fixture.statement.getNextRelationId();
            fixture.register(false);
            fixture.statement.preloadDeferredScanPartitionViewsBeforeLock(true);
            Assertions.assertFalse(fixture.preloadInfo().hasFilteredScanView(relation));
            Assertions.assertSame(fixture.fullView, fixture.preloadInfo().getScanPartitionView().get());
            fixture.lock();

            Expression latePredicate = new EqualTo(fixture.partitionSlot, new IntegerLiteral(3));
            SelectedPartitions selected = fixture.prune(new LogicalFilter<>(
                    Set.of(latePredicate), fixture.scan(relation)));

            fixture.assertLocalSelection(selected, "p=3");
            Mockito.verify(fixture.table, Mockito.never()).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            Mockito.verify(fixture.table, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
        }
    }

    private static ConnectorFilteredPartitionView filteredView(Map<String, PartitionItem> items,
            FilterApplicationResult<ConnectorTableHandle> result) {
        return Deencapsulation.newInstance(ConnectorFilteredPartitionView.class, items, result);
    }

    @Test
    public void testInferredExplainFilterWithoutWarmupDoesNotRequestMetadataUnderLock() throws Exception {
        try (PruningFixture fixture = new PruningFixture()) {
            fixture.register(false);
            fixture.statement.preloadDeferredScanPartitionViewsBeforeLock(false);
            Assertions.assertFalse(fixture.preloadInfo().hasScanPartitionView());
            fixture.lock();
            LogicalFileScan scan = fixture.scan(new RelationId(19));
            SelectedPartitions selected = fixture.prune(new LogicalFilter<>(
                    Set.of(new EqualTo(fixture.partitionSlot, new IntegerLiteral(1))), scan));
            Assertions.assertSame(scan.getSelectedPartitions(), selected);
            Mockito.verify(fixture.table, Mockito.never()).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            Mockito.verify(fixture.table, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        }
    }

    @Test
    public void testPlanOnlyLateFilterSkipIsIdempotentInTheActualTopDownJob() throws Exception {
        for (SelectedPartitions initial : List.of(
                SelectedPartitions.DEFERRED_PARTITION_PRUNING, SelectedPartitions.NOT_PRUNED)) {
            try (PruningFixture fixture = new PruningFixture()) {
                fixture.register(false);
                fixture.statement.preloadDeferredScanPartitionViewsBeforeLock(false);
                fixture.lock();
                LogicalFileScan scan = fixture.scan(new RelationId(19)).withSelectedPartitions(initial);
                Expression latePredicate = new EqualTo(fixture.partitionSlot, new IntegerLiteral(1));
                LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(Set.of(latePredicate), scan);
                CascadesContext cascades = CascadesContext.initContext(
                        fixture.statement, filter, PhysicalProperties.ANY);
                for (int pass = 0; pass < 2; pass++) {
                    Rewriter.getWholeTreeRewriterWithCustomJobs(cascades,
                            ImmutableList.of(Rewriter.topDown(new PruneFileScanPartition()))).execute();
                    LogicalFilter<?> rewritten = (LogicalFilter<?>) cascades.getRewritePlan();
                    LogicalFileScan rewrittenScan = (LogicalFileScan) rewritten.child();
                    Assertions.assertSame(initial, rewrittenScan.getSelectedPartitions());
                    Assertions.assertEquals(latePredicate, rewritten.getPredicate());
                    Assertions.assertFalse(rewrittenScan.getSelectedPartitions().isPruned);
                }
                Assertions.assertFalse(fixture.preloadInfo().hasScanPartitionView());
                Mockito.verify(fixture.table, Mockito.never()).applyPartitionFilterForScan(
                        Mockito.any(), Mockito.any());
                Mockito.verify(fixture.table, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
            }
        }
    }

    private static final class PruningFixture implements AutoCloseable {
        private final ConnectContext context = new ConnectContext();
        private final StatementContext statement = new StatementContext(context, null);
        private final PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        private final TableIf internalTable = Mockito.mock(TableIf.class);
        private final AtomicBoolean internalLockHeld = new AtomicBoolean();
        private final SlotReference partitionSlot = new SlotReference("p", IntegerType.INSTANCE, false);
        private final Map<String, PartitionItem> fullView = new LinkedHashMap<>();

        private PruningFixture() throws AnalysisException {
            context.setStatementContext(statement);
            context.getSessionVariable().setEnableMaterializedViewRewrite(false);
            context.getSessionVariable().setEnableDmlMaterializedViewRewrite(false);
            context.getSessionVariable().enableBinarySearchFilteringPartitions = false;
            context.setThreadLocalInfo();
            fullView.put("p=1", listItem(1));
            fullView.put("p=2", listItem(2));
            fullView.put("p=3", listItem(3));
            Mockito.when(table.getId()).thenReturn(22L);
            Mockito.when(table.supportsExternalMetadataPreload()).thenReturn(true);
            Mockito.when(table.supportsConnectorPartitionPruning()).thenReturn(true);
            Mockito.when(table.supportInternalPartitionPruned()).thenReturn(true);
            Mockito.when(table.supportInternalPartitionPruned(Mockito.any())).thenReturn(true);
            Mockito.when(table.getPartitionColumns(Mockito.any())).thenReturn(List.of(new Column("p", Type.INT)));
            Mockito.when(table.initSelectedPartitions(Mockito.any()))
                    .thenReturn(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
            Mockito.doAnswer(invocation -> {
                Assertions.assertFalse(internalLockHeld.get(), "full enumeration must precede lock()");
                return Optional.of(fullView);
            }).when(table).getNameToPartitionItemsForScan(Mockito.any());
            Mockito.doAnswer(invocation -> {
                Assertions.assertFalse(internalLockHeld.get(), "selective request must precede lock()");
                return Optional.empty();
            }).when(table).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            Mockito.when(internalTable.getId()).thenReturn(21L);
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
        }

        private void register(boolean filtered) {
            statement.registerExternalTableForPreload(table, Optional.empty(), Optional.empty(), filtered);
        }

        private ExternalTablePreloadInfo preloadInfo() {
            return statement.getExternalTablePreloadInfo(table.getId()).get();
        }

        private LogicalFileScan scan(RelationId relation) {
            return new LogicalFileScan(relation, table, List.of("hive", "db"), List.of(),
                    Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(List.of(partitionSlot)));
        }

        private void lock() {
            statement.lock();
            Assertions.assertTrue(internalLockHeld.get(), "exercise pruning inside the actual lock window");
            Mockito.verify(internalTable).tryReadLock(1, TimeUnit.MINUTES);
        }

        private SelectedPartitions prune(LogicalFilter<LogicalFileScan> filter) {
            CascadesContext cascades = CascadesContext.initContext(statement, filter, PhysicalProperties.ANY);
            return Deencapsulation.invoke(new PruneFileScanPartition(), "pruneExternalPartitions",
                    table, filter, filter.child(), cascades);
        }

        private void assertLocalSelection(SelectedPartitions selected, String name) {
            Assertions.assertEquals(Set.of(name), selected.selectedPartitions.keySet());
            Assertions.assertSame(fullView.get(name), selected.selectedPartitions.get(name));
            Assertions.assertEquals(fullView.size(), selected.totalPartitionNum);
            Assertions.assertTrue(selected.isPruned);
            Assertions.assertTrue(selected.getConnectorFilterResult().isEmpty());
            Assertions.assertTrue(selected.getCoveredConnectorFilter().isEmpty());
            Mockito.verify(table, Mockito.never()).getSortedPartitionRanges(Mockito.any());
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

    private static List<String> rangeNames(Optional<SortedPartitionRanges<String>> ranges) {
        return ranges.get().sortedPartitions.stream()
                .map(partition -> partition.id)
                .collect(Collectors.toList());
    }

    private static ListPartitionItem listItem(int value) throws AnalysisException {
        PartitionKey key = PartitionKey.createPartitionKey(
                List.of(new PartitionValue(String.valueOf(value))), List.of(new Column("p", Type.INT)));
        return new ListPartitionItem(List.of(key));
    }
}
