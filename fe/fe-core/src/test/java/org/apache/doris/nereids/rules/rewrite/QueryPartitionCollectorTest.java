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

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.PartitionCompensator;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.AbstractSet;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Tests {@link QueryPartitionCollector}'s MV partition accounting for a plugin-driven file scan.
 *
 * <p>WHY: this visitor runs from {@code afterRewrite}, i.e. while the statement holds read locks on every
 * internal table it touched, so every connector call it makes extends that lock window. A {@code DEFERRED}
 * (not yet enumerated) connector-pruning scan must therefore consume the view the pre-lock preload pass
 * materialized, and must never fall back to a live enumeration when such a view exists. The second contract
 * pinned here is state accounting: {@code NOT_PRUNED} is the "partitioning never got enumerated" sentinel, not
 * a concrete zero-partition selection, so it must be reported as scan-all rather than as "query no partitions"
 * (which makes the compensator reject an otherwise eligible MV rewrite).</p>
 */
public class QueryPartitionCollectorTest {

    private static final List<String> QUALIFIERS = ImmutableList.of("ctl", "db", "hive_tbl");
    private static final long TABLE_ID = 42L;
    private final StatementContext statement = new StatementContext();
    private final CascadesContext context = Mockito.mock(CascadesContext.class);
    private final ExternalTable table = Mockito.mock(ExternalTable.class);
    private final QueryPartitionCollector collector = new QueryPartitionCollector();

    @Test
    public void deferredScanReusesThePreloadedViewWithoutConnectorIo() {
        PluginDrivenExternalTable table = table(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        StatementContext statementContext = statementContext();
        try {
            registerPreloadedView(statementContext, table, Optional.of(partitions("p1", "p2")));

            collect(scan(table, Optional.empty()), statementContext);

            assertRecordedPartitions(statementContext, "p1", "p2");
            // MUTATION: re-enumerating through the connector here puts a full HMS round-trip back under the
            // reader's locks, which is exactly the hazard the preload hand-off exists to remove.
            Mockito.verify(table, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void defaultConfigurationMaterializesTheViewBeforeTheLock() {
        // No session variable is touched: with the opt-in preload switch OFF, the pre-lock step NereidsPlanner
        // runs just before lock() must still materialize the view, so the collector needs no connector I/O.
        PluginDrivenExternalTable table = table(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        Mockito.when(table.supportsConnectorPartitionPruning()).thenReturn(true);
        Mockito.when(table.getNameToPartitionItemsForScan(Mockito.any()))
                .thenReturn(Optional.of(partitions("p1", "p2")));
        StatementContext statementContext = statementContextWithInternalReadLock();
        try {
            Assertions.assertFalse(statementContext.getConnectContext().getSessionVariable()
                    .isEnablePreloadExternalMetadata(), "this test must stay on the default preload switch");
            statementContext.registerExternalTableForPreload(table, Optional.empty(), Optional.empty());

            statementContext.preloadDeferredScanPartitionViewsBeforeLock(false);
            // Everything after this point runs while the statement holds its internal table read locks.
            Mockito.clearInvocations(table);

            collect(scan(table, Optional.empty()), statementContext);

            assertRecordedPartitions(statementContext, "p1", "p2");
            Mockito.verify(table, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void deferredScanRecordsScanAllWhenThePreloadedViewIsUnavailable() {
        PluginDrivenExternalTable table = table(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        StatementContext statementContext = statementContext();
        try {
            // An unrepresentable connector partition makes the whole view unavailable; every scan-path
            // consumer must read that as "scan everything", never as "query nothing".
            registerPreloadedView(statementContext, table, Optional.empty());

            collect(scan(table, Optional.empty()), statementContext);

            assertScanAll(statementContext);
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void deferredScanWithoutPreloadStillEnumerates() {
        PluginDrivenExternalTable table = table(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        Mockito.when(table.getNameToPartitionItemsForScan(Mockito.any()))
                .thenReturn(Optional.of(partitions("p1")));
        StatementContext statementContext = statementContext();
        try {
            collect(scan(table, Optional.empty()), statementContext);

            assertRecordedPartitions(statementContext, "p1");
            // The enumeration performed here is recorded, so the physical scan reads the SAME generation the
            // compensation decision was made from instead of enumerating its own.
            Assertions.assertTrue(
                    statementContext.getExternalTablePreloadInfo(TABLE_ID).get().hasScanPartitionView());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void versionedReferenceDoesNotConsumeTheLatestPreload() {
        PluginDrivenExternalTable table = table(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        Mockito.when(table.getNameToPartitionItemsForScan(Mockito.any()))
                .thenReturn(Optional.of(partitions("asOf")));
        StatementContext statementContext = statementContext();
        try {
            registerPreloadedView(statementContext, table, Optional.of(partitions("latest")));

            collect(scan(table, Optional.of(new TableSnapshot("2024-01-01 00:00:00",
                    TableSnapshot.VersionType.TIME))), statementContext);

            // The preload pass warms the LATEST generation only, so a FOR-TIME reference must resolve its own.
            assertRecordedPartitions(statementContext, "asOf");
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void notPrunedScanRecordsScanAll() {
        PluginDrivenExternalTable table = table(SelectedPartitions.NOT_PRUNED);
        StatementContext statementContext = statementContext();
        try {
            collect(scan(table, Optional.empty()), statementContext);

            // MUTATION: recording the sentinel's empty map instead makes the compensator read this as "the
            // query reads no partitions" and reject an otherwise eligible MV rewrite.
            assertScanAll(statementContext);
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void materializedEmptySelectionStaysAGenuineZeroPartitionSelection() {
        PluginDrivenExternalTable table = table(new SelectedPartitions(0, ImmutableMap.of(), true));
        StatementContext statementContext = statementContext();
        try {
            collect(scan(table, Optional.empty()), statementContext);

            // A genuine prune-to-zero over a NON-empty universe must stay concrete: widening it to scan-all
            // would union-compensate partitions the query provably never reads.
            Collection<Pair<RelationId, Set<String>>> recorded = recorded(statementContext);
            Assertions.assertEquals(1, recorded.size());
            Assertions.assertTrue(recorded.iterator().next().value().isEmpty());
            Assertions.assertFalse(recorded.contains(PartitionCompensator.ALL_PARTITIONS));
        } finally {
            statementContext.close();
        }
    }

    private static void collect(LogicalFileScan scan, StatementContext statementContext) {
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        Mockito.when(cascadesContext.getStatementContext()).thenReturn(statementContext);
        MaterializedViewUtils.collectTableUsedPartitions(scan, cascadesContext);
    }

    private static void registerPreloadedView(StatementContext statementContext, PluginDrivenExternalTable table,
            Optional<Map<String, PartitionItem>> view) {
        statementContext.registerExternalTableForPreload(table, Optional.empty(), Optional.empty());
        statementContext.getExternalTablePreloadInfo(TABLE_ID).get().setScanPartitionView(view);
    }

    private static Collection<Pair<RelationId, Set<String>>> recorded(StatementContext statementContext) {
        Multimap<List<String>, Pair<RelationId, Set<String>>> used = statementContext
                .getTableUsedPartitionNameMap();
        return used.get(QUALIFIERS);
    }

    private static void assertRecordedPartitions(StatementContext statementContext, String... names) {
        Collection<Pair<RelationId, Set<String>>> recorded = recorded(statementContext);
        Assertions.assertEquals(1, recorded.size());
        Assertions.assertEquals(Sets.newHashSet(names), recorded.iterator().next().value());
    }

    private static void assertScanAll(StatementContext statementContext) {
        Assertions.assertTrue(recorded(statementContext).contains(PartitionCompensator.ALL_PARTITIONS));
    }

    private static Map<String, PartitionItem> partitions(String... names) {
        ImmutableMap.Builder<String, PartitionItem> builder = ImmutableMap.builder();
        for (String name : names) {
            builder.put(name, Mockito.mock(PartitionItem.class));
        }
        return builder.build();
    }

    private static LogicalFileScan scan(PluginDrivenExternalTable table,
            Optional<TableSnapshot> tableSnapshot) {
        return new LogicalFileScan(new RelationId(1), table, QUALIFIERS, Collections.emptyList(),
                Optional.empty(), tableSnapshot, Optional.empty(), Optional.empty());
    }

    private LogicalFileScan scan(int id, String... names) {
        Mockito.when(context.getStatementContext()).thenReturn(statement);
        Mockito.when(table.getDatabase()).thenReturn(Mockito.mock(DatabaseIf.class));
        Mockito.when(table.getFullQualifiers()).thenReturn(QUALIFIERS);
        Mockito.when(table.supportInternalPartitionPruned()).thenReturn(true);
        ImmutableMap.Builder<String, PartitionItem> items = ImmutableMap.builder();
        for (String name : names) {
            items.put(name, Mockito.mock(PartitionItem.class));
        }
        LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getRelationId()).thenReturn(new RelationId(id));
        Mockito.when(scan.getSelectedPartitions()).thenReturn(new SelectedPartitions(10, items.build(), true));
        return scan;
    }

    private static PluginDrivenExternalTable table(SelectedPartitions selectedPartitions) {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
        Mockito.when(table.getId()).thenReturn(TABLE_ID);
        Mockito.when(table.getName()).thenReturn("hive_tbl");
        Mockito.when(table.getDatabase()).thenReturn((DatabaseIf) database);
        Mockito.when(table.getFullQualifiers()).thenReturn(QUALIFIERS);
        Mockito.when(table.supportInternalPartitionPruned()).thenReturn(true);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(selectedPartitions);
        Mockito.when(table.getFullSchema(Mockito.any())).thenReturn(Collections.emptyList());
        Mockito.when(table.supportsExternalMetadataPreload()).thenReturn(true);
        return table;
    }

    private static StatementContext statementContext() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        return new StatementContext(connectContext, new OriginStatement("select 1", 0));
    }

    /** A statement that mixes an internal table needing a plan-time read lock with the external table. */
    private static StatementContext statementContextWithInternalReadLock() {
        StatementContext statementContext = statementContext();
        TableIf internalTable = Mockito.mock(TableIf.class);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
        return statementContext;
    }

    @Test
    void nativePlanningRecordsUnknownAsAllPartitionsWithoutLoadingTheView() {
        LogicalFileScan scan = scan(1);
        Mockito.when(scan.getSelectedPartitions()).thenReturn(SelectedPartitions.NATIVE);
        Mockito.clearInvocations(table);
        collector.visitLogicalCatalogRelation(scan, context);
        Assertions.assertTrue(PartitionCompensator.getQueryUsedPartitions(statement, new BitSet())
                .containsKey(QUALIFIERS));
        Assertions.assertSame(PartitionCompensator.ALL_PARTITIONS,
                statement.getTableUsedPartitionNameMap().get(QUALIFIERS).iterator().next());
        Mockito.verify(table, Mockito.never()).supportInternalPartitionPruned();
    }

    @Test
    void keepsTheSelectedSnapshotKeyView() {
        LogicalFileScan first = scan(1, "p1", "p2");
        collector.visitLogicalCatalogRelation(first, context);
        Set<String> recorded = statement.getTableUsedPartitionNameMap().get(QUALIFIERS).iterator().next().value();
        Assertions.assertSame(first.getSelectedPartitions().selectedPartitions.keySet(), recorded);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> recorded.remove("p1"));
        collector.visitLogicalCatalogRelation(scan(2, "p3"), context);
        Assertions.assertEquals(Set.of("p1", "p2"), recorded);
    }

    @Test
    void selfJoinAndRepeatedRecordsPreserveUnionAndRelationFiltering() {
        LogicalFileScan first = scan(1, "p1", "p2");
        collector.visitLogicalCatalogRelation(first, context);
        collector.visitLogicalCatalogRelation(scan(2, "p2", "p3"), context);
        collector.visitLogicalCatalogRelation(first, context);
        Map<List<String>, Set<String>> union = PartitionCompensator.getQueryUsedPartitions(statement, new BitSet());
        Assertions.assertEquals(Set.of("p1", "p2", "p3"), union.get(QUALIFIERS));
        BitSet relation = new BitSet();
        relation.set(1);
        Assertions.assertEquals(Set.of("p1", "p2"),
                PartitionCompensator.getQueryUsedPartitions(statement, relation).get(QUALIFIERS));
        union.get(QUALIFIERS).clear();
        Assertions.assertEquals(Set.of("p1", "p2"), first.getSelectedPartitions().selectedPartitions.keySet());
    }

    @Test
    void recordingDoesNotVisitOrHashPartitionNames() {
        Set<String> names = new AbstractSet<>() {
            @Override
            public Iterator<String> iterator() {
                throw new AssertionError("recording must not iterate partition names");
            }

            @Override
            public int size() {
                return 120000;
            }

            @Override
            public int hashCode() {
                throw new AssertionError("recording must not hash partition names");
            }
        };
        statement.getTableUsedPartitionNameMap().put(QUALIFIERS, Pair.of(new RelationId(1), names));
        Assertions.assertSame(names,
                statement.getTableUsedPartitionNameMap().get(QUALIFIERS).iterator().next().value());
    }

    @Test
    void emptySelectionAndAllPartitionsKeepTheirDistinctMeaning() {
        collector.visitLogicalCatalogRelation(scan(1), context);
        Assertions.assertEquals(Collections.emptySet(),
                PartitionCompensator.getQueryUsedPartitions(statement, new BitSet()).get(QUALIFIERS));
        statement.getTableUsedPartitionNameMap().put(QUALIFIERS, PartitionCompensator.ALL_PARTITIONS);
        statement.getTableUsedPartitionNameMap().put(QUALIFIERS, PartitionCompensator.ALL_PARTITIONS);
        Assertions.assertNull(PartitionCompensator.getQueryUsedPartitions(statement, new BitSet()).get(QUALIFIERS));
    }
}
