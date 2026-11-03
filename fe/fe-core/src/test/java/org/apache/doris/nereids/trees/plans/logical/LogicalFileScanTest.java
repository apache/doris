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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMvccSnapshot;
import org.apache.doris.datasource.iceberg.IcebergPartitionInfo;
import org.apache.doris.datasource.iceberg.IcebergSnapshot;
import org.apache.doris.datasource.iceberg.IcebergSnapshotCacheValue;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.rules.rewrite.QueryPartitionCollector;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class LogicalFileScanTest {

    @Test
    public void testSortedRangesStayWithImmutableSelectionGeneration() throws Exception {
        PartitionItem firstItem = integerPartition("1");
        PartitionItem secondItem = integerPartition("2");
        Map<String, PartitionItem> source = new HashMap<>();
        source.put("p=1", firstItem);
        SelectedPartitions first = new SelectedPartitions(1, source, false);
        SelectedPartitions equivalent = new SelectedPartitions(1, source, false);
        int originalHashCode = first.hashCode();
        Assertions.assertEquals(first, equivalent);
        Assertions.assertNotSame(first.selectedPartitions, equivalent.selectedPartitions);
        source.put("p=1", secondItem);
        SelectedPartitions nextGeneration = new SelectedPartitions(1, source, false);

        try (MockedStatic<SortedPartitionRanges> ranges =
                Mockito.mockStatic(SortedPartitionRanges.class, Mockito.CALLS_REAL_METHODS)) {
            ranges.verifyNoInteractions();
            SortedPartitionRanges<String> firstIndex = first.getSortedPartitionRanges().get();
            SortedPartitionRanges<String> equivalentIndex = equivalent.getSortedPartitionRanges().get();
            SortedPartitionRanges<String> nextIndex = nextGeneration.getSortedPartitionRanges().get();

            Assertions.assertSame(firstIndex, first.getSortedPartitionRanges().get());
            Assertions.assertNotSame(firstIndex, equivalentIndex);
            Assertions.assertNotSame(firstIndex, nextIndex);
            Assertions.assertSame(firstItem, firstIndex.sortedPartitions.get(0).partitionItem);
            Assertions.assertSame(secondItem, nextIndex.sortedPartitions.get(0).partitionItem);
            Assertions.assertEquals(first, equivalent);
            Assertions.assertEquals(originalHashCode, first.hashCode());
            Assertions.assertEquals(first.hashCode(), equivalent.hashCode());
            Assertions.assertEquals(SelectedPartitions.State.MATERIALIZED, first.state);
            ranges.verify(() -> SortedPartitionRanges.build(Mockito.same(first.selectedPartitions)), Mockito.times(1));
            ranges.verify(() -> SortedPartitionRanges.build(Mockito.same(equivalent.selectedPartitions)),
                    Mockito.times(1));
            ranges.verify(() -> SortedPartitionRanges.build(Mockito.same(nextGeneration.selectedPartitions)),
                    Mockito.times(1));
            ranges.verifyNoMoreInteractions();
        }
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> first.selectedPartitions.put("p=2", secondItem));
    }

    @Test
    public void testSelectionCopiesShareLazySortedRanges() throws Exception {
        SelectedPartitions view = new SelectedPartitions(1, ImmutableMap.of("p=1", integerPartition("1")), false);
        HMSExternalTable table = newDeferredHiveTable();
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(view);
        StatementContext statement = newStatementContext();
        try (MockedStatic<ConnectContext> current = Mockito.mockStatic(ConnectContext.class);
                MockedStatic<SortedPartitionRanges> ranges =
                        Mockito.mockStatic(SortedPartitionRanges.class, Mockito.CALLS_REAL_METHODS)) {
            current.when(ConnectContext::get).thenReturn(statement.getConnectContext());
            LogicalFileScan scan = newHiveScan(table);
            List<LogicalFileScan> copies = Arrays.asList(
                    scan.withRelationId(new RelationId(13)),
                    scan.withTableAlias("alias"),
                    scan.withOperativeSlots(Collections.emptyList()),
                    scan.withCachedOutput(scan.getOutput()),
                    scan.withGroupExpression(Optional.empty()));
            ranges.verifyNoInteractions();
            SortedPartitionRanges<String> index = view.getSortedPartitionRanges().get();
            for (LogicalFileScan copy : copies) {
                Assertions.assertSame(view, copy.getSelectedPartitions());
                Assertions.assertSame(index, copy.getSelectedPartitions().getSortedPartitionRanges().get());
                Assertions.assertFalse(copy.getSelectedPartitions().isPruned);
            }
            ranges.verify(() -> SortedPartitionRanges.build(Mockito.same(view.selectedPartitions)), Mockito.times(1));
            ranges.verifyNoMoreInteractions();
        }
    }

    private ListPartitionItem integerPartition(String value) throws Exception {
        return new ListPartitionItem(Collections.singletonList(PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue(value)), Collections.singletonList(Type.INT), true)));
    }

    @Test
    public void testPreloadedEmptyHiveViewDoesNotBecomeDeferredAgain() {
        HMSExternalTable table = newDeferredHiveTable();
        StatementContext statement = new StatementContext();
        SelectedPartitions emptyView = new SelectedPartitions(0, ImmutableMap.of(), false);
        Mockito.when(table.preloadPartitionView(Mockito.any())).thenReturn(emptyView);
        statement.preloadExternalPartitionView(table, Optional.empty());
        ConnectContext connect = Mockito.mock(ConnectContext.class);
        Mockito.when(connect.getStatementContext()).thenReturn(statement);
        Mockito.when(connect.getSessionVariable()).thenReturn(new SessionVariable());
        try (MockedStatic<ConnectContext> current = Mockito.mockStatic(ConnectContext.class)) {
            current.when(ConnectContext::get).thenReturn(connect);
            LogicalFileScan scan = newHiveScan(table);
            Assertions.assertSame(emptyView, scan.getSelectedPartitions());
            Assertions.assertFalse(scan.getSelectedPartitions().isDeferredPartitionPruning());
            Mockito.verify(table, Mockito.never()).initSelectedPartitions(Mockito.any());
            Mockito.verify(table, Mockito.never()).getNameToPartitionItems(Mockito.any());
        }
    }

    @Test
    public void testInsertHiveSourceKeepsCompleteViewInsteadOfLiveFilter() {
        HMSExternalTable table = newDeferredHiveTable();
        Mockito.when(table.getDlaType()).thenReturn(HMSExternalTable.DLAType.HIVE);
        SelectedPartitions view = new SelectedPartitions(1,
                ImmutableMap.of("p=x", Mockito.mock(PartitionItem.class)), false);
        Mockito.when(table.preloadPartitionView(Mockito.any())).thenReturn(view);
        StatementContext statement = new StatementContext();
        statement.setIsInsert(true);
        ConnectContext connect = Mockito.mock(ConnectContext.class);
        Mockito.when(connect.getStatementContext()).thenReturn(statement);
        Mockito.when(connect.getSessionVariable()).thenReturn(new SessionVariable());
        try (MockedStatic<ConnectContext> current = Mockito.mockStatic(ConnectContext.class)) {
            current.when(ConnectContext::get).thenReturn(connect);
            Assertions.assertSame(view, newHiveScan(table).getSelectedPartitions());
            Mockito.verify(table).preloadPartitionView(Mockito.any());
            Mockito.verify(table, Mockito.never()).getNameToPartitionItemsByFilter(Mockito.any(), Mockito.any());
        }
    }

    @Test
    public void testDeferredHiveMvCollectorAndPhysicalScanShareCompleteView() {
        HMSExternalTable table = newDeferredHiveTable();
        Mockito.when(table.supportInternalPartitionPruned()).thenReturn(true);
        List<String> qualifiers = Arrays.asList("hms", "db", "hive_tbl");
        Mockito.when(table.getFullQualifiers()).thenReturn(qualifiers);
        SelectedPartitions completeView = new SelectedPartitions(1,
                ImmutableMap.of("p=x", Mockito.mock(PartitionItem.class)), false);
        Mockito.when(table.preloadPartitionView(Mockito.any())).thenReturn(completeView);
        LogicalFileScan scan = newHiveScan(table);
        StatementContext statement = newStatementContext();
        CascadesContext cascades = CascadesContext.initContext(statement, scan, PhysicalProperties.ANY);
        scan.accept(new QueryPartitionCollector(), cascades);
        Assertions.assertEquals(Collections.singleton("p=x"),
                statement.getTableUsedPartitionNameMap().get(qualifiers).iterator().next().second);
        Assertions.assertSame(completeView, statement.getExternalPartitionView(table, Optional.empty()).get());

        PhysicalFileScan physical = Mockito.mock(PhysicalFileScan.class);
        Mockito.when(physical.getSelectedPartitions()).thenReturn(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        Mockito.when(physical.getRelationSnapshot()).thenReturn(Optional.empty());
        PlanTranslatorContext translatorContext = Mockito.mock(PlanTranslatorContext.class);
        Mockito.when(translatorContext.getStatementContext()).thenReturn(statement);
        SelectedPartitions translated = Deencapsulation.invoke(new PhysicalPlanTranslator(),
                "materializeDeferredHivePartitions", table, physical, translatorContext);
        Assertions.assertSame(completeView, translated);
        Mockito.verify(table, Mockito.times(1)).preloadPartitionView(Mockito.any());
    }

    @Test
    public void testDeferredEmptyHiveMvCollectorRecordsMaterializedEmptyView() {
        HMSExternalTable table = newDeferredHiveTable();
        Mockito.when(table.supportInternalPartitionPruned()).thenReturn(true);
        List<String> qualifiers = Arrays.asList("hms", "db", "hive_tbl");
        Mockito.when(table.getFullQualifiers()).thenReturn(qualifiers);
        SelectedPartitions completeView = new SelectedPartitions(0, ImmutableMap.of(), false);
        Mockito.when(table.preloadPartitionView(Mockito.any())).thenReturn(completeView);
        LogicalFileScan scan = newHiveScan(table);
        StatementContext statement = newStatementContext();
        scan.accept(new QueryPartitionCollector(),
                CascadesContext.initContext(statement, scan, PhysicalProperties.ANY));
        Assertions.assertTrue(statement.getTableUsedPartitionNameMap().get(qualifiers)
                .iterator().next().second.isEmpty());
        Assertions.assertSame(completeView, statement.getExternalPartitionView(table, Optional.empty()).get());
        Mockito.verify(table).preloadPartitionView(Mockito.any());
    }

    private HMSExternalTable newDeferredHiveTable() {
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getName()).thenReturn("hms");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.initSelectedPartitions(Mockito.any()))
                .thenReturn(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        Mockito.when(table.getFullSchema(Mockito.any()))
                .thenReturn(Collections.singletonList(new Column("p", Type.STRING, true)));
        Mockito.when(table.getPartitionColumns(Mockito.any()))
                .thenReturn(Collections.singletonList(new Column("p", Type.STRING, true)));
        Mockito.when(table.getName()).thenReturn("hive_tbl");
        return table;
    }

    private StatementContext newStatementContext() {
        ConnectContext connect = Mockito.mock(ConnectContext.class);
        Mockito.when(connect.getSessionVariable()).thenReturn(new SessionVariable());
        StatementContext statement = new StatementContext(connect, null);
        Mockito.when(connect.getStatementContext()).thenReturn(statement);
        return statement;
    }

    private LogicalFileScan newHiveScan(HMSExternalTable table) {
        return new LogicalFileScan(new RelationId(12), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test
    public void testDeferredSelectedPartitionsState() {
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any()))
                .thenReturn(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        Mockito.when(table.getFullSchema(Mockito.any()))
                .thenReturn(Collections.singletonList(new Column("id", Type.INT, true)));
        Mockito.when(table.getName()).thenReturn("hive_tbl");

        LogicalFileScan scan = new LogicalFileScan(new RelationId(12), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        Assertions.assertSame(SelectedPartitions.DEFERRED_PARTITION_PRUNING, scan.getSelectedPartitions());
        Assertions.assertTrue(scan.getSelectedPartitions().isDeferredPartitionPruning());
        Assertions.assertFalse(scan.getSelectedPartitions().isNotPruned());
    }

    @Test
    public void testComputeOutputIncludesInvisibleRowLineageColumnsForIcebergTable() {
        Column rowIdColumn = new Column(IcebergUtils.ICEBERG_ROW_ID_COL, Type.BIGINT, true);
        rowIdColumn.setIsVisible(false);
        Column lastUpdatedSequenceNumberColumn =
                new Column(IcebergUtils.ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL, Type.BIGINT, true);
        lastUpdatedSequenceNumberColumn.setIsVisible(false);
        List<Column> schema = Arrays.asList(
                new Column("id", Type.INT, true),
                rowIdColumn,
                lastUpdatedSequenceNumberColumn);

        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema(Mockito.any())).thenReturn(schema);
        Mockito.when(table.getName()).thenReturn("iceberg_tbl");

        LogicalFileScan scan = new LogicalFileScan(new RelationId(1), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        List<String> outputNames = scan.computeOutput().stream().map(slot -> slot.getName())
                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList(
                "id",
                IcebergUtils.ICEBERG_ROW_ID_COL,
                IcebergUtils.ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL), outputNames);
    }

    @Test
    public void testPaimonOptionsBindRelationScopedSnapshotSchema() {
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getName()).thenReturn("paimon_tbl");
        Map<String, String> options = Collections.singletonMap("scan.snapshot-id", "1");
        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS, options, Collections.emptyList());
        Mockito.when(table.getFullSchema(scanParams)).thenReturn(Arrays.asList(
                new Column("id", Type.INT, true),
                new Column("old_name", Type.STRING, true)));

        LogicalFileScan scan = new LogicalFileScan(new RelationId(1), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.of(scanParams), Optional.empty());

        Assertions.assertSame(SelectedPartitions.NOT_PRUNED, scan.getSelectedPartitions());
        Assertions.assertEquals(Arrays.asList("id", "old_name"),
                scan.computeOutput().stream().map(slot -> slot.getName()).collect(Collectors.toList()));
        Mockito.verify(table, Mockito.never()).initSelectedPartitions(Mockito.any());
    }

    @Test
    public void testPaimonSupportsNestedColumnPruning() {
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getName()).thenReturn("paimon_tbl");
        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS,
                Collections.singletonMap("scan.snapshot-id", "1"),
                Collections.emptyList());
        Mockito.when(table.getFullSchema(scanParams)).thenReturn(Collections.emptyList());

        LogicalFileScan scan = new LogicalFileScan(new RelationId(2), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.of(scanParams), Optional.empty());

        Assertions.assertTrue(scan.supportPruneNestedColumn());
    }

    @Test
    public void testCapturingRelationSchemaDoesNotAllocateOutputExprIds() throws Exception {
        StatementScopeIdGenerator.clear();
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema(Mockito.any()))
                .thenReturn(Collections.singletonList(new Column("id", Type.INT, true)));
        Mockito.when(table.getName()).thenReturn("iceberg_tbl");

        new LogicalFileScan(new RelationId(1), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        Assertions.assertEquals(new ExprId(10000), StatementScopeIdGenerator.newExprId());
    }

    @Test
    public void testHmsIcebergCapturesRelationSnapshotSchema() {
        Column historicalColumn = new Column("old_name", Type.INT, true);
        Column latestColumn = new Column("new_name", Type.INT, true);
        MvccSnapshot historicalSnapshot = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot latestSnapshot = Mockito.mock(MvccSnapshot.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema(Optional.of(historicalSnapshot)))
                .thenReturn(Collections.singletonList(historicalColumn));
        Mockito.when(table.getFullSchema(Optional.of(latestSnapshot)))
                .thenReturn(Collections.singletonList(latestColumn));
        Mockito.when(table.getName()).thenReturn("hms_iceberg_tbl");

        LogicalFileScan historicalFirst = new LogicalFileScan(new RelationId(3), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(historicalSnapshot));
        LogicalFileScan latestSecond = new LogicalFileScan(new RelationId(4), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(latestSnapshot));
        LogicalFileScan latestFirst = new LogicalFileScan(new RelationId(5), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(latestSnapshot));
        LogicalFileScan historicalSecond = new LogicalFileScan(new RelationId(6), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(historicalSnapshot));

        Assertions.assertEquals(Collections.singletonList("old_name"),
                historicalFirst.computeOutput().stream().map(slot -> slot.getName()).collect(Collectors.toList()));
        Assertions.assertEquals(Collections.singletonList("new_name"),
                latestSecond.computeOutput().stream().map(slot -> slot.getName()).collect(Collectors.toList()));
        Assertions.assertEquals(Collections.singletonList("new_name"),
                latestFirst.computeOutput().stream().map(slot -> slot.getName()).collect(Collectors.toList()));
        Assertions.assertEquals(Collections.singletonList("old_name"),
                historicalSecond.computeOutput().stream().map(slot -> slot.getName()).collect(Collectors.toList()));
        Mockito.verify(table, Mockito.times(2)).getFullSchema(Optional.of(historicalSnapshot));
        Mockito.verify(table, Mockito.times(2)).getFullSchema(Optional.of(latestSnapshot));
    }

    @Test
    public void testResolvedSnapshotParticipatesInScanEquality() {
        MvccSnapshot firstSnapshot = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot secondSnapshot = Mockito.mock(MvccSnapshot.class);
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema(Mockito.any()))
                .thenReturn(Collections.singletonList(new Column("id", Type.INT, true)));
        Mockito.when(table.getName()).thenReturn("moving_branch_table");
        TableScanParams branch = new TableScanParams(TableScanParams.BRANCH,
                Collections.singletonMap(TableScanParams.PARAMS_NAME, "moving"), Collections.emptyList());

        LogicalFileScan first = new LogicalFileScan(new RelationId(7), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.of(branch), Optional.empty(),
                Optional.of(firstSnapshot));
        LogicalFileScan second = new LogicalFileScan(new RelationId(8), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.of(branch), Optional.empty(),
                Optional.of(secondSnapshot));

        Assertions.assertFalse(first.hasSameScanState(second));
    }

    @Test
    public void testEquivalentResolvedSnapshotsKeepScanEquality() {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema(Mockito.any()))
                .thenReturn(Collections.singletonList(new Column("id", Type.INT, true)));
        Mockito.when(table.getName()).thenReturn("fixed_snapshot_table");
        MvccSnapshot firstSnapshot = icebergSnapshot(10L, 3L);
        MvccSnapshot secondSnapshot = icebergSnapshot(10L, 3L);

        LogicalFileScan first = new LogicalFileScan(new RelationId(10), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(firstSnapshot));
        LogicalFileScan second = new LogicalFileScan(new RelationId(11), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(secondSnapshot));

        Assertions.assertTrue(first.hasSameScanState(second));
    }

    @Test
    public void testDifferentFrozenNameMappingsBreakScanEquality() {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema(Mockito.any()))
                .thenReturn(Collections.singletonList(new Column("id", Type.INT, true)));
        Mockito.when(table.getName()).thenReturn("legacy_name_mapping_table");
        MvccSnapshot firstSnapshot = icebergSnapshot(10L, 3L,
                Collections.singletonMap(1, Collections.singletonList("legacy_a")));
        MvccSnapshot secondSnapshot = icebergSnapshot(10L, 3L,
                Collections.singletonMap(1, Collections.singletonList("legacy_b")));

        LogicalFileScan first = new LogicalFileScan(new RelationId(12), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(firstSnapshot));
        LogicalFileScan second = new LogicalFileScan(new RelationId(13), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(secondSnapshot));

        Assertions.assertFalse(first.hasSameScanState(second));
    }

    private static MvccSnapshot icebergSnapshot(long snapshotId, long schemaId) {
        return new IcebergMvccSnapshot(new IcebergSnapshotCacheValue(
                new IcebergPartitionInfo(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                new IcebergSnapshot(snapshotId, schemaId)));
    }

    private static MvccSnapshot icebergSnapshot(long snapshotId, long schemaId,
            Map<Integer, List<String>> nameMapping) {
        return new IcebergMvccSnapshot(new IcebergSnapshotCacheValue(
                new IcebergPartitionInfo(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                new IcebergSnapshot(snapshotId, schemaId), Optional.of(nameMapping)));
    }

    @Test
    public void testHudiCopiesKeepRelationSnapshotAndSchema() {
        MvccSnapshot historicalSnapshot = Mockito.mock(MvccSnapshot.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Optional.of(historicalSnapshot)))
                .thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema(Optional.of(historicalSnapshot)))
                .thenReturn(Collections.singletonList(new Column("old_name", Type.INT, true)));
        Mockito.when(table.getName()).thenReturn("hudi_table");

        LogicalHudiScan scan = new LogicalHudiScan(new RelationId(9), table,
                Collections.singletonList("db"), Collections.emptyList(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(historicalSnapshot));
        LogicalHudiScan copy = scan.withCachedOutput(scan.computeOutput());

        Assertions.assertSame(historicalSnapshot, copy.getRelationSnapshot().orElseThrow(AssertionError::new));
        Assertions.assertEquals(Collections.singletonList("old_name"),
                copy.computeOutput().stream().map(Slot::getName).collect(Collectors.toList()));
    }
}
