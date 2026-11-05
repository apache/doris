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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HivePartitionFilterBuilder;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PruneFileScanPartitionTest {
    @Test
    public void testRepruningPreservesPartitionPredicateAndOriginalTotal() throws Exception {
        for (long totalPartitionNum : Arrays.asList(5L, SelectedPartitions.UNKNOWN_TOTAL_PARTITION_NUM)) {
            SelectedPartitions view = new SelectedPartitions(totalPartitionNum, ImmutableMap.of(
                    "p=1", integerPartition("1"), "p=2", integerPartition("2")), true, true);
            HMSExternalTable table = partitionedHiveTable(view);
            StatementContext statement = statementContext();
            try (MockedStatic<ConnectContext> current = Mockito.mockStatic(ConnectContext.class);
                    MockedStatic<SortedPartitionRanges> ranges =
                            Mockito.mockStatic(SortedPartitionRanges.class, Mockito.CALLS_REAL_METHODS)) {
                current.when(ConnectContext::get).thenReturn(statement.getConnectContext());
                LogicalFileScan scan = hiveScan(table, 1).withPartitionPruned(false);
                SelectedPartitions dataOnlyResult = pruneFilter(new LogicalFilter<>(Collections.singleton(
                        new GreaterThan(scan.getOutput().get(1), new IntegerLiteral(0))), scan), statement);

                Assertions.assertEquals(view.selectedPartitions, dataOnlyResult.selectedPartitions);
                Assertions.assertEquals(totalPartitionNum, dataOnlyResult.totalPartitionNum);
                Assertions.assertTrue(dataOnlyResult.hasPartitionPredicate);
                ranges.verifyNoInteractions();

                LogicalFileScan reprunedScan = scan.withSelectedPartitions(dataOnlyResult).withPartitionPruned(false);
                SelectedPartitions partitionResult = pruneFilter(new LogicalFilter<>(Collections.singleton(
                        new EqualTo(reprunedScan.getOutput().get(0), new IntegerLiteral(1))), reprunedScan), statement);

                Assertions.assertEquals(Collections.singleton("p=1"), partitionResult.selectedPartitions.keySet());
                Assertions.assertEquals(totalPartitionNum, partitionResult.totalPartitionNum);
                Assertions.assertTrue(partitionResult.hasPartitionPredicate);
                ranges.verify(() -> SortedPartitionRanges.build(Mockito.anyMap()), Mockito.times(1));
                Mockito.verify(table, Mockito.never()).getNameToPartitionItemsByFilter(Mockito.any(), Mockito.any());
                Mockito.verify(table, Mockito.never()).preloadPartitionView(Mockito.any());
                Mockito.verify(table, Mockito.never()).getSortedPartitionRanges(Mockito.any());
            }
        }
    }

    @Test
    public void testDataOnlyPredicateDoesNotBuildSortedRanges() throws Exception {
        SelectedPartitions view = new SelectedPartitions(2, ImmutableMap.of(
                "p=1", integerPartition("1"), "p=2", integerPartition("2")), false);
        HMSExternalTable table = partitionedHiveTable(view);
        StatementContext statement = statementContext();
        try (MockedStatic<ConnectContext> current = Mockito.mockStatic(ConnectContext.class);
                MockedStatic<SortedPartitionRanges> ranges =
                        Mockito.mockStatic(SortedPartitionRanges.class, Mockito.CALLS_REAL_METHODS)) {
            current.when(ConnectContext::get).thenReturn(statement.getConnectContext());
            LogicalFileScan scan = hiveScan(table, 1);
            LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(Collections.singleton(
                    new GreaterThan(scan.getOutput().get(1), new IntegerLiteral(0))), scan);

            SelectedPartitions result = pruneFilter(filter, statement);

            Assertions.assertEquals(view.selectedPartitions, result.selectedPartitions);
            Assertions.assertFalse(result.hasPartitionPredicate);
            ranges.verifyNoInteractions();
            Mockito.verify(table, Mockito.never()).getNameToPartitionItemsByFilter(Mockito.any(), Mockito.any());
            Mockito.verify(table, Mockito.never()).getNameToPartitionItems(Mockito.any());
        }
    }

    @Test
    public void testCompleteViewAliasesReuseSortedRanges() throws Exception {
        Map<String, PartitionItem> partitions = ImmutableMap.of(
                "p=1", integerPartition("1"), "p=2", integerPartition("2"));
        SortedPartitionRanges<String> publishedRanges = SortedPartitionRanges.build(partitions);
        SelectedPartitions view = new SelectedPartitions(
                2, partitions, false, false, Optional.of(publishedRanges));
        HMSExternalTable table = partitionedHiveTable(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        Mockito.when(table.preloadPartitionView(Mockito.any())).thenReturn(view);
        StatementContext statement = statementContext();
        statement.preloadExternalPartitionView(table, Optional.empty());
        try (MockedStatic<ConnectContext> current = Mockito.mockStatic(ConnectContext.class);
                MockedStatic<SortedPartitionRanges> ranges =
                        Mockito.mockStatic(SortedPartitionRanges.class, Mockito.CALLS_REAL_METHODS)) {
            current.when(ConnectContext::get).thenReturn(statement.getConnectContext());
            LogicalFileScan first = hiveScan(table, 1).withTableAlias("first");
            LogicalFileScan second = hiveScan(table, 2).withTableAlias("second");
            Assertions.assertSame(view, first.getSelectedPartitions());
            Assertions.assertSame(view, second.getSelectedPartitions());
            ranges.verifyNoInteractions();

            SelectedPartitions firstResult = pruneFilter(new LogicalFilter<>(Collections.singleton(
                    new EqualTo(first.getOutput().get(0), new IntegerLiteral(1))), first), statement);
            SelectedPartitions secondResult = pruneFilter(new LogicalFilter<>(Collections.singleton(
                    new EqualTo(second.getOutput().get(0), new IntegerLiteral(2))), second), statement);

            Assertions.assertEquals(Collections.singleton("p=1"), firstResult.selectedPartitions.keySet());
            Assertions.assertEquals(Collections.singleton("p=2"), secondResult.selectedPartitions.keySet());
            Assertions.assertTrue(firstResult.hasPartitionPredicate);
            Assertions.assertTrue(secondResult.hasPartitionPredicate);
            ranges.verifyNoInteractions();
            Mockito.verify(table, Mockito.never()).getSortedPartitionRanges(Mockito.any());
            Mockito.verify(table, Mockito.never()).getNameToPartitionItemsByFilter(Mockito.any(), Mockito.any());
            Mockito.verify(table, Mockito.never()).getNameToPartitionItems(Mockito.any());
        }
    }

    @Test
    public void testFilteredPageSortsItsOwnCapturedPartitions() throws Exception {
        Map<String, PartitionItem> page = new LinkedHashMap<>();
        page.put("p=3", integerPartition("3"));
        page.put("p=1", integerPartition("1"));
        page.put("p=2", integerPartition("2"));
        HMSExternalTable table = partitionedHiveTable(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        Mockito.when(table.getNameToPartitionItemsByFilter(Mockito.any(), Mockito.any()))
                .thenReturn(Optional.of(page));
        StatementContext statement = statementContext();
        try (MockedStatic<ConnectContext> current = Mockito.mockStatic(ConnectContext.class);
                MockedStatic<SortedPartitionRanges> ranges =
                        Mockito.mockStatic(SortedPartitionRanges.class, Mockito.CALLS_REAL_METHODS)) {
            current.when(ConnectContext::get).thenReturn(statement.getConnectContext());
            LogicalFileScan scan = hiveScan(table, 1);
            SelectedPartitions result = pruneFilter(new LogicalFilter<>(Collections.singleton(
                    new LessThanEqual(scan.getOutput().get(0), new IntegerLiteral(2))), scan), statement);

            Assertions.assertEquals(new java.util.HashSet<>(Arrays.asList("p=1", "p=2")),
                    result.selectedPartitions.keySet());
            Assertions.assertEquals(SelectedPartitions.UNKNOWN_TOTAL_PARTITION_NUM, result.totalPartitionNum);
            Assertions.assertTrue(result.hasPartitionPredicate);
            ranges.verify(() -> SortedPartitionRanges.build(Mockito.anyMap()), Mockito.times(1));
            Mockito.verify(table).getNameToPartitionItemsByFilter(Mockito.any(), Mockito.any());
            Mockito.verify(table, Mockito.never()).getNameToPartitionItems(Mockito.any());
            Mockito.verify(table, Mockito.never()).getSortedPartitionRanges(Mockito.any());
        }
    }

    @Test
    public void testCompleteFallbackReusesPublishedGenerationIndex() throws Exception {
        Map<String, PartitionItem> partitions = ImmutableMap.of(
                "p=1", integerPartition("1"), "p=2", integerPartition("2"));
        SortedPartitionRanges<String> publishedRanges = SortedPartitionRanges.build(partitions);
        SelectedPartitions view = new SelectedPartitions(
                2, partitions, false, false, Optional.of(publishedRanges));
        HMSExternalTable table = partitionedHiveTable(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        Mockito.when(table.getNameToPartitionItemsByFilter(Mockito.any(), Mockito.any()))
                .thenReturn(Optional.empty());
        Mockito.when(table.preloadPartitionView(Mockito.any())).thenReturn(view);
        StatementContext statement = statementContext();
        try (MockedStatic<ConnectContext> current = Mockito.mockStatic(ConnectContext.class);
                MockedStatic<SortedPartitionRanges> ranges =
                        Mockito.mockStatic(SortedPartitionRanges.class, Mockito.CALLS_REAL_METHODS)) {
            current.when(ConnectContext::get).thenReturn(statement.getConnectContext());
            LogicalFileScan scan = hiveScan(table, 1);

            SelectedPartitions result = pruneFilter(new LogicalFilter<>(Collections.singleton(
                    new EqualTo(scan.getOutput().get(0), new IntegerLiteral(1))), scan), statement);

            Assertions.assertEquals(Collections.singleton("p=1"), result.selectedPartitions.keySet());
            Assertions.assertTrue(result.hasPartitionPredicate);
            ranges.verifyNoInteractions();
            Mockito.verify(table).preloadPartitionView(Mockito.any());
            Mockito.verify(table, Mockito.never()).getNameToPartitionItems(Mockito.any());
            Mockito.verify(table, Mockito.never()).getSortedPartitionRanges(Mockito.any());
        }
    }

    @Test
    public void testDisabledBinarySearchDoesNotBuildSortedRanges() throws Exception {
        SelectedPartitions view = new SelectedPartitions(2, ImmutableMap.of(
                "p=1", integerPartition("1"), "p=2", integerPartition("2")), false);
        HMSExternalTable table = partitionedHiveTable(view);
        StatementContext statement = statementContext();
        statement.getConnectContext().getSessionVariable().enableBinarySearchFilteringPartitions = false;
        try (MockedStatic<ConnectContext> current = Mockito.mockStatic(ConnectContext.class);
                MockedStatic<SortedPartitionRanges> ranges =
                        Mockito.mockStatic(SortedPartitionRanges.class, Mockito.CALLS_REAL_METHODS)) {
            current.when(ConnectContext::get).thenReturn(statement.getConnectContext());
            LogicalFileScan scan = hiveScan(table, 1);
            SelectedPartitions result = pruneFilter(new LogicalFilter<>(Collections.singleton(
                    new EqualTo(scan.getOutput().get(0), new IntegerLiteral(1))), scan), statement);

            Assertions.assertEquals(Collections.singleton("p=1"), result.selectedPartitions.keySet());
            Assertions.assertTrue(result.hasPartitionPredicate);
            ranges.verifyNoInteractions();
        }
    }

    @Test
    public void testMaterializedAndFilteredEmptyViewsDoNotFallBack() {
        SelectedPartitions empty = new SelectedPartitions(0, ImmutableMap.of(), false);
        HMSExternalTable table = partitionedHiveTable(empty);
        StatementContext statement = statementContext();
        try (MockedStatic<ConnectContext> current = Mockito.mockStatic(ConnectContext.class)) {
            current.when(ConnectContext::get).thenReturn(statement.getConnectContext());
            LogicalFileScan scan = hiveScan(table, 1);
            SelectedPartitions result = pruneFilter(new LogicalFilter<>(Collections.singleton(
                    new EqualTo(scan.getOutput().get(0), new IntegerLiteral(1))), scan), statement);
            Assertions.assertTrue(result.selectedPartitions.isEmpty());
            Assertions.assertEquals(SelectedPartitions.State.MATERIALIZED, result.state);
            Assertions.assertFalse(result.hasPartitionPredicate);
            Mockito.verify(table, Mockito.never()).getNameToPartitionItemsByFilter(Mockito.any(), Mockito.any());

            Mockito.when(table.initSelectedPartitions(Mockito.any()))
                    .thenReturn(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
            Mockito.when(table.getNameToPartitionItemsByFilter(Mockito.any(), Mockito.any()))
                    .thenReturn(Optional.of(Collections.emptyMap()));
            LogicalFileScan deferred = hiveScan(table, 2);
            SelectedPartitions filtered = pruneFilter(new LogicalFilter<>(Collections.singleton(
                    new EqualTo(deferred.getOutput().get(0), new IntegerLiteral(1))), deferred), statement);
            Assertions.assertTrue(filtered.selectedPartitions.isEmpty());
            Assertions.assertEquals(SelectedPartitions.State.MATERIALIZED, filtered.state);
            Assertions.assertEquals(SelectedPartitions.UNKNOWN_TOTAL_PARTITION_NUM, filtered.totalPartitionNum);
            Assertions.assertTrue(filtered.hasPartitionPredicate);
            Mockito.verify(table, Mockito.never()).getNameToPartitionItems(Mockito.any());
        }
    }

    private ListPartitionItem integerPartition(String value) throws Exception {
        return new ListPartitionItem(Collections.singletonList(PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue(value)), Collections.singletonList(Type.INT), true)));
    }

    private HMSExternalTable partitionedHiveTable(SelectedPartitions view) {
        Column partitionColumn = new Column("p", Type.INT, true);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(table.getName()).thenReturn("hive_tbl");
        Mockito.when(table.getFullSchema(Mockito.any()))
                .thenReturn(Arrays.asList(partitionColumn, new Column("v", Type.INT, true)));
        Mockito.when(table.getPartitionColumns(Mockito.any())).thenReturn(Collections.singletonList(partitionColumn));
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(view);
        Mockito.when(table.supportInternalPartitionPruned()).thenReturn(true);
        return table;
    }

    private StatementContext statementContext() {
        ConnectContext connect = Mockito.mock(ConnectContext.class);
        Mockito.when(connect.getSessionVariable()).thenReturn(new SessionVariable());
        StatementContext statement = new StatementContext(connect, null);
        Mockito.when(connect.getStatementContext()).thenReturn(statement);
        return statement;
    }

    private LogicalFileScan hiveScan(HMSExternalTable table, int relationId) {
        return new LogicalFileScan(new RelationId(relationId), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private SelectedPartitions pruneFilter(LogicalFilter<LogicalFileScan> filter, StatementContext statement) {
        LogicalFilter<?> rewritten = Assertions.assertInstanceOf(LogicalFilter.class,
                new PruneFileScanPartition().build().transform(filter,
                        CascadesContext.initContext(statement, filter, PhysicalProperties.ANY)).get(0));
        Assertions.assertEquals(filter.getConjuncts(), rewritten.getConjuncts());
        return Assertions.assertInstanceOf(LogicalFileScan.class, rewritten.child()).getSelectedPartitions();
    }

    @Test
    public void testIntegerJdoFallbackPreservesCanonicalAndLeadingZeroNames() throws Exception {
        assertIntegerPartitionPruning(false);
    }

    @Test
    public void testPreloadedPartitionViewIsLocallyPrunedWithoutAnotherHmsCall() throws Exception {
        assertIntegerPartitionPruning(true);
    }

    private void assertIntegerPartitionPruning(boolean preloaded) throws Exception {
        Column partitionColumn = new Column("p", Type.INT, true);
        PartitionKey canonical = PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue("1")), Collections.singletonList(Type.INT), true);
        PartitionKey leadingZero = PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue("01")), Collections.singletonList(Type.INT), true);
        PartitionKey other = PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue("2")), Collections.singletonList(Type.INT), true);
        Map<String, PartitionItem> partitions = ImmutableMap.of(
                "p=1", new ListPartitionItem(Collections.singletonList(canonical)),
                "p=01", new ListPartitionItem(Collections.singletonList(leadingZero)),
                "p=2", new ListPartitionItem(Collections.singletonList(other)));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(table.getFullSchema(Mockito.any())).thenReturn(Collections.singletonList(partitionColumn));
        Mockito.when(table.getPartitionColumns(Mockito.any())).thenReturn(Collections.singletonList(partitionColumn));
        Mockito.when(table.getName()).thenReturn("hive_tbl");
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(preloaded
                ? new SelectedPartitions(partitions.size(), partitions, false)
                : SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        Mockito.when(table.preloadPartitionView(Mockito.any()))
                .thenReturn(new SelectedPartitions(partitions.size(), partitions, false));
        Mockito.when(table.getNameToPartitionItemsByFilter(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
            String filter = HivePartitionFilterBuilder.build(invocation.getArgument(1),
                    Collections.singletonList(partitionColumn), ImmutableMap.of("p", "int"));
            return filter == null ? Optional.empty() : Optional.of(Collections.emptyMap());
        });
        LogicalFileScan scan = new LogicalFileScan(new RelationId(1), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(
                Collections.singleton(new EqualTo(scan.getOutput().get(0), new IntegerLiteral(1))), scan);
        ConnectContext connect = Mockito.mock(ConnectContext.class);
        Mockito.when(connect.getSessionVariable()).thenReturn(new SessionVariable());
        StatementContext statement = new StatementContext(connect, null);
        Mockito.when(connect.getStatementContext()).thenReturn(statement);
        try (MockedStatic<SortedPartitionRanges> ranges =
                Mockito.mockStatic(SortedPartitionRanges.class, Mockito.CALLS_REAL_METHODS)) {
            SelectedPartitions result = Deencapsulation.invoke(new PruneFileScanPartition(),
                    "pruneExternalPartitions", table, filter, scan,
                    CascadesContext.initContext(statement, filter, PhysicalProperties.ANY));
            Assertions.assertEquals(new java.util.HashSet<>(Arrays.asList("p=1", "p=01")),
                    result.selectedPartitions.keySet());
            Assertions.assertEquals(Collections.singletonList("01"), leadingZero.getOriginHiveKeys());
            Assertions.assertTrue(result.hasPartitionPredicate);
            ranges.verify(() -> SortedPartitionRanges.build(Mockito.anyMap()), Mockito.times(1));
        }
        Mockito.verify(table, Mockito.never()).getSortedPartitionRanges(Mockito.any());
        Mockito.verify(table, Mockito.never()).getNameToPartitionItems(Mockito.any());
        if (preloaded) {
            Mockito.verify(table, Mockito.never()).getNameToPartitionItemsByFilter(Mockito.any(), Mockito.any());
            Mockito.verify(table, Mockito.never()).preloadPartitionView(Mockito.any());
        } else {
            Mockito.verify(table).preloadPartitionView(Mockito.any());
        }
    }

    @Test
    public void testPartitionColumnsUseRelationSnapshot() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
        MvccSnapshot relationSnapshot = Mockito.mock(MvccSnapshot.class);
        List<Column> partitionColumns = Collections.singletonList(new Column("old_partition", Type.INT, true));
        Mockito.when(scan.getRelationSnapshot()).thenReturn(Optional.of(relationSnapshot));
        Mockito.when(table.getPartitionColumns(Optional.of(relationSnapshot))).thenReturn(partitionColumns);

        Assertions.assertSame(partitionColumns,
                PruneFileScanPartition.getPartitionColumnsForScan(table, scan));
        Mockito.verify(table).getPartitionColumns(Optional.of(relationSnapshot));
    }
}
