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

package org.apache.doris.datasource.mvcc;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.cost.CostWeight;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.rules.implementation.LogicalFileScanToPhysicalFileScan;
import org.apache.doris.nereids.rules.rewrite.PruneFileScanPartition;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.cache.StatisticsCache;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.statistics.model.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PluginDrivenFileScanStatsTest {
    private Column partitionColumn = new Column("dt", PrimitiveType.INT);
    private SlotReference slot = new SlotReference("dt", IntegerType.INSTANCE);
    private final SlotReference valueSlot = new SlotReference("v", IntegerType.INSTANCE, true);
    private final IdGenerator<GroupId> groupIds = GroupId.createGenerator();
    private ConnectContext connectContext;
    private StatementContext statementContext;
    private PluginDrivenMvccExternalTable table;
    private Map<String, PartitionItem> partitions;
    private boolean enableInternalSchemaDb;

    @BeforeEach
    public void setUp() throws Exception {
        enableInternalSchemaDb = FeConstants.enableInternalSchemaDb;
        FeConstants.enableInternalSchemaDb = false;
        connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setBeNumberForTest(1);
        statementContext = MemoTestUtils.createStatementContext(connectContext, "select dt from events");
        table = Mockito.mock(PluginDrivenMvccExternalTable.class);
        ExternalDatabase database = Mockito.mock(ExternalDatabase.class);
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(table.getName()).thenReturn("events");
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("test_connector");
        Mockito.when(table.getRowCount()).thenReturn(3L);
        Mockito.when(table.supportInternalPartitionPruned()).thenReturn(true);
        Mockito.when(table.supportInternalPartitionPruned(Mockito.any())).thenReturn(true);
        Mockito.when(table.getPartitionColumns(Mockito.any())).thenReturn(ImmutableList.of(partitionColumn));
        partitions = ImmutableMap.of("p1", newPartition(1), "p2", newPartition(2), "p3", newPartition(3));
        Mockito.when(table.initSelectedPartitions(Mockito.any()))
                .thenReturn(new SelectedPartitions(3, partitions, false));
        Mockito.when(table.getSortedPartitionRanges(Mockito.any())).thenReturn(
                Optional.of(SortedPartitionRanges.build(partitions)));
        setSnapshot(1, 1, 1, false);
    }

    @AfterEach
    public void tearDown() {
        FeConstants.enableInternalSchemaDb = enableInternalSchemaDb;
        ConnectContext.remove();
    }

    @Test
    public void testCompensationPruningChangesScanCostAndKeepsCardinality() {
        LogicalFileScan fullScan = newScan(table, new SelectedPartitions(3, partitions, true));
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(slot, new IntegerLiteral(3))), fullScan.withPartitionPruned(false));
        CascadesContext context = CascadesContext.initContext(statementContext, filter, PhysicalProperties.ANY);
        LogicalFileScan compensationScan = (LogicalFileScan) new PruneFileScanPartition().build()
                .transform(filter, context).get(0).child(0);

        Assertions.assertEquals(ImmutableSet.of("p3"),
                compensationScan.getSelectedPartitions().selectedPartitions.keySet());
        assertScanCostRows(fullScan, 3);
        assertScanCostRows(compensationScan, 1);
        Assertions.assertEquals(3, fullScan.accept(new StatsCalculator(null), null).getRowCount(), 0.001);
        Assertions.assertEquals(3, compensationScan.accept(new StatsCalculator(null), null).getRowCount(), 0.001);
        Assertions.assertEquals(15, scanCpuCost(fullScan), 0.001);
        Assertions.assertEquals(5, scanCpuCost(compensationScan), 0.001);
        Mockito.verify(table, Mockito.never()).loadSnapshot(Mockito.any(), Mockito.any());
    }

    @Test
    public void testSkewedPartitionRows() {
        setSnapshot(100, 10, 1, false);
        Mockito.when(table.getRowCount()).thenReturn(111L);
        assertScanCostRows(newScan(table, new SelectedPartitions(3, partitions, true)), 111);
        assertScanCostRows(selectedScan("p1"), 100);
        assertScanCostRows(selectedScan("p3"), 1);
    }

    @Test
    public void testStatementSnapshotOverridesTableRowCount() {
        Mockito.when(table.getRowCount()).thenReturn(1000L);
        assertScanCostRows(selectedScan("p3"), 1);

        statementContext = MemoTestUtils.createStatementContext(connectContext, "select dt from events");
        setSnapshot(100, 10, 7, false);
        assertScanCostRows(selectedScan("p3"), 7);
    }

    @Test
    public void testCostReadsPinnedRowsWithoutLoadingTableMetadata() {
        PhysicalFileScan scan = toPhysical(selectedScan("p3"));
        GroupExpression expression = new GroupExpression(scan);
        Group group = new Group(groupIds.getNextId(), expression, scan.getLogicalProperties());
        group.setStatistics(new Statistics(1000, ImmutableMap.of(slot, ColumnStatistic.UNKNOWN)));
        Assertions.assertEquals(5, CostCalculator.calculateCost(connectContext, expression, ImmutableList.of(),
                CostWeight.get(connectContext.getSessionVariable())).getCpuCost(), 0.001);
        Mockito.verify(table, Mockito.never()).getRowCount();
        Mockito.verify(table, Mockito.never()).loadSnapshot(Mockito.any(), Mockito.any());
    }

    @Test
    public void testNativeAndDeferredSelectionsKeepExistingEstimate() {
        assertScanCostRows(newScan(table, SelectedPartitions.NATIVE), 3);
        assertScanCostRows(newScan(table, SelectedPartitions.DEFERRED_PARTITION_PRUNING), 3);
    }

    @Test
    public void testStatisticsDoNotMaterializeDeferredMetadata() {
        AtomicInteger loads = new AtomicInteger();
        PluginDrivenMvccSnapshot pin = new PluginDrivenMvccSnapshot(
                ConnectorMvccSnapshot.builder().snapshotId(101).build(), null, () -> {
                    loads.incrementAndGet();
                    return newPin(1, 1, 1);
                });
        statementContext.setSnapshot(new MvccTableInfo(table), pin);
        assertScanCostRows(selectedScan("p3"), 3);
        Assertions.assertEquals(0, loads.get());
        pin.getNameToPartitionItem();
        assertScanCostRows(selectedScan("p3"), 1);
        Assertions.assertEquals(1, loads.get());
    }

    @Test
    public void testEachVersionedReferenceUsesItsOwnPin() {
        TableSnapshot historical = TableSnapshot.versionOf("7");
        Optional<TableSnapshot> selector = Optional.of(historical);
        statementContext.setSnapshot(new MvccTableInfo(table,
                MvccReadSelector.of(selector, Optional.empty()).getKey()), newPin(7, 100, 10, 7));
        LogicalFileScan historicalScan = new LogicalFileScan(StatementScopeIdGenerator.newRelationId(), table,
                ImmutableList.of("db"), ImmutableList.of(), Optional.empty(), selector, Optional.empty(),
                Optional.of(ImmutableList.<Slot>of(slot))).withSelectedPartitions(
                        new SelectedPartitions(3, ImmutableMap.of("p3", partitions.get("p3")), true));
        assertScanCostRows(selectedScan("p3"), 1);
        assertScanCostRows(historicalScan, 7);
    }

    @Test
    public void testEmptySelectionHasZeroScanCost() {
        assertScanCostRows(newScan(table, new SelectedPartitions(3, ImmutableMap.of(), true)), 0);
    }

    @Test
    public void testUnprunedScanUsesExistingEstimate() {
        assertScanCostRows(newScan(table, new SelectedPartitions(3, partitions, false)), 3);
        assertScanCostRows(newScan(table, SelectedPartitions.NOT_PRUNED), 3);
    }

    @Test
    public void testMissingSnapshotUsesExistingEstimate() {
        statementContext = MemoTestUtils.createStatementContext(connectContext, "select dt from events");
        assertScanCostRows(selectedScan("p3"), 3);
    }

    @Test
    public void testUnavailableMetadataDoesNotMeanZero() {
        statementContext.setSnapshot(new MvccTableInfo(table), PluginDrivenMvccSnapshot.unavailable(
                ConnectorMvccSnapshot.builder().snapshotId(101).build(), null));
        assertScanCostRows(newScan(table, new SelectedPartitions(0, ImmutableMap.of(), true)), 3);
    }

    @Test
    public void testUnknownPartitionRowsUseExistingEstimate() {
        setSnapshot(1, 1, 1, true);
        assertScanCostRows(selectedScan("p3"), 3);
        assertScanCostRows(selectedScan("p1"), 1);
    }

    @Test
    public void testOtherExternalTablesKeepExistingEstimate() {
        ExternalTable otherTable = Mockito.mock(ExternalTable.class);
        Mockito.when(otherTable.getRowCount()).thenReturn(20L);
        Mockito.when(otherTable.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        assertScanCostRows(newScan(otherTable,
                new SelectedPartitions(3, ImmutableMap.of("p3", partitions.get("p3")), true)), 20);
    }

    @Test
    public void testSelectedScanKeepsTableColumnStatsAndBaseLineage() {
        FeConstants.enableInternalSchemaDb = true;
        setSnapshot(10, 10, 10, false);
        Mockito.when(table.getRowCount()).thenReturn(30L);
        ColumnStatistic columnStats = new ColumnStatisticBuilder(30).setNdv(2).setNumNulls(9).build();
        Env env = Mockito.mock(Env.class);
        StatisticsCache statisticsCache = Mockito.mock(StatisticsCache.class);
        Mockito.when(env.getStatisticsCache()).thenReturn(statisticsCache);
        Mockito.when(statisticsCache.getColumnStatistics(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong(),
                Mockito.anyLong(), Mockito.anyString(), Mockito.any())).thenReturn(columnStats);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            LogicalFileScan selected = selectedScan("p3");
            Statistics statistics = selected.accept(new StatsCalculator(null), null);
            ColumnStatistic selectedStats = statistics.findColumnStatistics(slot);
            Assertions.assertEquals(30, statistics.getRowCount(), 0.001);
            Assertions.assertEquals(30, selectedStats.count, 0.001);
            Assertions.assertEquals(9, selectedStats.numNulls, 0.001);
            Assertions.assertEquals(selectedStats.count, selectedStats.original.count, 0.001);
            Assertions.assertTrue(statistics.checkColumnStatsValid(selectedStats, 30));

            setSnapshot(1, 1, 1, false);
            Statistics singleRowStats = selectedScan("p3").accept(new StatsCalculator(null), null);
            Assertions.assertEquals(30, singleRowStats.getRowCount(), 0.001);

            LogicalFileScan emptyScan = newScan(table, new SelectedPartitions(3, ImmutableMap.of(), true));
            Statistics emptyStats = emptyScan.accept(new StatsCalculator(null), null);
            ColumnStatistic emptyColumnStats = emptyStats.findColumnStatistics(slot);
            Assertions.assertEquals(30, emptyColumnStats.count, 0.001);
            Assertions.assertEquals(2, emptyColumnStats.ndv, 0.001);
            Assertions.assertEquals(9, emptyColumnStats.numNulls, 0.001);
        }
    }

    @Test
    public void testDatePartitionFilterKeepsSelectedRowsAndBroadcastLimit() throws Exception {
        configureTemporalPartitions(false, 1000);
        ColumnStatistic cached = temporalStats(3000, 3,
                new DateV2Literal("2024-01-01"), new DateV2Literal("2024-01-03"));
        try (MockedStatic<Env> ignored = mockColumnStats(cached, valueStats(3000))) {
            LogicalFilter<LogicalFileScan> filter = prune(
                    ImmutableSet.of(new EqualTo(slot, new DateV2Literal("2024-01-03"))));
            Assertions.assertEquals(ImmutableSet.of("p3"),
                    filter.child().getSelectedPartitions().selectedPartitions.keySet());
            Assertions.assertEquals(1000, estimateFilter(filter).getRowCount(), 0.001);
            PhysicalFilter<PhysicalFileScan> physical = physicalFilter(filter);
            Group build = estimateGroup(physical, estimateGroup(physical.child()));
            Assertions.assertEquals(1000, build.getStatistics().getRowCount(), 0.001);
            Assertions.assertEquals(1, build.getStatistics().findColumnStatistics(slot).ndv, 0.001);
            Assertions.assertFalse(canBroadcast(physical, build));
            Assertions.assertEquals(filter.getConjuncts(), physical.getConjuncts());
            Assertions.assertEquals(5000, scanCpuCost(filter.child()), 0.001);
        }
    }

    @Test
    public void testNullFilterOnPrunedScanKeepsBaseLineageInEitherOrder() throws Exception {
        configureTemporalPartitions(false, 1_000_000);
        ColumnStatistic dates = temporalStats(3_000_000, 3,
                new DateV2Literal("2024-01-01"), new DateV2Literal("2024-01-03"));
        ColumnStatistic values = valueStats(3_000_000);
        Assertions.assertNotNull(values.original);
        Expression date = new EqualTo(slot, new DateV2Literal("2024-01-03"));
        Expression isNull = new IsNull(valueSlot);
        try (MockedStatic<Env> ignored = mockColumnStats(dates, values)) {
            for (Expression predicate : ImmutableList.of(new And(date, isNull), new And(isNull, date))) {
                LogicalFilter<LogicalFileScan> filter = prune(ImmutableSet.of(predicate));
                Statistics scanStats = filter.child().accept(new StatsCalculator(null), null);
                ColumnStatistic selected = scanStats.findColumnStatistics(valueSlot);
                Assertions.assertEquals(selected.count, selected.original.count, 0.001);
                Assertions.assertEquals(3_000_000, selected.original.count, 0.001);
                Assertions.assertEquals(0, estimateFilter(filter).getRowCount(), 0.001);
                PhysicalFilter<PhysicalFileScan> physical = physicalFilter(filter);
                Group build = estimateGroup(physical, estimateGroup(physical.child()));
                Assertions.assertEquals(0, build.getStatistics().getRowCount(), 0.001);
                Assertions.assertTrue(canBroadcast(physical, build));
            }
            Assertions.assertEquals(3_000_000, values.original.count, 0.001);
        }
    }

    @Test
    public void testDisjointDatePartitionsDoNotApplyCoveredPredicateTwice() throws Exception {
        configureTemporalPartitions(false, 1000);
        ColumnStatistic dates = temporalStats(3000, 3,
                new DateV2Literal("2024-01-01"), new DateV2Literal("2024-01-03"));
        try (MockedStatic<Env> ignored = mockColumnStats(dates, valueStats(3000))) {
            LogicalFilter<LogicalFileScan> filter = prune(ImmutableSet.of(
                    new Not(new EqualTo(slot, new DateV2Literal("2024-01-02")))));
            Assertions.assertEquals(ImmutableSet.of("p1", "p3"),
                    filter.child().getSelectedPartitions().selectedPartitions.keySet());
            Assertions.assertEquals(2000, estimateFilter(filter).getRowCount(), 0.001);
            Assertions.assertEquals(2000, estimateGroup(physicalFilter(filter),
                    estimateGroup(toPhysical(filter.child()))).getStatistics().getRowCount(), 0.001);
            Assertions.assertEquals(10000, scanCpuCost(filter.child()), 0.001);
        }
    }

    @Test
    public void testPartialDayPredicateStillFiltersRows() throws Exception {
        configureTemporalPartitions(true, 1000);
        ColumnStatistic dates = temporalStats(3000, 3000,
                new DateTimeV2Literal("2024-01-01 00:00:00"),
                new DateTimeV2Literal("2024-01-03 23:59:59"));
        Expression predicate = new LessThan(slot, new DateTimeV2Literal("2024-01-02 12:00:00"));
        try (MockedStatic<Env> ignored = mockColumnStats(dates, valueStats(3000))) {
            LogicalFilter<LogicalFileScan> filter = prune(ImmutableSet.of(predicate));
            Assertions.assertEquals(ImmutableSet.of("p1", "p2"),
                    filter.child().getSelectedPartitions().selectedPartitions.keySet());
            Statistics scanStats = filter.child().accept(new StatsCalculator(null), null);
            Statistics filtered = estimateFilter(filter);
            Assertions.assertEquals(1500, filtered.getRowCount(), 1);
            Assertions.assertTrue(filtered.getRowCount() < scanStats.getRowCount());
            Assertions.assertEquals(ImmutableSet.of(predicate), filter.getConjuncts());
            Assertions.assertEquals(10000, scanCpuCost(filter.child()), 0.001);
        }
    }

    @Test
    public void testNewPartitionScanCostDoesNotDependOnAnalyzedBounds() throws Exception {
        configureTemporalPartitions(false, 1000);
        ColumnStatistic staleDates = temporalStats(2000, 2,
                new DateV2Literal("2024-01-01"), new DateV2Literal("2024-01-02"));
        try (MockedStatic<Env> ignored = mockColumnStats(staleDates, valueStats(2000))) {
            LogicalFilter<LogicalFileScan> filter = prune(
                    ImmutableSet.of(new EqualTo(slot, new DateV2Literal("2024-01-03"))));
            Assertions.assertEquals(5000, scanCpuCost(filter.child()), 0.001);
        }
    }

    @Test
    public void testSingleRowPartitionScanCostWithoutColumnStats() throws Exception {
        configureTemporalPartitions(false, 1);
        try (MockedStatic<Env> ignored = mockColumnStats(ColumnStatistic.UNKNOWN, valueStats(3))) {
            LogicalFilter<LogicalFileScan> filter = prune(
                    ImmutableSet.of(new EqualTo(slot, new DateV2Literal("2024-01-03"))));
            Assertions.assertEquals(5, scanCpuCost(filter.child()), 0.001);
        }
    }

    @Test
    public void testUnprunedAndUnknownRowsKeepLegacyCostAndFilterEstimate() throws Exception {
        configureTemporalPartitions(false, 1000);
        ColumnStatistic dates = temporalStats(3000, 3,
                new DateV2Literal("2024-01-01"), new DateV2Literal("2024-01-03"));
        try (MockedStatic<Env> ignored = mockColumnStats(dates, valueStats(3000))) {
            LogicalFilter<LogicalFileScan> filter = prune(
                    ImmutableSet.of(new EqualTo(slot, new DateV2Literal("2024-01-03"))));
            Assertions.assertEquals(5000, scanCpuCost(filter.child()), 0.001);
            Assertions.assertEquals(15000, scanCpuCost(filter.child().withPartitionPruned(false)), 0.001);
            setSnapshot(1000, 1000, 1000, true);
            Assertions.assertEquals(1000, estimateFilter(filter).getRowCount(), 0.001);
            Assertions.assertEquals(15000, scanCpuCost(filter.child()), 0.001);
        }
    }

    private void configureTemporalPartitions(boolean timestamp, long rowsPerPartition) throws Exception {
        partitionColumn = new Column("dt", timestamp ? PrimitiveType.DATETIMEV2 : PrimitiveType.DATEV2);
        slot = new SlotReference("dt", timestamp ? DateTimeV2Type.SYSTEM_DEFAULT : DateV2Type.INSTANCE);
        partitions = ImmutableMap.of("p1", rangePartition("2024-01-01", "2024-01-02"),
                "p2", rangePartition("2024-01-02", "2024-01-03"),
                "p3", rangePartition("2024-01-03", "2024-01-04"));
        Mockito.when(table.getPartitionColumns(Mockito.any())).thenReturn(ImmutableList.of(partitionColumn));
        Mockito.when(table.initSelectedPartitions(Mockito.any()))
                .thenReturn(new SelectedPartitions(3, partitions, false));
        Mockito.when(table.getSortedPartitionRanges(Mockito.any()))
                .thenReturn(Optional.of(SortedPartitionRanges.build(partitions)));
        Mockito.when(table.getRowCount()).thenReturn(3 * rowsPerPartition);
        setSnapshot(rowsPerPartition, rowsPerPartition, rowsPerPartition, false);
        FeConstants.enableInternalSchemaDb = true;
    }

    private PartitionItem rangePartition(String start, String end) throws Exception {
        PartitionKey lower = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue(start)), ImmutableList.of(partitionColumn));
        PartitionKey upper = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue(end)), ImmutableList.of(partitionColumn));
        return new RangePartitionItem(Range.closedOpen(lower, upper));
    }

    private ColumnStatistic temporalStats(double rows, double ndv, Literal min, Literal max) {
        return new ColumnStatisticBuilder(rows).setNdv(ndv).setNumNulls(0)
                .setMinValue(min.getDouble()).setMinExpr(min.toLegacyLiteral())
                .setMaxValue(max.getDouble()).setMaxExpr(max.toLegacyLiteral()).build();
    }

    private ColumnStatistic valueStats(double rows) {
        return new ColumnStatisticBuilder(rows).setNdv(1).setNumNulls(0)
                .setMinValue(1).setMinExpr(new IntegerLiteral(1).toLegacyLiteral())
                .setMaxValue(1).setMaxExpr(new IntegerLiteral(1).toLegacyLiteral()).build();
    }

    private MockedStatic<Env> mockColumnStats(ColumnStatistic dates, ColumnStatistic values) {
        Env realEnv = Env.getCurrentEnv();
        Env env = Mockito.mock(Env.class);
        StatisticsCache cache = Mockito.mock(StatisticsCache.class);
        Mockito.when(env.getStatisticsCache()).thenReturn(cache);
        Mockito.when(env.getHboPlanStatisticsManager()).thenReturn(realEnv.getHboPlanStatisticsManager());
        Mockito.when(cache.getColumnStatistics(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong(),
                Mockito.anyLong(), Mockito.anyString(), Mockito.any()))
                .thenAnswer(invocation -> "dt".equals(invocation.getArgument(4)) ? dates : values);
        MockedStatic<Env> mocked = Mockito.mockStatic(Env.class);
        mocked.when(Env::getCurrentEnv).thenReturn(env);
        return mocked;
    }

    @SuppressWarnings("unchecked")
    private LogicalFilter<LogicalFileScan> prune(Set<Expression> conjuncts) {
        LogicalFileScan scan = newScan(table, new SelectedPartitions(3, partitions, false))
                .withCachedOutput(ImmutableList.of(slot, valueSlot));
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(conjuncts, scan);
        CascadesContext context = CascadesContext.initContext(statementContext, filter, PhysicalProperties.ANY);
        return (LogicalFilter<LogicalFileScan>) new PruneFileScanPartition().build().transform(filter, context).get(0);
    }

    private Statistics estimateFilter(LogicalFilter<LogicalFileScan> filter) {
        return estimateGroup(filter, estimateGroup(filter.child())).getStatistics();
    }

    private PhysicalFilter<PhysicalFileScan> physicalFilter(LogicalFilter<LogicalFileScan> filter) {
        return new PhysicalFilter<>(filter.getConjuncts(), filter.getLogicalProperties(), toPhysical(filter.child()));
    }

    private Group estimateGroup(Plan plan, Group... children) {
        List<Group> childGroups = Arrays.asList(children);
        Plan memoPlan = plan.withChildren(childGroups.stream().map(Group::getGroupPlan).collect(Collectors.toList()));
        GroupExpression expression = new GroupExpression(memoPlan, childGroups);
        Group group = new Group(groupIds.getNextId(), expression, plan.getLogicalProperties());
        StatsCalculator.estimate(expression, null);
        return group;
    }

    private boolean canBroadcast(PhysicalFilter<PhysicalFileScan> filter, Group build) {
        SlotReference probeKey = new SlotReference("probe_dt", slot.getDataType());
        PhysicalFileScan probe = toPhysical(newScan(table, new SelectedPartitions(3, partitions, true))
                .withCachedOutput(ImmutableList.of(probeKey)));
        PhysicalHashJoin<PhysicalFileScan, PhysicalFilter<PhysicalFileScan>> join = new PhysicalHashJoin<>(
                JoinType.INNER_JOIN, ImmutableList.of(new EqualTo(probeKey, slot)), ImmutableList.of(),
                new DistributeHint(DistributeType.NONE), Optional.empty(), filter.getLogicalProperties(), probe, filter);
        GroupExpression expression = new GroupExpression(join, ImmutableList.of(estimateGroup(probe), build));
        connectContext.getSessionVariable().setBroadcastRowCountLimit(600);
        return JoinUtils.checkBroadcastJoinStats((PhysicalHashJoin<?, ?>) expression.getPlan());
    }

    private void setSnapshot(long p1Rows, long p2Rows, long p3Rows, boolean unknownRows) {
        statementContext.setSnapshot(new MvccTableInfo(table), newPin(p1Rows, p2Rows,
                unknownRows ? ConnectorPartitionInfo.UNKNOWN : p3Rows));
    }

    private PluginDrivenMvccSnapshot newPin(long p1Rows, long p2Rows, long p3Rows) {
        return newPin(101, p1Rows, p2Rows, p3Rows);
    }

    private PluginDrivenMvccSnapshot newPin(long snapshotId, long p1Rows, long p2Rows, long p3Rows) {
        Map<String, Long> freshness = ImmutableMap.of("p1", snapshotId, "p2", snapshotId, "p3", snapshotId);
        return new PluginDrivenMvccSnapshot(ConnectorMvccSnapshot.builder().snapshotId(snapshotId).build(),
                new PluginDrivenPartitionView(partitions, freshness,
                        ImmutableMap.of("p1", p1Rows, "p2", p2Rows, "p3", p3Rows), true),
                null, PartitionType.RANGE, true, 0, 0);
    }

    private LogicalFileScan selectedScan(String name) {
        return newScan(table, new SelectedPartitions(3, ImmutableMap.of(name, partitions.get(name)), true));
    }

    private LogicalFileScan newScan(ExternalTable externalTable, SelectedPartitions selectedPartitions) {
        return new LogicalFileScan(StatementScopeIdGenerator.newRelationId(), externalTable,
                ImmutableList.of("db"), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(ImmutableList.<Slot>of(slot))).withSelectedPartitions(selectedPartitions);
    }

    private PhysicalFileScan toPhysical(LogicalFileScan scan) {
        CascadesContext context = CascadesContext.initContext(statementContext, scan, PhysicalProperties.ANY);
        return (PhysicalFileScan) new LogicalFileScanToPhysicalFileScan().build().transform(scan, context).get(0);
    }

    private void assertScanCostRows(LogicalFileScan scan, double expected) {
        Assertions.assertEquals(expected * 5, scanCpuCost(scan), 0.001);
    }

    private double scanCpuCost(LogicalFileScan scan) {
        PhysicalFileScan physical = toPhysical(scan);
        GroupExpression expression = new GroupExpression(physical);
        new Group(null, expression, physical.getLogicalProperties());
        StatsCalculator.estimate(expression, null);
        return CostCalculator.calculateCost(connectContext, expression, ImmutableList.of(),
                CostWeight.get(connectContext.getSessionVariable())).getCpuCost();
    }

    private PartitionItem newPartition(int value) throws Exception {
        PartitionKey key = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue(Integer.toString(value))), ImmutableList.of(partitionColumn));
        return new ListPartitionItem(ImmutableList.of(key));
    }
}
