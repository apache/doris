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
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.rules.implementation.LogicalFileScanToPhysicalFileScan;
import org.apache.doris.nereids.rules.rewrite.PruneFileScanPartition;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsCache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class PluginDrivenFileScanStatsTest {
    private final Column partitionColumn = new Column("dt", PrimitiveType.INT);
    private final SlotReference slot = new SlotReference("dt", IntegerType.INSTANCE);
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
    public void testCompensationPruningChangesScanRowsAndCost() {
        LogicalFileScan fullScan = newScan(table, new SelectedPartitions(3, partitions, true));
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(slot, new IntegerLiteral(3))), fullScan.withPartitionPruned(false));
        CascadesContext context = CascadesContext.initContext(statementContext, filter, PhysicalProperties.ANY);
        LogicalFileScan compensationScan = (LogicalFileScan) new PruneFileScanPartition().build()
                .transform(filter, context).get(0).child(0);

        Assertions.assertEquals(ImmutableSet.of("p3"),
                compensationScan.getSelectedPartitions().selectedPartitions.keySet());
        assertRowCount(fullScan, 3);
        assertRowCount(compensationScan, 1);
        Assertions.assertEquals(15, scanCpuCost(fullScan), 0.001);
        Assertions.assertEquals(5, scanCpuCost(compensationScan), 0.001);
        Mockito.verify(table, Mockito.never()).getRowCount();
        Mockito.verify(table, Mockito.never()).loadSnapshot(Mockito.any(), Mockito.any());
    }

    @Test
    public void testSkewedPartitionRows() {
        setSnapshot(100, 10, 1, false);
        Mockito.when(table.getRowCount()).thenReturn(111L);
        assertRowCount(newScan(table, new SelectedPartitions(3, partitions, true)), 111);
        assertRowCount(selectedScan("p1"), 100);
        assertRowCount(selectedScan("p3"), 1);
    }

    @Test
    public void testStatementSnapshotOverridesTableRowCount() {
        Mockito.when(table.getRowCount()).thenReturn(1000L);
        assertRowCount(selectedScan("p3"), 1);

        statementContext = MemoTestUtils.createStatementContext(connectContext, "select dt from events");
        setSnapshot(100, 10, 7, false);
        assertRowCount(selectedScan("p3"), 7);
        Mockito.verify(table, Mockito.never()).getRowCount();
    }

    @Test
    public void testNativeAndDeferredSelectionsKeepExistingEstimate() {
        assertRowCount(newScan(table, SelectedPartitions.NATIVE), 3);
        assertRowCount(newScan(table, SelectedPartitions.DEFERRED), 3);
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
        assertRowCount(selectedScan("p3"), 3);
        Assertions.assertEquals(0, loads.get());
        pin.getNameToPartitionItem();
        assertRowCount(selectedScan("p3"), 1);
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
        assertRowCount(selectedScan("p3"), 1);
        assertRowCount(historicalScan, 7);
        Mockito.verify(table, Mockito.never()).getRowCount();
    }

    @Test
    public void testEmptySelectionRemainsZero() {
        assertRowCount(newScan(table, new SelectedPartitions(3, ImmutableMap.of(), true)), 0);
        Mockito.verify(table, Mockito.never()).getRowCount();
    }

    @Test
    public void testUnprunedScanUsesExistingEstimate() {
        assertRowCount(newScan(table, new SelectedPartitions(3, partitions, false)), 3);
        assertRowCount(newScan(table, SelectedPartitions.NOT_PRUNED), 3);
    }

    @Test
    public void testMissingSnapshotUsesExistingEstimate() {
        statementContext = MemoTestUtils.createStatementContext(connectContext, "select dt from events");
        assertRowCount(selectedScan("p3"), 3);
    }

    @Test
    public void testUnavailableMetadataDoesNotMeanZero() {
        statementContext.setSnapshot(new MvccTableInfo(table), PluginDrivenMvccSnapshot.unavailable(
                ConnectorMvccSnapshot.builder().snapshotId(101).build(), null));
        assertRowCount(newScan(table, new SelectedPartitions(0, ImmutableMap.of(), true)), 3);
    }

    @Test
    public void testUnknownPartitionRowsUseExistingEstimate() {
        setSnapshot(1, 1, 1, true);
        assertRowCount(selectedScan("p3"), 3);
        assertRowCount(selectedScan("p1"), 1);
    }

    @Test
    public void testOtherExternalTablesKeepExistingEstimate() {
        ExternalTable otherTable = Mockito.mock(ExternalTable.class);
        Mockito.when(otherTable.getRowCount()).thenReturn(20L);
        Mockito.when(otherTable.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        assertRowCount(newScan(otherTable,
                new SelectedPartitions(3, ImmutableMap.of("p3", partitions.get("p3")), true)), 20);
    }

    @Test
    public void testColumnStatsFitSelectedRows() {
        FeConstants.enableInternalSchemaDb = true;
        setSnapshot(10, 10, 10, false);
        ColumnStatistic columnStats = new ColumnStatisticBuilder(30).setNdv(2).setNumNulls(9).build();
        new MockUp<StatisticsCache>() {
            @Mock
            public ColumnStatistic getColumnStatistics(long catalogId, long dbId, long tableId,
                    long indexId, String columnName, ConnectContext context) {
                return columnStats;
            }
        };
        LogicalFileScan selected = selectedScan("p3");
        Statistics statistics = selected.accept(new StatsCalculator(null), null);
        ColumnStatistic selectedStats = statistics.findColumnStatistics(slot);
        Assertions.assertEquals(10, selectedStats.count, 0.001);
        Assertions.assertEquals(3, selectedStats.numNulls, 0.001);
        Assertions.assertTrue(statistics.checkColumnStatsValid(selectedStats, 10));
        assertRowCount(selected, 10);

        setSnapshot(1, 1, 1, false);
        Statistics singleRowStats = selectedScan("p3").accept(new StatsCalculator(null), null);
        Assertions.assertTrue(singleRowStats.checkColumnStatsValid(singleRowStats.findColumnStatistics(slot), 1));

        LogicalFileScan emptyScan = newScan(table, new SelectedPartitions(3, ImmutableMap.of(), true));
        Statistics emptyStats = emptyScan.accept(new StatsCalculator(null), null);
        ColumnStatistic emptyColumnStats = emptyStats.findColumnStatistics(slot);
        Assertions.assertEquals(0, emptyColumnStats.count, 0.001);
        Assertions.assertEquals(0, emptyColumnStats.ndv, 0.001);
        Assertions.assertEquals(0, emptyColumnStats.numNulls, 0.001);
        assertRowCount(emptyScan, 0);
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

    private void assertRowCount(LogicalFileScan scan, double expected) {
        StatsCalculator calculator = new StatsCalculator(null);
        Assertions.assertEquals(expected, scan.accept(calculator, null).getRowCount(), 0.001);
        Assertions.assertEquals(expected, toPhysical(scan).accept(calculator, null).getRowCount(), 0.001);
    }

    private double scanCpuCost(LogicalFileScan scan) {
        PhysicalFileScan physical = toPhysical(scan);
        GroupExpression expression = new GroupExpression(physical);
        new Group(null, expression, physical.getLogicalProperties());
        StatsCalculator.estimate(expression, null);
        return CostCalculator.calculateCost(connectContext, expression, ImmutableList.of()).getCpuCost();
    }

    private PartitionItem newPartition(int value) throws Exception {
        PartitionKey key = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue(Integer.toString(value))), ImmutableList.of(partitionColumn));
        return new ListPartitionItem(ImmutableList.of(key));
    }
}
