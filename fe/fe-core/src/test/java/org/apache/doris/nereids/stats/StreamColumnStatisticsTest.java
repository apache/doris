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

package org.apache.doris.nereids.stats;

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapChangelogTableWrapper;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTableWrapper;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.analysis.AnalysisManager;
import org.apache.doris.statistics.cache.StatisticsCache;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.statistics.model.PartitionColumnStatistic;
import org.apache.doris.statistics.model.PartitionColumnStatisticBuilder;
import org.apache.doris.statistics.model.Statistics;
import org.apache.doris.statistics.util.Hll128;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.BitSet;

class StreamColumnStatisticsTest {
    @Test
    void testLowAndHighNdv() {
        ColumnStatistic low = statistic(10_000_000, 24, 1_000_000);
        ColumnStatistic high = statistic(10_000_000, 9_000_000, 1_000_000);
        ColumnStatistic lowResult = estimate(low, 1_000_000);
        ColumnStatistic highResult = estimate(high, 1_000_000);
        Assertions.assertEquals(24, lowResult.ndv);
        Assertions.assertEquals(900_000, highResult.ndv);
        Assertions.assertEquals(100_000, lowResult.numNulls);
        Assertions.assertEquals(1_000_000, highResult.count);
        Assertions.assertEquals(3_600_004, highResult.dataSize);
        Assertions.assertEquals(highResult.ndv, highResult.original.ndv);
    }

    @Test
    void testNullsMissingStatisticsAndEmptyPopulation() {
        ColumnStatistic allNull = estimate(statistic(100, 0, 100), 20);
        Assertions.assertEquals(20, allNull.numNulls);
        Assertions.assertEquals(0, allNull.ndv);
        Assertions.assertSame(ColumnStatistic.UNKNOWN, estimate(ColumnStatistic.UNKNOWN, 20));
        Assertions.assertSame(ColumnStatistic.UNKNOWN, estimate(statistic(0, 0, 0), 20));
        Assertions.assertSame(ColumnStatistic.UNKNOWN, estimate(statistic(-1, 10, 0), 20));
        Assertions.assertSame(ColumnStatistic.UNKNOWN, estimate(statistic(100, 10, 0), -1));
        ColumnStatistic empty = estimate(ColumnStatistic.UNKNOWN, 0);
        Assertions.assertFalse(empty.isUnKnown);
        Assertions.assertEquals(0, empty.count);
        Assertions.assertEquals(0, empty.ndv);
        Assertions.assertEquals(0, empty.numNulls);
        Assertions.assertNull(empty.minExpr);
    }

    @Test
    void testSourcePopulationAndCacheArePreserved() {
        ColumnStatistic source = new ColumnStatisticBuilder(statistic(1000, 100, 900))
                .setMinValue(1).setMinExpr(new IntLiteral(1))
                .setMaxValue(100).setMaxExpr(new IntLiteral(100))
                .setUpdatedTime("2026-11-01 00:00:00")
                .setHotValues(ImmutableMap.of(new IntegerLiteral(1), 10f)).build();
        ColumnStatistic small = estimate(source, 10);
        ColumnStatistic large = estimate(source, 100);
        Assertions.assertEquals(9, small.numNulls);
        Assertions.assertEquals(1, small.ndv);
        Assertions.assertEquals(90, large.numNulls);
        Assertions.assertEquals(10, large.ndv);
        Assertions.assertEquals(1000, source.count);
        Assertions.assertEquals(900, source.numNulls);
        Assertions.assertEquals(100, source.ndv);
        Assertions.assertEquals(100, source.original.ndv);
        Assertions.assertEquals(1, source.hotValues.size());
        Assertions.assertNull(small.hotValues);
        Assertions.assertEquals(source.updatedTime, small.updatedTime);
        Assertions.assertSame(source.minExpr, small.minExpr);
        Assertions.assertSame(source.maxExpr, small.maxExpr);
        Assertions.assertEquals(source.minValue, small.minValue);
        Assertions.assertEquals(source.maxValue, small.maxValue);
        Assertions.assertEquals(small.ndv, small.original.ndv);
        Assertions.assertNotSame(source.original, small.original);
    }

    @Test
    void testAverageWidthFallback() {
        ColumnStatistic source = new ColumnStatisticBuilder(statistic(100, 10, 0)).setAvgSizeByte(0).build();
        Assertions.assertEquals(4, estimate(source, 20).avgSizeByte);
    }

    @Test
    void testChangelogMapsToBaseColumnAndChecksAdjustedNdv() {
        try (ScanFixture fixture = new ScanFixture(OlapChangelogTableWrapper.class, 100)) {
            // The current table has grown since ANALYZE. Use the source count=1000, not
            // the current row count=10000, as the denominator for its 900 NULLs.
            Mockito.when(fixture.source.getRowCount()).thenReturn(10_000L);
            fixture.tableStatistic(statistic(1000, 100, 900));
            Statistics stats = fixture.compute();
            ColumnStatistic column = stats.findColumnStatistics(fixture.slot);
            Assertions.assertEquals(90, column.numNulls);
            Assertions.assertEquals(10, column.ndv);
            Assertions.assertEquals(100, column.count);
            Assertions.assertTrue(stats.findColumnStatistics(fixture.internalSlot).isUnKnown);
            Mockito.verify(fixture.cache).getOlapTableStats(fixture.source, 10L);

            CascadesContext context = Mockito.mock(CascadesContext.class);
            Mockito.when(context.getConnectContext()).thenReturn(fixture.context);
            Assertions.assertFalse(StatsCalculator.disableJoinReorderIfStatsInvalid(
                    ImmutableList.of(fixture.scan), context).isPresent());
            Assertions.assertFalse(fixture.context.getSessionVariable().isDisableJoinReorder());
        }
    }

    @Test
    void testHiddenBeforeColumnMapsToOriginalBusinessColumn() {
        try (ScanFixture fixture = new ScanFixture(OlapChangelogTableWrapper.class, 100)) {
            fixture.tableStatistic(statistic(1000, 100, 900));
            Column before = Column.generateBeforeValueColumn(new Column("val", PrimitiveType.INT));
            before.setIsVisible(false);
            SlotReference beforeSlot = SlotReference.fromColumn(new ExprId(3), fixture.table, before,
                    ImmutableList.of("test", "tbl"));
            Mockito.when(fixture.scan.getOutput()).thenReturn(ImmutableList.of(fixture.slot, beforeSlot));
            ColumnStatistic result = fixture.compute().findColumnStatistics(beforeSlot);
            Assertions.assertEquals(90, result.numNulls);
            Assertions.assertEquals(10, result.ndv);
        }
    }

    @Test
    void testStreamAndHistoricalScansUseSourceStatistics() {
        for (Class<? extends OlapTable> type : ImmutableList.of(OlapTableStreamWrapper.class, OlapTableWrapper.class)) {
            try (ScanFixture fixture = new ScanFixture(type, 100)) {
                fixture.tableStatistic(statistic(1000, 100, 900));
                ColumnStatistic column = fixture.compute().findColumnStatistics(fixture.slot);
                Assertions.assertEquals(90, column.numNulls);
                Assertions.assertEquals(10, column.ndv);
                Mockito.verify(fixture.cache).getOlapTableStats(fixture.source, 10L);
            }
        }
    }

    @Test
    void testResetStreamUsesOrdinarySourceDistribution() {
        try (ScanFixture fixture = new ScanFixture(OlapTableStreamWrapper.class, 100)) {
            Mockito.when(((LogicalOlapTableStreamScan) fixture.scan).isReset()).thenReturn(true);
            fixture.tableStatistic(statistic(1000, 24, 10));
            ColumnStatistic result = fixture.compute().findColumnStatistics(fixture.slot);
            Assertions.assertEquals(24, result.ndv);
            Assertions.assertEquals(10, result.numNulls);
        }
    }

    @Test
    void testPrunedScanUsesItsOutputRowCountOnce() {
        try (ScanFixture fixture = new ScanFixture(OlapChangelogTableWrapper.class, 1000)) {
            Mockito.when(fixture.table.getPartitionNum()).thenReturn(3);
            Mockito.when(fixture.table.getRowCountForPartitionIndex(1L, 30L, true)).thenReturn(10L);
            Mockito.when(fixture.table.getRowCountForPartitionIndex(2L, 30L, true)).thenReturn(30L);
            fixture.tableStatistic(statistic(1000, 100, 900));
            Statistics result = fixture.compute();
            Assertions.assertEquals(40, result.getRowCount());
            Assertions.assertEquals(36, result.findColumnStatistics(fixture.slot).numNulls);
            Assertions.assertEquals(4, result.findColumnStatistics(fixture.slot).ndv);
        }
    }

    @Test
    void testUnknownRowCountDoesNotBecomeKnownOneRowDistribution() {
        try (ScanFixture fixture = new ScanFixture(OlapChangelogTableWrapper.class, -1)) {
            fixture.tableStatistic(statistic(1000, 100, 100));
            Statistics stats = fixture.compute();
            Assertions.assertEquals(1, stats.getRowCount());
            Assertions.assertTrue(stats.findColumnStatistics(fixture.slot).isUnKnown);
        }
    }

    @Test
    void testScanMinimumUsesOneConsistentPopulation() {
        try (ScanFixture fixture = new ScanFixture(OlapChangelogTableWrapper.class, 0)) {
            fixture.tableStatistic(statistic(1000, 100, 900));
            Statistics stats = fixture.compute();
            ColumnStatistic column = stats.findColumnStatistics(fixture.slot);
            Assertions.assertEquals(1, stats.getRowCount());
            Assertions.assertEquals(1, column.count);
            Assertions.assertEquals(0.9, column.numNulls, 1e-9);
            Assertions.assertEquals(0.1, column.ndv, 1e-9);
        }
    }

    @Test
    void testWeightedPartitionPopulationAndNoCacheMutation() {
        PartitionColumnStatistic first = partitionStatistic(100, 90, 1);
        PartitionColumnStatistic second = partitionStatistic(900, 90, 2);
        try (ScanFixture fixture = new ScanFixture(OlapChangelogTableWrapper.class, 100)) {
            fixture.context.getSessionVariable().enablePartitionAnalyze = true;
            fixture.tableStatistic(statistic(10_000, 1000, 9000));
            fixture.partitionStatistics(first, second);
            ColumnStatistic result = fixture.compute().findColumnStatistics(fixture.slot);
            Assertions.assertEquals(18, result.numNulls);
            Assertions.assertEquals(2, result.ndv);
            Assertions.assertEquals(100, result.count);
            Assertions.assertEquals(1, first.ndv.estimateCardinality());
            Assertions.assertEquals(1, second.ndv.estimateCardinality());
            Assertions.assertEquals(18, fixture.compute().findColumnStatistics(fixture.slot).numNulls);
        }
    }

    @Test
    void testMissingPartitionFallsBackAsOnePopulation() {
        try (ScanFixture fixture = new ScanFixture(OlapChangelogTableWrapper.class, 100)) {
            fixture.context.getSessionVariable().enablePartitionAnalyze = true;
            fixture.tableStatistic(statistic(1000, 100, 900));
            fixture.partitionStatistics(partitionStatistic(100, 10, 1), PartitionColumnStatistic.UNKNOWN);
            ColumnStatistic result = fixture.compute().findColumnStatistics(fixture.slot);
            Assertions.assertEquals(90, result.numNulls);
            Assertions.assertEquals(10, result.ndv);
        }
    }

    @Test
    void testCurrentSnapshotWrapperKeepsOrdinaryStatistics() {
        try (ScanFixture fixture = new ScanFixture(OlapTableWrapper.class, 100)) {
            Mockito.when(((OlapTableWrapper) fixture.table).getPartitionOffset(Mockito.anyLong())).thenReturn(null);
            ColumnStatistic source = statistic(1000, 24, 100);
            fixture.tableStatistic(source);
            ColumnStatistic result = fixture.compute().findColumnStatistics(fixture.slot);
            Assertions.assertEquals(24, result.ndv);
            Assertions.assertEquals(100, result.numNulls);
            Mockito.verify(fixture.cache).getOlapTableStats(fixture.source, 10L);
        }
    }

    @Test
    void testAliasesPropagateScanOriginalWithoutRescaling() {
        try (ScanFixture fixture = new ScanFixture(OlapChangelogTableWrapper.class, 100)) {
            fixture.tableStatistic(statistic(1000, 100, 900));
            Statistics scanStats = fixture.compute();
            Alias alias = new Alias(fixture.slot, "before_key");
            LogicalProject<LogicalOlapScan> project = new LogicalProject<>(ImmutableList.of(alias), fixture.scan);
            Statistics result = new StatsCalculator((CascadesContext) null).computeProject(project, scanStats);
            ColumnStatistic column = result.findColumnStatistics(alias.toSlot());
            Assertions.assertEquals(90, column.numNulls);
            Assertions.assertEquals(10, column.ndv);
            Assertions.assertEquals(10, column.original.ndv);
        }
    }

    @Test
    void testFilterAndSnapshotUnionUseAdjustedPopulationOnce() {
        try (ScanFixture fixture = new ScanFixture(OlapChangelogTableWrapper.class, 100)) {
            ColumnStatistic source = new ColumnStatisticBuilder(statistic(1000, 100, 900))
                    .setMinValue(1).setMaxValue(100).build();
            fixture.tableStatistic(source);
            Statistics left = fixture.compute();
            StatsCalculator calculator = new StatsCalculator((CascadesContext) null);
            LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                    ImmutableSet.of(new IsNull(fixture.slot)), fixture.scan);
            Statistics filtered = calculator.computeFilter(filter, left);
            Assertions.assertEquals(90, filtered.getRowCount());

            SlotReference rightSlot = new SlotReference("right_key", IntegerType.INSTANCE, true);
            ColumnStatistic rightColumn = estimate(source, 200);
            Statistics right = new Statistics(200, ImmutableMap.of(rightSlot, rightColumn));
            LogicalUnion union = new LogicalUnion(Qualifier.ALL, ImmutableList.of(fixture.slot),
                    ImmutableList.of(ImmutableList.of(fixture.slot), ImmutableList.of(rightSlot)),
                    ImmutableList.of(), false, ImmutableList.of(fixture.scan, fixture.scan));
            Statistics combined = calculator.computeUnion(union, ImmutableList.of(left, right));
            Assertions.assertEquals(300, combined.getRowCount());
            Assertions.assertEquals(270, combined.findColumnStatistics(fixture.slot).numNulls);
            Assertions.assertEquals(20, combined.findColumnStatistics(fixture.slot).ndv);
        }
    }

    private static ColumnStatistic statistic(double count, double ndv, double nulls) {
        return new ColumnStatisticBuilder(count).setNdv(ndv).setNumNulls(nulls).setAvgSizeByte(4).build();
    }

    private static ColumnStatistic estimate(ColumnStatistic source, double rows) {
        return StatsCalculator.estimateStreamColumnStatistics(source, rows, IntegerType.INSTANCE);
    }

    private static PartitionColumnStatistic partitionStatistic(double count, double nulls, long value) {
        Hll128 ndv = new Hll128();
        ndv.update(value);
        return new PartitionColumnStatisticBuilder(count).setNdv(ndv).setNumNulls(nulls).setAvgSizeByte(4).build();
    }

    private static class ScanFixture implements AutoCloseable {
        private final ConnectContext previousContext = ConnectContext.get();
        private final boolean previousInternalSchema = FeConstants.enableInternalSchemaDb;
        private final ConnectContext context = new ConnectContext();
        private final StatisticsCache cache = Mockito.mock(StatisticsCache.class);
        private final StatisticsCache.OlapTableStatistics sourceStats
                = Mockito.mock(StatisticsCache.OlapTableStatistics.class);
        private final OlapTable source = Mockito.mock(OlapTable.class);
        private final OlapTable table;
        private final LogicalOlapScan scan;
        private final SlotReference slot;
        private final SlotReference internalSlot;
        private final MockedStatic<Env> mockedEnv;

        ScanFixture(Class<? extends OlapTable> type, long rowCount) {
            context.setThreadLocalInfo();
            context.getSessionVariable().enableStats = true;
            context.getSessionVariable().enablePartitionAnalyze = false;
            FeConstants.enableInternalSchemaDb = true;
            table = Mockito.mock(type);
            scan = type == OlapTableStreamWrapper.class ? Mockito.mock(LogicalOlapTableStreamScan.class)
                    : Mockito.mock(LogicalOlapScan.class);
            if (table instanceof OlapTableStreamWrapper) {
                Mockito.when(((OlapTableStreamWrapper) table).getBaseTable()).thenReturn(source);
            } else {
                Mockito.when(((OlapTableWrapper) table).getOriginTable()).thenReturn(source);
                Mockito.when(((OlapTableWrapper) table).getPartitionOffset(Mockito.anyLong()))
                        .thenReturn(Pair.of(1L, 2L));
            }
            Column column = new Column("val", PrimitiveType.INT);
            slot = new SlotReference(new ExprId(1), "val", IntegerType.INSTANCE, true,
                    ImmutableList.of("test", "tbl"), table, column, table, column);
            Column internal = new Column(Column.CHANGELOG_TSO_COL, PrimitiveType.BIGINT);
            internal.setIsVisible(false);
            internalSlot = SlotReference.fromColumn(new ExprId(2), table, internal, ImmutableList.of("test", "tbl"));
            Mockito.when(source.getColumn("val")).thenReturn(column);
            Mockito.when(source.getBaseIndexId()).thenReturn(10L);
            Mockito.when(source.getId()).thenReturn(20L);
            long indexId = type == OlapChangelogTableWrapper.class ? 30L : 10L;
            Mockito.when(table.getBaseIndexId()).thenReturn(indexId);
            Mockito.when(table.getRowCountForIndex(indexId, true)).thenReturn(rowCount);
            Mockito.when(table.getPartitionNum()).thenReturn(2);
            Mockito.when(table.getQualifiedDbName()).thenReturn("test");
            Mockito.when(scan.getTable()).thenReturn(table);
            Mockito.when(scan.getSelectedIndexId()).thenReturn(indexId);
            Mockito.when(scan.getSelectedPartitionIds()).thenReturn(ImmutableList.of(1L, 2L));
            Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(slot, internalSlot));
            Mockito.when(scan.getAllChildrenTypes()).thenReturn(new BitSet());
            Mockito.when(scan.getVirtualColumns()).thenReturn(ImmutableList.of());
            for (long id = 1; id <= 2; id++) {
                Partition partition = Mockito.mock(Partition.class);
                Mockito.when(partition.getName()).thenReturn("p" + id);
                Mockito.when(source.getPartition(id)).thenReturn(partition);
            }
            Env env = Mockito.mock(Env.class);
            Mockito.when(env.getAnalysisManager()).thenReturn(Mockito.mock(AnalysisManager.class));
            Mockito.when(env.getStatisticsCache()).thenReturn(cache);
            Mockito.when(cache.getOlapTableStats(scan)).thenReturn(sourceStats);
            Mockito.when(cache.getOlapTableStats(source, 10L)).thenReturn(sourceStats);
            mockedEnv = Mockito.mockStatic(Env.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        }

        void tableStatistic(ColumnStatistic statistic) {
            Mockito.when(sourceStats.getColumnStatistics("val", context)).thenReturn(statistic);
        }

        void partitionStatistics(PartitionColumnStatistic first, PartitionColumnStatistic second) {
            Mockito.when(sourceStats.getPartitionColumnStatistics("p1", "val", context)).thenReturn(first);
            Mockito.when(sourceStats.getPartitionColumnStatistics("p2", "val", context)).thenReturn(second);
        }

        Statistics compute() {
            return new StatsCalculator((CascadesContext) null).computeOlapScan(scan);
        }

        @Override
        public void close() {
            mockedEnv.close();
            FeConstants.enableInternalSchemaDb = previousInternalSchema;
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }
}
