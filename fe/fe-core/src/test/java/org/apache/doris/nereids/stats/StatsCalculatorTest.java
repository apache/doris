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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.PartitionPrunablePredicate;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.analysis.AnalysisManager;
import org.apache.doris.statistics.cache.StatisticsCache;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.statistics.model.Statistics;
import org.apache.doris.statistics.model.StatisticsBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StatsCalculatorTest {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    private Group newFakeGroup() {
        GroupExpression groupExpression = new GroupExpression(scan1);
        Group group = new Group(null, groupExpression,
                new LogicalProperties(Collections::emptyList, () -> DataTrait.EMPTY_TRAIT));
        group.getLogicalExpressions().remove(0);
        return group;
    }

    private SelectedPartitions selectedPartitions(SlotReference partitionSlot, Expression partitionPredicate) {
        PartitionItem p1 = Mockito.mock(PartitionItem.class);
        PartitionItem p2 = Mockito.mock(PartitionItem.class);
        SelectedPartitions initial = new SelectedPartitions(
                2, ImmutableMap.of("p1", p1, "p2", p2), false);
        return initial.withPruneResult(ImmutableMap.of("p1", p1), true,
                ImmutableList.of(partitionSlot), ImmutableSet.of(partitionPredicate));
    }

    @Test
    public void testFilter() {
        List<String> qualifier = Lists.newArrayList();
        qualifier.add("test");
        qualifier.add("t");
        SlotReference slot1 = new SlotReference("c1", IntegerType.INSTANCE, true, qualifier);
        SlotReference slot2 = new SlotReference("c2", IntegerType.INSTANCE, true, qualifier);

        ColumnStatisticBuilder columnStat1 = new ColumnStatisticBuilder();
        columnStat1.setNdv(10);
        columnStat1.setMinValue(0);
        columnStat1.setMaxValue(1000);
        columnStat1.setNumNulls(10);
        ColumnStatisticBuilder columnStat2 = new ColumnStatisticBuilder();
        columnStat2.setNdv(20);
        columnStat2.setMinValue(0);
        columnStat2.setMaxValue(1000);
        columnStat2.setNumNulls(10);

        Map<Expression, ColumnStatistic> slotColumnStatsMap = new HashMap<>();
        slotColumnStatsMap.put(slot1, columnStat1.build());
        slotColumnStatsMap.put(slot2, columnStat2.build());
        Statistics childStats = new Statistics(10000, slotColumnStatsMap);

        EqualTo eq1 = new EqualTo(slot1, new IntegerLiteral(1));
        EqualTo eq2 = new EqualTo(slot2, new IntegerLiteral(2));

        ImmutableSet and = ImmutableSet.of(eq1, eq2);
        ImmutableSet or = ImmutableSet.of(new Or(eq1, eq2));

        Group childGroup = newFakeGroup();
        GroupPlan groupPlan = new GroupPlan(childGroup);
        childGroup.setStatistics(childStats);

        LogicalFilter<GroupPlan> logicalFilter = new LogicalFilter<>(and, groupPlan);
        GroupExpression groupExpression = new GroupExpression(logicalFilter, ImmutableList.of(childGroup));
        Group ownerGroup = new Group(null, groupExpression, null);
        StatsCalculator.estimate(groupExpression, null);
        Assertions.assertEquals(49.90005, ownerGroup.getStatistics().getRowCount(), 0.001);

        LogicalFilter<GroupPlan> logicalFilterOr = new LogicalFilter<>(or, groupPlan);
        GroupExpression groupExpressionOr = new GroupExpression(logicalFilterOr, ImmutableList.of(childGroup));
        Group ownerGroupOr = new Group(null, groupExpressionOr, null);
        StatsCalculator.estimate(groupExpressionOr, null);
        Assertions.assertEquals(1448.555,
                ownerGroupOr.getStatistics().getRowCount(), 0.1);
    }

    @Test
    public void testFilterSkipsConjunctAlreadyAppliedToScanRowCount() {
        SlotReference partitionSlot = new SlotReference("p", IntegerType.INSTANCE);
        SlotReference valueSlot = new SlotReference("v", IntegerType.INSTANCE);
        EqualTo partitionPredicate = new EqualTo(partitionSlot, new IntegerLiteral(1));
        EqualTo valuePredicate = new EqualTo(valueSlot, new IntegerLiteral(2));
        Statistics input = new StatisticsBuilder()
                .setRowCount(1000)
                .putColumnStatistics(partitionSlot, new ColumnStatisticBuilder(1000).setNdv(100).build())
                .putColumnStatistics(valueSlot, new ColumnStatisticBuilder(1000).setNdv(10).build())
                .setConjunctsAppliedToRowCount(ImmutableSet.of(partitionPredicate))
                .build();
        StatsCalculator calculator = new StatsCalculator((CascadesContext) null);

        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(partitionPredicate, valuePredicate), scan1);
        Statistics filtered = calculator.computeFilter(filter, input);
        Assertions.assertEquals(100, filtered.getRowCount(), 0.001);
        Assertions.assertEquals(1, filtered.findColumnStatistics(partitionSlot).ndv, 0.001);
        Assertions.assertEquals(1, filtered.findColumnStatistics(valueSlot).ndv, 0.001);
    }

    @Test
    public void testAppliedConjunctsDoNotCapUnrelatedPartitionColumnNdv() {
        SlotReference firstPartitionSlot = new SlotReference("p1", IntegerType.INSTANCE);
        SlotReference secondPartitionSlot = new SlotReference("p2", IntegerType.INSTANCE);
        EqualTo firstPredicate = new EqualTo(firstPartitionSlot, new IntegerLiteral(1));
        GreaterThanEqual secondPredicate = new GreaterThanEqual(secondPartitionSlot, new IntegerLiteral(1));
        ColumnStatistic partitionColumnStats = new ColumnStatisticBuilder(1000)
                .setNdv(100)
                .setMinValue(1)
                .setMinExpr(new IntLiteral(1))
                .setMaxValue(100)
                .setMaxExpr(new IntLiteral(100))
                .build();
        Statistics input = new StatisticsBuilder()
                .setRowCount(1000)
                .putColumnStatistics(firstPartitionSlot, partitionColumnStats)
                .putColumnStatistics(secondPartitionSlot, partitionColumnStats)
                .setConjunctsAppliedToRowCount(ImmutableSet.of(firstPredicate, secondPredicate))
                .build();
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(firstPredicate, secondPredicate), scan1);

        Statistics filtered = new StatsCalculator((CascadesContext) null).computeFilter(filter, input);

        Assertions.assertEquals(1000, filtered.getRowCount(), 0.001);
        Assertions.assertEquals(1, filtered.findColumnStatistics(firstPartitionSlot).ndv, 0.001);
        Assertions.assertEquals(100, filtered.findColumnStatistics(secondPartitionSlot).ndv, 0.001);
    }

    @Test
    public void testAppliedBoundsOnSameSlotShareOneEstimationBasis() {
        SlotReference partitionSlot = new SlotReference("p", IntegerType.INSTANCE);
        GreaterThanEqual lowerBound = new GreaterThanEqual(partitionSlot, new IntegerLiteral(4501));
        LessThanEqual upperBound = new LessThanEqual(partitionSlot, new IntegerLiteral(4600));
        ColumnStatistic tableColumnStats = new ColumnStatisticBuilder(100_000)
                .setNdv(10_000)
                .setMinValue(1)
                .setMinExpr(new IntLiteral(1))
                .setMaxValue(10_000)
                .setMaxExpr(new IntLiteral(10_000))
                .build();
        ColumnStatistic selectedColumnStats = new ColumnStatisticBuilder(tableColumnStats, 1000)
                .setNdv(1000)
                .build();
        Statistics input = new StatisticsBuilder()
                .setRowCount(1000)
                .putColumnStatistics(partitionSlot, selectedColumnStats)
                .setConjunctsAppliedToRowCount(ImmutableSet.of(lowerBound, upperBound))
                .build();
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(lowerBound, upperBound), scan1);

        Statistics filtered = new StatsCalculator((CascadesContext) null).computeFilter(filter, input);

        Assertions.assertEquals(1000, filtered.getRowCount(), 0.001);
        double ndv = filtered.findColumnStatistics(partitionSlot).ndv;
        Assertions.assertTrue(ndv > 90 && ndv <= 100);
        Assertions.assertEquals(4501, filtered.findColumnStatistics(partitionSlot).minValue, 0.001);
        Assertions.assertEquals(4600, filtered.findColumnStatistics(partitionSlot).maxValue, 0.001);
    }

    @Test
    public void testAppliedPredicateKeepsExistingPartitionBoundsForRemainingPredicate() {
        SlotReference partitionSlot = new SlotReference("p", IntegerType.INSTANCE);
        SlotReference valueSlot = new SlotReference("v", IntegerType.INSTANCE);
        GreaterThanEqual appliedPredicate = new GreaterThanEqual(partitionSlot, new IntegerLiteral(1));
        LessThan remainingPredicate = new LessThan(partitionSlot, valueSlot);
        ColumnStatistic tablePartitionStats = new ColumnStatisticBuilder(100_000)
                .setNdv(10_000)
                .setMinValue(1)
                .setMinExpr(new IntLiteral(1))
                .setMaxValue(10_000)
                .setMaxExpr(new IntLiteral(10_000))
                .build();
        ColumnStatistic selectedPartitionStats = new ColumnStatisticBuilder(tablePartitionStats, 1000)
                .setNdv(1000)
                .setMinValue(1)
                .setMinExpr(new IntLiteral(1))
                .setMaxValue(2)
                .setMaxExpr(new IntLiteral(2))
                .build();
        ColumnStatistic valueStats = new ColumnStatisticBuilder(1000)
                .setNdv(100)
                .setMinValue(100)
                .setMinExpr(new IntLiteral(100))
                .setMaxValue(200)
                .setMaxExpr(new IntLiteral(200))
                .build();
        Statistics input = new StatisticsBuilder()
                .setRowCount(1000)
                .putColumnStatistics(partitionSlot, selectedPartitionStats)
                .putColumnStatistics(valueSlot, valueStats)
                .setConjunctsAppliedToRowCount(ImmutableSet.of(appliedPredicate))
                .build();
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(appliedPredicate, remainingPredicate), scan1);

        Statistics filtered = new StatsCalculator((CascadesContext) null).computeFilter(filter, input);

        Assertions.assertEquals(1000, filtered.getRowCount(), 0.001);
        Assertions.assertEquals(1, filtered.findColumnStatistics(partitionSlot).minValue, 0.001);
        Assertions.assertEquals(2, filtered.findColumnStatistics(partitionSlot).maxValue, 0.001);
    }

    @Test
    public void testRestoredAppliedConjunctStatsKeepNullCountNonNegative() {
        SlotReference partitionSlot = new SlotReference("p", IntegerType.INSTANCE);
        GreaterThanEqual appliedPredicate = new GreaterThanEqual(partitionSlot, new IntegerLiteral(9901));
        Not remainingPredicate = new Not(new EqualTo(partitionSlot, new IntegerLiteral(9999)));
        ColumnStatistic tableColumnStats = new ColumnStatisticBuilder(100_000)
                .setNdv(10_000)
                .setNumNulls(0)
                .setMinValue(1)
                .setMinExpr(new IntLiteral(1))
                .setMaxValue(10_000)
                .setMaxExpr(new IntLiteral(10_000))
                .build();
        ColumnStatistic selectedColumnStats = new ColumnStatisticBuilder(tableColumnStats, 1000)
                .setNdv(1000)
                .setMinValue(9901)
                .setMinExpr(new IntLiteral(9901))
                .setMaxValue(10_000)
                .setMaxExpr(new IntLiteral(10_000))
                .build();
        Statistics input = new StatisticsBuilder()
                .setRowCount(1000)
                .putColumnStatistics(partitionSlot, selectedColumnStats)
                .setConjunctsAppliedToRowCount(ImmutableSet.of(appliedPredicate))
                .build();
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(appliedPredicate, remainingPredicate), scan1);

        Statistics filtered = new StatsCalculator((CascadesContext) null).computeFilter(filter, input);

        Assertions.assertTrue(filtered.getRowCount() <= 1000);
        Assertions.assertEquals(0, filtered.findColumnStatistics(partitionSlot).numNulls, 0.001);
    }

    @Test
    public void testAppliedMultiColumnConjunctDoesNotCapColumnNdv() {
        SlotReference firstPartitionSlot = new SlotReference("p1", IntegerType.INSTANCE);
        SlotReference secondPartitionSlot = new SlotReference("p2", IntegerType.INSTANCE);
        EqualTo partitionPredicate = new EqualTo(firstPartitionSlot, secondPartitionSlot);
        ColumnStatistic partitionColumnStats = new ColumnStatisticBuilder(1000)
                .setNdv(100)
                .setMinValue(1)
                .setMinExpr(new IntLiteral(1))
                .setMaxValue(100)
                .setMaxExpr(new IntLiteral(100))
                .build();
        Statistics input = new StatisticsBuilder()
                .setRowCount(1000)
                .putColumnStatistics(firstPartitionSlot, partitionColumnStats)
                .putColumnStatistics(secondPartitionSlot, partitionColumnStats)
                .setConjunctsAppliedToRowCount(ImmutableSet.of(partitionPredicate))
                .build();
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(partitionPredicate), scan1);

        Statistics filtered = new StatsCalculator((CascadesContext) null).computeFilter(filter, input);

        Assertions.assertEquals(1000, filtered.getRowCount(), 0.001);
        Assertions.assertEquals(100, filtered.findColumnStatistics(firstPartitionSlot).ndv, 0.001);
        Assertions.assertEquals(100, filtered.findColumnStatistics(secondPartitionSlot).ndv, 0.001);
    }

    @Test
    public void testAppliedIsNullConjunctRestoresSelectedRowNullCount() {
        SlotReference partitionSlot = new SlotReference("p", IntegerType.INSTANCE);
        IsNull partitionPredicate = new IsNull(partitionSlot);
        Statistics input = new StatisticsBuilder()
                .setRowCount(1000)
                .putColumnStatistics(partitionSlot, new ColumnStatisticBuilder(1000)
                        .setNdv(100)
                        .setNumNulls(100)
                        .build())
                .setConjunctsAppliedToRowCount(ImmutableSet.of(partitionPredicate))
                .build();
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(partitionPredicate), scan1);

        Statistics filtered = new StatsCalculator((CascadesContext) null).computeFilter(filter, input);

        Assertions.assertEquals(1000, filtered.getRowCount(), 0.001);
        Assertions.assertEquals(0, filtered.findColumnStatistics(partitionSlot).ndv, 0.001);
        Assertions.assertEquals(1000, filtered.findColumnStatistics(partitionSlot).numNulls, 0.001);
    }

    @ParameterizedTest(name = "selected row count {0} produces scan row count {1}")
    @CsvSource({"7, 7", "0, 1"})
    public void testFileScanUsesKnownSelectedPartitionRowCount(long selectedRowCount, double expectedRowCount) {
        boolean previous = FeConstants.enableInternalSchemaDb;
        try {
            FeConstants.enableInternalSchemaDb = false;
            SlotReference partitionSlot = new SlotReference("p", IntegerType.INSTANCE);
            EqualTo partitionPredicate = new EqualTo(partitionSlot, new IntegerLiteral(1));
            SelectedPartitions selectedPartitions = selectedPartitions(partitionSlot, partitionPredicate);
            ExternalTable table = Mockito.mock(ExternalTable.class);
            LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
            Mockito.when(scan.getTable()).thenReturn(table);
            Mockito.when(scan.getSelectedPartitions()).thenReturn(selectedPartitions);
            Mockito.when(scan.getTableSnapshot()).thenReturn(Optional.empty());
            Mockito.when(scan.getScanParams()).thenReturn(Optional.empty());
            Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(partitionSlot));
            Mockito.when(table.getRowCountForSelectedPartitions(
                    Mockito.eq(selectedPartitions), Mockito.any())).thenReturn(selectedRowCount);

            Statistics statistics = new StatsCalculator((CascadesContext) null).computeFileScan(scan);

            Assertions.assertEquals(expectedRowCount, statistics.getRowCount(), 0.001);
            Assertions.assertEquals(ImmutableSet.of(partitionPredicate),
                    statistics.getConjunctsAppliedToRowCount());
            Mockito.verify(table, Mockito.never()).getRowCount();
        } finally {
            FeConstants.enableInternalSchemaDb = previous;
        }
    }

    @Test
    public void testFileScanKeepsPartitionPredicateWhenSelectedRowCountIsUnknown() {
        boolean previous = FeConstants.enableInternalSchemaDb;
        try {
            FeConstants.enableInternalSchemaDb = false;
            SlotReference partitionSlot = new SlotReference("p", IntegerType.INSTANCE);
            EqualTo partitionPredicate = new EqualTo(partitionSlot, new IntegerLiteral(1));
            SelectedPartitions selectedPartitions = selectedPartitions(partitionSlot, partitionPredicate);
            ExternalTable table = Mockito.mock(ExternalTable.class);
            LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
            Mockito.when(scan.getTable()).thenReturn(table);
            Mockito.when(scan.getSelectedPartitions()).thenReturn(selectedPartitions);
            Mockito.when(scan.getTableSnapshot()).thenReturn(Optional.empty());
            Mockito.when(scan.getScanParams()).thenReturn(Optional.empty());
            Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(partitionSlot));
            Mockito.when(table.getRowCountForSelectedPartitions(
                    Mockito.eq(selectedPartitions), Mockito.any())).thenReturn(TableIf.UNKNOWN_ROW_COUNT);
            Mockito.when(table.getRowCount()).thenReturn(1000L);

            StatsCalculator calculator = new StatsCalculator((CascadesContext) null);
            Statistics scanStatistics = calculator.computeFileScan(scan);

            Assertions.assertEquals(1000, scanStatistics.getRowCount(), 0.001);
            Assertions.assertTrue(scanStatistics.getConjunctsAppliedToRowCount().isEmpty());

            Statistics input = new StatisticsBuilder(scanStatistics)
                    .putColumnStatistics(partitionSlot,
                            new ColumnStatisticBuilder(1000).setNdv(100).build())
                    .build();
            LogicalFilter<LogicalOlapScan> filter =
                    new LogicalFilter<>(ImmutableSet.of(partitionPredicate), scan1);
            Assertions.assertEquals(10, calculator.computeFilter(filter, input).getRowCount(), 0.001);
        } finally {
            FeConstants.enableInternalSchemaDb = previous;
        }
    }

    @Test
    public void testFileScanDoesNotApplyPartitionProofForDeferredEmptyPartitionUniverse() {
        boolean previous = FeConstants.enableInternalSchemaDb;
        try {
            FeConstants.enableInternalSchemaDb = false;
            SlotReference partitionSlot = new SlotReference("p", IntegerType.INSTANCE);
            EqualTo partitionPredicate = new EqualTo(partitionSlot, new IntegerLiteral(1));
            SelectedPartitions selectedPartitions = new SelectedPartitions(0, ImmutableMap.of(), false)
                    .withPruneResult(ImmutableMap.of(), false,
                            ImmutableList.of(partitionSlot), ImmutableSet.of(partitionPredicate));
            ExternalTable table = Mockito.mock(ExternalTable.class);
            LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
            Mockito.when(scan.getTable()).thenReturn(table);
            Mockito.when(scan.getSelectedPartitions()).thenReturn(selectedPartitions);
            Mockito.when(scan.getTableSnapshot()).thenReturn(Optional.empty());
            Mockito.when(scan.getScanParams()).thenReturn(Optional.empty());
            Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(partitionSlot));
            Mockito.when(table.getRowCountForSelectedPartitions(
                    Mockito.eq(selectedPartitions), Mockito.any())).thenReturn(TableIf.UNKNOWN_ROW_COUNT);
            Mockito.when(table.getRowCount()).thenReturn(1000L);

            Statistics statistics = new StatsCalculator((CascadesContext) null).computeFileScan(scan);

            Assertions.assertEquals(1000, statistics.getRowCount(), 0.001);
            Assertions.assertTrue(statistics.getConjunctsAppliedToRowCount().isEmpty());
        } finally {
            FeConstants.enableInternalSchemaDb = previous;
        }
    }

    @Test
    public void testFileScanScalesNullCountToSelectedPartitionRows() {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        boolean previous = FeConstants.enableInternalSchemaDb;
        FeConstants.enableInternalSchemaDb = true;

        Env env = Mockito.mock(Env.class);
        StatisticsCache statisticsCache = Mockito.mock(StatisticsCache.class);
        ExternalTable table = Mockito.mock(ExternalTable.class);
        LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
        SlotReference slot = new SlotReference("v", IntegerType.INSTANCE);
        PartitionItem p1 = Mockito.mock(PartitionItem.class);
        PartitionItem p2 = Mockito.mock(PartitionItem.class);
        SelectedPartitions selectedPartitions = new SelectedPartitions(
                2, ImmutableMap.of("p1", p1, "p2", p2), false)
                .withPruneResult(ImmutableMap.of("p1", p1), true,
                        ImmutableList.of(), ImmutableSet.of());
        ColumnStatistic columnStatistic = new ColumnStatisticBuilder(1_000_000)
                .setNdv(500_000)
                .setNumNulls(500_000)
                .build();

        Mockito.when(env.getStatisticsCache()).thenReturn(statisticsCache);
        Mockito.when(statisticsCache.getColumnStatistics(
                -1, -1, 1, -1, "v", connectContext)).thenReturn(columnStatistic);
        Mockito.when(table.getId()).thenReturn(1L);
        Mockito.when(table.getRowCountForSelectedPartitions(
                Mockito.eq(selectedPartitions), Mockito.any())).thenReturn(1000L);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getSelectedPartitions()).thenReturn(selectedPartitions);
        Mockito.when(scan.getTableSnapshot()).thenReturn(Optional.empty());
        Mockito.when(scan.getScanParams()).thenReturn(Optional.empty());
        Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(slot));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            Statistics statistics = new StatsCalculator((CascadesContext) null).computeFileScan(scan);

            Assertions.assertEquals(1000, statistics.getRowCount(), 0.001);
            Assertions.assertEquals(500, statistics.findColumnStatistics(slot).ndv, 0.001);
            Assertions.assertEquals(500, statistics.findColumnStatistics(slot).numNulls, 0.001);
        } finally {
            FeConstants.enableInternalSchemaDb = previous;
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    // a, b are in (0,100)
    // a=200 and b=300 => output: 0 rows
    @Test
    public void testFilterOutofRange() {
        List<String> qualifier = ImmutableList.of("test", "t");
        SlotReference slot1 = new SlotReference("c1", IntegerType.INSTANCE, true, qualifier);
        SlotReference slot2 = new SlotReference("c2", IntegerType.INSTANCE, true, qualifier);

        ColumnStatisticBuilder columnStat1 = new ColumnStatisticBuilder();
        columnStat1.setNdv(10);
        columnStat1.setMinValue(0);
        columnStat1.setMaxValue(100);
        columnStat1.setNumNulls(10);
        ColumnStatisticBuilder columnStat2 = new ColumnStatisticBuilder();
        columnStat2.setNdv(20);
        columnStat2.setMinValue(0);
        columnStat2.setMaxValue(100);
        columnStat2.setNumNulls(10);

        Map<Expression, ColumnStatistic> slotColumnStatsMap = new HashMap<>();
        slotColumnStatsMap.put(slot1, columnStat1.build());
        slotColumnStatsMap.put(slot2, columnStat2.build());
        Statistics childStats = new Statistics(10000, slotColumnStatsMap);

        EqualTo eq1 = new EqualTo(slot1, new IntegerLiteral(200));
        EqualTo eq2 = new EqualTo(slot2, new IntegerLiteral(300));

        ImmutableSet and = ImmutableSet.of(eq1, eq2);
        ImmutableSet or = ImmutableSet.of(new Or(eq1, eq2));

        Group childGroup = newFakeGroup();
        GroupPlan groupPlan = new GroupPlan(childGroup);
        childGroup.setStatistics(childStats);

        LogicalFilter<GroupPlan> logicalFilter = new LogicalFilter<>(and, groupPlan);
        GroupExpression groupExpression = new GroupExpression(logicalFilter, ImmutableList.of(childGroup));
        Group ownerGroup = new Group(null, groupExpression, null);
        groupExpression.setOwnerGroup(ownerGroup);
        StatsCalculator.estimate(groupExpression, null);
        Assertions.assertEquals(0, ownerGroup.getStatistics().getRowCount(), 0.001);

        LogicalFilter<GroupPlan> logicalFilterOr = new LogicalFilter<>(or, groupPlan);
        GroupExpression groupExpressionOr = new GroupExpression(logicalFilterOr, ImmutableList.of(childGroup));
        Group ownerGroupOr = new Group(null, groupExpressionOr, null);
        groupExpressionOr.setOwnerGroup(ownerGroupOr);
        StatsCalculator.estimate(groupExpressionOr, null);
        Assertions.assertEquals(0, ownerGroupOr.getStatistics().getRowCount(), 0.001);
    }

    @Test
    public void testOlapScan() {
        long tableId1 = 0;
        OlapTable table1 = PlanConstructor.newOlapTable(tableId1, "t1", 0);
        List<String> qualifier = ImmutableList.of("test", "t");
        SlotReference slot1 = new SlotReference(new ExprId(0), "c1", IntegerType.INSTANCE, true, qualifier,
                table1, new Column("c1", PrimitiveType.INT),
                table1, new Column("c1", PrimitiveType.INT));

        LogicalOlapScan logicalOlapScan1 = (LogicalOlapScan) new LogicalOlapScan(
                StatementScopeIdGenerator.newRelationId(), table1,
                Collections.emptyList()).withGroupExprLogicalPropChildren(Optional.empty(),
                Optional.of(new LogicalProperties(() -> ImmutableList.of(slot1), () -> DataTrait.EMPTY_TRAIT)), ImmutableList.of());

        GroupExpression groupExpression = new GroupExpression(logicalOlapScan1, ImmutableList.of());
        Group ownerGroup = new Group(null, groupExpression, null);
        StatsCalculator.estimate(groupExpression, null);
        Statistics stats = ownerGroup.getStatistics();
        Assertions.assertEquals(1, stats.columnStatistics().size());
        Assertions.assertNotNull(stats.columnStatistics().get(slot1));
    }

    @Test
    public void testComputeOlapScanScalesNumNullsForSelectedPartitions() {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();

        Env env = Mockito.mock(Env.class);
        AnalysisManager analysisManager = Mockito.mock(AnalysisManager.class);
        StatisticsCache statisticsCache = Mockito.mock(StatisticsCache.class);
        StatisticsCache.OlapTableStatistics olapTableStatistics
                = Mockito.mock(StatisticsCache.OlapTableStatistics.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        LogicalOlapScan scan = Mockito.mock(LogicalOlapScan.class);
        Partition selectedPartition = Mockito.mock(Partition.class);

        long selectedPartitionId = 1L;
        long baseIndexId = 10L;
        Column column = new Column("val", PrimitiveType.INT);
        SlotReference slot = new SlotReference(new ExprId(1), "val", IntegerType.INSTANCE, true,
                ImmutableList.of("test", "tbl"), table, column, table, column);
        ColumnStatistic tableStatistic = new ColumnStatisticBuilder(12)
                .setNdv(1)
                .setMinValue(2)
                .setMaxValue(2)
                .setMinExpr(new IntLiteral(2))
                .setMaxExpr(new IntLiteral(2))
                .setNumNulls(3)
                .build();

        Mockito.when(env.getAnalysisManager()).thenReturn(analysisManager);
        Mockito.when(env.getStatisticsCache()).thenReturn(statisticsCache);
        Mockito.when(statisticsCache.getOlapTableStats(scan)).thenReturn(olapTableStatistics);
        Mockito.when(olapTableStatistics.getColumnStatistics("val", connectContext)).thenReturn(tableStatistic);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getSelectedIndexId()).thenReturn(baseIndexId);
        Mockito.when(scan.getSelectedPartitionIds()).thenReturn(ImmutableList.of(selectedPartitionId));
        Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(slot));
        Mockito.when(scan.getVirtualColumns()).thenReturn(ImmutableList.of());
        EqualTo partitionPredicate = new EqualTo(slot, new IntegerLiteral(2));
        Mockito.when(scan.getPartitionPrunablePredicates()).thenReturn(Optional.of(
                new PartitionPrunablePredicate(ImmutableSet.of(selectedPartitionId),
                        ImmutableList.of(slot), ImmutableSet.of(partitionPredicate))));
        Mockito.when(table.getBaseIndexId()).thenReturn(baseIndexId);
        Mockito.when(table.getRowCountForIndex(baseIndexId, true)).thenReturn(12L);
        Mockito.when(table.getRowCountForSelectedPartitions(
                ImmutableList.of(selectedPartitionId), baseIndexId, 12D)).thenReturn(4D);
        Mockito.when(table.getPartitionNum()).thenReturn(3);
        Mockito.when(table.getPartition(selectedPartitionId)).thenReturn(selectedPartition);
        Mockito.when(table.getQualifiedDbName()).thenReturn("test");
        Mockito.when(selectedPartition.getName()).thenReturn("p1");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            Statistics statistics = new StatsCalculator((CascadesContext) null).computeOlapScan(scan);
            ColumnStatistic result = statistics.findColumnStatistics(slot);

            Assertions.assertEquals(4, statistics.getRowCount(), 0.001);
            Assertions.assertNotNull(result);
            Assertions.assertEquals(1, result.ndv, 0.001);
            Assertions.assertEquals(2, result.minValue, 0.001);
            Assertions.assertEquals(2, result.maxValue, 0.001);
            Assertions.assertEquals(4, result.count, 0.001);
            Assertions.assertEquals(1, result.numNulls, 0.001);
            Assertions.assertEquals("2", result.minExpr.getStringValue());
            Assertions.assertEquals("2", result.maxExpr.getStringValue());
            Assertions.assertEquals(ImmutableSet.of(partitionPredicate),
                    statistics.getConjunctsAppliedToRowCount());
        } finally {
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testLimit() {
        List<String> qualifier = ImmutableList.of("test", "t");
        SlotReference slot1 = new SlotReference(new ExprId(0), "c1", IntegerType.INSTANCE, true, qualifier,
                null, new Column("c1", PrimitiveType.INT),
                null, new Column("c1", PrimitiveType.INT));
        ColumnStatisticBuilder columnStat1 = new ColumnStatisticBuilder();
        columnStat1.setNdv(10);
        columnStat1.setNumNulls(5);
        Map<Expression, ColumnStatistic> slotColumnStatsMap = new HashMap<>();
        slotColumnStatsMap.put(slot1, columnStat1.build());
        Statistics childStats = new Statistics(20, slotColumnStatsMap);

        Group childGroup = newFakeGroup();
        GroupPlan groupPlan = new GroupPlan(childGroup);
        childGroup.setStatistics(childStats);

        LogicalLimit<? extends Plan> logicalLimit = new LogicalLimit<>(1, 2,
                LimitPhase.GLOBAL, new LogicalLimit<>(1, 2, LimitPhase.LOCAL, groupPlan));
        GroupExpression groupExpression = new GroupExpression(logicalLimit, ImmutableList.of(childGroup));
        Group ownerGroup = new Group(null, groupExpression, null);
        StatsCalculator.estimate(groupExpression, null);
        Statistics limitStats = ownerGroup.getStatistics();
        Assertions.assertEquals(1, limitStats.getRowCount());
        ColumnStatistic slot1Stats = limitStats.columnStatistics().get(slot1);
        Assertions.assertEquals(1, slot1Stats.ndv, 0.1);
        Assertions.assertEquals(1, slot1Stats.numNulls, 0.1);
    }

    @Test
    public void testTopN() {
        List<String> qualifier = ImmutableList.of("test", "t");
        SlotReference slot1 = new SlotReference("c1", IntegerType.INSTANCE, true, qualifier);
        ColumnStatisticBuilder columnStat1 = new ColumnStatisticBuilder();
        columnStat1.setNdv(10);
        columnStat1.setNumNulls(5);
        Map<Expression, ColumnStatistic> slotColumnStatsMap = new HashMap<>();
        slotColumnStatsMap.put(slot1, columnStat1.build());
        Statistics childStats = new Statistics(20, slotColumnStatsMap);

        Group childGroup = newFakeGroup();
        GroupPlan groupPlan = new GroupPlan(childGroup);
        childGroup.setStatistics(childStats);

        LogicalTopN<GroupPlan> logicalTopN = new LogicalTopN<>(Collections.emptyList(), 1, 2, groupPlan);
        GroupExpression groupExpression = new GroupExpression(logicalTopN, ImmutableList.of(childGroup));
        Group ownerGroup = new Group(null, groupExpression, null);
        StatsCalculator.estimate(groupExpression, null);
        Statistics topNStats = ownerGroup.getStatistics();
        Assertions.assertEquals(1, topNStats.getRowCount());
        ColumnStatistic slot1Stats = topNStats.columnStatistics().get(slot1);
        Assertions.assertEquals(1, slot1Stats.ndv, 0.1);
        Assertions.assertEquals(1, slot1Stats.numNulls, 0.1);
    }

    @Test
    public void testHashJoinSkew() {
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair = StatsTestUtil.instance.createExpr("ia = ib");
        Expression joinCondition = pair.first;
        SlotReference ia = pair.second.get(0);
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, new String[]{"1", "2"});

        SlotReference ib = pair.second.get(1);
        ColumnStatistic ibStats = StatsTestUtil.instance.createColumnStatistic("ib", 10,
                rowCount, "1", "10", 0, new String[]{"2", "3", "4"});

        SlotReference ic = new SlotReference("ic", IntegerType.INSTANCE);
        ColumnStatistic icStats = StatsTestUtil.instance.createColumnStatistic("ic", 10,
                rowCount, "1", "10", 0, new String[]{"6", "7", "8", "9", "10"});

        LogicalJoin join = new LogicalJoin(JoinType.INNER_JOIN, Lists.newArrayList(joinCondition),
                new DummyPlan(), new DummyPlan(),
                JoinReorderContext.EMPTY);

        StatsCalculator calculator = new StatsCalculator(null);

        Statistics leftStats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats, ic, icStats));
        Statistics rightStats = new Statistics(rowCount, ImmutableMap.of(ib, ibStats));
        Statistics outputStats = calculator.computeJoin(join, leftStats, rightStats);

        ColumnStatistic icStatsOut = outputStats.findColumnStatistics(ic);
        Assertions.assertEquals(5, icStatsOut.getHotValues().size());

        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertEquals(1, iaStatsOut.getHotValues().size());

        ColumnStatistic ibStatsOut = outputStats.findColumnStatistics(ib);
        Assertions.assertEquals(1, ibStatsOut.getHotValues().size());
    }

    @Test
    public void testHashJoinPkFkSkew() {
        double leftRowCount = 100;
        double rightRowCount = 10;
        Pair<Expression, ArrayList<SlotReference>> pair = StatsTestUtil.instance.createExpr("ia = ib");
        Expression joinCondition = pair.first;
        SlotReference ia = pair.second.get(0);
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                leftRowCount, "1", "10", 0, new String[]{"1", "2"});

        SlotReference ib = pair.second.get(1);
        ColumnStatistic ibStats = StatsTestUtil.instance.createColumnStatistic("ib", 10,
                rightRowCount, "1", "10", 0, new String[]{"2", "3", "4"});

        LogicalJoin join = new LogicalJoin(JoinType.INNER_JOIN, Lists.newArrayList(joinCondition),
                new DummyPlan(), new DummyPlan(),
                JoinReorderContext.EMPTY);

        StatsCalculator calculator = new StatsCalculator(null);

        Statistics leftStats = new Statistics(leftRowCount, ImmutableMap.of(ia, iaStats));
        Statistics rightStats = new Statistics(rightRowCount, ImmutableMap.of(ib, ibStats));
        Statistics outputStats = calculator.computeJoin(join, leftStats, rightStats);

        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertEquals(1, iaStatsOut.getHotValues().size());

        ColumnStatistic ibStatsOut = outputStats.findColumnStatistics(ib);
        Assertions.assertEquals(1, ibStatsOut.getHotValues().size());
    }

    @Test
    public void testLeftOuterJoinSkew() {
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair = StatsTestUtil.instance.createExpr("ia = ib");
        Expression joinCondition = pair.first;
        SlotReference ia = pair.second.get(0);
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, new String[]{"1", "2"});

        SlotReference ib = pair.second.get(1);
        ColumnStatistic ibStats = StatsTestUtil.instance.createColumnStatistic("ib", 10,
                rowCount, "1", "10", 0, new String[]{"2", "3", "4"});

        SlotReference ic = new SlotReference("ic", IntegerType.INSTANCE);
        ColumnStatistic icStats = StatsTestUtil.instance.createColumnStatistic("ic", 10,
                rowCount, "1", "10", 0, new String[]{"4", "5", "6", "7"});

        LogicalJoin join = new LogicalJoin(JoinType.LEFT_OUTER_JOIN, Lists.newArrayList(joinCondition),
                new DummyPlan(), new DummyPlan(),
                JoinReorderContext.EMPTY);

        StatsCalculator calculator = new StatsCalculator(null);

        Statistics leftStats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats, ic, icStats));
        Statistics rightStats = new Statistics(rowCount, ImmutableMap.of(ib, ibStats));
        Statistics outputStats = calculator.computeJoin(join, leftStats, rightStats);

        ColumnStatistic icStatsOut = outputStats.findColumnStatistics(ic);
        Assertions.assertEquals(4, icStatsOut.getHotValues().size());

        // left outer join,
        // ia.hotValues:  "2", "3", "4" -> "2", "3", "4"
        // ib.hotValues: "4", "5" -> "4"
        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertEquals(2, iaStatsOut.getHotValues().size());

        ColumnStatistic ibStatsOut = outputStats.findColumnStatistics(ib);
        Assertions.assertEquals(1, ibStatsOut.getHotValues().size());
    }

    @Test
    public void testRightOuterJoinSkew() {
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair = StatsTestUtil.instance.createExpr("ia = ib");
        Expression joinCondition = pair.first;
        SlotReference ia = pair.second.get(0);
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, new String[]{"1", "2"});

        SlotReference ib = pair.second.get(1);
        ColumnStatistic ibStats = StatsTestUtil.instance.createColumnStatistic("ib", 10,
                rowCount, "1", "10", 0, new String[]{"2", "3", "4"});

        SlotReference ic = new SlotReference("ic", IntegerType.INSTANCE);
        ColumnStatistic icStats = StatsTestUtil.instance.createColumnStatistic("ic", 10,
                rowCount, "1", "10", 0, new String[]{"4", "5", "6", "7"});

        LogicalJoin join = new LogicalJoin(JoinType.RIGHT_OUTER_JOIN, Lists.newArrayList(joinCondition),
                new DummyPlan(), new DummyPlan(),
                JoinReorderContext.EMPTY);

        StatsCalculator calculator = new StatsCalculator(null);

        Statistics leftStats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats, ic, icStats));
        Statistics rightStats = new Statistics(rowCount, ImmutableMap.of(ib, ibStats));
        Statistics outputStats = calculator.computeJoin(join, leftStats, rightStats);

        ColumnStatistic icStatsOut = outputStats.findColumnStatistics(ic);
        Assertions.assertEquals(4, icStatsOut.getHotValues().size());

        // right outer join,
        // ia.hotValues:  "1", "2" -> "2"
        // ib.hotValues: "2", "3", "4" -> "2", "3", "4"
        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertEquals(1, iaStatsOut.getHotValues().size());

        ColumnStatistic ibStatsOut = outputStats.findColumnStatistics(ib);
        Assertions.assertEquals(3, ibStatsOut.getHotValues().size());
    }

    @Test
    public void testLeftSemiJoinSkew() {
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair = StatsTestUtil.instance.createExpr("ia = ib");
        Expression joinCondition = pair.first;
        SlotReference ia = pair.second.get(0);
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, new String[]{"0", "1", "2"});

        SlotReference ib = pair.second.get(1);
        ColumnStatistic ibStats = StatsTestUtil.instance.createColumnStatistic("ib", 10,
                rowCount, "1", "10", 0, new String[]{"2", "3", "4"});

        SlotReference ic = new SlotReference("ic", IntegerType.INSTANCE);
        ColumnStatistic icStats = StatsTestUtil.instance.createColumnStatistic("ic", 10,
                rowCount, "1", "10", 0, new String[]{"4", "5", "6", "7"});

        LogicalJoin join = new LogicalJoin(JoinType.LEFT_SEMI_JOIN, Lists.newArrayList(joinCondition),
                new DummyPlan(), new DummyPlan(),
                JoinReorderContext.EMPTY);

        StatsCalculator calculator = new StatsCalculator(null);

        Statistics leftStats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats, ic, icStats));
        Statistics rightStats = new Statistics(rowCount, ImmutableMap.of(ib, ibStats));
        Statistics outputStats = calculator.computeJoin(join, leftStats, rightStats);

        ColumnStatistic icStatsOut = outputStats.findColumnStatistics(ic);
        Assertions.assertEquals(4, icStatsOut.getHotValues().size());

        // left semi join,
        // ia.hotValues:  "0", "1", "2" -> "0", "1", "2"
        // ib.hotValues: "2", "3", "4"
        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertEquals(3, iaStatsOut.getHotValues().size());
    }

    @Test
    public void testAggSkew() {
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair = StatsTestUtil.instance.createExpr("ia");
        SlotReference ia = pair.second.get(0);
        LogicalAggregate agg = new LogicalAggregate(
                ImmutableList.of(ia),
                ImmutableList.of(ia),
                new DummyPlan()
        );

        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, new String[]{"0", "1", "2"});

        Statistics inputStats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats));
        StatsCalculator calculator = new StatsCalculator(null);
        Statistics outputStats = calculator.computeAggregate(agg, inputStats);
        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertNull(iaStatsOut.getHotValues());
    }

    @Test
    public void testExceptSkew() {
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair1 = StatsTestUtil.instance.createExpr("ia");
        SlotReference ia = pair1.second.get(0);
        SlotReference ib = StatsTestUtil.instance.createExpr("ia").second.get(0);
        LogicalExcept exceptAll = new LogicalExcept(
                Qualifier.ALL,
                ImmutableList.of(ia),
                ImmutableList.of(
                        ImmutableList.of(ia),
                        ImmutableList.of(ib)
                ),
                ImmutableList.of(new DummyPlan(), new DummyPlan())
        );
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, new String[]{"0", "1", "2"});
        Statistics child0Stats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats));
        StatsCalculator calculator = new StatsCalculator(null);
        Statistics outputStats = calculator.computeExcept(exceptAll, child0Stats);
        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertEquals(3, iaStatsOut.getHotValues().size());
    }

    @Test
    public void testUnionSkew() {
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair1 = StatsTestUtil.instance.createExpr("ia");
        SlotReference ia = pair1.second.get(0);
        SlotReference ib = StatsTestUtil.instance.createExpr("ia").second.get(0);
        LogicalUnion unionAll = new LogicalUnion(
                Qualifier.ALL,
                ImmutableList.of(ia),
                ImmutableList.of(
                        ImmutableList.of(ia),
                        ImmutableList.of(ib)
                ),
                ImmutableList.of(),
                true,
                ImmutableList.of(new DummyPlan(), new DummyPlan())
        );
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 30,
                rowCount, "1", "10", 0, ImmutableMap.of("0", 0.40f, "1", 0.50f));
        ColumnStatistic ibStats = StatsTestUtil.instance.createColumnStatistic("ia", 30,
                rowCount, "1", "10", 0, ImmutableMap.of("1", 0.40f, "2", 0.40f));
        Statistics child0Stats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats));
        Statistics child1Stats = new Statistics(rowCount, ImmutableMap.of(ib, ibStats));

        StatsCalculator calculator = new StatsCalculator(null);
        Statistics outputStats = calculator.computeUnion(unionAll, ImmutableList.of(child0Stats, child1Stats));
        ColumnStatistic iaStatsOut = outputStats.findColumnStatistics(ia);
        Assertions.assertEquals(3, iaStatsOut.getHotValues().size());
        Assertions.assertTrue(containsHotValue(iaStatsOut, "1"));
    }

    private boolean containsHotValue(ColumnStatistic columnStatistic, String value) {
        if (columnStatistic.getHotValues() == null) {
            return false;
        }
        return columnStatistic.getHotValues().keySet().stream()
                .anyMatch(literal -> literal.getStringValue().equals(value));
    }

    @Test
    public void testLimitSkew() {
        LogicalLimit limit = new LogicalLimit(10, 10, LimitPhase.GLOBAL, new DummyPlan());
        double rowCount = 100;
        Pair<Expression, ArrayList<SlotReference>> pair1 = StatsTestUtil.instance.createExpr("ia");
        SlotReference ia = pair1.second.get(0);
        ColumnStatistic iaStats = StatsTestUtil.instance.createColumnStatistic("ia", 10,
                rowCount, "1", "10", 0, ImmutableMap.of("0", 0.40f, "1", 0.50f));

        Statistics childStats = new Statistics(rowCount, ImmutableMap.of(ia, iaStats));
        StatsCalculator calculator = new StatsCalculator(null);
        Statistics outputStats = calculator.computeLimit(limit, childStats);
        Assertions.assertNull(outputStats.findColumnStatistics(ia).getHotValues());
    }

    @Test
    public void testDisableJoinReorderIfStatsInvalid() throws Exception {
        // Spy on the actual OlapTable instances to make getRowCountForIndex return -1.
        // JMockit's MockUp globally mocked ALL instances, but Mockito mockConstruction
        // only intercepts NEW instances. Since scan1/scan2/scan3 already contain
        // pre-existing OlapTable instances, we spy on their tables and replace via reflection.
        Field tableField = LogicalCatalogRelation.class.getDeclaredField("table");
        tableField.setAccessible(true);
        for (LogicalOlapScan scan : new LogicalOlapScan[]{scan1, scan2, scan3}) {
            OlapTable spyTable = Mockito.spy(scan.getTable());
            Mockito.doReturn(-1L).when(spyTable)
                    .getRowCountForIndex(Mockito.anyLong(), Mockito.anyBoolean());
            tableField.set(scan, spyTable);
        }

        LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .join(scan3, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .build();

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(join);
        cascadesContext.getConnectContext().getSessionVariable()
                .setVarOnce(SessionVariable.DISABLE_JOIN_REORDER, "false");
        StatsCalculator.disableJoinReorderIfStatsInvalid(ImmutableList.of(scan1, scan2, scan3), cascadesContext);
        // because table row count is -1, so disable join reorder
        Assertions.assertTrue(cascadesContext.getConnectContext().getSessionVariable().isDisableJoinReorder());
    }

    @Test
    public void testOlapScanWithPlanWithUnknownColumnStats() {
        boolean prevFlag = false;
        if (ConnectContext.get() != null) {
            prevFlag = ConnectContext.get().getState().isPlanWithUnKnownColumnStats();
            ConnectContext.get().getState().setPlanWithUnKnownColumnStats(true);
        }
        try {
            long tableId1 = 100;
            OlapTable table1 = PlanConstructor.newOlapTable(tableId1, "t_unknown", 0);
            List<String> qualifier = ImmutableList.of("test", "t");
            SlotReference slot1 = new SlotReference(new ExprId(0), "c1", IntegerType.INSTANCE, true, qualifier,
                    table1, new Column("c1", PrimitiveType.INT),
                    table1, new Column("c1", PrimitiveType.INT));

            LogicalOlapScan logicalOlapScan1 = (LogicalOlapScan) new LogicalOlapScan(
                    StatementScopeIdGenerator.newRelationId(), table1,
                    Collections.emptyList()).withGroupExprLogicalPropChildren(Optional.empty(),
                    Optional.of(new LogicalProperties(() -> ImmutableList.of(slot1), () -> DataTrait.EMPTY_TRAIT)), ImmutableList.of());

            GroupExpression groupExpression = new GroupExpression(logicalOlapScan1, ImmutableList.of());
            Group ownerGroup = new Group(null, groupExpression, null);
            StatsCalculator.estimate(groupExpression, null);
            Statistics stats = ownerGroup.getStatistics();
            Assertions.assertEquals(1, stats.columnStatistics().size());
            ColumnStatistic colStat = stats.columnStatistics().get(slot1);
            Assertions.assertTrue(colStat.isUnKnown);
        } finally {
            if (ConnectContext.get() != null) {
                ConnectContext.get().getState().setPlanWithUnKnownColumnStats(prevFlag);
            }
        }
    }

}
