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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Id;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.EmptyRelation;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Limit;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.algebra.Repeat;
import org.apache.doris.nereids.trees.plans.algebra.Scan;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLocalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.collect.Maps;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to calculate the stats for each plan
 */
public class StatsCalculator extends DefaultPlanVisitor<StatsDeriveResult, Void> {

    private final GroupExpression groupExpression;

    private StatsCalculator(GroupExpression groupExpression) {
        this.groupExpression = groupExpression;
    }

    /**
     * estimate stats
     */
    public static void estimate(GroupExpression groupExpression) {
        StatsCalculator statsCalculator = new StatsCalculator(groupExpression);
        statsCalculator.estimate();
    }

    private void estimate() {
        StatsDeriveResult stats = groupExpression.getPlan().accept(this, null);
        StatsDeriveResult originStats = groupExpression.getOwnerGroup().getStatistics();
        /*
        in an ideal cost model, every group expression in a group are equivalent, but in fact the cost are different.
        we record the lowest expression cost as group cost to avoid missing this group.
        */
        if (originStats == null || originStats.getRowCount() > stats.getRowCount()) {
            groupExpression.getOwnerGroup().setStatistics(stats);
        }
        groupExpression.setEstOutputRowCount((long) stats.getRowCount());
        groupExpression.setStatDerived(true);
    }

    @Override
    public StatsDeriveResult visitLogicalEmptyRelation(LogicalEmptyRelation emptyRelation, Void context) {
        return computeEmptyRelation(emptyRelation);
    }

    @Override
    public StatsDeriveResult visitLogicalLimit(LogicalLimit<? extends Plan> limit, Void context) {
        return computeLimit(limit);
    }

    @Override
    public StatsDeriveResult visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, Void context) {
        return computeLimit(limit);
    }

    @Override
    public StatsDeriveResult visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation, Void context) {
        return computeOneRowRelation(oneRowRelation);
    }

    @Override
    public StatsDeriveResult visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
        return computeAggregate(aggregate);
    }

    @Override
    public StatsDeriveResult visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, Void context) {
        return computeRepeat(repeat);
    }

    @Override
    public StatsDeriveResult visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
        return computeFilter(filter);
    }

    @Override
    public StatsDeriveResult visitLogicalOlapScan(LogicalOlapScan olapScan, Void context) {
        olapScan.getExpressions();
        return computeScan(olapScan);
    }

    @Override
    public StatsDeriveResult visitLogicalTVFRelation(LogicalTVFRelation tvfRelation, Void context) {
        return tvfRelation.getFunction().computeStats(tvfRelation.getOutput());
    }

    @Override
    public StatsDeriveResult visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
        return computeProject(project);
    }

    @Override
    public StatsDeriveResult visitLogicalSort(LogicalSort<? extends Plan> sort, Void context) {
        return groupExpression.childStatistics(0);
    }

    @Override
    public StatsDeriveResult visitLogicalTopN(LogicalTopN<? extends Plan> topN, Void context) {
        return computeTopN(topN);
    }

    @Override
    public StatsDeriveResult visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
        return JoinEstimation.estimate(groupExpression.childStatistics(0),
                groupExpression.childStatistics(1), join);
    }

    @Override
    public StatsDeriveResult visitLogicalAssertNumRows(
            LogicalAssertNumRows<? extends Plan> assertNumRows, Void context) {
        return computeAssertNumRows(assertNumRows.getAssertNumRowsElement().getDesiredNumOfRows());
    }

    @Override
    public StatsDeriveResult visitPhysicalEmptyRelation(PhysicalEmptyRelation emptyRelation, Void context) {
        return computeEmptyRelation(emptyRelation);
    }

    @Override
    public StatsDeriveResult visitPhysicalAggregate(PhysicalAggregate<? extends Plan> agg, Void context) {
        return computeAggregate(agg);
    }

    @Override
    public StatsDeriveResult visitPhysicalRepeat(PhysicalRepeat<? extends Plan> repeat, Void context) {
        return computeRepeat(repeat);
    }

    @Override
    public StatsDeriveResult visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRowRelation, Void context) {
        return computeOneRowRelation(oneRowRelation);
    }

    @Override
    public StatsDeriveResult visitPhysicalOlapScan(PhysicalOlapScan olapScan, Void context) {
        return computeScan(olapScan);
    }

    @Override
    public StatsDeriveResult visitPhysicalTVFRelation(PhysicalTVFRelation tvfRelation, Void context) {
        return tvfRelation.getFunction().computeStats(tvfRelation.getOutput());
    }

    @Override
    public StatsDeriveResult visitPhysicalQuickSort(PhysicalQuickSort<? extends Plan> sort, Void context) {
        return groupExpression.childStatistics(0);
    }

    @Override
    public StatsDeriveResult visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, Void context) {
        return computeTopN(topN);
    }

    public StatsDeriveResult visitPhysicalLocalQuickSort(PhysicalLocalQuickSort<? extends Plan> sort, Void context) {
        return groupExpression.childStatistics(0);
    }

    @Override
    public StatsDeriveResult visitPhysicalHashJoin(
            PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin, Void context) {
        return JoinEstimation.estimate(groupExpression.childStatistics(0),
                groupExpression.childStatistics(1), hashJoin);
    }

    @Override
    public StatsDeriveResult visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin,
            Void context) {
        return JoinEstimation.estimate(groupExpression.childStatistics(0),
                groupExpression.childStatistics(1), nestedLoopJoin);
    }

    // TODO: We should subtract those pruned column, and consider the expression transformations in the node.
    @Override
    public StatsDeriveResult visitPhysicalProject(PhysicalProject<? extends Plan> project, Void context) {
        return computeProject(project);
    }

    @Override
    public StatsDeriveResult visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, Void context) {
        return computeFilter(filter);
    }

    @Override
    public StatsDeriveResult visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute,
            Void context) {
        return groupExpression.childStatistics(0);
    }

    @Override
    public StatsDeriveResult visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
            Void context) {
        return computeAssertNumRows(assertNumRows.getAssertNumRowsElement().getDesiredNumOfRows());
    }

    private StatsDeriveResult computeAssertNumRows(long desiredNumOfRows) {
        StatsDeriveResult statsDeriveResult = groupExpression.childStatistics(0);
        statsDeriveResult.updateRowCountByLimit(1);
        return statsDeriveResult;
    }

    private StatsDeriveResult computeFilter(Filter filter) {
        StatsDeriveResult stats = groupExpression.childStatistics(0);
        FilterEstimation filterEstimation =
                new FilterEstimation(stats);
        return filterEstimation.estimate(filter.getPredicates());
    }

    // TODO: 1. Subtract the pruned partition
    //       2. Consider the influence of runtime filter
    //       3. Get NDV and column data size from StatisticManger, StatisticManager doesn't support it now.
    private StatsDeriveResult computeScan(Scan scan) {
        Set<SlotReference> slotSet = scan.getOutput().stream().filter(SlotReference.class::isInstance)
                .map(s -> (SlotReference) s).collect(Collectors.toSet());
        Map<Id, ColumnStatistic> columnStatisticMap = new HashMap<>();
        Table table = scan.getTable();
        double rowCount = scan.getTable().estimatedRowCount();
        for (SlotReference slotReference : slotSet) {
            String colName = slotReference.getName();
            if (colName == null) {
                throw new RuntimeException(String.format("Column %s not found", colName));
            }
            ColumnStatistic colStats =
                    Env.getCurrentEnv().getStatisticsCache().getColumnStatistics(table.getId(), colName);
            if (!colStats.isUnKnown) {
                rowCount = colStats.count;

            }
            columnStatisticMap.put(slotReference.getExprId(), colStats);
        }
        StatsDeriveResult stats = new StatsDeriveResult(rowCount, columnStatisticMap);
        return stats;
    }

    private StatsDeriveResult computeTopN(TopN topN) {
        StatsDeriveResult stats = groupExpression.childStatistics(0);
        return stats.updateRowCountByLimit(topN.getLimit());
    }

    private StatsDeriveResult computeLimit(Limit limit) {
        StatsDeriveResult stats = groupExpression.childStatistics(0);
        return stats.updateRowCountByLimit(limit.getLimit());
    }

    private StatsDeriveResult computeAggregate(Aggregate aggregate) {
        // TODO: since we have no column stats here. just use a fix ratio to compute the row count.
        List<Expression> groupByExpressions = aggregate.getGroupByExpressions();
        StatsDeriveResult childStats = groupExpression.childStatistics(0);
        Map<Id, ColumnStatistic> childSlotToColumnStats = childStats.getSlotIdToColumnStats();
        double resultSetCount = groupByExpressions.stream().flatMap(expr -> expr.getInputSlots().stream())
                .map(Slot::getExprId)
                .filter(childSlotToColumnStats::containsKey).map(childSlotToColumnStats::get).map(s -> s.ndv)
                .reduce(1d, (a, b) -> a * b);
        if (resultSetCount <= 0) {
            resultSetCount = 1L;
        }

        Map<Id, ColumnStatistic> slotToColumnStats = Maps.newHashMap();
        List<NamedExpression> outputExpressions = aggregate.getOutputExpressions();
        // TODO: 1. Estimate the output unit size by the type of corresponding AggregateFunction
        //       2. Handle alias, literal in the output expression list
        for (NamedExpression outputExpression : outputExpressions) {
            ColumnStatistic columnStat = ExpressionEstimation.estimate(outputExpression, childStats);
            ColumnStatisticBuilder builder = new ColumnStatisticBuilder(columnStat);
            builder.setNdv(Math.min(columnStat.ndv, resultSetCount));
            slotToColumnStats.put(outputExpression.toSlot().getExprId(), columnStat);
        }
        StatsDeriveResult statsDeriveResult = new StatsDeriveResult(resultSetCount, childStats.getWidth(),
                childStats.getPenalty(), slotToColumnStats);
        statsDeriveResult.setWidth(childStats.getWidth());
        statsDeriveResult.setPenalty(childStats.getPenalty() + childStats.getRowCount());
        // TODO: Update ColumnStats properly, add new mapping from output slot to ColumnStats
        return statsDeriveResult;
    }

    private StatsDeriveResult computeRepeat(Repeat repeat) {
        StatsDeriveResult childStats = groupExpression.childStatistics(0);
        Map<Id, ColumnStatistic> slotIdToColumnStats = childStats.getSlotIdToColumnStats();
        int groupingSetNum = repeat.getGroupingSets().size();
        double rowCount = childStats.getRowCount();
        Map<Id, ColumnStatistic> columnStatisticMap = slotIdToColumnStats.entrySet()
                .stream().map(kv -> {
                    ColumnStatistic stats = kv.getValue();
                    return Pair.of(kv.getKey(), new ColumnStatistic(
                            stats.count < 0 ? stats.count : stats.count * groupingSetNum,
                            stats.ndv,
                            stats.avgSizeByte,
                            stats.numNulls < 0 ? stats.numNulls : stats.numNulls * groupingSetNum,
                            stats.dataSize < 0 ? stats.dataSize : stats.dataSize * groupingSetNum,
                            stats.minValue,
                            stats.maxValue,
                            stats.selectivity,
                            stats.minExpr,
                            stats.maxExpr,
                            stats.isUnKnown
                    ));
                }).collect(Collectors.toMap(Pair::key, Pair::value));
        return new StatsDeriveResult(rowCount < 0 ? rowCount : rowCount * groupingSetNum, columnStatisticMap);
    }

    // TODO: do real project on column stats
    private StatsDeriveResult computeProject(Project project) {
        List<NamedExpression> projections = project.getProjects();
        StatsDeriveResult childStats = groupExpression.childStatistics(0);
        Map<Id, ColumnStatistic> childColumnStats = childStats.getSlotIdToColumnStats();
        Map<Id, ColumnStatistic> columnsStats = projections.stream().map(projection -> {
            ColumnStatistic value = null;
            Set<Slot> slots = projection.getInputSlots();
            if (slots.isEmpty()) {
                value = ColumnStatistic.DEFAULT;
            } else {
                // TODO: just a trick here, need to do real project on column stats
                for (Slot slot : slots) {
                    if (childColumnStats.containsKey(slot.getExprId())) {
                        value = childColumnStats.get(slot.getExprId());
                        break;
                    }
                }
                if (value == null) {
                    value = ColumnStatistic.DEFAULT;
                }
            }
            return new SimpleEntry<>(projection.toSlot().getExprId(), value);
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (item1, item2) -> item1));
        return new StatsDeriveResult(childStats.getRowCount(), childStats.getWidth(),
                childStats.getPenalty(), columnsStats);
    }

    private StatsDeriveResult computeOneRowRelation(OneRowRelation oneRowRelation) {
        Map<Id, ColumnStatistic> columnStatsMap = oneRowRelation.getProjects()
                .stream()
                .map(project -> {
                    ColumnStatistic statistic = new ColumnStatisticBuilder().setNdv(1).build();
                    // TODO: compute the literal size
                    return Pair.of(project.toSlot().getExprId(), statistic);
                })
                .collect(Collectors.toMap(Pair::key, Pair::value));
        int rowCount = 1;
        return new StatsDeriveResult(rowCount, columnStatsMap);
    }

    private StatsDeriveResult computeEmptyRelation(EmptyRelation emptyRelation) {
        Map<Id, ColumnStatistic> columnStatsMap = emptyRelation.getProjects()
                .stream()
                .map(project -> {
                    ColumnStatisticBuilder columnStat = new ColumnStatisticBuilder()
                            .setNdv(0)
                            .setNumNulls(0)
                            .setAvgSizeByte(0);
                    return Pair.of(project.toSlot().getExprId(), columnStat.build());
                })
                .collect(Collectors.toMap(Pair::key, Pair::value));
        int rowCount = 0;
        return new StatsDeriveResult(rowCount, columnStatsMap);
    }
}
