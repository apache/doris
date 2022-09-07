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

import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Limit;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.algebra.Scan;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLocalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStats;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatsDeriveResult;
import org.apache.doris.statistics.TableStats;

import com.google.common.collect.Maps;

import java.util.AbstractMap;
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
        groupExpression.getOwnerGroup().setStatistics(stats);
        groupExpression.setStatDerived(true);
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
    public StatsDeriveResult visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
        return computeAggregate(aggregate);
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
    public StatsDeriveResult visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
        return computeProject(project);
    }

    @Override
    public StatsDeriveResult visitLogicalSort(LogicalSort<? extends Plan> sort, Void context) {
        return groupExpression.getCopyOfChildStats(0);
    }

    @Override
    public StatsDeriveResult visitLogicalTopN(LogicalTopN<? extends Plan> topN, Void context) {
        return computeTopN(topN);
    }

    @Override
    public StatsDeriveResult visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
        return JoinEstimation.estimate(groupExpression.getCopyOfChildStats(0),
                groupExpression.getCopyOfChildStats(1), join);
    }

    @Override
    public StatsDeriveResult visitLogicalAssertNumRows(
            LogicalAssertNumRows<? extends Plan> assertNumRows, Void context) {
        return groupExpression.getCopyOfChildStats(0);
    }

    @Override
    public StatsDeriveResult visitPhysicalAggregate(PhysicalAggregate<? extends Plan> agg, Void context) {
        return computeAggregate(agg);
    }

    @Override
    public StatsDeriveResult visitPhysicalOlapScan(PhysicalOlapScan olapScan, Void context) {
        return computeScan(olapScan);
    }

    @Override
    public StatsDeriveResult visitPhysicalQuickSort(PhysicalQuickSort<? extends Plan> sort, Void context) {
        return groupExpression.getCopyOfChildStats(0);
    }

    @Override
    public StatsDeriveResult visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, Void context) {
        return computeTopN(topN);
    }

    public StatsDeriveResult visitPhysicalLocalQuickSort(PhysicalLocalQuickSort<? extends Plan> sort, Void context) {
        return groupExpression.getCopyOfChildStats(0);
    }

    @Override
    public StatsDeriveResult visitPhysicalHashJoin(
            PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin, Void context) {
        return JoinEstimation.estimate(groupExpression.getCopyOfChildStats(0),
                groupExpression.getCopyOfChildStats(1), hashJoin);
    }

    @Override
    public StatsDeriveResult visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin,
            Void context) {
        return JoinEstimation.estimate(groupExpression.getCopyOfChildStats(0),
                groupExpression.getCopyOfChildStats(1), nestedLoopJoin);
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
        return groupExpression.getCopyOfChildStats(0);
    }

    @Override
    public StatsDeriveResult visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
            Void context) {
        return groupExpression.getCopyOfChildStats(0);
    }

    private StatsDeriveResult computeFilter(Filter filter) {
        StatsDeriveResult stats = groupExpression.getCopyOfChildStats(0);
        FilterSelectivityCalculator selectivityCalculator =
                new FilterSelectivityCalculator(stats.getSlotToColumnStats());
        double selectivity = selectivityCalculator.estimate(filter.getPredicates());
        stats.updateRowCountBySelectivity(selectivity);
        return stats;
    }

    // TODO: 1. Subtract the pruned partition
    //       2. Consider the influence of runtime filter
    //       3. Get NDV and column data size from StatisticManger, StatisticManager doesn't support it now.
    private StatsDeriveResult computeScan(Scan scan) {
        TableStats tableStats = Utils.execWithReturnVal(() ->
                // TODO: tmp mock the table stats, after we support the table stats, we should remove this mock.
                mockRowCountInStatistic(scan)
        );
        Map<Slot, ColumnStats> slotToColumnStats = new HashMap<>();
        Set<SlotReference> slotSet = scan.getOutput().stream().filter(SlotReference.class::isInstance)
                .map(s -> (SlotReference) s).collect(Collectors.toSet());
        for (SlotReference slotReference : slotSet) {
            String colName = slotReference.getName();
            if (colName == null) {
                throw new RuntimeException("Column name of SlotReference shouldn't be null here");
            }
            ColumnStats columnStats = tableStats.getColumnStats(colName);
            slotToColumnStats.put(slotReference, columnStats);
        }
        long rowCount = tableStats.getRowCount();
        StatsDeriveResult stats = new StatsDeriveResult(rowCount,
                new HashMap<>(), new HashMap<>());
        stats.setSlotToColumnStats(slotToColumnStats);
        return stats;
    }

    // TODO: tmp mock the table stats, after we support the table stats, we should remove this mock.
    private TableStats mockRowCountInStatistic(Scan scan) throws AnalysisException {
        long cardinality = 0;
        if (scan instanceof PhysicalOlapScan || scan instanceof LogicalOlapScan) {
            OlapTable table = (OlapTable) scan.getTable();
            for (long selectedPartitionId : table.getPartitionIds()) {
                final Partition partition = table.getPartition(selectedPartitionId);
                final MaterializedIndex baseIndex = partition.getBaseIndex();
                cardinality += baseIndex.getRowCount();
            }
        }
        Statistics statistics = ConnectContext.get().getEnv().getStatisticsManager().getStatistics();

        statistics.mockTableStatsWithRowCount(scan.getTable().getId(), cardinality);
        return statistics.getTableStats(scan.getTable().getId());
    }

    private StatsDeriveResult computeTopN(TopN topN) {
        StatsDeriveResult stats = groupExpression.getCopyOfChildStats(0);
        return stats.updateRowCountByLimit(topN.getLimit());
    }

    private StatsDeriveResult computeLimit(Limit limit) {
        StatsDeriveResult stats = groupExpression.getCopyOfChildStats(0);
        return stats.updateRowCountByLimit(limit.getLimit());
    }

    private StatsDeriveResult computeAggregate(Aggregate aggregate) {
        List<Expression> groupByExpressions = aggregate.getGroupByExpressions();
        StatsDeriveResult childStats = groupExpression.getCopyOfChildStats(0);
        Map<Slot, ColumnStats> childSlotToColumnStats = childStats.getSlotToColumnStats();
        long resultSetCount = 1;
        for (Expression groupByExpression : groupByExpressions) {
            Set<Slot> slots = groupByExpression.getInputSlots();
            // TODO: Support more complex group expr.
            //       For example:
            //              select max(col1+col3) from t1 group by col1+col3;
            if (slots.size() != 1) {
                continue;
            }
            Slot slotReference = slots.iterator().next();
            ColumnStats columnStats = childSlotToColumnStats.get(slotReference);
            resultSetCount *= columnStats.getNdv();
        }
        Map<Slot, ColumnStats> slotToColumnStats = Maps.newHashMap();
        List<NamedExpression> outputExpressions = aggregate.getOutputExpressions();
        // TODO: 1. Estimate the output unit size by the type of corresponding AggregateFunction
        //       2. Handle alias, literal in the output expression list
        for (NamedExpression outputExpression : outputExpressions) {
            slotToColumnStats.put(outputExpression.toSlot(), new ColumnStats());
        }
        StatsDeriveResult statsDeriveResult = new StatsDeriveResult(resultSetCount, slotToColumnStats);
        // TODO: Update ColumnStats properly, add new mapping from output slot to ColumnStats
        return statsDeriveResult;
    }

    // TODO: do real project on column stats
    private StatsDeriveResult computeProject(Project project) {
        List<NamedExpression> projections = project.getProjects();
        StatsDeriveResult statsDeriveResult = groupExpression.getCopyOfChildStats(0);
        Map<Slot, ColumnStats> childColumnStats = statsDeriveResult.getSlotToColumnStats();
        Map<Slot, ColumnStats> columnsStats = projections.stream().map(projection -> {
            Set<Slot> slots = projection.getInputSlots();
            if (slots.isEmpty()) {
                return new AbstractMap.SimpleEntry<>(projection.toSlot(), ColumnStats.createDefaultColumnStats());
            } else {
                // TODO: just a trick here, need to do real project on column stats
                return new AbstractMap.SimpleEntry<>(projection.toSlot(),
                        childColumnStats.get(slots.iterator().next()));
            }
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        statsDeriveResult.setSlotToColumnStats(columnsStats);
        return statsDeriveResult;
    }
}
