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

import org.apache.doris.catalog.Table;
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
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribution;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHeapSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStats;
import org.apache.doris.statistics.StatsDeriveResult;
import org.apache.doris.statistics.TableStats;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to calculate the stats for each operator
 */
public class StatsCalculator extends DefaultPlanVisitor<StatsDeriveResult, Void> {

    private final GroupExpression groupExpression;

    public StatsCalculator(GroupExpression groupExpression) {
        this.groupExpression = groupExpression;
    }

    /**
     * Do estimate.
     */
    public void estimate() {

        StatsDeriveResult stats = groupExpression.getPlan().accept(this, null);
        groupExpression.getOwnerGroup().setStatistics(stats);
        groupExpression.setStatDerived(true);

    }

    @Override
    public StatsDeriveResult visitLogicalLimit(LogicalLimit<Plan> limit, Void context) {
        return computeLimit(limit);
    }

    @Override
    public StatsDeriveResult visitPhysicalLimit(PhysicalLimit<Plan> limit, Void context) {
        return computeLimit(limit);
    }

    @Override
    public StatsDeriveResult visitLogicalAggregate(LogicalAggregate<Plan> aggregate, Void context) {
        return computeAggregate(aggregate);
    }

    @Override
    public StatsDeriveResult visitLogicalFilter(LogicalFilter<Plan> filter, Void context) {
        return computeFilter(filter);
    }

    @Override
    public StatsDeriveResult visitLogicalOlapScan(LogicalOlapScan olapScan, Void context) {
        olapScan.getExpressions();
        return computeScan(olapScan);
    }

    @Override
    public StatsDeriveResult visitLogicalProject(LogicalProject<Plan> project, Void context) {
        return computeProject(project);
    }

    @Override
    public StatsDeriveResult visitLogicalSort(LogicalSort<Plan> sort, Void context) {
        return groupExpression.getCopyOfChildStats(0);
    }

    @Override
    public StatsDeriveResult visitLogicalJoin(LogicalJoin<Plan, Plan> join, Void context) {
        return JoinEstimation.estimate(groupExpression.getCopyOfChildStats(0),
                groupExpression.getCopyOfChildStats(1), join);
    }

    @Override
    public StatsDeriveResult visitPhysicalAggregate(PhysicalAggregate<Plan> agg, Void context) {
        return computeAggregate(agg);
    }

    @Override
    public StatsDeriveResult visitPhysicalOlapScan(PhysicalOlapScan olapScan, Void context) {
        return computeScan(olapScan);
    }

    @Override
    public StatsDeriveResult visitPhysicalHeapSort(PhysicalHeapSort<Plan> sort, Void context) {
        return groupExpression.getCopyOfChildStats(0);
    }

    @Override
    public StatsDeriveResult visitPhysicalHashJoin(PhysicalHashJoin<Plan, Plan> hashJoin, Void context) {
        return JoinEstimation.estimate(groupExpression.getCopyOfChildStats(0),
                groupExpression.getCopyOfChildStats(1), hashJoin);
    }

    @Override
    public StatsDeriveResult visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<Plan, Plan> nestedLoopJoin,
            Void context) {
        return JoinEstimation.estimate(groupExpression.getCopyOfChildStats(0),
                groupExpression.getCopyOfChildStats(1), nestedLoopJoin);
    }

    // TODO: We should subtract those pruned column, and consider the expression transformations in the node.
    @Override
    public StatsDeriveResult visitPhysicalProject(PhysicalProject<Plan> project, Void context) {
        return computeProject(project);
    }

    @Override
    public StatsDeriveResult visitPhysicalFilter(PhysicalFilter<Plan> filter, Void context) {
        return computeFilter(filter);
    }

    @Override
    public StatsDeriveResult visitPhysicalDistribution(PhysicalDistribution<Plan> distribution,
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
        Table table = scan.getTable();
        TableStats tableStats = Utils.execWithReturnVal(() ->
                ConnectContext.get().getEnv().getStatisticsManager().getStatistics().getTableStats(table.getId())
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

    private StatsDeriveResult computeLimit(Limit limit) {
        StatsDeriveResult stats = groupExpression.getCopyOfChildStats(0);
        return stats.updateRowCountByLimit(limit.getLimit());
    }

    private StatsDeriveResult computeAggregate(Aggregate aggregate) {
        List<Expression> groupByExprList = aggregate.getGroupByExpressions();
        StatsDeriveResult childStats = groupExpression.getCopyOfChildStats(0);
        Map<Slot, ColumnStats> childSlotColumnStatsMap = childStats.getSlotToColumnStats();
        long resultSetCount = 1;
        for (Expression expression : groupByExprList) {
            List<SlotReference> slotRefList = expression.collect(SlotReference.class::isInstance);
            // TODO: Support more complex group expr.
            //       For example:
            //              select max(col1+col3) from t1 group by col1+col3;
            if (slotRefList.size() != 1) {
                continue;
            }
            SlotReference slotRef = slotRefList.get(0);
            ColumnStats columnStats = childSlotColumnStatsMap.get(slotRef);
            resultSetCount *= columnStats.getNdv();
        }
        Map<Slot, ColumnStats> slotColumnStatsMap = new HashMap<>();
        List<NamedExpression> namedExpressionList = aggregate.getOutputExpressions();
        // TODO: 1. Estimate the output unit size by the type of corresponding AggregateFunction
        //       2. Handle alias, literal in the output expression list
        for (NamedExpression namedExpression : namedExpressionList) {
            if (namedExpression instanceof SlotReference) {
                slotColumnStatsMap.put((SlotReference) namedExpression, new ColumnStats());
            }
        }
        StatsDeriveResult statsDeriveResult = new StatsDeriveResult(resultSetCount, slotColumnStatsMap);
        // TODO: Update ColumnStats properly, add new mapping from output slot to ColumnStats
        return statsDeriveResult;
    }

    // TODO: Update data size and min/max value.
    private StatsDeriveResult computeProject(Project project) {
        List<NamedExpression> namedExpressionList = project.getProjects();
        Set<Slot> slotSet = new HashSet<>();
        for (NamedExpression namedExpression : namedExpressionList) {
            List<SlotReference> slotReferenceList = namedExpression.collect(SlotReference.class::isInstance);
            slotSet.addAll(slotReferenceList);
        }
        StatsDeriveResult stat = groupExpression.getCopyOfChildStats(0);
        Map<Slot, ColumnStats> slotColumnStatsMap = stat.getSlotToColumnStats();
        slotColumnStatsMap.entrySet().removeIf(entry -> !slotSet.contains(entry.getKey()));
        return stat;
    }

}
