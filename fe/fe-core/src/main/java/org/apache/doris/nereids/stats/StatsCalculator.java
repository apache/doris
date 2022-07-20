// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.ScanOperator;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribution;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHeapSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.ColumnStats;
import org.apache.doris.statistics.StatsDeriveResult;
import org.apache.doris.statistics.TableStats;


import java.util.HashMap;
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

    public void estimate() {
        StatsDeriveResult stats = groupExpression.getPlan().accept(this, null);
        groupExpression.getParent().setStatistics(stats);
    }

    @Override
    public StatsDeriveResult visitLogicalAggregate(LogicalAggregate<Plan> agg, Void context) {
        List<Expression> expressionList = agg.getGroupByExpressionList();
        for (Expression expression : expressionList) {

        }
        return super.visitLogicalAggregate(agg, context);
    }

    @Override
    public StatsDeriveResult visitLogicalFilter(LogicalFilter<Plan> filter, Void context) {
        return super.visitLogicalFilter(filter, context);
    }

    @Override
    public StatsDeriveResult visitLogicalOlapScan(LogicalOlapScan olapScan, Void context) {
        olapScan.getExpressions();
        return computeOlapScan(olapScan);
    }

    @Override
    public StatsDeriveResult visitLogicalProject(LogicalProject<Plan> project, Void context) {
        return super.visitLogicalProject(project, context);
    }

    @Override
    public StatsDeriveResult visitLogicalSort(LogicalSort<Plan> sort, Void context) {
        return super.visitLogicalSort(sort, context);
    }

    @Override
    public StatsDeriveResult visitLogicalJoin(LogicalJoin<Plan, Plan> join, Void context) {
        return HashJoinEstimation.estimate(groupExpression.getChildStats(0), groupExpression.getChildStats(1),
                join.getCondition().get(), join.getJoinType());
    }

    @Override
    public StatsDeriveResult visitGroupPlan(GroupPlan groupPlan, Void context) {
        return super.visitGroupPlan(groupPlan, context);
    }

    @Override
    public StatsDeriveResult visitPhysicalAggregate(PhysicalAggregate<Plan> agg, Void context) {
        return super.visitPhysicalAggregate(agg, context);
    }

    @Override
    public StatsDeriveResult visitPhysicalOlapScan(PhysicalOlapScan olapScan, Void context) {
        return computeOlapScan(olapScan);
    }

    @Override
    public StatsDeriveResult visitPhysicalHeapSort(PhysicalHeapSort<Plan> sort, Void context) {
        return super.visitPhysicalHeapSort(sort, context);
    }

    @Override
    public StatsDeriveResult visitPhysicalHashJoin(PhysicalHashJoin<Plan, Plan> hashJoin, Void context) {
        return HashJoinEstimation.estimate(groupExpression.getChildStats(0), groupExpression.getChildStats(1),
                hashJoin.getCondition().get(), hashJoin.getJoinType());
    }

    // TODO: We should subtract those pruned column, and consider the expression transformations in the node.
    @Override
    public StatsDeriveResult visitPhysicalProject(PhysicalProject<Plan> project, Void context) {
        return groupExpression.getChildStats(0);
    }

    @Override
    public StatsDeriveResult visitPhysicalFilter(PhysicalFilter<Plan> filter, Void context) {
        StatsDeriveResult childStats = groupExpression.getChildStats(0);
        StatsDeriveResult stats = new StatsDeriveResult(childStats);
        FilterSelectivityCalculator selectivityCalculator =
                new FilterSelectivityCalculator(childStats.getSlotRefToColumnStatsMap());
        double selectivity = selectivityCalculator.estimate(filter.getPredicates());
        stats.multiplyDouble(selectivity);
        // TODO: It's very likely to write wrong code in this way, we need to find a graceful way to pass the
        //  slotRefToColStatsMap between stats of each operator.
        stats.setSlotRefToColumnStatsMap(childStats.getSlotRefToColumnStatsMap());
        return stats;
    }

    @Override
    public StatsDeriveResult visitPhysicalDistribution(PhysicalDistribution<Plan> distribution,
            Void context) {
        return groupExpression.getChildStats(0);
    }

    // TODO: 1. Subtract the pruned partition
    //       2. Consider the influence of runtime filter
    //       3. Get NDV and column data size from StatisticManger, StatisticManager doesn't support it now.
    private StatsDeriveResult computeOlapScan(ScanOperator scanOperator) {
        Table table = scanOperator.getTable();
        TableStats tableStats = Utils.execWithReturnVal(() ->
                Catalog.getCurrentCatalog().getStatisticsManager().getStatistics().getTableStats(table.getId())
        );
        Map<Slot, ColumnStats> slotToColumnStats = new HashMap<>();
        Set<SlotReference> slotSet = scanOperator.getOutput().stream().filter(SlotReference.class::isInstance)
                .map(s -> (SlotReference) s).collect(Collectors.toSet());
        for (SlotReference slotReference : slotSet) {
            Column column = slotReference.getColumn();
            if (column == null) {
                // TODO: should we throw an exception here?
                continue;
            }
            String columnName = column.getName();
            ColumnStats columnStats = tableStats.getColumnStats(columnName).copy();
            slotToColumnStats.put(slotReference, columnStats);
        }
        long rowCount = tableStats.getRowCount();
        StatsDeriveResult stats = new StatsDeriveResult((long)(rowCount),
                new HashMap<>(), new HashMap<>());
        stats.setSlotRefToColumnStatsMap(slotToColumnStats);
        return stats;
    }

    private StatsDeriveResult computeAggregate()

}
