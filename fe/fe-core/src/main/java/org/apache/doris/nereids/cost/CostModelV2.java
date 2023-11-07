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

package org.apache.doris.nereids.cost;

import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEsScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSchemaScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;

/**
 * This is a cost model to calculate the runCost and startCost of each operator
 */

class CostModelV2 extends PlanVisitor<Cost, PlanContext> {
    static double HASH_COST = 1.0;
    static double PROBE_COST = 1.2;
    static double CMP_COST = 1.5;
    static double PUSH_DOWN_AGG_COST = 0.1;

    private final SessionVariable sessionVariable;

    CostModelV2(SessionVariable sessionVariable) {
        this.sessionVariable = sessionVariable;
    }

    public static Cost addChildCost(Plan plan, Cost planCost, Cost childCost, int index) {
        Preconditions.checkArgument(childCost instanceof CostV2 && planCost instanceof CostV2);
        CostV2 planCostV2 = (CostV2) planCost;
        CostV2 childCostV2 = (CostV2) childCost;
        if (plan instanceof PhysicalLimit) {
            planCostV2 = new CostV2(childCostV2.getStartCost(), childCostV2.getRunCost() * planCostV2.getLimitRation(),
                    childCostV2.getMemory());
        } else if (plan instanceof AbstractPhysicalJoin) {
            if (index == 0) {
                planCostV2.updateChildCost(childCostV2.getStartCost(), childCostV2.getRunCost(),
                        childCostV2.getMemory());
            } else {
                planCostV2.updateChildCost(childCostV2.getRunCost(), 0, childCostV2.getMemory());
            }
        } else {
            planCostV2.updateChildCost(childCostV2.getStartCost(), childCostV2.getRunCost(), childCostV2.getMemory());
        }
        if (index == plan.arity() - 1) {
            planCostV2.finish();
        }
        return planCostV2;
    }

    @Override
    public Cost visit(Plan plan, PlanContext context) {
        return CostV2.zero();
    }

    @Override
    public Cost visitPhysicalOlapScan(PhysicalOlapScan physicalOlapScan, PlanContext context) {
        return calculateScanWithoutRF(context.getStatisticsWithCheck());
    }

    public Cost visitPhysicalSchemaScan(PhysicalSchemaScan physicalSchemaScan, PlanContext context) {
        return calculateScanWithoutRF(context.getStatisticsWithCheck());
    }

    @Override
    public Cost visitPhysicalStorageLayerAggregate(PhysicalStorageLayerAggregate storageLayerAggregate,
            PlanContext context) {

        Statistics stats = context.getStatisticsWithCheck();

        double ioCost = stats.computeSize();

        double runCost1 = CostWeight.get(sessionVariable).weightSum(0, ioCost, 0) / stats.getBENumber();

        // Note the stats of this operator is the stats of relation.
        // We need add a plenty for this cost. Maybe changing rowCount of storageLayer is better
        double startCost = runCost1 / 2;
        double totalCost = startCost;

        double runCost = totalCost - startCost;
        return new CostV2(startCost, runCost, 0);
    }

    @Override
    public Cost visitPhysicalFileScan(PhysicalFileScan physicalFileScan, PlanContext context) {
        return calculateScanWithoutRF(context.getStatisticsWithCheck());
    }

    @Override
    public Cost visitPhysicalProject(PhysicalProject<? extends Plan> physicalProject, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        double cpuCost = statistics.getRowCount() * ExprCostModel.calculateExprCost(physicalProject.getProjects());

        double startCost = 0;
        double runCost = CostWeight.get(sessionVariable).weightSum(cpuCost, 0, 0) / statistics.getBENumber();

        return new CostV2(startCost, runCost, 0);
    }

    @Override
    public Cost visitPhysicalJdbcScan(PhysicalJdbcScan physicalJdbcScan, PlanContext context) {
        return calculateScanWithoutRF(context.getStatisticsWithCheck());
    }

    @Override
    public Cost visitPhysicalEsScan(PhysicalEsScan physicalEsScan, PlanContext context) {
        return calculateScanWithoutRF(context.getStatisticsWithCheck());
    }

    @Override
    public Cost visitAbstractPhysicalSort(AbstractPhysicalSort<? extends Plan> sort, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        Statistics childStatistics = context.getChildStatistics(0);

        double runCost;
        if (sort.getSortPhase().isMerge()) {
            runCost = statistics.getRowCount() * CMP_COST * Math.log(childStatistics.getBENumber());
        } else {
            runCost = childStatistics.getRowCount() * CMP_COST * Math.log(statistics.getRowCount())
                    / statistics.getBENumber();
        }

        double startCost = runCost;
        return new CostV2(startCost, runCost, statistics.computeSize());
    }

    @Override
    public Cost visitPhysicalPartitionTopN(PhysicalPartitionTopN<? extends Plan> partitionTopN, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        Statistics childStatistics = context.getChildStatistics(0);

        // Random set a value. The partitionTopN is generated in the rewrite phase,
        // and it only has one physical implementation. So this cost will not affect the result.
        double runCost = childStatistics.getRowCount() * CMP_COST * Math.log(statistics.getRowCount())
                / statistics.getBENumber();

        double startCost = runCost;
        return new CostV2(startCost, runCost, statistics.computeSize());
    }

    @Override
    public Cost visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute, PlanContext context) {
        Statistics childStatistics = context.getChildStatistics(0);
        double size = childStatistics.computeSize();

        DistributionSpec spec = distribute.getDistributionSpec();
        double netCost;
        if (spec instanceof DistributionSpecReplicated) {
            netCost = getNetCost(size * childStatistics.getBENumber());
        } else {
            netCost = getNetCost(size);
        }

        double startCost = 0;
        double runCost = CostWeight.get(sessionVariable).weightSum(0, 0, netCost) / childStatistics.getBENumber();
        return new CostV2(startCost, runCost, 0);
    }

    @Override
    public Cost visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> aggregate, PlanContext context) {
        Statistics stats = context.getStatisticsWithCheck();
        Statistics childStats = context.getChildStatistics(0);

        double exprCost = ExprCostModel.calculateExprCost(aggregate.getExpressions());
        return calculateAggregate(stats, childStats, exprCost);
    }

    @Override
    public Cost visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> physicalHashJoin,
            PlanContext context) {
        Statistics stats = context.getStatisticsWithCheck();
        Statistics leftStats = context.getChildStatistics(0);
        Statistics rightStats = context.getChildStatistics(1);
        double otherExprCost = ExprCostModel.calculateExprCost(physicalHashJoin.getOtherJoinConjuncts());

        double buildTableCost = rightStats.getRowCount() * HASH_COST;
        if (context.isBroadcastJoin()) {
            buildTableCost *= stats.getBENumber();
        }
        double probeCost = leftStats.getRowCount() * PROBE_COST + stats.getRowCount() * otherExprCost;

        double startCost = CostWeight.get(sessionVariable).weightSum(buildTableCost, 0, 0);
        double runCost = CostWeight.get(sessionVariable).weightSum(probeCost, 0, 0) / stats.getBENumber();
        return new CostV2(startCost, runCost, rightStats.computeSize());
    }

    @Override
    public Cost visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin,
            PlanContext context) {
        Statistics stats = context.getStatisticsWithCheck();
        Statistics leftStats = context.getChildStatistics(0);
        Statistics rightStats = context.getChildStatistics(1);
        double otherExprCost = ExprCostModel.calculateExprCost(nestedLoopJoin.getOtherJoinConjuncts());

        //NSL materialized right child
        double probeCost = leftStats.getRowCount() * rightStats.getRowCount() * otherExprCost;
        if (!context.isBroadcastJoin()) {
            probeCost /= stats.getBENumber();
        }

        double startCost = 0;
        double runCost = CostWeight.get(sessionVariable).weightSum(probeCost, 0, 0) / stats.getBENumber();
        return new CostV2(startCost, runCost, rightStats.computeSize());
    }

    @Override
    public Cost visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows, PlanContext context) {
        return new CostV2(0, 0, 0);
    }

    @Override
    public Cost visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, PlanContext context) {
        CostV2 cost = new CostV2(0, 0, 0);
        long rows = limit.getLimit() + limit.getOffset();
        cost.setLimitRation(rows / context.getChildStatistics(0).getRowCount());
        return cost;
    }

    @Override
    public Cost visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();

        double exprCost = ExprCostModel.calculateExprCost(generate.getGenerators());
        double cpuCost = exprCost * statistics.getRowCount();

        double startCost = 0;
        double runCost = CostWeight.get(sessionVariable).weightSum(cpuCost, 0, 0) / statistics.getBENumber();
        return new CostV2(startCost, runCost, 0);
    }

    @Override
    public Cost visitPhysicalRepeat(PhysicalRepeat<? extends Plan> repeat, PlanContext context) {
        //Repeat expand the tuple according the groupSet
        return new CostV2(0, 0, 0);
    }

    @Override
    public Cost visitPhysicalWindow(PhysicalWindow<? extends Plan> window, PlanContext context) {
        Statistics stats = context.getStatisticsWithCheck();
        double exprCost = ExprCostModel.calculateExprCost(window.getWindowExpressions());
        double cpuCost = stats.getRowCount() * exprCost;

        double startCost = 0;
        double runCost = CostWeight.get(sessionVariable).weightSum(cpuCost, 0, 0) / stats.getBENumber();
        return new CostV2(startCost, runCost, 0);
    }

    @Override
    public Cost visitPhysicalUnion(PhysicalUnion union, PlanContext context) {
        //Union all operation just concat all tuples
        return new CostV2(0, 0, 0);
    }

    @Override
    public Cost visitPhysicalSetOperation(PhysicalSetOperation intersect, PlanContext context) {
        int rowCount = 0;
        double size = 0;
        for (Statistics childStats : context.getChildrenStatistics()) {
            rowCount += childStats.getRowCount();
            size += childStats.computeSize();
        }

        double startCost = CostWeight.get(sessionVariable).weightSum(rowCount * HASH_COST, 0, 0);
        double runCost = 0;

        return new CostV2(startCost, runCost, size);
    }

    @Override
    public Cost visitPhysicalFilter(PhysicalFilter physicalFilter, PlanContext context) {
        Statistics stats = context.getStatisticsWithCheck();

        double exprCost = ExprCostModel.calculateExprCost(physicalFilter.getExpressions());
        double cpuCost = exprCost * stats.getRowCount();

        double startCost = 0;
        double runCost = CostWeight.get(sessionVariable).weightSum(cpuCost, 0, 0) / stats.getBENumber();

        return new CostV2(startCost, runCost, 0);
    }

    private CostV2 calculateScanWithoutRF(Statistics stats) {
        //TODO: consider runtimeFilter
        double io = stats.computeSize();
        double startCost = 0;
        double runCost = CostWeight.get(sessionVariable).weightSum(0, io, 0) / stats.getBENumber();
        return new CostV2(startCost, runCost, 0);
    }

    private CostV2 calculateAggregate(Statistics stats, Statistics childStats, double exprCost) {
        // Build HashTable
        double startCost = CostWeight.get(sessionVariable)
                .weightSum(HASH_COST * childStats.getRowCount() + exprCost * childStats.getRowCount(), 0, 0);
        double runCost = 0;
        return new CostV2(startCost, runCost, stats.computeSize());
    }

    private double getNetCost(double size) {
        // we assume the transferRate is 4MB/s.
        // TODO: setting in session variable
        int transferRate = 4096 * 1024;
        return Math.ceil(size / transferRate);
    }
}
