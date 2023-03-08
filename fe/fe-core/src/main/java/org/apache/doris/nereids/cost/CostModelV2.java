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
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSchemaScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;

//TODO: add inst number for each expression

class CostModelV2 extends PlanVisitor<Cost, PlanContext> {
    static double HASH_COST = 1.0;
    static double PROBE_COST = 1.2;
    static double CMP_COST = 1.5;
    static double PUSH_DOWN_AGG_COST = 0.1;

    public static Cost addChildCost(Plan plan, Cost planCost, Cost childCost, int index) {
        Preconditions.checkArgument(childCost instanceof CostV2 && planCost instanceof CostV2);
        CostV2 planCostV2 = (CostV2) planCost;
        CostV2 childCostV2 = (CostV2) childCost;
        if (plan instanceof PhysicalLimit) {
            planCostV2 = new CostV2(childCostV2.getStartCost(), childCostV2.getRunCost() * planCostV2.getLimitRation());
        } else if (plan instanceof AbstractPhysicalJoin) {
            if (index == 0) {
                planCostV2.updateChildCost(childCostV2.getStartCost(), childCostV2.getRunCost());
            } else {
                planCostV2.updateChildCost(childCostV2.getRunCost(), 0);
            }
        } else {
            planCostV2.updateChildCost(childCostV2.getStartCost(), childCostV2.getRunCost());
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

        StatsDeriveResult stats = context.getStatisticsWithCheck();

        double ioCost = stats.computeSize();
        double cpuCost = stats.getRowCount() * PUSH_DOWN_AGG_COST;

        double runCost1 = CostWeight.get().weightSum(0, ioCost, 0) / stats.getBENumber();
        double runCost2 = CostWeight.get().weightSum(cpuCost, 0, 0) / stats.getBENumber();

        // Note the stats of this operator is the stats of relation.
        // We need add a plenty for this cost. Maybe set rowCount=1 is better for storageLayer
        double startCost = Double.max(runCost1, runCost2) / 2;
        double totalCost = startCost / 2;

        double runCost = totalCost - startCost;
        return new CostV2(startCost, runCost);
    }

    @Override
    public Cost visitPhysicalFileScan(PhysicalFileScan physicalFileScan, PlanContext context) {
        return calculateScanWithoutRF(context.getStatisticsWithCheck());
    }

    @Override
    public Cost visitPhysicalProject(PhysicalProject<? extends Plan> physicalProject, PlanContext context) {
        StatsDeriveResult statistics = context.getStatisticsWithCheck();
        double cpuCost = statistics.getRowCount() * ExprCostModel.calculateExprCost(physicalProject.getProjects());

        double startCost = 0;
        double runCost = CostWeight.get().weightSum(cpuCost, 0, 0) / statistics.getBENumber();

        return new CostV2(startCost, runCost);
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
        StatsDeriveResult statistics = context.getStatisticsWithCheck();
        StatsDeriveResult childStatistics = context.getChildStatistics(0);

        double runCost;
        if (sort.getSortPhase().isMerge()) {
            runCost = statistics.getRowCount() * CMP_COST * Math.log(childStatistics.getBENumber());
        } else {
            runCost = childStatistics.getRowCount() * CMP_COST * Math.log(statistics.getRowCount())
                    / statistics.getBENumber();
        }

        double startCost = runCost;
        return new CostV2(startCost, runCost);
    }

    @Override
    public Cost visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute, PlanContext context) {
        StatsDeriveResult childStatistics = context.getChildStatistics(0);
        double size = childStatistics.computeSize();

        DistributionSpec spec = distribute.getDistributionSpec();
        double netCost;
        if (spec instanceof DistributionSpecReplicated) {
            netCost = getNetCost(size * childStatistics.getBENumber());
        } else {
            netCost = getNetCost(size);
        }

        double startCost = 0;
        double runCost = CostWeight.get().weightSum(0, 0, netCost) / childStatistics.getBENumber();
        return new CostV2(startCost, runCost);
    }

    @Override
    public Cost visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> aggregate, PlanContext context) {
        StatsDeriveResult stats = context.getStatisticsWithCheck();
        StatsDeriveResult childStats = context.getChildStatistics(0);

        double exprCost = ExprCostModel.calculateExprCost(aggregate.getExpressions());
        return calculateAggregate(stats, childStats, exprCost);
    }

    @Override
    public Cost visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> physicalHashJoin,
            PlanContext context) {
        StatsDeriveResult stats = context.getStatisticsWithCheck();
        StatsDeriveResult leftStats = context.getChildStatistics(0);
        StatsDeriveResult rightStats = context.getChildStatistics(1);
        double otherExprCost = ExprCostModel.calculateExprCost(physicalHashJoin.getOtherJoinConjuncts());

        double buildTableCost = rightStats.getRowCount() * HASH_COST;
        if (context.isBroadcastJoin()) {
            buildTableCost *= stats.getBENumber();
        }
        double probeCost = leftStats.getRowCount() * PROBE_COST + stats.getRowCount() * otherExprCost;

        double startCost = CostWeight.get().weightSum(buildTableCost, 0, 0);
        double runCost = CostWeight.get().weightSum(probeCost, 0, 0) / stats.getBENumber();
        return new CostV2(startCost, runCost);
    }

    @Override
    public Cost visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin,
            PlanContext context) {
        StatsDeriveResult stats = context.getStatisticsWithCheck();
        StatsDeriveResult leftStats = context.getChildStatistics(0);
        StatsDeriveResult rightStats = context.getChildStatistics(1);
        double otherExprCost = ExprCostModel.calculateExprCost(nestedLoopJoin.getOtherJoinConjuncts());

        //NSL materialized right child
        double probeCost = leftStats.getRowCount() * rightStats.getRowCount() * otherExprCost;
        if (!context.isBroadcastJoin()) {
            probeCost /= stats.getBENumber();
        }

        double startCost = 0;
        double runCost = CostWeight.get().weightSum(probeCost, 0, 0) / stats.getBENumber();
        return new CostV2(startCost, runCost);
    }

    @Override
    public Cost visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows, PlanContext context) {
        return new CostV2(0, 0);
    }

    @Override
    public Cost visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, PlanContext context) {
        CostV2 cost = new CostV2(0, 0);
        long rows = limit.getLimit() + limit.getOffset();
        cost.setLimitRation(rows / context.getChildStatistics(0).getRowCount());
        return cost;
    }

    @Override
    public Cost visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate, PlanContext context) {
        StatsDeriveResult statistics = context.getStatisticsWithCheck();

        double exprCost = ExprCostModel.calculateExprCost(generate.getGenerators());
        double cpuCost = exprCost * statistics.getRowCount();

        double startCost = 0;
        double runCost = CostWeight.get().weightSum(cpuCost, 0, 0) / statistics.getBENumber();
        return new CostV2(startCost, runCost);
    }

    @Override
    public Cost visitPhysicalRepeat(PhysicalRepeat<? extends Plan> repeat, PlanContext context) {
        //Repeat expand the tuple according the groupSet
        return new CostV2(0, 0);
    }

    @Override
    public Cost visitPhysicalWindow(PhysicalWindow<? extends Plan> window, PlanContext context) {
        StatsDeriveResult stats = context.getStatisticsWithCheck();
        double exprCost = ExprCostModel.calculateExprCost(window.getWindowExpressions());
        double cpuCost = stats.getRowCount() * exprCost;

        double startCost = 0;
        double runCost = CostWeight.get().weightSum(cpuCost, 0, 0) / stats.getBENumber();
        return new CostV2(startCost, runCost);
    }

    @Override
    public Cost visitPhysicalUnion(PhysicalUnion union, PlanContext context) {
        //Union all operation just concat all tuples
        return new CostV2(0, 0);
    }

    @Override
    public Cost visitPhysicalSetOperation(PhysicalSetOperation intersect, PlanContext context) {
        int rowCount = 0;
        for (StatsDeriveResult childStats : context.getChildrenStatistics()) {
            rowCount += childStats.getRowCount();
        }

        double startCost = CostWeight.get().weightSum(rowCount * HASH_COST, 0, 0);
        double runCost = 0;

        return new CostV2(startCost, runCost);
    }

    @Override
    public Cost visitPhysicalFilter(PhysicalFilter physicalFilter, PlanContext context) {
        StatsDeriveResult stats = context.getStatisticsWithCheck();

        double exprCost = ExprCostModel.calculateExprCost(physicalFilter.getExpressions());
        double cpuCost = exprCost * stats.getRowCount();

        double startCost = 0;
        double runCost = CostWeight.get().weightSum(cpuCost, 0, 0) / stats.getBENumber();

        return new CostV2(startCost, runCost);
    }

    private CostV2 calculateScanWithoutRF(StatsDeriveResult stats) {
        //TODO: consider runtimeFilter
        double io = stats.computeSize();
        double startCost = 0;
        double runCost = CostWeight.get().weightSum(0, io, 0) / stats.getBENumber();
        return new CostV2(startCost, runCost);
    }

    private CostV2 calculateAggregate(StatsDeriveResult stats, StatsDeriveResult childStats, double exprCost) {
        // Build HashTable
        double startCost = CostWeight.get()
                .weightSum(HASH_COST * childStats.getRowCount() + exprCost * childStats.getRowCount(), 0, 0);
        double runCost = 0;
        return new CostV2(startCost, runCost);
    }

    private double getNetCost(double size) {
        // we assume the transferRate is 4MB/s.
        // TODO: setting in session variable
        int transferRate = 4096 * 1024;
        return Math.ceil(size / transferRate);
    }
}
