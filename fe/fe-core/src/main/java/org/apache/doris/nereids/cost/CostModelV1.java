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
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEsScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSchemaScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;

class CostModelV1 extends PlanVisitor<Cost, PlanContext> {
    /**
     * The intuition behind `HEAVY_OPERATOR_PUNISH_FACTOR` is we need to avoid this form of join patterns:
     * Plan1: L join ( AGG1(A) join AGG2(B))
     * But
     * Plan2: L join AGG1(A) join AGG2(B) is welcomed.
     * AGG is time-consuming operator. From the perspective of rowCount, nereids may choose Plan1,
     * because `Agg1 join Agg2` generates few tuples. But in Plan1, Agg1 and Agg2 are done in serial, in Plan2, Agg1 and
     * Agg2 are done in parallel. And hence, Plan1 should be punished.
     * <p>
     * An example is tpch q15.
     */
    static final double HEAVY_OPERATOR_PUNISH_FACTOR = 0.0;

    public static Cost addChildCost(Plan plan, Cost planCost, Cost childCost, int index) {
        Preconditions.checkArgument(childCost instanceof CostV1 && planCost instanceof CostV1);
        CostV1 childCostV1 = (CostV1) childCost;
        CostV1 planCostV1 = (CostV1) planCost;
        return new CostV1(childCostV1.getCpuCost() + planCostV1.getCpuCost(),
                childCostV1.getMemoryCost() + planCostV1.getMemoryCost(),
                childCostV1.getNetworkCost() + planCostV1.getNetworkCost(),
                childCostV1.getPenalty() + planCostV1.getPenalty());
    }

    @Override
    public Cost visit(Plan plan, PlanContext context) {
        return CostV1.zero();
    }

    @Override
    public Cost visitPhysicalOlapScan(PhysicalOlapScan physicalOlapScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return CostV1.ofCpu(statistics.getRowCount());
    }

    public Cost visitPhysicalSchemaScan(PhysicalSchemaScan physicalSchemaScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return CostV1.ofCpu(statistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalStorageLayerAggregate(
            PhysicalStorageLayerAggregate storageLayerAggregate, PlanContext context) {
        CostV1 costValue = (CostV1) storageLayerAggregate.getRelation().accept(this, context);
        // multiply a factor less than 1, so we can select PhysicalStorageLayerAggregate as far as possible
        return new CostV1(costValue.getCpuCost() * 0.7, costValue.getMemoryCost(),
                costValue.getNetworkCost(), costValue.getPenalty());
    }

    @Override
    public Cost visitPhysicalFileScan(PhysicalFileScan physicalFileScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return CostV1.ofCpu(statistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalProject(PhysicalProject<? extends Plan> physicalProject, PlanContext context) {
        return CostV1.ofCpu(1);
    }

    @Override
    public Cost visitPhysicalJdbcScan(PhysicalJdbcScan physicalJdbcScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return CostV1.ofCpu(statistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalEsScan(PhysicalEsScan physicalEsScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return CostV1.ofCpu(statistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalQuickSort(
            PhysicalQuickSort<? extends Plan> physicalQuickSort, PlanContext context) {
        // TODO: consider two-phase sort and enforcer.
        Statistics statistics = context.getStatisticsWithCheck();
        Statistics childStatistics = context.getChildStatistics(0);
        if (physicalQuickSort.getSortPhase().isGather()) {
            // Now we do more like two-phase sort, so penalise one-phase sort
            statistics = statistics.withRowCount(statistics.getRowCount() * 100);
        }
        return CostV1.of(
                childStatistics.getRowCount(),
                statistics.getRowCount(),
                childStatistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, PlanContext context) {
        // TODO: consider two-phase sort and enforcer.
        Statistics statistics = context.getStatisticsWithCheck();
        Statistics childStatistics = context.getChildStatistics(0);
        if (topN.getSortPhase().isGather()) {
            // Now we do more like two-phase sort, so penalise one-phase sort
            statistics = statistics.withRowCount(statistics.getRowCount() * 100);
        }
        return CostV1.of(
                childStatistics.getRowCount(),
                statistics.getRowCount(),
                childStatistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalPartitionTopN(PhysicalPartitionTopN<? extends Plan> partitionTopN, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        Statistics childStatistics = context.getChildStatistics(0);
        return CostV1.of(
            childStatistics.getRowCount(),
            statistics.getRowCount(),
            childStatistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalDistribute(
            PhysicalDistribute<? extends Plan> distribute, PlanContext context) {
        Statistics childStatistics = context.getChildStatistics(0);
        DistributionSpec spec = distribute.getDistributionSpec();
        // shuffle
        if (spec instanceof DistributionSpecHash) {
            return CostV1.of(
                    0,
                    0,
                    childStatistics.getRowCount());
        }

        // replicate
        if (spec instanceof DistributionSpecReplicated) {
            int beNumber = ConnectContext.get().getEnv().getClusterInfo().getBackendsNumber(true);
            int instanceNumber = ConnectContext.get().getSessionVariable().getParallelExecInstanceNum();
            beNumber = Math.max(1, beNumber);
            double memLimit = ConnectContext.get().getSessionVariable().getMaxExecMemByte();
            //if build side is big, avoid use broadcast join
            double rowsLimit = ConnectContext.get().getSessionVariable().getBroadcastRowCountLimit();
            double brMemlimit = ConnectContext.get().getSessionVariable().getBroadcastHashtableMemLimitPercentage();
            double buildSize = childStatistics.computeSize();
            if (buildSize * instanceNumber > memLimit * brMemlimit
                    || childStatistics.getRowCount() > rowsLimit) {
                return CostV1.of(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);
            }
            // estimate broadcast cost by an experience formula: beNumber^0.5 * rowCount
            // - sender number and receiver number is not available at RBO stage now, so we use beNumber
            // - senders and receivers work in parallel, that why we use square of beNumber
            return CostV1.of(
                    0,
                    0,
                    childStatistics.getRowCount() * Math.pow(beNumber, 0.5));

        }

        // gather
        if (spec instanceof DistributionSpecGather) {
            return CostV1.of(
                    0,
                    0,
                    childStatistics.getRowCount());
        }

        // any
        return CostV1.of(
                childStatistics.getRowCount(),
                0,
                0);
    }

    @Override
    public Cost visitPhysicalHashAggregate(
            PhysicalHashAggregate<? extends Plan> aggregate, PlanContext context) {
        // TODO: stage.....

        Statistics statistics = context.getStatisticsWithCheck();
        Statistics inputStatistics = context.getChildStatistics(0);
        return CostV1.of(inputStatistics.getRowCount(), statistics.getRowCount(), 0);
    }

    @Override
    public Cost visitPhysicalHashJoin(
            PhysicalHashJoin<? extends Plan, ? extends Plan> physicalHashJoin, PlanContext context) {
        Preconditions.checkState(context.arity() == 2);
        Statistics outputStats = context.getStatisticsWithCheck();
        double outputRowCount = outputStats.getRowCount();

        Statistics probeStats = context.getChildStatistics(0);
        Statistics buildStats = context.getChildStatistics(1);

        double leftRowCount = probeStats.getRowCount();
        double rightRowCount = buildStats.getRowCount();
        /*
        pattern1: L join1 (Agg1() join2 Agg2())
        result number of join2 may much less than Agg1.
        but Agg1 and Agg2 are slow. so we need to punish this pattern1.

        pattern2: (L join1 Agg1) join2 agg2
        in pattern2, join1 and join2 takes more time, but Agg1 and agg2 can be processed in parallel.
        */
        double penalty = HEAVY_OPERATOR_PUNISH_FACTOR
                * Math.min(probeStats.getPenalty(), buildStats.getPenalty());
        if (buildStats.getWidth() >= 2) {
            //penalty for right deep tree
            penalty += rightRowCount;
        }
        if (physicalHashJoin.getJoinType().isCrossJoin()) {
            return CostV1.of(leftRowCount + rightRowCount + outputRowCount,
                    0,
                    leftRowCount + rightRowCount,
                    penalty);
        }
        return CostV1.of(leftRowCount + rightRowCount + outputRowCount,
                rightRowCount,
                0,
                penalty
        );
    }

    @Override
    public Cost visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin,
            PlanContext context) {
        // TODO: copy from physicalHashJoin, should update according to physical nested loop join properties.
        Preconditions.checkState(context.arity() == 2);
        Statistics leftStatistics = context.getChildStatistics(0);
        Statistics rightStatistics = context.getChildStatistics(1);

        return CostV1.of(
                leftStatistics.getRowCount() * rightStatistics.getRowCount(),
                rightStatistics.getRowCount(),
                0);
    }

    @Override
    public Cost visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
            PlanContext context) {
        return CostV1.of(
                assertNumRows.getAssertNumRowsElement().getDesiredNumOfRows(),
                assertNumRows.getAssertNumRowsElement().getDesiredNumOfRows(),
                0
        );
    }

    @Override
    public Cost visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return CostV1.of(
                statistics.getRowCount(),
                statistics.getRowCount(),
                0
        );
    }
}
