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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeTopN;
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
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;

import java.util.Collections;

class CostModelV1 extends PlanVisitor<Cost, PlanContext> {

    // for a join, skew = leftRowCount/rightRowCount
    // the higher skew is, the more we prefer broadcast join than shuffle join
    // if skew < BROADCAST_JOIN_SKEW_RATIO, broadcast join will be punished,
    // the penalty factor is no more than BROADCAST_JOIN_SKEW_PENALTY_LIMIT
    static final double BROADCAST_JOIN_SKEW_RATIO = 30.0;
    static final double BROADCAST_JOIN_SKEW_PENALTY_LIMIT = 2.0;
    static final double RANDOM_SHUFFLE_TO_HASH_SHUFFLE_FACTOR = 0.1;
    private final int beNumber;
    private final int parallelInstance;

    public CostModelV1(ConnectContext connectContext) {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        if (sessionVariable.isPlayNereidsDump()) {
            // TODO: @bingfeng refine minidump setting, and pass testMinidumpUt
            beNumber = 1;
            parallelInstance = Math.max(1, connectContext.getSessionVariable().getParallelExecInstanceNum());
        } else if (sessionVariable.getBeNumberForTest() != -1) {
            // shape test, fix the BE number and instance number
            beNumber = sessionVariable.getBeNumberForTest();
            parallelInstance = 8;
        } else {
            beNumber = Math.max(1, ConnectContext.get().getEnv().getClusterInfo().getBackendsNumber(true));
            parallelInstance = Math.max(1, connectContext.getSessionVariable().getParallelExecInstanceNum());
        }
    }

    public static Cost addChildCost(SessionVariable sessionVariable, Cost planCost, Cost childCost) {
        Preconditions.checkArgument(childCost instanceof CostV1 && planCost instanceof CostV1);
        CostV1 childCostV1 = (CostV1) childCost;
        CostV1 planCostV1 = (CostV1) planCost;
        return new CostV1(sessionVariable,
                childCostV1.getCpuCost() + planCostV1.getCpuCost(),
                childCostV1.getMemoryCost() + planCostV1.getMemoryCost(),
                childCostV1.getNetworkCost() + planCostV1.getNetworkCost());
    }

    @Override
    public Cost visit(Plan plan, PlanContext context) {
        return CostV1.zero();
    }

    @Override
    public Cost visitPhysicalOlapScan(PhysicalOlapScan physicalOlapScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return CostV1.ofCpu(context.getSessionVariable(), statistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalDeferMaterializeOlapScan(PhysicalDeferMaterializeOlapScan deferMaterializeOlapScan,
            PlanContext context) {
        return visitPhysicalOlapScan(deferMaterializeOlapScan.getPhysicalOlapScan(), context);
    }

    public Cost visitPhysicalSchemaScan(PhysicalSchemaScan physicalSchemaScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return CostV1.ofCpu(context.getSessionVariable(), statistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalStorageLayerAggregate(
            PhysicalStorageLayerAggregate storageLayerAggregate, PlanContext context) {
        CostV1 costValue = (CostV1) storageLayerAggregate.getRelation().accept(this, context);
        // multiply a factor less than 1, so we can select PhysicalStorageLayerAggregate as far as possible
        return new CostV1(context.getSessionVariable(), costValue.getCpuCost() * 0.7, costValue.getMemoryCost(),
                costValue.getNetworkCost());
    }

    @Override
    public Cost visitPhysicalFileScan(PhysicalFileScan physicalFileScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return CostV1.ofCpu(context.getSessionVariable(), statistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalProject(PhysicalProject<? extends Plan> physicalProject, PlanContext context) {
        return CostV1.ofCpu(context.getSessionVariable(), 1);
    }

    @Override
    public Cost visitPhysicalJdbcScan(PhysicalJdbcScan physicalJdbcScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return CostV1.ofCpu(context.getSessionVariable(), statistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalEsScan(PhysicalEsScan physicalEsScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return CostV1.ofCpu(context.getSessionVariable(), statistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalQuickSort(
            PhysicalQuickSort<? extends Plan> physicalQuickSort, PlanContext context) {
        // TODO: consider two-phase sort and enforcer.
        Statistics statistics = context.getStatisticsWithCheck();
        Statistics childStatistics = context.getChildStatistics(0);

        double childRowCount = childStatistics.getRowCount();
        double rowCount = statistics.getRowCount();
        if (physicalQuickSort.getSortPhase().isGather()) {
            // Now we do more like two-phase sort, so penalise one-phase sort
            rowCount *= 100;
        }
        return CostV1.of(context.getSessionVariable(), childRowCount, rowCount, childRowCount);
    }

    @Override
    public Cost visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, PlanContext context) {
        // TODO: consider two-phase sort and enforcer.
        Statistics statistics = context.getStatisticsWithCheck();
        Statistics childStatistics = context.getChildStatistics(0);

        double childRowCount = childStatistics.getRowCount();
        double rowCount = statistics.getRowCount();
        if (topN.getSortPhase().isGather()) {
            // Now we do more like two-phase sort, so penalise one-phase sort
            rowCount *= 100;
        }
        return CostV1.of(context.getSessionVariable(), childRowCount, rowCount, childRowCount);
    }

    @Override
    public Cost visitPhysicalDeferMaterializeTopN(PhysicalDeferMaterializeTopN<? extends Plan> topN,
            PlanContext context) {
        return visitPhysicalTopN(topN.getPhysicalTopN(), context);
    }

    @Override
    public Cost visitPhysicalPartitionTopN(PhysicalPartitionTopN<? extends Plan> partitionTopN, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        Statistics childStatistics = context.getChildStatistics(0);
        return CostV1.of(context.getSessionVariable(),
                childStatistics.getRowCount(),
                statistics.getRowCount(),
                childStatistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalDistribute(
            PhysicalDistribute<? extends Plan> distribute, PlanContext context) {
        Statistics childStatistics = context.getChildStatistics(0);
        double intputRowCount = childStatistics.getRowCount();
        DistributionSpec spec = distribute.getDistributionSpec();

        // shuffle
        if (spec instanceof DistributionSpecHash) {
            return CostV1.of(context.getSessionVariable(),
                    0,
                    0,
                    intputRowCount * childStatistics.dataSizeFactor() / beNumber);
        }

        // replicate
        if (spec instanceof DistributionSpecReplicated) {
            // estimate broadcast cost by an experience formula: beNumber^0.5 * rowCount
            // - sender number and receiver number is not available at RBO stage now, so we use beNumber
            // - senders and receivers work in parallel, that why we use square of beNumber
            return CostV1.of(context.getSessionVariable(),
                    0,
                    0,
                    intputRowCount * childStatistics.dataSizeFactor());

        }

        // gather
        if (spec instanceof DistributionSpecGather) {
            return CostV1.of(context.getSessionVariable(),
                    0,
                    0,
                    intputRowCount * childStatistics.dataSizeFactor() / beNumber);
        }

        // any
        // cost of random shuffle is lower than hash shuffle.
        return CostV1.of(context.getSessionVariable(),
                0,
                0,
                intputRowCount * childStatistics.dataSizeFactor() * RANDOM_SHUFFLE_TO_HASH_SHUFFLE_FACTOR / beNumber);
    }

    @Override
    public Cost visitPhysicalHashAggregate(
            PhysicalHashAggregate<? extends Plan> aggregate, PlanContext context) {
        Statistics inputStatistics = context.getChildStatistics(0);
        if (aggregate.getAggPhase().isLocal()) {
            return CostV1.of(context.getSessionVariable(), inputStatistics.getRowCount() / beNumber,
                    inputStatistics.getRowCount() / beNumber, 0);
        } else {
            // global
            return CostV1.of(context.getSessionVariable(), inputStatistics.getRowCount(),
                    inputStatistics.getRowCount(), 0);
        }
    }

    private double broadCastJoinBalancePenalty(Statistics probeStats, Statistics buildStats) {
        // if build side is small enough (<1M), broadcast is also good, no penalty
        if (buildStats.computeSize() < 1024 * 1024) {
            return 1;
        }
        double broadcastJoinPenalty = (BROADCAST_JOIN_SKEW_RATIO * buildStats.getRowCount()) / probeStats.getRowCount();
        broadcastJoinPenalty = Math.max(1, broadcastJoinPenalty);
        broadcastJoinPenalty = Math.min(BROADCAST_JOIN_SKEW_PENALTY_LIMIT, broadcastJoinPenalty);
        return broadcastJoinPenalty;
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
        if (leftRowCount == rightRowCount
                && physicalHashJoin.getGroupExpression().isPresent()
                && physicalHashJoin.getGroupExpression().get().getOwnerGroup() != null
                && !physicalHashJoin.getGroupExpression().get().getOwnerGroup().isStatsReliable()) {
            int leftConnectivity = computeConnectivity(physicalHashJoin.left(), context);
            int rightConnectivity = computeConnectivity(physicalHashJoin.right(), context);
            if (rightConnectivity < leftConnectivity) {
                leftRowCount += 1;
            }
        }

        /*
        pattern1: L join1 (Agg1() join2 Agg2())
        result number of join2 may much less than Agg1.
        but Agg1 and Agg2 are slow. so we need to punish this pattern1.

        pattern2: (L join1 Agg1) join2 agg2
        in pattern2, join1 and join2 takes more time, but Agg1 and agg2 can be processed in parallel.
        */
        if (physicalHashJoin.getJoinType().isCrossJoin()) {
            return CostV1.of(context.getSessionVariable(), leftRowCount + rightRowCount + outputRowCount,
                    0,
                    leftRowCount + rightRowCount
            );
        }

        if (context.isBroadcastJoin()) {
            // compared with shuffle join, bc join will be taken a penalty for both build and probe side;
            // currently we use the following factor as the penalty factor:
            // build side factor: totalInstanceNumber to the power of 2, standing for the additional effort for
            //                    bigger cost for building hash table, taken on rightRowCount
            // probe side factor: totalInstanceNumber to the power of 2, standing for the additional effort for
            //                    bigger cost for ProbeWhenBuildSideOutput effort and ProbeWhenSearchHashTableTime
            //                    on the output rows, taken on outputRowCount()
            double probeSideFactor = 1.0;
            double buildSideFactor = context.getSessionVariable().getBroadcastRightTableScaleFactor();
            int totalInstanceNumber = parallelInstance * beNumber;
            if (buildSideFactor <= 1.0) {
                if (buildStats.computeSize() < 1024 * 1024) {
                    // no penalty to broadcast if build side is small
                    buildSideFactor = 1.0;
                } else {
                    // use totalInstanceNumber to the power of 2 as the default factor value
                    buildSideFactor = Math.pow(totalInstanceNumber, 0.5);
                }
            }
            return CostV1.of(context.getSessionVariable(),
                    leftRowCount + rightRowCount * buildSideFactor + outputRowCount * probeSideFactor,
                    rightRowCount,
                    0
            );
        }
        return CostV1.of(context.getSessionVariable(), leftRowCount + rightRowCount + outputRowCount,
                rightRowCount,
                0
        );
    }

    /*
    in a join cluster graph, if a node has higher connectivity, it is more likely to be reduced
    by runtime filters, and it is also more likely to produce effective runtime filters.
    Thus, we prefer to put the node with higher connectivity on the join right side.
     */
    private int computeConnectivity(
            Plan plan, PlanContext context) {
        int connectCount = 0;
        for (Expression expr : context.getStatementContext().getJoinFilters()) {
            connectCount += Collections.disjoint(expr.getInputSlots(), plan.getOutputSet()) ? 0 : 1;
        }
        return connectCount;
    }

    @Override
    public Cost visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin,
            PlanContext context) {
        // TODO: copy from physicalHashJoin, should update according to physical nested loop join properties.
        Preconditions.checkState(context.arity() == 2);
        Statistics leftStatistics = context.getChildStatistics(0);
        Statistics rightStatistics = context.getChildStatistics(1);
        return CostV1.of(context.getSessionVariable(),
                leftStatistics.getRowCount() * rightStatistics.getRowCount(),
                rightStatistics.getRowCount(),
                0);
    }

    @Override
    public Cost visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
            PlanContext context) {
        return CostV1.of(context.getSessionVariable(),
                assertNumRows.getAssertNumRowsElement().getDesiredNumOfRows(),
                assertNumRows.getAssertNumRowsElement().getDesiredNumOfRows(),
                0
        );
    }

    @Override
    public Cost visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return CostV1.of(context.getSessionVariable(),
                statistics.getRowCount(),
                statistics.getRowCount(),
                0
        );
    }
}
