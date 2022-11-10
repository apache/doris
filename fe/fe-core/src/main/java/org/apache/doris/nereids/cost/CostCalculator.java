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

import org.apache.doris.common.Id;
import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLocalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Calculate the cost of a plan.
 * Inspired by Presto.
 */
public class CostCalculator {
    static final double CPU_WEIGHT = 1;
    static final double MEMORY_WEIGHT = 1;
    static final double NETWORK_WEIGHT = 1.5;

    /**
     * The intuition behind `HEAVY_OPERATOR_PUNISH_FACTOR` is we need to avoid this form of join patterns:
     * Plan1: L join ( AGG1(A) join AGG2(B))
     * But
     * Plan2: L join AGG1(A) join AGG2(B) is welcomed.
     * AGG is time-consuming operator. From the perspective of rowCount, nereids may choose Plan1,
     * because `Agg1 join Agg2` generates few tuples. But in Plan1, Agg1 and Agg2 are done in serial, in Plan2, Agg1 and
     * Agg2 are done in parallel. And hence, Plan1 should be punished.
     *
     * An example is tpch q15.
     */
    static final double HEAVY_OPERATOR_PUNISH_FACTOR = 6.0;

    /**
     * Constructor.
     */
    public static double calculateCost(GroupExpression groupExpression) {
        PlanContext planContext = new PlanContext(groupExpression);
        CostEstimator costCalculator = new CostEstimator();
        CostEstimate costEstimate = groupExpression.getPlan().accept(costCalculator, planContext);
        groupExpression.setCostEstimate(costEstimate);
        /*
         * About PENALTY:
         * Except stats information, there are some special criteria in doris.
         * For example, in hash join cluster, BE could build hash tables
         * in parallel for left deep tree. And hence, we need to punish right deep tree.
         * penalyWeight is the factor of punishment.
         * The punishment is denoted by stats.penalty.
         */
        CostWeight costWeight = new CostWeight(CPU_WEIGHT, MEMORY_WEIGHT, NETWORK_WEIGHT,
                ConnectContext.get().getSessionVariable().getNereidsCboPenaltyFactor());
        return costWeight.calculate(costEstimate);
    }

    private static class CostEstimator extends PlanVisitor<CostEstimate, PlanContext> {
        @Override
        public CostEstimate visit(Plan plan, PlanContext context) {
            return CostEstimate.zero();
        }

        @Override
        public CostEstimate visitPhysicalOlapScan(PhysicalOlapScan physicalOlapScan, PlanContext context) {
            StatsDeriveResult statistics = context.getStatisticsWithCheck();
            return CostEstimate.ofCpu(statistics.computeSize());
        }

        @Override
        public CostEstimate visitPhysicalProject(PhysicalProject<? extends Plan> physicalProject, PlanContext context) {
            return CostEstimate.ofCpu(1);
        }

        @Override
        public CostEstimate visitPhysicalQuickSort(
                PhysicalQuickSort<? extends Plan> physicalQuickSort, PlanContext context) {
            // TODO: consider two-phase sort and enforcer.
            StatsDeriveResult statistics = context.getStatisticsWithCheck();
            StatsDeriveResult childStatistics = context.getChildStatistics(0);

            return CostEstimate.of(
                    childStatistics.computeSize(),
                    statistics.computeSize(),
                    childStatistics.computeSize());
        }

        @Override
        public CostEstimate visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, PlanContext context) {
            // TODO: consider two-phase sort and enforcer.
            StatsDeriveResult statistics = context.getStatisticsWithCheck();
            StatsDeriveResult childStatistics = context.getChildStatistics(0);

            return CostEstimate.of(
                    childStatistics.computeSize(),
                    statistics.computeSize(),
                    childStatistics.computeSize());
        }

        @Override
        public CostEstimate visitPhysicalLocalQuickSort(
                PhysicalLocalQuickSort<? extends Plan> sort, PlanContext context) {
            // TODO: consider two-phase sort and enforcer.
            StatsDeriveResult statistics = context.getStatisticsWithCheck();
            StatsDeriveResult childStatistics = context.getChildStatistics(0);

            return CostEstimate.of(
                    childStatistics.computeSize(),
                    statistics.computeSize(),
                    0);
        }

        @Override
        public CostEstimate visitPhysicalDistribute(
                PhysicalDistribute<? extends Plan> distribute, PlanContext context) {
            StatsDeriveResult childStatistics = context.getChildStatistics(0);
            DistributionSpec spec = distribute.getDistributionSpec();
            // shuffle
            if (spec instanceof DistributionSpecHash) {
                return CostEstimate.of(
                        childStatistics.computeSize(),
                        0,
                        childStatistics.computeSize());
            }

            // replicate
            if (spec instanceof DistributionSpecReplicated) {
                int beNumber = ConnectContext.get().getEnv().getClusterInfo().getBackendIds(true).size();
                int instanceNumber = ConnectContext.get().getSessionVariable().getParallelExecInstanceNum();
                beNumber = Math.max(1, beNumber);

                return CostEstimate.of(
                        childStatistics.computeSize() * beNumber,
                        childStatistics.computeSize() * beNumber * instanceNumber,
                        childStatistics.computeSize() * beNumber * instanceNumber);
            }

            // gather
            if (spec instanceof DistributionSpecGather) {
                return CostEstimate.of(
                        childStatistics.computeSize(),
                        0,
                        childStatistics.computeSize());
            }

            // any
            return CostEstimate.of(
                    childStatistics.computeSize(),
                    0,
                    0);
        }

        @Override
        public CostEstimate visitPhysicalAggregate(PhysicalAggregate<? extends Plan> aggregate, PlanContext context) {
            // TODO: stage.....

            StatsDeriveResult statistics = context.getStatisticsWithCheck();
            StatsDeriveResult inputStatistics = context.getChildStatistics(0);
            return CostEstimate.of(inputStatistics.computeSize(), statistics.computeSize(), 0);
        }

        @Override
        public CostEstimate visitPhysicalHashJoin(
                PhysicalHashJoin<? extends Plan, ? extends Plan> physicalHashJoin, PlanContext context) {
            Preconditions.checkState(context.getGroupExpression().arity() == 2);
            Preconditions.checkState(context.getChildrenStats().size() == 2);

            StatsDeriveResult outputStats = physicalHashJoin.getGroupExpression().get().getOwnerGroup().getStatistics();
            double outputRowCount = outputStats.computeSize();

            StatsDeriveResult probeStats = context.getChildStatistics(0);
            StatsDeriveResult buildStats = context.getChildStatistics(1);
            List<Id> leftIds = context.getChildOutputIds(0);
            List<Id> rightIds = context.getChildOutputIds(1);

            double leftRowCount = probeStats.computeColumnSize(leftIds);
            double rightRowCount = buildStats.computeColumnSize(rightIds);
            /*
            pattern1: L join1 (Agg1() join2 Agg2())
            result number of join2 may much less than Agg1.
            but Agg1 and Agg2 are slow. so we need to punish this pattern1.

            pattern2: (L join1 Agg1) join2 agg2
            in pattern2, join1 and join2 takes more time, but Agg1 and agg2 can be processed in parallel.
            */
            double penalty = CostCalculator.HEAVY_OPERATOR_PUNISH_FACTOR
                    * Math.min(probeStats.getPenalty(), buildStats.getPenalty());
            if (buildStats.getWidth() >= 2) {
                //penalty for right deep tree
                penalty += Math.abs(leftRowCount - rightRowCount);
            }

            if (physicalHashJoin.getJoinType().isCrossJoin()) {
                return CostEstimate.of(leftRowCount + rightRowCount + outputRowCount,
                        0,
                        leftRowCount + rightRowCount,
                        penalty);
            }
            return CostEstimate.of(leftRowCount + rightRowCount + outputRowCount,
                    rightRowCount,
                    0,
                    penalty
            );
        }

        @Override
        public CostEstimate visitPhysicalNestedLoopJoin(
                PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin,
                PlanContext context) {
            // TODO: copy from physicalHashJoin, should update according to physical nested loop join properties.
            Preconditions.checkState(context.getGroupExpression().arity() == 2);
            Preconditions.checkState(context.getChildrenStats().size() == 2);

            StatsDeriveResult leftStatistics = context.getChildStatistics(0);
            StatsDeriveResult rightStatistics = context.getChildStatistics(1);

            return CostEstimate.of(
                    leftStatistics.computeSize() * rightStatistics.computeSize(),
                    rightStatistics.computeSize(),
                    0);
        }

        @Override
        public CostEstimate visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
                PlanContext context) {
            return CostEstimate.of(
                    assertNumRows.getAssertNumRowsElement().getDesiredNumOfRows(),
                    assertNumRows.getAssertNumRowsElement().getDesiredNumOfRows(),
                    0
            );
        }
    }
}
