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

    /**
     * Constructor.
     */
    public static double calculateCost(GroupExpression groupExpression) {
        PlanContext planContext = new PlanContext(groupExpression);
        CostEstimator costCalculator = new CostEstimator();
        CostEstimate costEstimate = groupExpression.getPlan().accept(costCalculator, planContext);
        groupExpression.estimate = costEstimate;
        CostWeight costWeight = new CostWeight(1, 1, 1.5, 0.5);
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
            double rightDeepPenalty = 0.0;
            if (buildStats.width > 2) {
                rightDeepPenalty = Math.abs(leftRowCount - rightRowCount);
            }
            if (physicalHashJoin.getJoinType().isCrossJoin()) {
                return CostEstimate.of(leftRowCount + rightRowCount + outputRowCount,
                        0,
                        leftRowCount + rightRowCount,
                        rightDeepPenalty);
            }
            return CostEstimate.of(leftRowCount + rightRowCount + outputRowCount,
                    rightRowCount,
                    0,
                    rightDeepPenalty
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
    }

    //private static CostEstimate calculateJoinInputCost(PlanContext context) {
    //    StatsDeriveResult probeStats = context.getChildStatistics(0);
    //    StatsDeriveResult buildStats = context.getChildStatistics(1);
    //    List<Id> leftIds = context.getChildOutputIds(0);
    //    List<Id> rightIds = context.getChildOutputIds(1);
    //
    //    double cpuCost = probeStats.computeColumnSize(leftIds) + buildStats.computeColumnSize(rightIds);
    //    double memoryCost = buildStats.computeColumnSize(rightIds);
    //
    //    return CostEstimate.of(cpuCost, memoryCost, 0);
    //}

    //private static CostEstimate calculateJoinOutputCost(
    //        PhysicalHashJoin<? extends Plan, ? extends Plan> physicalHashJoin) {
    //    StatsDeriveResult outputStats = physicalHashJoin.getGroupExpression().get().getOwnerGroup().getStatistics();
    //
    //    double size = outputStats.computeSize();
    //    return CostEstimate.ofCpu(size);
    //}
}
