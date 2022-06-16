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
import org.apache.doris.nereids.PlanOperatorVisitor;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.operators.plans.physical.PhysicalAggregation;
import org.apache.doris.nereids.operators.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.operators.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.operators.plans.physical.PhysicalOperator;
import org.apache.doris.nereids.operators.plans.physical.PhysicalProject;
import org.apache.doris.nereids.cost.statistics.StatisticsEstimate;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Preconditions;

/**
 * Calculate the cost of a plan.
 */
public class CostCalculator {
    /**
     * Constructor.
     */
    public double calculateCost(GroupExpression expression) {
        assert expression.getOperator() instanceof PhysicalOperator;
        PlanContext planContext = new PlanContext();
        CostEstimator costCalculator = new CostEstimator();
        CostEstimate costEstimate = expression.getOperator().accept(costCalculator, planContext);
        return costFormula(costEstimate);
    }

    private double costFormula(CostEstimate costEstimate) {
        double cpuCostWeight = 1;
        double memoryCostWeight = 1;
        double networkCostWeight = 1;
        return costEstimate.getCpuCost() * cpuCostWeight + costEstimate.getMemoryCost() * memoryCostWeight
                + costEstimate.getNetworkCost() * networkCostWeight;
    }

    private static class CostEstimator extends PlanOperatorVisitor<CostEstimate, PlanContext> {
        @Override
        public CostEstimate visitPlan(Plan plan, PlanContext context) {
            return null;
        }

        @Override
        public CostEstimate visitOperator(Operator operator, PlanContext context) {
            return CostEstimate.zero();
        }

        @Override
        public CostEstimate visitPhysicalAggregation(PhysicalAggregation physicalAggregation, PlanContext context) {
            StatisticsEstimate statistics = context.getStatisticsWithCheck();
            return CostEstimate.ofCpu(-1);
        }

        @Override
        public CostEstimate visitPhysicalOlapScan(PhysicalOlapScan physicalOlapScan, PlanContext context) {
            StatisticsEstimate statistics = context.getStatisticsWithCheck();
            return CostEstimate.ofCpu(-1);
        }

        @Override
        public CostEstimate visitPhysicalHashJoin(PhysicalHashJoin physicalHashJoin, PlanContext context) {
            Preconditions.checkState(context.getGroupExpression().arity() == 2);
            Preconditions.checkState(context.getChildrenStats().size() == 2);

            StatisticsEstimate statistics = context.getStatisticsWithCheck();
            StatisticsEstimate leftStatistics = context.getChildStatistics(0);
            StatisticsEstimate rightStatistics = context.getChildStatistics(1);

            // TODO: predicate in onClause.

            return new CostEstimate(-1, -1, -1);
        }

        @Override
        public CostEstimate visitPhysicalProject(PhysicalProject physicalProject, PlanContext context) {
            StatisticsEstimate statistics = context.getStatisticsWithCheck();
            return CostEstimate.ofCpu(-1);
        }
    }
}
