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

import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribution;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHeapSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.statistics.AggStatsDerive;
import org.apache.doris.statistics.ExchangeStatsDerive;
import org.apache.doris.statistics.FilterStatsDerive;
import org.apache.doris.statistics.HashJoinStatsDerive;
import org.apache.doris.statistics.OlapScanStatsDerive;
import org.apache.doris.statistics.SelectStatsDerive;
import org.apache.doris.statistics.StatsDeriveResult;

/**
 * This class is used to get the statistics info for each group.
 */
public class StatsCalculator extends DefaultPlanVisitor<StatsDeriveResult, PlanContext> {

    /**
     * Gather statistic information of GroupExpression's parent group.
     */
    public void estimate(GroupExpression groupExpression) {
        Plan plan = groupExpression.getPlan();
        PlanContext planContext = new PlanContext(groupExpression);
        planContext.setStatsDeriveResult(plan.accept(this, planContext));
    }

    @Override
    public StatsDeriveResult visitPhysicalAggregate(PhysicalAggregate<Plan> agg, PlanContext context) {
        AggStatsDerive aggStatsDerive = new AggStatsDerive();
        aggStatsDerive.init(context);
        return aggStatsDerive.deriveStats();
    }

    @Override
    public StatsDeriveResult visitPhysicalOlapScan(PhysicalOlapScan olapScan, PlanContext context) {
        OlapScanStatsDerive olapScanStatsDerive = new OlapScanStatsDerive();
        olapScanStatsDerive.init(context);
        return olapScanStatsDerive.deriveStats();
    }

    @Override
    public StatsDeriveResult visitPhysicalHeapSort(PhysicalHeapSort<Plan> sort, PlanContext context) {
        return new SelectStatsDerive().deriveStats();
    }

    @Override
    public StatsDeriveResult visitPhysicalHashJoin(PhysicalHashJoin<Plan, Plan> hashJoin, PlanContext context) {
        HashJoinStatsDerive hashJoinStatsDerive = new HashJoinStatsDerive();
        hashJoinStatsDerive.init(context);
        return hashJoinStatsDerive.deriveStats();
    }

    @Override
    public StatsDeriveResult visitPhysicalProject(PhysicalProject<Plan> project, PlanContext context) {
        return new SelectStatsDerive().deriveStats();
    }

    @Override
    public StatsDeriveResult visitPhysicalFilter(PhysicalFilter<Plan> filter, PlanContext context) {
        FilterStatsDerive filterStatsDerive = new FilterStatsDerive();
        filterStatsDerive.init(context);
        return filterStatsDerive.deriveStats();
    }

    @Override
    public StatsDeriveResult visitPhysicalDistribution(PhysicalDistribution<Plan> distribution, PlanContext context) {
        return new ExchangeStatsDerive().deriveStats();
    }
}
