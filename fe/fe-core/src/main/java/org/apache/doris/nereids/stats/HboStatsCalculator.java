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

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanNodeAndHash;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.hbo.PlanStatistics;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatistics;

import java.util.Map;
import java.util.Objects;

/**
 * StatsCalculator by using hbo plan stats. to do estimation.
 */
public class HboStatsCalculator extends StatsCalculator {
    private final HboPlanStatisticsProvider hboPlanStatisticsProvider;

    public HboStatsCalculator(GroupExpression groupExpression, boolean forbidUnknownColStats,
            Map<String, ColumnStatistic> columnStatisticMap, boolean isPlayNereidsDump,
            Map<CTEId, Statistics> cteIdToStats, CascadesContext context) {
        super(groupExpression, forbidUnknownColStats, columnStatisticMap, isPlayNereidsDump,
                cteIdToStats, context);
        this.hboPlanStatisticsProvider = Objects.requireNonNull(Env.getCurrentEnv().getHboPlanStatisticsManager()
                        .getHboPlanStatisticsProvider(), "HboPlanStatisticsProvider is null");
    }

    /**
     * NOTE: Can't override computeScan since the publishing side's plan hash of scan node
     * use the scan's hbo string but embedding the filter info into the input table structure.
     * if the matching logic here want to support filter node's hbo plan stats. reusing, it only needs
     * to hook the computeFilter and use original scan's plan hash string, and also embedding the
     * parent filter info into the scan node also.
     * @param filter filter
     * @return Statistics
     */
    @Override
    public Statistics computeFilter(Filter filter, Statistics inputStats) {
        Statistics legacyStats = super.computeFilter(filter, inputStats);
        boolean isLogicalFilterOnTs = HboUtils.isLogicalFilterOnLogicalScan(filter);
        boolean isPhysicalFilterOnTs = HboUtils.isPhysicalFilterOnPhysicalScan(filter);
        if (isLogicalFilterOnTs || isPhysicalFilterOnTs) {
            AbstractPlan scanPlan = HboUtils.getScanUnderFilterNode(filter);
            return getStatsFromHboPlanStats(scanPlan, legacyStats);
        } else {
            return legacyStats;
        }
    }

    @Override
    public Statistics computeJoin(Join join, Statistics leftStats, Statistics rightStats) {
        Statistics legacyStats = super.computeJoin(join, groupExpression.childStatistics(0),
                groupExpression.childStatistics(1));
        return getStatsFromHboPlanStats((AbstractPlan) join, legacyStats);
    }

    @Override
    public Statistics computeAggregate(Aggregate<? extends Plan> aggregate, Statistics inputStats) {
        Statistics legacyStats = super.computeAggregate(aggregate, inputStats);
        // NOTE: aggr has two times matching, one is the global but logical aggr,
        // another is local but physical aggr.
        // the physical one can be matched but the logical one is hard to be matched.
        // e.g, logical one likes "count(*) AS `count(*)`#4"
        //      local physical one likes "partial_count(*) AS `partial_count(*)`#5"
        //      global physical one likes "count(partial_count(*)#5) AS `count(*)`#4"
        return getStatsFromHboPlanStats((AbstractPlan) aggregate, legacyStats);
    }

    private Statistics getStatsFromHboPlanStats(AbstractPlan planNode, Statistics delegateStats) {
        PlanNodeAndHash planNodeAndHash = HboUtils.getPlanNodeHash(planNode);
        RecentRunsPlanStatistics planStatistics = hboPlanStatisticsProvider.getHboPlanStats(planNodeAndHash);
        PlanStatistics matchedPlanStatistics = HboUtils.getMatchedPlanStatistics(planStatistics,
                cascadesContext.getConnectContext());
        if (matchedPlanStatistics != null) {
            delegateStats = delegateStats.withRowCountAndHboFlag(matchedPlanStatistics.getOutputRows());
        }
        return delegateStats;
    }
}

