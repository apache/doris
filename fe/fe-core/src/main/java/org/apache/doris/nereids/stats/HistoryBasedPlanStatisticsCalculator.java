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
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeWithHash;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.HistoricalPlanStatistics;
import org.apache.doris.statistics.HistoricalPlanStatisticsEntry;
import org.apache.doris.statistics.HistoryBasedPlanStatisticsProvider;
import org.apache.doris.statistics.HistoryBasedSourceInfo;
import org.apache.doris.statistics.PlanStatistics;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import static com.google.common.collect.ImmutableList.toImmutableList;
import com.google.common.collect.ImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.nereids.stats.HistoryBasedPlanStatisticsTracker.getSimilarStatsIndex;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * HistoryBasedPlanStatisticsCalculator
 */
public class HistoryBasedPlanStatisticsCalculator extends StatsCalculator {
    private final HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider;
    private final HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager;
    private final String queryId;
    public HistoryBasedPlanStatisticsCalculator(GroupExpression groupExpression, boolean forbidUnknownColStats,
            Map<String, ColumnStatistic> columnStatisticMap, boolean isPlayNereidsDump,
            Map<CTEId, Statistics> cteIdToStats, CascadesContext context) {
        super(groupExpression, forbidUnknownColStats, columnStatisticMap, isPlayNereidsDump,
                cteIdToStats, context);
        //WorkloadRuntimeStatusMgr mgr = Env.getCurrentEnv().getWorkloadRuntimeStatusMgr();
        //List<AuditEvent> auditEventList = mgr.getQueryNeedAudit();
        //AuditEvent event = auditEventList.get(0);
        this.queryId = "123";//event.queryId;
        this.historyBasedPlanStatisticsProvider = requireNonNull(context.getStatementContext().getHistoryBasedPlanStatisticsTracker().getHistoryBasedPlanStatisticsProvider(),
                "historyBasedPlanStatisticsProvider is null");
        this.historyBasedStatisticsCacheManager = requireNonNull(context.getStatementContext().getHistoryBasedPlanStatisticsTracker().getHistoryBasedStatisticsCacheManager(),
                "historyBasedStatisticsCacheManager is null");
    }

    @Override
    public void estimate() {
        super.estimate();
    }

    @Override
    protected Statistics computeFilter(Filter filter) {
        Statistics childStats = groupExpression.childStatistics(0);
        return getHistoricalStatistics((AbstractPlan) filter, childStats);
    }

    @Override
    protected Statistics computeJoin(Join join) {
        Statistics legacyStats = JoinEstimation.estimate(
                groupExpression.childStatistics(0),
                groupExpression.childStatistics(1), join);
        return getHistoricalStatistics((AbstractPlan) join, legacyStats);
    }

    @Override
    protected Statistics computeAggregate(Aggregate<? extends Plan> aggregate) {
        Statistics childStats = groupExpression.childStatistics(0);
        return getHistoricalStatistics((AbstractPlan) aggregate, childStats);
    }

    private Statistics getHistoricalStatistics(AbstractPlan planNode, Statistics delegateStats)
    {
        boolean isEnableHbo = false;
        if (!isEnableHbo) {
            return delegateStats;
        } else {
            String hash;
            if (planNode instanceof AbstractPhysicalPlan) {
                hash = planNode.hboTreeString();
            } else if (planNode instanceof AbstractLogicalPlan) {
                hash = planNode.hboTreeString();
            } else {
                throw new IllegalStateException("hbo get neither physical plan nor logical plan");
            }
            PlanNodeWithHash planNodeWithHash = new PlanNodeWithHash(null, Optional.of(hash));
            HistoricalPlanStatistics planStatistics = historyBasedStatisticsCacheManager
                    .getStatisticsCache(queryId, historyBasedPlanStatisticsProvider, 1000)
                    .getUnchecked(planNodeWithHash);

            Optional<List<PlanStatistics>> inputTableStatistics = getPlanNodeInputTableStatistics(planNode, true);
            // TODO: get current inputTableStatistics
            Optional<HistoricalPlanStatisticsEntry> historicalPlanStatisticsEntry
                    = getSelectedHistoricalPlanStatisticsEntry
                    (planStatistics, inputTableStatistics.get(), 0.1);
            if (historicalPlanStatisticsEntry.isPresent()) {
                PlanStatistics predictedPlanStatistics = historicalPlanStatisticsEntry.get().getPlanStatistics();
                // todo: choose which one is the output rows count
                delegateStats.withRowCountAndEnforceValid(predictedPlanStatistics.getPushRows());
            }
            return delegateStats;
        }
    }

    private Optional<List<PlanStatistics>> getPlanNodeInputTableStatistics(AbstractPlan plan, boolean cacheOnly)
    {
        return Optional.empty(); //getInputTableStatistics(plan, cacheOnly);
    }

    public static Optional<HistoricalPlanStatisticsEntry> getSelectedHistoricalPlanStatisticsEntry(
            HistoricalPlanStatistics historicalPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double historyMatchingThreshold)
    {
        List<HistoricalPlanStatisticsEntry> lastRunsStatistics = historicalPlanStatistics.getLastRunsStatistics();
        if (lastRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        Optional<Integer> similarStatsIndex = getSimilarStatsIndex(historicalPlanStatistics, inputTableStatistics, historyMatchingThreshold);


        if (similarStatsIndex.isPresent()) {
            return Optional.of(lastRunsStatistics.get(similarStatsIndex.get()));
        }

        // TODO: Use linear regression to predict stats if we have only 1 table.
        return Optional.empty();
    }
}