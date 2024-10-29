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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.HistoryBasedPlanStatisticsProvider;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.function.Supplier;

/**
 * HistoryBasedPlanStatisticsCalculator
 */
public class HistoryBasedPlanStatisticsCalculator extends StatsCalculator {
    private final Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider;
    private final HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager;
    public HistoryBasedPlanStatisticsCalculator(GroupExpression groupExpression, boolean forbidUnknownColStats,
            Map<String, ColumnStatistic> columnStatisticMap, boolean isPlayNereidsDump,
            Map<CTEId, Statistics> cteIdToStats, CascadesContext context) {
        super(groupExpression, forbidUnknownColStats, columnStatisticMap, isPlayNereidsDump,
                cteIdToStats, context);
        this.historyBasedPlanStatisticsProvider = requireNonNull(historyBasedPlanStatisticsProvider, "historyBasedPlanStatisticsProvider is null");
        this.historyBasedStatisticsCacheManager = requireNonNull(historyBasedStatisticsCacheManager, "historyBasedStatisticsCacheManager is null");
    }

    @Override
    public void estimate() {
        super.estimate();
    }

    @Override
    protected Statistics computeFilter(Filter filter) {
        Statistics childStats = groupExpression.childStatistics(0);
        return getStatistics(filter, childStats);
    }

    @Override
    protected Statistics computeJoin(Join join) {
        Statistics legacyStats = JoinEstimation.estimate(
                groupExpression.childStatistics(0),
                groupExpression.childStatistics(1), join);
        return getStatistics(join, legacyStats);
    }

    @Override
    protected Statistics computeAggregate(Aggregate<? extends Plan> aggregate) {
        Statistics childStats = groupExpression.childStatistics(0);
        return getStatistics(aggregate, childStats);
    }


    private Statistics getStatistics(Plan planNode, Statistics delegateStats)
    {
        //if (!useHistoryBasedPlanStatisticsEnabled(session)) {
        //    return delegateStats;
        //}

        Plan plan = resolveGroupReferences(planNode, lookup);
        Map<PlanCanonicalizationStrategy, PlanNodeWithHash> allHashes = getPlanNodeHashes(plan, session, true);

        Map<PlanNodeWithHash, HistoricalPlanStatistics> statistics = ImmutableMap.of();
        try {
            statistics = historyBasedStatisticsCacheManager
                    .getStatisticsCache(session.getQueryId(), historyBasedPlanStatisticsProvider, getHistoryBasedOptimizerTimeoutLimit(session).toMillis())
                    .getAll(allHashes.values().stream().distinct().collect(toImmutableList()));
        }
        catch (ExecutionException e) {
            throw new RuntimeException(format("Unable to get plan statistics for %s", planNode), e.getCause());
        }
        double historyMatchingThreshold = getHistoryInputTableStatisticsMatchingThreshold(session);
        // Return statistics corresponding to first strategy that we find, in order specified by `historyBasedPlanCanonicalizationStrategyList`
        for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList(session)) {
            for (Map.Entry<PlanNodeWithHash, HistoricalPlanStatistics> entry : statistics.entrySet()) {
                if (allHashes.containsKey(strategy) && entry.getKey().getHash().isPresent() && allHashes.get(strategy).equals(entry.getKey())) {
                    Optional<List<PlanStatistics>> inputTableStatistics = getPlanNodeInputTableStatistics(plan, session, strategy, true);
                    if (inputTableStatistics.isPresent()) {
                        Optional<HistoricalPlanStatisticsEntry> historicalPlanStatisticsEntry = getSelectedHistoricalPlanStatisticsEntry(entry.getValue(), inputTableStatistics.get(), historyMatchingThreshold);
                        if (historicalPlanStatisticsEntry.isPresent()) {
                            PlanStatistics predictedPlanStatistics = historicalPlanStatisticsEntry.get().getPlanStatistics();
                            if ((toConfidenceLevel(predictedPlanStatistics.getConfidence()).getConfidenceOrdinal() >= delegateStats.confidenceLevel().getConfidenceOrdinal())) {
                                return delegateStats.combineStats(
                                        predictedPlanStatistics,
                                        new HistoryBasedSourceInfo(entry.getKey().getHash(), inputTableStatistics, Optional.ofNullable(historicalPlanStatisticsEntry.get().getHistoricalPlanStatisticsEntryInfo())));
                            }
                        }
                    }
                }
            }
        }

        return delegateStats;
    }
}