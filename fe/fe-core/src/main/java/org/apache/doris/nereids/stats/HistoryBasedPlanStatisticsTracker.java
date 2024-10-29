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
import org.apache.doris.planner.PlanNodeWithHash;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.HistoricalPlanStatistics;
import org.apache.doris.statistics.HistoricalPlanStatisticsEntry;
import org.apache.doris.statistics.PlanStatistics;
import org.apache.doris.statistics.PlanStatisticsWithSourceInfo;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import static com.google.common.collect.ImmutableList.toImmutableList;
import com.google.common.collect.ImmutableMap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class HistoryBasedPlanStatisticsTracker {
    private final Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider;
    private final HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager;
    Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> planStatisticsMap = new HashMap<>();

    public HistoryBasedPlanStatisticsTracker(
            Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider,
            HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager) {
        this.historyBasedPlanStatisticsProvider = historyBasedPlanStatisticsProvider;
        this.historyBasedStatisticsCacheManager = historyBasedStatisticsCacheManager;
    }

    public Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> getQueryStats() {
        // todo: session var checking
        // todo: get from where: nodeExecStatsItems
        for (NodeExecStatsItemPB itemPB : nodeExecStatsItems) {
            //nodeExecStatsMap.put(itemPB.getNodeId(), PlanStatistics.buildFromPB(itemPB));
            PlanStatistics planStatistics = PlanStatistics.buildFromPB(itemPB);
            PlanStatisticsWithSourceInfo planStatsWithSourceInfo = new PlanStatisticsWithSourceInfo (
                    planNode.getId(), planStatistics);
            String hash = planNodeCanonicalInfo.get().getHash();
            PlanNodeWithHash planNodeWithHash = new PlanNodeWithHash(statsEquivalentPlanNode, Optional.of(hash));
            planStatisticsMap.put(planNodeWithHash, planStatsWithSourceInfo);
        }
        return ImmutableMap.copyOf(planStatisticsMap);
    }

    public void updateStatistics() {
        // todo: session variables checking
        // get plan statistics from be
        Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> planStatistics = getQueryStats();

        // provider
        Map<PlanNodeWithHash, HistoricalPlanStatistics> historicalPlanStatisticsMap =
                historyBasedPlanStatisticsProvider.get().getStats(
                        planStatistics.keySet().stream().collect(toImmutableList()),
                        getHistoryBasedOptimizerTimeoutLimit(session).toMillis());

        Map<PlanNodeWithHash, HistoricalPlanStatistics> newPlanStatistics = planStatistics.entrySet().stream()
                .filter(entry -> entry.getKey().getHash().isPresent())
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> {
                            HistoricalPlanStatistics historicalPlanStatistics = Optional.ofNullable(historicalPlanStatisticsMap.get(entry.getKey()))
                                    .orElseGet(HistoricalPlanStatistics::empty);
                            return updatePlanStatistics(
                                    historicalPlanStatistics,
                                    entry.getValue().getPlanStatistics());
                        }));

        if (!newPlanStatistics.isEmpty()) {
            historyBasedPlanStatisticsProvider.get().putStats(ImmutableMap.copyOf(newPlanStatistics));
        }
        historyBasedStatisticsCacheManager.invalidate(queryInfo.getQueryId());
    }

    public static HistoricalPlanStatistics updatePlanStatistics(
            HistoricalPlanStatistics historicalPlanStatistics,
            PlanStatistics current)
    {
        List<HistoricalPlanStatisticsEntry> lastRunsStatistics = historicalPlanStatistics.getLastRunsStatistics();

        List<HistoricalPlanStatisticsEntry> newLastRunsStatistics = new ArrayList<>(lastRunsStatistics);

        Optional<Integer> similarStatsIndex = getSimilarStatsIndex(historicalPlanStatistics,
                inputTableStatistics, config.getHistoryMatchingThreshold());
        if (similarStatsIndex.isPresent()) {
            newLastRunsStatistics.remove(similarStatsIndex.get().intValue());
        }

        newLastRunsStatistics.add(new HistoricalPlanStatisticsEntry(current, inputTableStatistics));
        int maxLastRuns = inputTableStatistics.isEmpty() ? 1 : config.getMaxLastRunsHistory();
        if (newLastRunsStatistics.size() > maxLastRuns) {
            newLastRunsStatistics.remove(0);
        }

        return new HistoricalPlanStatistics(newLastRunsStatistics);
    }

}