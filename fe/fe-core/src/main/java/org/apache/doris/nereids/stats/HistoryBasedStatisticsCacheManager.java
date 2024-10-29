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

public class HistoryBasedStatisticsCacheManager
{
    // Cache historical statistics of plan node.
    private final Map<QueryId, LoadingCache<PlanNodeWithHash, HistoricalPlanStatistics>> statisticsCache = new ConcurrentHashMap<>();

    // Cache hashes of plan node.
    private final Map<QueryId, Map<CachingPlanCanonicalInfoProvider.CacheKey, PlanNodeCanonicalInfo>> canonicalInfoCache = new ConcurrentHashMap<>();

    private final Map<QueryId, Map<CachingPlanCanonicalInfoProvider.InputTableCacheKey, PlanStatistics>> inputTableStatistics = new ConcurrentHashMap<>();

    // Stores query IDs which timeout during history optimizer registration
    private final Set<QueryId> queryIdsRegistrationTimeOut = ConcurrentHashMap.newKeySet();
    private final Map<QueryId, Map<PlanCanonicalizationStrategy, String>> canonicalPlan = new ConcurrentHashMap<>();
    private final Map<QueryId, PlanNode> statsEquivalentPlanRootNode = new ConcurrentHashMap<>();

    public HistoryBasedStatisticsCacheManager() {}

    public LoadingCache<PlanNodeWithHash, HistoricalPlanStatistics> getStatisticsCache(QueryId queryId, Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider, long timeoutInMilliSeconds)
    {
        return statisticsCache.computeIfAbsent(queryId, ignored -> CacheBuilder.newBuilder()
                .build(new CacheLoader<PlanNodeWithHash, HistoricalPlanStatistics>()
                {
                    @Override
                    public HistoricalPlanStatistics load(PlanNodeWithHash key)
                    {
                        return loadAll(Collections.singleton(key)).values().stream().findAny().orElseGet(HistoricalPlanStatistics::empty);
                    }

                    @Override
                    public Map<PlanNodeWithHash, HistoricalPlanStatistics> loadAll(Iterable<? extends PlanNodeWithHash> keys)
                    {
                        Map<PlanNodeWithHash, HistoricalPlanStatistics> statistics = new HashMap<>(historyBasedPlanStatisticsProvider.get().getStats(ImmutableList.copyOf(keys), timeoutInMilliSeconds));
                        // loadAll excepts all keys to be written
                        for (PlanNodeWithHash key : keys) {
                            statistics.putIfAbsent(key, empty());
                        }
                        return ImmutableMap.copyOf(statistics);
                    }
                }));
    }

    public Map<CachingPlanCanonicalInfoProvider.CacheKey, PlanNodeCanonicalInfo> getCanonicalInfoCache(QueryId queryId)
    {
        return canonicalInfoCache.computeIfAbsent(queryId, ignored -> new ConcurrentHashMap());
    }

    public Map<CachingPlanCanonicalInfoProvider.InputTableCacheKey, PlanStatistics> getInputTableStatistics(QueryId queryId)
    {
        return inputTableStatistics.computeIfAbsent(queryId, ignored -> new ConcurrentHashMap());
    }

    public Map<PlanCanonicalizationStrategy, String> getCanonicalPlan(QueryId queryId)
    {
        return canonicalPlan.computeIfAbsent(queryId, ignored -> new ConcurrentHashMap());
    }

    public void setStatsEquivalentPlanRootNode(QueryId queryId, PlanNode plan)
    {
        statsEquivalentPlanRootNode.put(queryId, plan);
    }

    public Optional<PlanNode> getStatsEquivalentPlanRootNode(QueryId queryId)
    {
        if (statsEquivalentPlanRootNode.containsKey(queryId)) {
            return Optional.of(statsEquivalentPlanRootNode.get(queryId));
        }
        return Optional.empty();
    }

    public void invalidate(QueryId queryId)
    {
        statisticsCache.remove(queryId);
        canonicalInfoCache.remove(queryId);
        inputTableStatistics.remove(queryId);
        queryIdsRegistrationTimeOut.remove(queryId);
        canonicalPlan.remove(queryId);
        statsEquivalentPlanRootNode.remove(queryId);
    }

    @VisibleForTesting
    public Map<QueryId, Map<CachingPlanCanonicalInfoProvider.CacheKey, PlanNodeCanonicalInfo>> getCanonicalInfoCache()
    {
        return canonicalInfoCache;
    }

    public boolean historyBasedQueryRegistrationTimeout(QueryId queryId)
    {
        return queryIdsRegistrationTimeOut.contains(queryId);
    }

    public void setHistoryBasedQueryRegistrationTimeout(QueryId queryId)
    {
        queryIdsRegistrationTimeOut.add(queryId);
    }
}
