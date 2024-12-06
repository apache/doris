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

import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeWithHash;
import org.apache.doris.statistics.HistoricalPlanStatistics;
import org.apache.doris.statistics.HistoryBasedPlanStatisticsProvider;
import org.apache.doris.statistics.PlanNodeCanonicalInfo;
import org.apache.doris.statistics.PlanStatistics;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class HistoryBasedStatisticsCacheManager
{
    // Cache historical statistics of plan node.
    private final Map<String, LoadingCache<PlanNodeWithHash, HistoricalPlanStatistics>> statisticsCache = new ConcurrentHashMap<>();

    // Stores query IDs which timeout during history optimizer registration
    private final Set<String> queryIdsRegistrationTimeOut = ConcurrentHashMap.newKeySet();

    public HistoryBasedStatisticsCacheManager() {}

    public LoadingCache<PlanNodeWithHash, HistoricalPlanStatistics> getStatisticsCache(String queryId, HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider, long timeoutInMilliSeconds)
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
                        Map<PlanNodeWithHash, HistoricalPlanStatistics> statistics = new HashMap<>(historyBasedPlanStatisticsProvider.getStats(
                                ImmutableList.copyOf(keys), timeoutInMilliSeconds));
                        // loadAll excepts all keys to be written
                        for (PlanNodeWithHash key : keys) {
                            statistics.putIfAbsent(key, HistoricalPlanStatistics.empty());
                        }
                        return ImmutableMap.copyOf(statistics);
                    }
                }));
    }

    public void invalidate(String queryId)
    {
        statisticsCache.remove(queryId);
        //canonicalInfoCache.remove(queryId);
        //inputTableStatistics.remove(queryId);
        queryIdsRegistrationTimeOut.remove(queryId);
    }
}
