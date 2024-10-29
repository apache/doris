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

package org.apache.doris.statistics;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class InMemoryHistoryBasedPlanStatisticsProvider
        implements HistoryBasedPlanStatisticsProvider {
    private final Map<String, HistoricalPlanStatistics> cache = new ConcurrentHashMap<>();
    // Since, event processing happens in a different thread, we use a semaphore to wait for
    // all query events to process and finish.
    private final Semaphore semaphore = new Semaphore(1);

    public InMemoryHistoryBasedPlanStatisticsProvider() {
        semaphore.acquireUninterruptibly();
    }

    @Override
    public String getName() {
        return "test";
    }

    @Override
    public Map<PlanNodeWithHash, HistoricalPlanStatistics> getStats(List<PlanNodeWithHash> planNodeHashes,
            long timeoutInMilliSeconds) {
        return planNodeHashes.stream().collect(toImmutableMap(
                planNodeWithHash -> planNodeWithHash,
                planNodeWithHash -> {
                    if (planNodeWithHash.getHash().isPresent()) {
                        return cache.getOrDefault(planNodeWithHash.getHash().get(), HistoricalPlanStatistics.empty());
                    }
                    return HistoricalPlanStatistics.empty();
                }));
    }

    @Override
    public void putStats(Map<PlanNodeWithHash, HistoricalPlanStatistics> hashesAndStatistics) {
        hashesAndStatistics.forEach((planNodeWithHash, historicalPlanStatistics) -> {
            if (planNodeWithHash.getHash().isPresent()) {
                cache.put(planNodeWithHash.getHash().get(), historicalPlanStatistics);
            }
        });
        semaphore.release();
    }
}