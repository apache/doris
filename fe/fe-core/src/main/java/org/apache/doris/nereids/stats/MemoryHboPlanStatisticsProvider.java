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

import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase.DefaultConfHandler;
import org.apache.doris.nereids.trees.plans.PlanNodeAndHash;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatistics;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * MemoryHboPlanStatisticsProvider
 */
public class MemoryHboPlanStatisticsProvider implements HboPlanStatisticsProvider {
    private volatile Cache<String, RecentRunsPlanStatistics> hboCache;

    public MemoryHboPlanStatisticsProvider() {
        hboCache = buildHboCaches(
                Config.hbo_cache_manage_num,
                Config.expire_hbo_cache_in_fe_second
        );
    }

    @Override
    public RecentRunsPlanStatistics getHboStats(PlanNodeAndHash planNodeAndHash) {
        if (planNodeAndHash.getHash().isPresent()) {
            return hboCache.asMap().getOrDefault(planNodeAndHash.getHash().get(), RecentRunsPlanStatistics.empty());
        }
        return RecentRunsPlanStatistics.empty();
    }

    @Override
    public Map<PlanNodeAndHash, RecentRunsPlanStatistics> getHboStats(List<PlanNodeAndHash> planNodeHashes) {
        return planNodeHashes.stream().collect(Collectors.toMap(
                planNodeAndHash -> planNodeAndHash,
                planNodeAndHash -> {
                    if (planNodeAndHash.getHash().isPresent()) {
                        return hboCache.asMap().getOrDefault(planNodeAndHash.getHash().get(),
                                RecentRunsPlanStatistics.empty());
                    }
                    return RecentRunsPlanStatistics.empty();
                }));
    }

    @Override
    public void putHboStats(Map<PlanNodeAndHash, RecentRunsPlanStatistics> hashStatisticsMap) {
        hashStatisticsMap.forEach((planNodeAndHash, recentRunsPlanStatistics) -> {
            if (planNodeAndHash.getHash().isPresent()) {
                hboCache.put(planNodeAndHash.getHash().get(), recentRunsPlanStatistics);
            }
        });
    }

    private static Cache<String, RecentRunsPlanStatistics> buildHboCaches(int hboCacheNum,
            long expireAfterAccessSeconds) {
        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder()
                // auto evict cache when jvm memory too low
                .softValues();
        if (hboCacheNum > 0) {
            cacheBuilder.maximumSize(hboCacheNum);
        }
        if (expireAfterAccessSeconds > 0) {
            cacheBuilder = cacheBuilder.expireAfterAccess(Duration.ofSeconds(expireAfterAccessSeconds));
        }

        return cacheBuilder.build();
    }

    /**
     * NOTE: used in Config.sql_cache_manage_num.callbackClassString and
     * Config.cache_last_version_interval_second.callbackClassString,
     * don't remove it!
     */
    public static class UpdateConfig extends DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            super.handle(field, confVal);
            MemoryHboPlanStatisticsProvider.updateConfig();
        }
    }

    /**
     * NOTE: used in Config.sql_cache_manage_num.callbackClassString and
     * Config.cache_last_version_interval_second.callbackClassString,
     * don't remove it!
     */
    public static synchronized void updateConfig() {
        HboPlanStatisticsManager hboManger = HboPlanStatisticsManager.getInstance();
        if (hboManger == null) {
            return;
        }
        HboPlanStatisticsProvider hboProvider = hboManger.getHboPlanStatisticsProvider();
        if (!(hboProvider instanceof MemoryHboPlanStatisticsProvider)) {
            return;
        }

        MemoryHboPlanStatisticsProvider inMemHboProvider =
                (MemoryHboPlanStatisticsProvider) hboProvider;

        Cache<String, RecentRunsPlanStatistics> hboCaches = buildHboCaches(
                Config.sql_cache_manage_num,
                Config.expire_sql_cache_in_fe_second
        );
        hboCaches.putAll(inMemHboProvider.hboCache.asMap());
        inMemHboProvider.hboCache = hboCaches;
    }
}
