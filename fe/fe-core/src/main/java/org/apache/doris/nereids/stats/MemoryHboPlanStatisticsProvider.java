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
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase.DefaultConfHandler;
import org.apache.doris.nereids.trees.plans.PlanNodeAndHash;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatistics;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUpdatePlanStatsCacheRequest;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * HboPlanStatisticsProvider's in-memory implementation.
 */
public class MemoryHboPlanStatisticsProvider implements HboPlanStatisticsProvider {
    private static final Logger LOG = LogManager.getLogger(MemoryHboPlanStatisticsProvider.class);
    private volatile Cache<String, RecentRunsPlanStatistics> hboPlanStatsCache;

    public MemoryHboPlanStatisticsProvider() {
        hboPlanStatsCache = buildHboPlanStatsCaches(
                Config.hbo_plan_stats_cache_num,
                Config.expire_hbo_plan_stats_cache_in_fe_second
        );
    }

    @Override
    public RecentRunsPlanStatistics getHboPlanStats(PlanNodeAndHash planNodeAndHash) {
        if (planNodeAndHash.getHash().isPresent()) {
            return hboPlanStatsCache.asMap().getOrDefault(planNodeAndHash.getHash().get(),
                    RecentRunsPlanStatistics.empty());
        }
        return RecentRunsPlanStatistics.empty();
    }

    @Override
    public Map<PlanNodeAndHash, RecentRunsPlanStatistics> getHboPlanStats(List<PlanNodeAndHash> planNodeHashes) {
        return planNodeHashes.stream().collect(Collectors.toMap(
                planNodeAndHash -> planNodeAndHash,
                planNodeAndHash -> {
                    if (planNodeAndHash.getHash().isPresent()) {
                        return hboPlanStatsCache.asMap().getOrDefault(planNodeAndHash.getHash().get(),
                                RecentRunsPlanStatistics.empty());
                    }
                    return RecentRunsPlanStatistics.empty();
                }));
    }

    @Override
    public void putHboPlanStats(Map<PlanNodeAndHash, RecentRunsPlanStatistics> hashStatisticsMap) {
        hashStatisticsMap.forEach((planNodeAndHash, recentRunsPlanStatistics) -> {
            if (planNodeAndHash.getHash().isPresent()) {
                hboPlanStatsCache.put(planNodeAndHash.getHash().get(), recentRunsPlanStatistics);
            }
        });
    }

    @Override
    public void updatePlanStats(PlanNodeAndHash hash, RecentRunsPlanStatistics planStatistics) {
        hboPlanStatsCache.put(hash.getHash().get(), planStatistics);
    }

    /**
     * sync hbo plan stats to other fe client.
     * @param planKey planKey
     * @param planStatsData planStatsData
     */
    public void syncHboPlanStats(PlanNodeAndHash planKey, RecentRunsPlanStatistics planStatsData) {
        TUpdatePlanStatsCacheRequest updateFollowerPlanStatsCacheRequest = new TUpdatePlanStatsCacheRequest();
        updateFollowerPlanStatsCacheRequest.key = GsonUtils.GSON.toJson(planKey);
        updateFollowerPlanStatsCacheRequest.planStatsData = GsonUtils.GSON.toJson(planStatsData);
        SystemInfoService.HostInfo selfNode = Env.getCurrentEnv().getSelfNode();
        for (Frontend frontend : Env.getCurrentEnv().getFrontends(null)) {
            if (selfNode.getHost().equals(frontend.getHost())) {
                continue;
            }
            sendPlanStats(frontend, updateFollowerPlanStatsCacheRequest);
        }
    }

    private void sendPlanStats(Frontend frontend, TUpdatePlanStatsCacheRequest updateFollowerPlanStatsCacheRequest) {
        TNetworkAddress address = new TNetworkAddress(frontend.getHost(), frontend.getRpcPort());
        FrontendService.Client client = null;
        try {
            client = ClientPool.frontendPool.borrowObject(address);
            client.updatePlanStatsCache(updateFollowerPlanStatsCacheRequest);
        } catch (Throwable t) {
            LOG.warn("Failed to sync plan stats to fe client: {}", address, t);
        } finally {
            if (client != null) {
                ClientPool.frontendPool.returnObject(address, client);
            }
        }
    }

    private static Cache<String, RecentRunsPlanStatistics> buildHboPlanStatsCaches(
            int cacheNum, long expireAfterAccessSeconds) {
        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder()
                .softValues();
        if (cacheNum > 0) {
            cacheBuilder.maximumSize(cacheNum);
        }
        if (expireAfterAccessSeconds > 0) {
            cacheBuilder = cacheBuilder.expireAfterAccess(Duration.ofSeconds(expireAfterAccessSeconds));
        }

        return cacheBuilder.build();
    }

    /**
     * NOTE: used in Config.hbo_plan_stats_cache_num.callbackClassString and
     * Config.expire_hbo_plan_stats_cache_in_fe_second.callbackClassString,
     */
    public static class UpdateConfig extends DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            super.handle(field, confVal);
            MemoryHboPlanStatisticsProvider.updateConfig();
        }
    }

    /**
     * Reference the above UpdateConfig comments.
     */
    public static synchronized void updateConfig() {
        HboPlanStatisticsManager hboManger = Env.getCurrentEnv().getHboPlanStatisticsManager();
        if (hboManger == null) {
            return;
        }
        HboPlanStatisticsProvider hboPlanStatsProvider = hboManger.getHboPlanStatisticsProvider();
        if (!(hboPlanStatsProvider instanceof MemoryHboPlanStatisticsProvider)) {
            return;
        }

        MemoryHboPlanStatisticsProvider inMemHboPlanStatsProvider =
                (MemoryHboPlanStatisticsProvider) hboPlanStatsProvider;

        Cache<String, RecentRunsPlanStatistics> hboPlanStatsCache = buildHboPlanStatsCaches(
                Config.hbo_plan_stats_cache_num,
                Config.expire_hbo_plan_stats_cache_in_fe_second
        );
        hboPlanStatsCache.putAll(inMemHboPlanStatsProvider.hboPlanStatsCache.asMap());
        inMemHboPlanStatsProvider.hboPlanStatsCache = hboPlanStatsCache;
    }
}
