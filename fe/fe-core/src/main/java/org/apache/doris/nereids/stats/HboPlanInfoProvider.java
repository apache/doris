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
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase.DefaultConfHandler;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HboPlanInfoProvider maintains 3 kinds of cache for each queryId:
 * - scanToFilterCache:
 *   scan relation id <-> filter expr sets on the scan
 *   collected during rewriting stage
 * - idToPlanCache:
 *   real plan id(not nereids id) <-> physical plan
 *   collected after physical plan generation
 * - planToIdCache:
 *   physical plan <-> real plan id(not nereids id)
 *   collected the same time as idToPlanCache
 */
public class HboPlanInfoProvider {
    private volatile Cache<String, Map<Integer, PhysicalPlan>> idToPlanCache;
    private volatile Cache<String, Map<PhysicalPlan, Integer>> planToIdCache;
    private volatile Cache<String, Map<RelationId, Set<Expression>>> scanToFilterCache;

    /**
     * Hbo plan info provider.
     */
    public HboPlanInfoProvider() {
        idToPlanCache = buildHboIdToPlanCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
        planToIdCache = buildHboPlanToIdCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
        scanToFilterCache = buildHboScanToFilterCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
    }

    private static Cache<String, Map<RelationId, Set<Expression>>> buildHboScanToFilterCache(
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

    private static Cache<String, Map<Integer, PhysicalPlan>> buildHboIdToPlanCache(
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

    private static Cache<String, Map<PhysicalPlan, Integer>> buildHboPlanToIdCache(
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

    public Map<Integer, PhysicalPlan> getIdToPlanMap(String queryId) {
        return idToPlanCache.asMap().getOrDefault(queryId, new ConcurrentHashMap<>());
    }

    public void putIdToPlanMap(String queryId, Map<Integer, PhysicalPlan> idToPlanMap) {
        idToPlanCache.put(queryId, idToPlanMap);
    }

    public Map<PhysicalPlan, Integer> getPlanToIdMap(String queryId) {
        return planToIdCache.asMap().getOrDefault(queryId, new ConcurrentHashMap<>());
    }

    public void putPlanToIdMap(String queryId, Map<PhysicalPlan, Integer> idToPlanMap) {
        planToIdCache.put(queryId, idToPlanMap);
    }

    public Map<RelationId, Set<Expression>> getScanToFilterMap(String queryId) {
        return scanToFilterCache.asMap().getOrDefault(queryId, new ConcurrentHashMap<>());
    }

    public void putScanToFilterMap(String queryId, Map<RelationId, Set<Expression>> scanToFilterMap) {
        scanToFilterCache.put(queryId, scanToFilterMap);
    }

    /**
     * NOTE: used in Config.hbo_plan_info_cache_num.callbackClassString and
     * Config.expire_hbo_plan_info_cache_in_fe_second.callbackClassString,
     */
    public static class UpdateConfig extends DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            super.handle(field, confVal);
            HboPlanInfoProvider.updateConfig();
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
        HboPlanInfoProvider planInfoProvider = hboManger.getHboPlanInfoProvider();
        if (planInfoProvider == null) {
            return;
        }

        Cache<String, Map<Integer, PhysicalPlan>> idToPlanCache = buildHboIdToPlanCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
        Cache<String, Map<PhysicalPlan, Integer>> planToIdCache = buildHboPlanToIdCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
        Cache<String, Map<RelationId, Set<Expression>>> scanToFilterCache = buildHboScanToFilterCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
        idToPlanCache.putAll(planInfoProvider.idToPlanCache.asMap());
        planInfoProvider.idToPlanCache = idToPlanCache;
        planToIdCache.putAll(planInfoProvider.planToIdCache.asMap());
        planInfoProvider.planToIdCache = planToIdCache;
        scanToFilterCache.putAll(planInfoProvider.scanToFilterCache.asMap());
        planInfoProvider.scanToFilterCache = scanToFilterCache;
    }
}
