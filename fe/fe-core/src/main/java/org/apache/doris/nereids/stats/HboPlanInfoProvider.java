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
 * HboPlanInfoProvider
 */
public class HboPlanInfoProvider {
    private volatile Cache<String, Map<Integer, PhysicalPlan>> idToPlanCache;
    private volatile Cache<String, Map<PhysicalPlan, Integer>> planToIdCache;
    private volatile Cache<String, Map<RelationId, Set<Expression>>> tableToFilterCache;

    /**
     * HboPlanInfoProvider
     */
    public HboPlanInfoProvider() {
        idToPlanCache = buildHboIdToPlanCache(
                Config.hbo_cache_manage_num,
                Config.expire_hbo_cache_in_fe_second
        );
        planToIdCache = buildHboPlanToIdCache(
                Config.hbo_cache_manage_num,
                Config.expire_hbo_cache_in_fe_second
        );
        tableToFilterCache = buildHboTableToFilterCache(
                Config.hbo_cache_manage_num,
                Config.expire_hbo_cache_in_fe_second
        );
    }

    private static Cache<String, Map<RelationId, Set<Expression>>> buildHboTableToFilterCache(int hboCacheNum,
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
     * buildHboIdToPlanCache
     * @param: hbo cache Num
     * @param: expireAfterAccessSeconds
     * @return: buildHboIdToPlanCache
     */
    private static Cache<String, Map<Integer, PhysicalPlan>> buildHboIdToPlanCache(int hboCacheNum,
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
     * buildHboPlanToIdCache
     * @param hboCacheNum hbo cache number
     * @param expireAfterAccessSeconds  expire After Access Seconds
     * @return cache
     */
    private static Cache<String, Map<PhysicalPlan, Integer>> buildHboPlanToIdCache(int hboCacheNum,
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

    public Map<RelationId, Set<Expression>> getTableToExprMap(String queryId) {
        return tableToFilterCache.asMap().getOrDefault(queryId, new ConcurrentHashMap<>());
    }

    public void putTableToExprMap(String queryId, Map<RelationId, Set<Expression>> tableToExprMap) {
        tableToFilterCache.put(queryId, tableToExprMap);
    }

    /**
     * UpdateConfig
     */
    public static class UpdateConfig extends DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            super.handle(field, confVal);
            HboPlanInfoProvider.updateConfig();
        }
    }

    /**
     * UpdateConfig
     */
    public static synchronized void updateConfig() {
        HboPlanStatisticsManager hboManger = HboPlanStatisticsManager.getInstance();
        if (hboManger == null) {
            return;
        }
        HboPlanInfoProvider hboIdToPlanProvider = hboManger.getHboPlanInfoProvider();
        if (hboIdToPlanProvider == null) {
            return;
        }

        Cache<String, Map<Integer, PhysicalPlan>> idToPlanCache = buildHboIdToPlanCache(
                Config.hbo_cache_manage_num,
                Config.expire_hbo_cache_in_fe_second
        );
        Cache<String, Map<PhysicalPlan, Integer>> planToIdCache = buildHboPlanToIdCache(
                Config.hbo_cache_manage_num,
                Config.expire_hbo_cache_in_fe_second
        );
        Cache<String, Map<RelationId, Set<Expression>>> tableToExprCache = buildHboTableToFilterCache(
                Config.hbo_cache_manage_num,
                Config.expire_hbo_cache_in_fe_second
        );
        idToPlanCache.putAll(hboIdToPlanProvider.idToPlanCache.asMap());
        hboIdToPlanProvider.idToPlanCache = idToPlanCache;
        planToIdCache.putAll(hboIdToPlanProvider.planToIdCache.asMap());
        hboIdToPlanProvider.planToIdCache = planToIdCache;
        tableToExprCache.putAll(hboIdToPlanProvider.tableToFilterCache.asMap());
        hboIdToPlanProvider.tableToFilterCache = tableToExprCache;
    }
}
