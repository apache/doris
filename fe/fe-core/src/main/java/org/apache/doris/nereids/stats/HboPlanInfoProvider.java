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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HboPlanInfoProvider maintains these kinds of cache for each queryId:
 * - scanToFilterCache:
 *   scan relation id <-> filter expr sets on the scan
 *   collected during rewriting stage
 * - idToPlanCache:
 *   real plan id(not nereids id) <-> physical plan
 *   collected after physical plan generation
 * - planToIdCache:
 *   physical plan <-> real plan id(not nereids id)
 *   collected the same time as idToPlanCache
 * - nodeIdToFingerprintCache:
 *   nereids plan node id <-> hbo fingerprint (simplified group struct info sha256)
 *   collected at the same time as idToPlanCache; consumed later by the profile publish
 *   path after the memo has been released
 */
public class HboPlanInfoProvider {
    private volatile Cache<String, Map<Integer, PhysicalPlan>> idToPlanCache;
    private volatile Cache<String, Map<PhysicalPlan, Integer>> planToIdCache;
    private volatile Cache<String, Map<RelationId, Set<Expression>>> scanToFilterCache;
    private volatile Cache<String, Map<Integer, String>> nodeIdToFingerprintCache;
    private volatile Cache<String, Map<String, String>> pinnedGuardSkipCache;
    private volatile Cache<String, Map<String, String>> pinnedExpansionAppliedCache;
    // per query: fingerprint -> the literal mode of the struct info which matched it
    private volatile Cache<String, Map<String, String>> pinnedLiteralModeCache;
    // per query: fingerprint -> how the pinned entry of that fingerprint was applied
    // (live / drifted(...) / unknown), the read side's data state verdict
    private volatile Cache<String, Map<String, String>> pinnedApplyStateCache;

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
        nodeIdToFingerprintCache = buildHboNodeIdToFingerprintCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
        pinnedGuardSkipCache = buildHboPinnedGuardSkipCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
        pinnedExpansionAppliedCache = buildHboPinnedGuardSkipCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
        pinnedLiteralModeCache = buildHboPinnedGuardSkipCache(
                Config.hbo_pinned_stats_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second);
        pinnedApplyStateCache = buildHboPinnedGuardSkipCache(
                Config.hbo_pinned_stats_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second);
    }

    private static Cache<String, Map<Integer, String>> buildHboNodeIdToFingerprintCache(
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

    public Map<Integer, String> getNodeIdToFingerprintMap(String queryId) {
        return nodeIdToFingerprintCache.asMap().getOrDefault(queryId, new ConcurrentHashMap<>());
    }

    public void putNodeIdToFingerprintMap(String queryId, Map<Integer, String> nodeIdToFingerprintMap) {
        nodeIdToFingerprintCache.put(queryId, nodeIdToFingerprintMap);
    }

    private static Cache<String, Map<String, String>> buildHboPinnedGuardSkipCache(
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
     * Record that a pinned {@code FILTER_SMALL} entry was skipped by the extreme-small guard for
     * the given query, so the explain annotation can report it.
     */
    public void putPinnedGuardSkip(String queryId, String fingerprint, String reason) {
        Map<String, String> skips = pinnedGuardSkipCache.getIfPresent(queryId);
        if (skips == null) {
            skips = new HashMap<>();
            pinnedGuardSkipCache.put(queryId, skips);
        }
        skips.put(fingerprint, reason);
    }

    /** Guard skip reasons of a query, keyed by hbo fingerprint; empty when none was recorded. */
    public Map<String, String> getPinnedGuardSkip(String queryId) {
        Map<String, String> skips = pinnedGuardSkipCache.getIfPresent(queryId);
        return skips == null ? Collections.emptyMap() : skips;
    }

    /** Matched pinned entry types of a query, keyed by hbo fingerprint. */
    public void putPinnedLiteralMode(String queryId, String fingerprint, String literalMode) {
        Map<String, String> modes = pinnedLiteralModeCache.getIfPresent(queryId);
        if (modes == null) {
            modes = new ConcurrentHashMap<>();
            pinnedLiteralModeCache.put(queryId, modes);
        }
        modes.put(fingerprint, literalMode);
    }

    public Map<String, String> getPinnedLiteralMode(String queryId) {
        Map<String, String> modes = pinnedLiteralModeCache.getIfPresent(queryId);
        return modes == null ? Collections.emptyMap() : modes;
    }

    /**
     * Record how the pinned entry of {@code fingerprint} was applied for this query: the data state
     * verdict of the read side ({@code live}, {@code drifted(...)} or {@code unknown}). An entry
     * which was rejected is reported through {@link #putPinnedGuardSkip} instead.
     */
    public void putPinnedApplyState(String queryId, String fingerprint, String state) {
        Map<String, String> states = pinnedApplyStateCache.getIfPresent(queryId);
        if (states == null) {
            states = new HashMap<>();
            pinnedApplyStateCache.put(queryId, states);
        }
        states.put(fingerprint, state);
    }

    /** How the pinned entries of a query were applied, keyed by hbo fingerprint. */
    public Map<String, String> getPinnedApplyState(String queryId) {
        Map<String, String> states = pinnedApplyStateCache.getIfPresent(queryId);
        return states == null ? Collections.emptyMap() : states;
    }

    /** Record that a pinned join expansion entry was applied for the given query. */
    public void putExpansionApplied(String queryId, String condFingerprint, String detail) {
        Map<String, String> applied = pinnedExpansionAppliedCache.getIfPresent(queryId);
        if (applied == null) {
            applied = new HashMap<>();
            pinnedExpansionAppliedCache.put(queryId, applied);
        }
        applied.put(condFingerprint, detail);
    }

    /** Applied join expansion entries of a query, keyed by condition fingerprint. */
    public Map<String, String> getExpansionApplied(String queryId) {
        Map<String, String> applied = pinnedExpansionAppliedCache.getIfPresent(queryId);
        return applied == null ? Collections.emptyMap() : applied;
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
     * Reference the above UpdateConfig comments. Every per-query cache of this provider is rebuilt
     * (they are all sized and expired by the same two configs), and the entries of a running query
     * are carried over so that a runtime change cannot drop the plan info of a query which is still
     * in flight.
     */
    public static synchronized void updateConfig() {
        HboPlanStatisticsManager hboManger = Env.getCurrentEnv().getHboPlanStatisticsManager();
        if (hboManger == null) {
            return;
        }
        HboPlanInfoProvider provider = hboManger.getHboPlanInfoProvider();
        if (provider == null) {
            return;
        }
        int cacheNum = Config.hbo_plan_info_cache_num;
        int pinnedCacheNum = Config.hbo_pinned_stats_cache_num;
        long expireSeconds = Config.expire_hbo_plan_info_cache_in_fe_second;

        Cache<String, Map<Integer, PhysicalPlan>> idToPlanCache =
                buildHboIdToPlanCache(cacheNum, expireSeconds);
        idToPlanCache.putAll(provider.idToPlanCache.asMap());
        provider.idToPlanCache = idToPlanCache;

        Cache<String, Map<PhysicalPlan, Integer>> planToIdCache =
                buildHboPlanToIdCache(cacheNum, expireSeconds);
        planToIdCache.putAll(provider.planToIdCache.asMap());
        provider.planToIdCache = planToIdCache;

        Cache<String, Map<RelationId, Set<Expression>>> scanToFilterCache =
                buildHboScanToFilterCache(cacheNum, expireSeconds);
        scanToFilterCache.putAll(provider.scanToFilterCache.asMap());
        provider.scanToFilterCache = scanToFilterCache;

        Cache<String, Map<Integer, String>> nodeIdToFingerprintCache =
                buildHboNodeIdToFingerprintCache(cacheNum, expireSeconds);
        nodeIdToFingerprintCache.putAll(provider.nodeIdToFingerprintCache.asMap());
        provider.nodeIdToFingerprintCache = nodeIdToFingerprintCache;

        Cache<String, Map<String, String>> pinnedGuardSkipCache =
                buildHboPinnedGuardSkipCache(cacheNum, expireSeconds);
        pinnedGuardSkipCache.putAll(provider.pinnedGuardSkipCache.asMap());
        provider.pinnedGuardSkipCache = pinnedGuardSkipCache;

        Cache<String, Map<String, String>> pinnedExpansionAppliedCache =
                buildHboPinnedGuardSkipCache(cacheNum, expireSeconds);
        pinnedExpansionAppliedCache.putAll(provider.pinnedExpansionAppliedCache.asMap());
        provider.pinnedExpansionAppliedCache = pinnedExpansionAppliedCache;

        Cache<String, Map<String, String>> pinnedLiteralModeCache =
                buildHboPinnedGuardSkipCache(pinnedCacheNum, expireSeconds);
        pinnedLiteralModeCache.putAll(provider.pinnedLiteralModeCache.asMap());
        provider.pinnedLiteralModeCache = pinnedLiteralModeCache;

        Cache<String, Map<String, String>> pinnedApplyStateCache =
                buildHboPinnedGuardSkipCache(pinnedCacheNum, expireSeconds);
        pinnedApplyStateCache.putAll(provider.pinnedApplyStateCache.asMap());
        provider.pinnedApplyStateCache = pinnedApplyStateCache;
    }
}
