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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TActiveTabletStat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Active tablet access statistics reported by backends.
 */
public class TabletSlidingWindowAccessStats {
    private static volatile TabletSlidingWindowAccessStats instance;

    private static final Comparator<AccessStatsResult> QUERY_RATE_COMPARATOR =
            Comparator.comparingDouble((AccessStatsResult r) -> r.scanRate).reversed()
                    .thenComparing(Comparator.comparingLong((AccessStatsResult r) -> r.lastAccessTime).reversed());
    private static final Comparator<AccessStatsResult> LOAD_RATE_COMPARATOR =
            Comparator.comparingDouble((AccessStatsResult r) -> r.loadRate).reversed()
                    .thenComparing(Comparator.comparingLong((AccessStatsResult r) -> r.lastAccessTime).reversed());

    // beId -> (tabletId -> stats). Each report replaces the complete snapshot for one backend.
    // Backend removal is handled by removeBackend(), so no cleanup daemon is needed.
    private final ConcurrentHashMap<Long, Map<Long, AccessStatsResult>> beToStats = new ConcurrentHashMap<>();
    private final AtomicLong totalAccessCount = new AtomicLong(0);

    // Merging every backend snapshot is O(total reported tablets) -- up to backendCount * 2 *
    // be report_active_tablet_max_num entries. MetricRepo scrapes two of the aggregate getters
    // below on every /metrics request, so they share a short-lived snapshot the same way the
    // pre-BE-report implementation cached its aggregates. getTopNActive() deliberately does NOT
    // use this cache: it runs once per cloud_active_tablet_ids_refresh_interval_second and feeds
    // scheduling decisions, so it always merges fresh.
    private static final long MERGED_CACHE_TTL_MS = 10_000L;
    private volatile Map<Long, AccessStatsResult> mergedCache = Collections.emptyMap();
    private final AtomicLong mergedCacheTimeMs = new AtomicLong(0);

    TabletSlidingWindowAccessStats() {
    }

    public void updateFromReport(long beId, List<TActiveTabletStat> topQuery, List<TActiveTabletStat> topLoad) {
        if (!Config.enable_active_tablet_sliding_window_access_stats) {
            beToStats.remove(beId);
            return;
        }

        // long[]{scan, load, lastQueryMs, lastLoadMs, scanWindowMs, loadWindowMs}
        Map<Long, long[]> accumulated = new HashMap<>((topQuery.size() + topLoad.size()) * 2);
        for (TActiveTabletStat stat : topQuery) {
            long[] values = accumulated.computeIfAbsent(stat.getTabletId(), key -> new long[6]);
            values[0] = stat.getScanCountDelta();
            values[2] = stat.getLastQueryTimeMs();
            values[4] = Math.max(1L, stat.getDeltaWindowMs());
        }
        for (TActiveTabletStat stat : topLoad) {
            long[] values = accumulated.computeIfAbsent(stat.getTabletId(), key -> new long[6]);
            values[1] = stat.getLoadCountDelta();
            values[3] = stat.getLastLoadTimeMs();
            values[5] = Math.max(1L, stat.getDeltaWindowMs());
        }

        Map<Long, AccessStatsResult> backendStats = new HashMap<>(accumulated.size() * 2);
        long delta = 0;
        for (Map.Entry<Long, long[]> entry : accumulated.entrySet()) {
            long[] values = entry.getValue();
            // Compare rates across backends because a skipped report makes the raw delta cover multiple periods.
            double scanRate = values[4] > 0 ? values[0] * 60_000.0 / values[4] : 0.0;
            double loadRate = values[5] > 0 ? values[1] * 60_000.0 / values[5] : 0.0;
            delta += values[0] + values[1];
            backendStats.put(entry.getKey(), new AccessStatsResult(entry.getKey(), values[0] + values[1],
                    Math.max(values[2], values[3]), scanRate, loadRate));
        }
        totalAccessCount.addAndGet(delta);
        beToStats.put(beId, backendStats);
    }

    public void removeBackend(long beId) {
        beToStats.remove(beId);
    }

    /**
     * Get total access count in the latest backend snapshots.
     */
    public long getRecentAccessCountInWindow() {
        if (!Config.enable_active_tablet_sliding_window_access_stats) {
            return 0L;
        }
        return cachedMergedStats().values().stream().mapToLong(result -> result.accessCount).sum();
    }

    /**
     * Get the number of distinct tablets in the latest backend snapshots.
     */
    public long getActiveIdsInWindow() {
        if (!Config.enable_active_tablet_sliding_window_access_stats) {
            return 0L;
        }
        return cachedMergedStats().size();
    }

    /**
     * Get total access count reported since FE start.
     */
    public long getTotalAccessCount() {
        if (!Config.enable_active_tablet_sliding_window_access_stats) {
            return 0L;
        }
        return totalAccessCount.get();
    }

    /**
     * Get access information for a tablet.
     */
    public AccessStatsResult getAccessInfo(long id) {
        if (!Config.enable_active_tablet_sliding_window_access_stats) {
            return null;
        }

        AccessStatsResult result = null;
        for (Map<Long, AccessStatsResult> backendStats : beToStats.values()) {
            AccessStatsResult candidate = backendStats.get(id);
            if (candidate != null && (result == null || candidate.accessCount > result.accessCount)) {
                result = candidate;
            }
        }
        return result;
    }

    /**
     * Result for top N query.
     */
    public static class AccessStatsResult {
        public final long id;
        public final long accessCount;
        public final long lastAccessTime;
        public final double scanRate;
        public final double loadRate;

        public AccessStatsResult(long id, long accessCount, long lastAccessTime, double scanRate, double loadRate) {
            this.id = id;
            this.accessCount = accessCount;
            this.lastAccessTime = lastAccessTime;
            this.scanRate = scanRate;
            this.loadRate = loadRate;
        }

        @Override
        public String toString() {
            return "AccessStatsResult{"
                    + "id=" + id
                    + ", accessCount=" + accessCount
                    + ", lastAccessTime=" + lastAccessTime
                    + ", scanRate=" + scanRate
                    + ", loadRate=" + loadRate
                    + '}';
        }
    }

    /**
     * Get top N active tablets, reserving half of the capacity for each access type.
     */
    public List<AccessStatsResult> getTopNActive(int topN) {
        if (!Config.enable_active_tablet_sliding_window_access_stats || topN <= 0) {
            return Collections.emptyList();
        }

        List<AccessStatsResult> queryStats = new ArrayList<>();
        List<AccessStatsResult> loadStats = new ArrayList<>();
        for (AccessStatsResult result : mergeBackendStats().values()) {
            if (result.scanRate > 0) {
                queryStats.add(result);
            }
            if (result.loadRate > 0) {
                loadStats.add(result);
            }
        }
        queryStats.sort(QUERY_RATE_COMPARATOR);
        loadStats.sort(LOAD_RATE_COMPARATOR);

        int queryQuota = topN / 2;
        int loadQuota = topN - queryQuota;
        int queryLimit = Math.min(queryQuota, queryStats.size());
        int loadLimit = Math.min(loadQuota, loadStats.size());
        if (queryLimit < queryQuota) {
            loadLimit = Math.min(loadStats.size(), topN - queryLimit);
        }
        if (loadLimit < loadQuota) {
            queryLimit = Math.min(queryStats.size(), topN - loadLimit);
        }

        Map<Long, AccessStatsResult> selected = new LinkedHashMap<>();
        for (int i = 0; i < queryLimit; i++) {
            AccessStatsResult result = queryStats.get(i);
            selected.put(result.id, result);
        }
        for (int i = 0; i < loadLimit; i++) {
            AccessStatsResult result = loadStats.get(i);
            selected.putIfAbsent(result.id, result);
        }
        return new ArrayList<>(selected.values());
    }

    /**
     * Get statistics summary.
     */
    public String getStatsSummary() {
        if (!Config.enable_active_tablet_sliding_window_access_stats) {
            return String.format("Active tablet sliding window access stats is disabled");
        }

        Map<Long, AccessStatsResult> mergedStats = cachedMergedStats();
        long totalAccess = mergedStats.values().stream().mapToLong(result -> result.accessCount).sum();
        return String.format(
                "SlidingWindowAccessStats{type=tablet, beCount=%d, activeIds=%d, "
                        + "totalAccess=%d, totalAccessCount=%d}",
                beToStats.size(), mergedStats.size(), totalAccess, totalAccessCount.get());
    }

    private Map<Long, AccessStatsResult> cachedMergedStats() {
        long now = System.currentTimeMillis();
        long last = mergedCacheTimeMs.get();
        if (now - last < MERGED_CACHE_TTL_MS) {
            return mergedCache;
        }
        if (!mergedCacheTimeMs.compareAndSet(last, now)) {
            // Another thread is already refreshing; the previous snapshot is good enough here.
            return mergedCache;
        }
        Map<Long, AccessStatsResult> merged = mergeBackendStats();
        mergedCache = merged;
        return merged;
    }

    private Map<Long, AccessStatsResult> mergeBackendStats() {
        Map<Long, AccessStatsResult> mergedStats = new HashMap<>();
        for (Map<Long, AccessStatsResult> backendStats : beToStats.values()) {
            for (AccessStatsResult result : backendStats.values()) {
                mergedStats.merge(result.id, result, TabletSlidingWindowAccessStats::mergeByMax);
            }
        }
        return mergedStats;
    }

    private static AccessStatsResult mergeByMax(AccessStatsResult left, AccessStatsResult right) {
        return new AccessStatsResult(left.id,
                Math.max(left.accessCount, right.accessCount),
                Math.max(left.lastAccessTime, right.lastAccessTime),
                Math.max(left.scanRate, right.scanRate),
                Math.max(left.loadRate, right.loadRate));
    }

    public static TabletSlidingWindowAccessStats getInstance() {
        if (instance == null) {
            synchronized (TabletSlidingWindowAccessStats.class) {
                if (instance == null) {
                    instance = new TabletSlidingWindowAccessStats();
                }
            }
        }
        return instance;
    }
}
