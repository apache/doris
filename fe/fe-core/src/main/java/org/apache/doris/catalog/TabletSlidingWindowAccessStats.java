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

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

    // Hottest first, most recently touched breaking a tie. Reversing the whole chain is the
    // same as reversing each key, and reads as the one sentence above.
    private static final Comparator<AccessStatsResult> QUERY_RATE_COMPARATOR =
            Comparator.comparingDouble((AccessStatsResult r) -> r.scanRate)
                    .thenComparingLong(r -> r.lastAccessTime)
                    .reversed();
    private static final Comparator<AccessStatsResult> LOAD_RATE_COMPARATOR =
            Comparator.comparingDouble((AccessStatsResult r) -> r.loadRate)
                    .thenComparingLong(r -> r.lastAccessTime)
                    .reversed();

    // beId -> (tabletId -> stats). A report updates the tablets it carries and ages out the
    // rest by active_tablet_sliding_window_time_window_second, so entries expire on write
    // instead of needing a cleanup daemon; a backend going away is handled by removeBackend().
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

        // A tablet hot on both dimensions arrives once in each list, each entry carrying only
        // its own dimension, so the two lists are merged by tablet id before anything is built.
        Map<Long, Accumulator> accumulated = Maps.newHashMapWithExpectedSize(
                topQuery.size() + topLoad.size());
        for (TActiveTabletStat stat : topQuery) {
            Accumulator acc = accumulated.computeIfAbsent(stat.getTabletId(), key -> new Accumulator());
            acc.scanDelta = stat.getScanCountDelta();
            acc.lastQueryMs = stat.getLastQueryTimeMs();
            acc.scanWindowMs = Math.max(1L, stat.getDeltaWindowMs());
        }
        for (TActiveTabletStat stat : topLoad) {
            Accumulator acc = accumulated.computeIfAbsent(stat.getTabletId(), key -> new Accumulator());
            acc.loadDelta = stat.getLoadCountDelta();
            acc.lastLoadMs = stat.getLastLoadTimeMs();
            acc.loadWindowMs = Math.max(1L, stat.getDeltaWindowMs());
        }

        Map<Long, AccessStatsResult> backendStats = Maps.newHashMapWithExpectedSize(accumulated.size());
        long reportedAccesses = 0;
        for (Map.Entry<Long, Accumulator> entry : accumulated.entrySet()) {
            long tabletId = entry.getKey();
            Accumulator acc = entry.getValue();
            long accessCount = acc.scanDelta + acc.loadDelta;
            reportedAccesses += accessCount;
            backendStats.put(tabletId, new AccessStatsResult(tabletId, accessCount,
                    Math.max(acc.lastQueryMs, acc.lastLoadMs), acc.scanRate(), acc.loadRate()));
        }
        totalAccessCount.addAndGet(reportedAccesses);
        beToStats.put(beId, retainWithinWindow(beToStats.get(beId), backendStats));
    }

    /**
     * A report only carries the tablets that saw traffic since the previous one, so taking it
     * as the whole truth would shrink "active" to one report interval - a tablet queried hard
     * four minutes ago would read as cold and become a migration candidate. Entries the backend
     * did not repeat are therefore kept until they fall outside
     * active_tablet_sliding_window_time_window_second, which is the window this feature has
     * advertised since it lived in FE memory.
     *
     * <p>Retention is bounded by cloud_active_partition_scheduling_topn: the scheduler never
     * consumes more actives than that in total, and a backend whose hot set churns would
     * otherwise accumulate a whole window's worth of distinct tablets. The oldest are dropped
     * first. lastAccessTime comes from the backend's clock while the cutoff comes from FE's,
     * but a window measured in hours absorbs the skew between them.
     */
    private static Map<Long, AccessStatsResult> retainWithinWindow(
            Map<Long, AccessStatsResult> previous, Map<Long, AccessStatsResult> reported) {
        int retainLimit = Config.cloud_active_partition_scheduling_topn;
        if (previous == null || previous.isEmpty() || retainLimit <= 0) {
            // retainLimit <= 0 disables TopN segmentation, so getTopNActive() returns nothing
            // and anything retained here would only cost memory.
            return reported;
        }

        long windowMs = Math.max(1L, Config.active_tablet_sliding_window_time_window_second) * 1000L;
        long oldestKept = System.currentTimeMillis() - windowMs;
        Map<Long, AccessStatsResult> merged = Maps.newHashMapWithExpectedSize(
                previous.size() + reported.size());
        for (AccessStatsResult stale : previous.values()) {
            if (stale.lastAccessTime >= oldestKept) {
                merged.put(stale.id, stale);
            }
        }
        // Whatever the backend just reported is fresher than anything retained for it.
        merged.putAll(reported);
        if (merged.size() <= retainLimit) {
            return merged;
        }

        List<AccessStatsResult> byRecency = new ArrayList<>(merged.values());
        byRecency.sort(Comparator.comparingLong((AccessStatsResult r) -> r.lastAccessTime).reversed());
        Map<Long, AccessStatsResult> capped = Maps.newHashMapWithExpectedSize(retainLimit);
        for (int i = 0; i < retainLimit; i++) {
            AccessStatsResult result = byRecency.get(i);
            capped.put(result.id, result);
        }
        return capped;
    }

    /**
     * One tablet's half-built stats while the query and load lists are being merged.
     * Each dimension keeps its own window: the two entries for one tablet come from the
     * same report, but a backend that skipped a round carries a wider window on the
     * dimension that was reported then.
     */
    private static class Accumulator {
        private long scanDelta;
        private long loadDelta;
        private long lastQueryMs;
        private long lastLoadMs;
        // Never zero, so no division guard is needed below.
        private long scanWindowMs = 1L;
        private long loadWindowMs = 1L;

        // Accesses per minute. Backends must be compared by rate, not by raw delta: a
        // skipped report makes the next delta cover several periods, and backends report
        // on independent phases.
        private double scanRate() {
            return scanDelta * 60_000.0 / scanWindowMs;
        }

        private double loadRate() {
            return loadDelta * 60_000.0 / loadWindowMs;
        }
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

        // Half the budget is reserved for each dimension, then each side takes whatever the
        // other could not fill, so a cluster that only queries or only loads does not forfeit
        // half of topN. When both sides are short, every candidate is returned and the result
        // is simply smaller than topN.
        int queryLimit = Math.min(queryStats.size(), topN / 2);
        int loadLimit = Math.min(loadStats.size(), topN - queryLimit);
        queryLimit = Math.min(queryStats.size(), topN - loadLimit);

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

    /**
     * Flatten beId -> tabletId -> stats into one view keyed by tablet. A tablet with several
     * replicas is reported once per backend holding one, so the collisions are resolved by
     * mergeByMax().
     */
    private Map<Long, AccessStatsResult> mergeBackendStats() {
        int upperBound = 0;
        for (Map<Long, AccessStatsResult> backendStats : beToStats.values()) {
            upperBound += backendStats.size();
        }
        Map<Long, AccessStatsResult> mergedStats = Maps.newHashMapWithExpectedSize(upperBound);
        for (Map<Long, AccessStatsResult> backendStats : beToStats.values()) {
            for (AccessStatsResult result : backendStats.values()) {
                mergedStats.merge(result.id, result, TabletSlidingWindowAccessStats::mergeByMax);
            }
        }
        return mergedStats;
    }

    /**
     * Per-field maximum, never a sum: each backend reports its own replica, so summing would
     * make a three-replica tablet look three times hotter than a one-replica tablet carrying
     * the same traffic. Every field independently answers "the busiest replica", which is what
     * both consumers want - the rates rank tablets, and the raw count is only displayed.
     */
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
