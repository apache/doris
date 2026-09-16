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
import java.util.Collection;
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
    // rest by active_tablet_sliding_window_time_window_second. Reads also filter expired entries
    // and reclaim expired snapshots when reports stop; backend removal calls removeBackend().
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
            reportedAccesses += acc.scanDelta + acc.loadDelta;
            backendStats.put(tabletId, new AccessStatsResult(tabletId, acc.scanDelta, acc.loadDelta,
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
     * otherwise accumulate a whole window's worth of distinct tablets. The coldest are dropped
     * first, by rate rather than by age - everything that survives the cutoff above is inside
     * the window already, and dropping by age alone would evict a tablet hammered early in the
     * window in favour of one touched once at the end.
     *
     * <p>The cap is applied per dimension, exactly the way getTopNActive() spends its budget.
     * Ranking the two dimensions against each other here would compare unlike units: scans
     * outnumber flushes by one to two orders of magnitude, so a churning query set would evict
     * every load tablet before the reserved load quota downstream ever got to see it - and with
     * the backend's own lists well under their cap, nothing would report the loss.
     *
     * <p>lastAccessTime comes from the backend's clock while the cutoff comes from FE's, but a
     * window measured in hours absorbs the skew between them.
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
        //
        // Known limitation: a report carrying only one dimension replaces the whole record, so
        // a query-only report zeroes a load rate that is still inside the window and the tablet
        // can drop out of the load bucket. Not fixed - expiring the dimensions independently
        // needs a separate timestamp per dimension, which is more machinery than a scheduling
        // hint is worth. The tablet stays in the map and keeps its query ranking.
        merged.putAll(reported);
        if (merged.size() <= retainLimit) {
            return merged;
        }

        Map<Long, AccessStatsResult> capped = Maps.newHashMapWithExpectedSize(retainLimit);
        for (AccessStatsResult result : pickAcrossDimensions(merged.values(), retainLimit)) {
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
     * Get access information for a tablet, merged across the backends holding its replicas the
     * same way getTopNActive() does. Returning a single backend's record instead would report
     * that backend's LastAccessTime even when another replica was touched more recently, and
     * would show one replica's share of a split query load as the whole tablet's.
     */
    public AccessStatsResult getAccessInfo(long id) {
        if (!Config.enable_active_tablet_sliding_window_access_stats) {
            return null;
        }

        long oldestKept = System.currentTimeMillis()
                - Math.max(1L, Config.active_tablet_sliding_window_time_window_second) * 1000L;
        AccessStatsResult result = null;
        for (Map<Long, AccessStatsResult> backendStats : beToStats.values()) {
            AccessStatsResult candidate = backendStats.get(id);
            if (candidate != null && candidate.lastAccessTime >= oldestKept) {
                result = (result == null) ? candidate : mergeReplicas(result, candidate);
            }
        }
        return result;
    }

    /**
     * Result for top N query.
     */
    public static class AccessStatsResult {
        public final long id;
        // Kept apart because the two dimensions merge differently across replicas -- see
        // mergeReplicas(). accessCount is their sum, the single number the SHOW / PROC
        // views display.
        public final long scanCount;
        public final long loadCount;
        public final long accessCount;
        public final long lastAccessTime;
        public final double scanRate;
        public final double loadRate;

        public AccessStatsResult(long id, long scanCount, long loadCount, long lastAccessTime,
                double scanRate, double loadRate) {
            this.id = id;
            this.scanCount = scanCount;
            this.loadCount = loadCount;
            this.accessCount = scanCount + loadCount;
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
        return pickAcrossDimensions(mergeBackendStats().values(), topN);
    }

    /**
     * Spend a budget of {@code limit} tablets across the two dimensions, each ranked on its own
     * rate. Half is reserved for queries, counted in tablets actually added so a tablet hot on
     * both dimensions costs one slot and not two; loads then fill the rest, and queries backfill
     * whatever is still free - which happens when loads ran out, or when the two lists
     * overlapped. Neither dimension is ever ranked against the other: scans and flushes differ
     * by one to two orders of magnitude, so a single ordering drops load-heavy tablets as a class.
     */
    private static List<AccessStatsResult> pickAcrossDimensions(
            Collection<AccessStatsResult> candidates, int limit) {
        List<AccessStatsResult> queryStats = new ArrayList<>();
        List<AccessStatsResult> loadStats = new ArrayList<>();
        for (AccessStatsResult result : candidates) {
            if (result.scanRate > 0) {
                queryStats.add(result);
            }
            if (result.loadRate > 0) {
                loadStats.add(result);
            }
        }
        queryStats.sort(QUERY_RATE_COMPARATOR);
        loadStats.sort(LOAD_RATE_COMPARATOR);

        Map<Long, AccessStatsResult> selected = new LinkedHashMap<>();
        int queryIdx = 0;
        int loadIdx = 0;
        int queryReserve = limit / 2;
        while (queryIdx < queryStats.size() && selected.size() < queryReserve) {
            AccessStatsResult result = queryStats.get(queryIdx++);
            selected.putIfAbsent(result.id, result);
        }
        while (loadIdx < loadStats.size() && selected.size() < limit) {
            AccessStatsResult result = loadStats.get(loadIdx++);
            selected.putIfAbsent(result.id, result);
        }
        while (queryIdx < queryStats.size() && selected.size() < limit) {
            AccessStatsResult result = queryStats.get(queryIdx++);
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
     * mergeReplicas().
     */
    private Map<Long, AccessStatsResult> mergeBackendStats() {
        int upperBound = 0;
        for (Map<Long, AccessStatsResult> backendStats : beToStats.values()) {
            upperBound += backendStats.size();
        }
        Map<Long, AccessStatsResult> mergedStats = Maps.newHashMapWithExpectedSize(upperBound);
        long oldestKept = System.currentTimeMillis()
                - Math.max(1L, Config.active_tablet_sliding_window_time_window_second) * 1000L;
        for (Map.Entry<Long, Map<Long, AccessStatsResult>> backend : beToStats.entrySet()) {
            Map<Long, AccessStatsResult> backendStats = backend.getValue();
            boolean hasActiveStats = false;
            for (AccessStatsResult result : backendStats.values()) {
                if (result.lastAccessTime >= oldestKept) {
                    hasActiveStats = true;
                    mergedStats.merge(result.id, result, TabletSlidingWindowAccessStats::mergeReplicas);
                }
            }
            if (!hasActiveStats) {
                // A concurrent report may have replaced this snapshot; only remove the one read.
                beToStats.remove(backend.getKey(), backendStats);
            }
        }
        return mergedStats;
    }

    /**
     * Query traffic always SUMS across backends: the planner assigns each scan range to exactly
     * one replica, so a tablet's query traffic is split between the backends holding it and only
     * the sum is the tablet's real rate - taking the max would make a three-replica tablet read
     * three times colder than a single-replica one carrying the same load.
     *
     * <p>Load traffic depends on the deployment. Shared-nothing replicates every write, so all
     * three backends report the same flushes and summing would multiply them by the replication
     * factor - max is the tablet's real rate there. Cloud does not replicate on the write path:
     * a CloudTablet holds a single CloudReplica, and CloudReplica#getBackendIdImpl resolves it
     * to one backend per compute group (a random one of cloud_replica_num when
     * enable_cloud_multi_replica is on), so two backends reporting the same tablet did
     * independent work and max would silently discard half of it.
     *
     * <p>lastAccessTime is the most recent touch either way.
     */
    private static AccessStatsResult mergeReplicas(AccessStatsResult left, AccessStatsResult right) {
        boolean loadIsSplit = Config.isCloudMode();
        return new AccessStatsResult(left.id,
                left.scanCount + right.scanCount,
                loadIsSplit ? left.loadCount + right.loadCount
                        : Math.max(left.loadCount, right.loadCount),
                Math.max(left.lastAccessTime, right.lastAccessTime),
                left.scanRate + right.scanRate,
                loadIsSplit ? left.loadRate + right.loadRate
                        : Math.max(left.loadRate, right.loadRate));
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
