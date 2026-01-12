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

package org.apache.doris.common.util;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.Collectors;

public class SlidingWindowAccessStats {
    private static final Logger LOG = LogManager.getLogger(SlidingWindowAccessStats.class);

    private static volatile SlidingWindowAccessStats instance;

    // Sort active tablets by accessCount desc, then lastAccessTime desc
    private static final Comparator<AccessStatsResult> TOPN_ACTIVE_TABLET_COMPARATOR =
            Comparator.comparingLong((AccessStatsResult r) -> r.accessCount).reversed()
                    .thenComparing(Comparator.comparingLong((AccessStatsResult r) -> r.lastAccessTime).reversed());

    // Time window in milliseconds (default: 1 hour)
    private final long timeWindowMs;

    // Bucket size in milliseconds (default: 1 minute)
    // Smaller bucket = more accurate but more memory
    private final long bucketSizeMs;

    // Number of buckets in the sliding window
    private final int numBuckets;

    // Cleanup interval in milliseconds (default: 5 minutes)
    private final long cleanupIntervalMs;

    // Shard size to reduce lock contention
    private static final int SHARD_SIZE = 1024;

    /**
     * Sliding window counter for a single replica/tablet
     */
    private static class SlidingWindowCounter {
        // Each bucket stores count for a time period
        private final AtomicLongArray buckets;
        // Timestamp of each bucket (to detect expired buckets)
        private final AtomicLongArray bucketTimestamps;
        // Last access time (for TopN sorting)
        private volatile long lastAccessTime = 0;
        // Total count in current window (cached for performance)
        private volatile long cachedTotalCount = 0;
        private volatile long cachedCountTime = 0;

        SlidingWindowCounter(int numBuckets) {
            this.buckets = new AtomicLongArray(numBuckets);
            this.bucketTimestamps = new AtomicLongArray(numBuckets);
        }

        /**
         * Get current bucket index based on current time
         */
        private int getBucketIndex(long currentTimeMs, long bucketSizeMs, int numBuckets) {
            return (int) ((currentTimeMs / bucketSizeMs) % numBuckets);
        }

        /**
         * Add an access count
         */
        void add(long currentTimeMs, long bucketSizeMs, int numBuckets) {
            int bucketIndex = getBucketIndex(currentTimeMs, bucketSizeMs, numBuckets);
            long bucketStartTime = (currentTimeMs / bucketSizeMs) * bucketSizeMs;

            // Check if this bucket is expired (belongs to a different time window)
            long bucketTimestamp = bucketTimestamps.get(bucketIndex);
            if (bucketTimestamp != bucketStartTime) {
                // Reset expired bucket
                buckets.set(bucketIndex, 0);
                bucketTimestamps.set(bucketIndex, bucketStartTime);
            }

            // Increment count
            buckets.addAndGet(bucketIndex, 1);
            lastAccessTime = currentTimeMs;
            cachedTotalCount = -1; // Invalidate cache
        }

        /**
         * Get total count within the time window
         */
        long getCount(long currentTimeMs, long timeWindowMs, long bucketSizeMs, int numBuckets) {
            // Use cached value if still valid (within 1 second)
            if (cachedTotalCount >= 0 && (currentTimeMs - cachedCountTime) < 1000) {
                return cachedTotalCount;
            }

            long windowStart = currentTimeMs - timeWindowMs;
            long count = 0;

            for (int i = 0; i < numBuckets; i++) {
                long bucketTimestamp = bucketTimestamps.get(i);
                if (bucketTimestamp >= windowStart && bucketTimestamp > 0) {
                    count += buckets.get(i);
                }
            }

            cachedTotalCount = count;
            cachedCountTime = currentTimeMs;
            return count;
        }

        long getLastAccessTime() {
            return lastAccessTime;
        }

        /**
         * Clean up expired buckets
         */
        void cleanup(long currentTimeMs, long timeWindowMs) {
            long windowStart = currentTimeMs - timeWindowMs;
            for (int i = 0; i < buckets.length(); i++) {
                long bucketTimestamp = bucketTimestamps.get(i);
                if (bucketTimestamp > 0 && bucketTimestamp < windowStart) {
                    buckets.set(i, 0);
                    bucketTimestamps.set(i, 0);
                }
            }
            cachedTotalCount = -1; // Invalidate cache
        }

        /**
         * Check if this counter has any recent activity
         */
        boolean hasRecentActivity(long currentTimeMs, long timeWindowMs) {
            return lastAccessTime >= (currentTimeMs - timeWindowMs);
        }
    }

    /**
     * Shard structure to reduce lock contention
     */
    private static class AccessStatsShard {
        // Tablet counters: tabletId -> SlidingWindowCounter
        private final ConcurrentHashMap<Long, SlidingWindowCounter> tabletCounters = new ConcurrentHashMap<>();
    }

    // Sharded access stats to reduce lock contention
    private final AccessStatsShard[] shards = new AccessStatsShard[SHARD_SIZE];

    // Access counter for monitoring
    private final AtomicLong totalAccessCount = new AtomicLong(0);

    // Cleanup daemon
    private AccessStatsCleanupDaemon cleanupDaemon;

    // Thread pool for async recordAccess execution
    private ThreadPoolExecutor asyncExecutor;

    // Default time window: 1 hour
    private static final long DEFAULT_TIME_WINDOW_SECOND = 3600L;

    // Default bucket size: 1 minute (60 buckets for 1 hour window)
    private static final long DEFAULT_BUCKET_SIZE_SECOND = 60L;

    // Default cleanup interval: 5 minutes
    private static final long DEFAULT_CLEANUP_INTERVAL_SECOND = 300L;

    private SlidingWindowAccessStats() {
        this.timeWindowMs = DEFAULT_TIME_WINDOW_SECOND * 1000L;
        this.bucketSizeMs = DEFAULT_BUCKET_SIZE_SECOND * 1000L;
        this.numBuckets = (int) (DEFAULT_TIME_WINDOW_SECOND / DEFAULT_BUCKET_SIZE_SECOND); // 60 buckets
        this.cleanupIntervalMs = DEFAULT_CLEANUP_INTERVAL_SECOND * 1000L;

        // Initialize shards
        for (int i = 0; i < SHARD_SIZE; i++) {
            shards[i] = new AccessStatsShard();
        }

        // Start cleanup daemon
        if (Config.enable_cloud_active_tablet_priority_scheduling) {
            this.cleanupDaemon = new AccessStatsCleanupDaemon();
            this.cleanupDaemon.start();
            // Initialize async executor for recordAccess
            // Use a small thread pool with bounded queue to avoid blocking
            // If queue is full, discard the task (statistics can tolerate some loss)
            this.asyncExecutor = new ThreadPoolExecutor(
                    2,  // core pool size
                    4,  // maximum pool size
                    60L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(1000), // queue capacity
                    r -> {
                        Thread t = new Thread(r, "cloud-tablet-access-stats-async");
                        t.setDaemon(true);
                        return t;
                    },
                    new ThreadPoolExecutor.DiscardPolicy() // discard when queue is full
            );

            LOG.info("CloudReplicaAccessStats initialized: timeWindow={}ms, bucketSize={}ms, "
                    + "numBuckets={}, shardSize={}, cleanupInterval={}ms",
                    timeWindowMs, bucketSizeMs, numBuckets, SHARD_SIZE, cleanupIntervalMs);
        }
    }

    public static SlidingWindowAccessStats getInstance() {
        if (instance == null) {
            synchronized (SlidingWindowAccessStats.class) {
                if (instance == null) {
                    instance = new SlidingWindowAccessStats();
                }
            }
        }
        return instance;
    }

    /**
     * Get shard index for a given ID
     */
    private int getShardIndex(long id) {
        return (int) (id & (SHARD_SIZE - 1));
    }

    /**
     * Record an access to a replica asynchronously
     * This method is non-blocking and should be used in high-frequency call paths
     * to avoid blocking the caller thread.
     */
    public void recordAccessAsync(long id) {
        if (asyncExecutor == null) {
            return;
        }

        try {
            asyncExecutor.execute(() -> {
                try {
                    recordAccess(id);
                } catch (Exception e) {
                    // Log but don't propagate exception to avoid affecting caller
                    LOG.debug("Failed to record access asynchronously for replicaId={}", id, e);
                }
            });
        } catch (Exception e) {
            // If executor is shutdown or queue is full, silently ignore
            // Statistics can tolerate some loss
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to submit async recordAccess task for replicaId={}", id, e);
            }
        }
    }

    /**
     * Record an access to a replica
     */
    public void recordAccess(long id) {
        if (!Config.enable_cloud_active_tablet_priority_scheduling) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        int tabletShardIndex = getShardIndex(id);
        AccessStatsShard tabletShard = shards[tabletShardIndex];

        SlidingWindowCounter tabletCounter = tabletShard.tabletCounters.computeIfAbsent(id,
                k -> new SlidingWindowCounter(numBuckets));
        tabletCounter.add(currentTime, bucketSizeMs, numBuckets);

        totalAccessCount.incrementAndGet();
    }

    /**
     * Get access count for a tablet within the time window
     */
    public AccessStatsResult getTabletAccessInfo(long tabletId) {
        if (!Config.enable_cloud_active_tablet_priority_scheduling) {
            return null;
        }

        int shardIndex = getShardIndex(tabletId);
        AccessStatsShard shard = shards[shardIndex];
        SlidingWindowCounter counter = shard.tabletCounters.get(tabletId);

        if (counter == null) {
            return null;
        }

        long currentTime = System.currentTimeMillis();
        return new AccessStatsResult(
                tabletId,
                counter.getCount(currentTime, timeWindowMs, bucketSizeMs, numBuckets),
                counter.getLastAccessTime());
    }

    /**
     * Result for top N query
     */
    public static class AccessStatsResult {
        public final long id;
        public final long accessCount;
        public final long lastAccessTime;

        public AccessStatsResult(long id, long accessCount, long lastAccessTime) {
            this.id = id;
            this.accessCount = accessCount;
            this.lastAccessTime = lastAccessTime;
        }

        @Override
        public String toString() {
            return "AccessStatsResult{"
                    + "id=" + id
                    + ", accessCount=" + accessCount
                    + ", lastAccessTime=" + lastAccessTime
                    + '}';
        }
    }

    /**
     * Get top N most active tablets
     */
    public List<AccessStatsResult> getTopNActiveTablets(int topN) {
        if (!Config.enable_cloud_active_tablet_priority_scheduling) {
            return Collections.emptyList();
        }

        long currentTime = System.currentTimeMillis();
        List<AccessStatsResult> results = new ArrayList<>();

        // Collect from all shards
        for (AccessStatsShard shard : shards) {
            for (Map.Entry<Long, SlidingWindowCounter> entry : shard.tabletCounters.entrySet()) {
                long tabletId = entry.getKey();
                SlidingWindowCounter counter = entry.getValue();

                // Skip if no recent activity
                if (!counter.hasRecentActivity(currentTime, timeWindowMs)) {
                    continue;
                }

                long accessCount = counter.getCount(currentTime, timeWindowMs, bucketSizeMs, numBuckets);
                if (accessCount > 0) {
                    results.add(new AccessStatsResult(tabletId, accessCount, counter.getLastAccessTime()));
                }
            }
        }

        // Sort and return top N
        results.sort(TOPN_ACTIVE_TABLET_COMPARATOR);

        return results.stream().limit(topN).collect(Collectors.toList());
    }

    /**
     * Clean up expired access records
     */
    private void cleanupExpiredRecords() {
        if (!Config.enable_cloud_active_tablet_priority_scheduling) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        int replicaCleaned = 0;
        int tabletCleaned = 0;

        // Clean each shard
        for (AccessStatsShard shard : shards) {
            // Clean tablet counters
            for (Map.Entry<Long, SlidingWindowCounter> entry : shard.tabletCounters.entrySet()) {
                SlidingWindowCounter counter = entry.getValue();
                counter.cleanup(currentTime, timeWindowMs);

                if (!counter.hasRecentActivity(currentTime, timeWindowMs)) {
                    shard.tabletCounters.remove(entry.getKey());
                    tabletCleaned++;
                }
            }
        }

        if (LOG.isDebugEnabled() && (replicaCleaned > 0 || tabletCleaned > 0)) {
            LOG.debug("Cleaned up expired access records: {} replicas, {} tablets",
                    replicaCleaned, tabletCleaned);
        }
    }

    /**
     * Get statistics summary
     */
    public String getStatsSummary() {
        if (!Config.enable_cloud_active_tablet_priority_scheduling) {
            return "CloudReplicaAccessStats is disabled";
        }

        long currentTime = System.currentTimeMillis();
        int activeTablets = 0;
        long totalTabletAccess = 0;

        for (AccessStatsShard shard : shards) {
            for (SlidingWindowCounter counter : shard.tabletCounters.values()) {
                if (counter.hasRecentActivity(currentTime, timeWindowMs)) {
                    activeTablets++;
                    totalTabletAccess += counter.getCount(currentTime, timeWindowMs, bucketSizeMs, numBuckets);
                }
            }
        }

        return String.format(
                "SlidingWindowAccessStats{timeWindow=%ds, bucketSize=%ds, numBuckets=%d, "
                + "shardSize=%d, activeTablets=%d, "
                + "totalTabletAccess=%d, totalAccessCount=%d}",
                timeWindowMs / 1000, bucketSizeMs / 1000, numBuckets, SHARD_SIZE,
                activeTablets, totalTabletAccess, totalAccessCount.get());
    }

    /**
     * Cleanup daemon for expired records
     */
    private class AccessStatsCleanupDaemon extends MasterDaemon {
        public AccessStatsCleanupDaemon() {
            super("sliding-window-access-stats-cleanup", cleanupIntervalMs);
        }

        @Override
        protected void runAfterCatalogReady() {
            if (!Env.getCurrentEnv().isMaster()) {
                return;
            }

            try {
                cleanupExpiredRecords();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("tablet stat = {}, top 10 active tablet = {}",
                            getStatsSummary(), getTopNActiveTablets(10));
                }
            } catch (Exception e) {
                LOG.warn("Failed to cleanup expired access records", e);
            }
        }
    }
}
