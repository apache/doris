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
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.zip.CRC32;

/**
 * Sliding window access statistics utility class.
 * Supports tracking access statistics for different types of IDs (tablet, replica, backend, etc.)
 */
public class SlidingWindowAccessStats {
    private static final Logger LOG = LogManager.getLogger(SlidingWindowAccessStats.class);

    // ID type for this instance
    private final AccessStatsIdType idType;

    // Sort active IDs by accessCount desc, then lastAccessTime desc
    private static final Comparator<AccessStatsResult> TOPN_ACTIVE_COMPARATOR =
            Comparator.comparingLong((AccessStatsResult r) -> r.accessCount).reversed()
                    .thenComparing(Comparator.comparingLong((AccessStatsResult r) -> r.lastAccessTime).reversed());

    // Time window in milliseconds (default: 1 hour)
    private final long timeWindowMs;

    // Bucket size in milliseconds (default: 1 minute)
    // The time window is divided into multiple buckets, each bucket stores access count for a time period.
    // For example: if timeWindowMs=1hour and bucketSizeMs=1minute, there will be 60 buckets.
    // Smaller bucket size = more accurate statistics but more memory usage.
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
        // ID counters: id -> SlidingWindowCounter
        private final ConcurrentHashMap<Long, SlidingWindowCounter> idCounters = new ConcurrentHashMap<>();
    }

    // Sharded access stats to reduce lock contention
    private final AccessStatsShard[] shards = new AccessStatsShard[SHARD_SIZE];

    // Access counter for monitoring
    private final AtomicLong totalAccessCount = new AtomicLong(0);

    // Cleanup daemon
    private AccessStatsCleanupDaemon cleanupDaemon;

    // Thread pool for async recordAccess execution
    private ThreadPoolExecutor asyncExecutor;

    // Default bucket size: 1 minute (60 buckets for 1 hour window)
    private static final long DEFAULT_BUCKET_SIZE_SECOND = 60L;

    // Default cleanup interval: 5 minutes
    private static final long DEFAULT_CLEANUP_INTERVAL_SECOND = 300L;

    SlidingWindowAccessStats(AccessStatsIdType idType) {
        this.idType = idType;
        this.timeWindowMs = Config.sliding_window_time_window_second * 1000L;
        this.bucketSizeMs = DEFAULT_BUCKET_SIZE_SECOND * 1000L;
        this.numBuckets = (int) (Config.sliding_window_time_window_second / DEFAULT_BUCKET_SIZE_SECOND);
        this.cleanupIntervalMs = DEFAULT_CLEANUP_INTERVAL_SECOND * 1000L;

        // Initialize shards
        for (int i = 0; i < SHARD_SIZE; i++) {
            shards[i] = new AccessStatsShard();
        }

        // Start cleanup daemon and async executor if enabled
        if (Config.enable_sliding_window_access_stats) {
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
                        Thread t = new Thread(r, "sliding-window-access-stats-async-" + idType.name().toLowerCase());
                        t.setDaemon(true);
                        return t;
                    },
                    new ThreadPoolExecutor.DiscardPolicy() // discard when queue is full
            );

            LOG.info("SlidingWindowAccessStats initialized for type {}: timeWindow={}ms, bucketSize={}ms, "
                    + "numBuckets={}, shardSize={}, cleanupInterval={}ms",
                    idType, timeWindowMs, bucketSizeMs, numBuckets, SHARD_SIZE, cleanupIntervalMs);
        }
    }

    /**
     * Get the ID type for this instance
     */
    public AccessStatsIdType getIdType() {
        return idType;
    }

    /**
     * Get shard index for a given ID
     * Uses CRC32 hash function to ensure better distribution when IDs are sequential or have patterns.
     * CRC32 processes the full 64-bit long value to avoid information loss.
     */
    private int getShardIndex(long id) {
        // Use CRC32 to hash the full 64-bit ID, then take modulo
        // This ensures better distribution even when IDs are sequential and preserves all 64 bits
        CRC32 crc = new CRC32();
        crc.update((int) (id >>> 32));  // Update with high 32 bits
        crc.update((int) id);            // Update with low 32 bits
        long hashValue = crc.getValue();
        return (int) (hashValue % SHARD_SIZE);
    }

    /**
     * Record an access asynchronously
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
                    LOG.warn("Failed to record access asynchronously for {} id={}", idType, id, e);
                }
            });
        } catch (Exception e) {
            // If executor is shutdown or queue is full, silently ignore
            // Statistics can tolerate some loss
            LOG.warn("Failed to submit async recordAccess task for {} id={}", idType, id, e);
        }
    }

    /**
     * Record an access
     */
    public void recordAccess(long id) {
        long currentTime = System.currentTimeMillis();
        int shardIndex = getShardIndex(id);
        AccessStatsShard shard = shards[shardIndex];

        SlidingWindowCounter counter = shard.idCounters.computeIfAbsent(id,
                k -> new SlidingWindowCounter(numBuckets));
        counter.add(currentTime, bucketSizeMs, numBuckets);
        totalAccessCount.incrementAndGet();
    }

    /**
     * Get access count for an ID within the time window
     */
    public AccessStatsResult getAccessInfo(long id) {
        if (!Config.enable_sliding_window_access_stats) {
            return null;
        }

        int shardIndex = getShardIndex(id);
        AccessStatsShard shard = shards[shardIndex];
        SlidingWindowCounter counter = shard.idCounters.get(id);

        if (counter == null) {
            return null;
        }

        long currentTime = System.currentTimeMillis();
        return new AccessStatsResult(
                id,
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
     * Get top N most active IDs
     * Uses a min-heap (PriorityQueue) to maintain TopN efficiently without sorting all results.
     */
    public List<AccessStatsResult> getTopNActive(int topN) {
        if (!Config.enable_sliding_window_access_stats) {
            return Collections.emptyList();
        }

        if (topN <= 0) {
            return Collections.emptyList();
        }

        long currentTime = System.currentTimeMillis();
        // Use a min-heap with reversed comparator to maintain TopN
        // The heap keeps the smallest element at the top, so we can efficiently replace it
        // when we find a larger element
        PriorityQueue<AccessStatsResult> minHeap = new PriorityQueue<>(
                topN + 1, // Initial capacity: topN + 1 to avoid resizing
                Collections.reverseOrder(TOPN_ACTIVE_COMPARATOR) // Reversed: min-heap for TopN
        );

        // Collect from all shards and maintain TopN using min-heap
        for (AccessStatsShard shard : shards) {
            for (Map.Entry<Long, SlidingWindowCounter> entry : shard.idCounters.entrySet()) {
                long id = entry.getKey();
                SlidingWindowCounter counter = entry.getValue();

                // Skip if no recent activity
                if (!counter.hasRecentActivity(currentTime, timeWindowMs)) {
                    continue;
                }

                long accessCount = counter.getCount(currentTime, timeWindowMs, bucketSizeMs, numBuckets);
                if (accessCount > 0) {
                    AccessStatsResult result = new AccessStatsResult(id, accessCount, counter.getLastAccessTime());

                    if (minHeap.size() < topN) {
                        // Heap not full, directly add
                        minHeap.offer(result);
                    } else {
                        // Heap is full, compare with the smallest element (heap top)
                        // If current element is larger, replace the heap top
                        if (TOPN_ACTIVE_COMPARATOR.compare(result, minHeap.peek()) > 0) {
                            minHeap.poll();
                            minHeap.offer(result);
                        }
                    }
                }
            }
        }

        // Convert heap to list and sort in descending order (TopN)
        List<AccessStatsResult> results = new ArrayList<>(minHeap);
        results.sort(TOPN_ACTIVE_COMPARATOR);
        return results;
    }

    /**
     * Clean up expired access records
     */
    private void cleanupExpiredRecords() {
        if (!Config.enable_sliding_window_access_stats) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        int cleaned = 0;

        // Clean each shard
        for (AccessStatsShard shard : shards) {
            // Clean ID counters
            for (Map.Entry<Long, SlidingWindowCounter> entry : shard.idCounters.entrySet()) {
                SlidingWindowCounter counter = entry.getValue();
                counter.cleanup(currentTime, timeWindowMs);

                if (!counter.hasRecentActivity(currentTime, timeWindowMs)) {
                    shard.idCounters.remove(entry.getKey());
                    cleaned++;
                }
            }
        }

        if (LOG.isDebugEnabled() && cleaned > 0) {
            LOG.debug("Cleaned up {} expired access records for type {}", cleaned, idType);
        }
    }

    /**
     * Get statistics summary
     */
    public String getStatsSummary() {
        if (!Config.enable_sliding_window_access_stats) {
            return String.format("SlidingWindowAccessStats (type: %s) is disabled", idType);
        }

        long currentTime = System.currentTimeMillis();
        int activeIds = 0;
        long totalAccess = 0;

        for (AccessStatsShard shard : shards) {
            for (SlidingWindowCounter counter : shard.idCounters.values()) {
                if (counter.hasRecentActivity(currentTime, timeWindowMs)) {
                    activeIds++;
                    totalAccess += counter.getCount(currentTime, timeWindowMs, bucketSizeMs, numBuckets);
                }
            }
        }

        return String.format(
            "SlidingWindowAccessStats{type=%s, timeWindow=%ds, bucketSize=%ds, numBuckets=%d, "
                + "shardSize=%d, activeIds=%d, "
                + "totalAccess=%d, totalAccessCount=%d}",
            idType, timeWindowMs / 1000, bucketSizeMs / 1000, numBuckets, SHARD_SIZE,
            activeIds, totalAccess, totalAccessCount.get());
    }

    /**
     * Cleanup daemon for expired records
     */
    private class AccessStatsCleanupDaemon extends MasterDaemon {
        public AccessStatsCleanupDaemon() {
            super("sliding-window-access-stats-cleanup-" + idType.name().toLowerCase(), cleanupIntervalMs);
        }

        @Override
        protected void runAfterCatalogReady() {
            if (!Env.getCurrentEnv().isMaster()) {
                return;
            }

            try {
                cleanupExpiredRecords();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} stat = {}, top 10 active = {}",
                            idType, getStatsSummary(), getTopNActive(10));
                }
            } catch (Exception e) {
                LOG.warn("Failed to cleanup expired access records for type {}", idType, e);
            }
        }
    }
}
