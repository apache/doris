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

package org.apache.doris.datasource.metacache;

import java.util.Objects;

/**
 * Immutable stats snapshot of one {@link MetaCacheEntry}.
 *
 * <p>Time fields use the following units:
 * <ul>
 *   <li>{@code totalLoadTimeNanos}/{@code averageLoadPenaltyNanos}: nanoseconds</li>
 *   <li>{@code lastLoadSuccessTimeMs}/{@code lastLoadFailureTimeMs}: epoch milliseconds</li>
 * </ul>
 *
 * <p>For last-load timestamps, {@code -1} means no corresponding event happened yet.
 * {@code lastError} keeps the latest load failure message; empty string means no failure recorded.
 */
public final class MetaCacheEntryStats {
    private final boolean configEnabled;
    private final boolean effectiveEnabled;
    private final boolean autoRefresh;
    private final long ttlSecond;
    private final long capacity;
    private final long estimatedSize;
    private final long requestCount;
    private final long hitCount;
    private final long missCount;
    private final double hitRate;
    private final long loadSuccessCount;
    private final long loadFailureCount;
    private final long totalLoadTimeNanos;
    private final double averageLoadPenaltyNanos;
    private final long evictionCount;
    private final long invalidateCount;
    private final long lastLoadSuccessTimeMs;
    private final long lastLoadFailureTimeMs;
    private final String lastError;

    /**
     * Build an immutable stats snapshot.
     */
    public MetaCacheEntryStats(
            boolean configEnabled,
            boolean effectiveEnabled,
            boolean autoRefresh,
            long ttlSecond,
            long capacity,
            long estimatedSize,
            long requestCount,
            long hitCount,
            long missCount,
            double hitRate,
            long loadSuccessCount,
            long loadFailureCount,
            long totalLoadTimeNanos,
            double averageLoadPenaltyNanos,
            long evictionCount,
            long invalidateCount,
            long lastLoadSuccessTimeMs,
            long lastLoadFailureTimeMs,
            String lastError) {
        this.configEnabled = configEnabled;
        this.effectiveEnabled = effectiveEnabled;
        this.autoRefresh = autoRefresh;
        this.ttlSecond = ttlSecond;
        this.capacity = capacity;
        this.estimatedSize = estimatedSize;
        this.requestCount = requestCount;
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.hitRate = hitRate;
        this.loadSuccessCount = loadSuccessCount;
        this.loadFailureCount = loadFailureCount;
        this.totalLoadTimeNanos = totalLoadTimeNanos;
        this.averageLoadPenaltyNanos = averageLoadPenaltyNanos;
        this.evictionCount = evictionCount;
        this.invalidateCount = invalidateCount;
        this.lastLoadSuccessTimeMs = lastLoadSuccessTimeMs;
        this.lastLoadFailureTimeMs = lastLoadFailureTimeMs;
        this.lastError = Objects.requireNonNull(lastError, "lastError");
    }

    public boolean isConfigEnabled() {
        return configEnabled;
    }

    /**
     * Effective cache enable state evaluated by {@link CacheSpec#isCacheEnabled(boolean, long, long)}.
     */
    public boolean isEffectiveEnabled() {
        return effectiveEnabled;
    }

    public boolean isAutoRefresh() {
        return autoRefresh;
    }

    public long getTtlSecond() {
        return ttlSecond;
    }

    public long getCapacity() {
        return capacity;
    }

    public long getEstimatedSize() {
        return estimatedSize;
    }

    public long getRequestCount() {
        return requestCount;
    }

    public long getHitCount() {
        return hitCount;
    }

    public long getMissCount() {
        return missCount;
    }

    public double getHitRate() {
        return hitRate;
    }

    public long getLoadSuccessCount() {
        return loadSuccessCount;
    }

    public long getLoadFailureCount() {
        return loadFailureCount;
    }

    public long getTotalLoadTimeNanos() {
        return totalLoadTimeNanos;
    }

    /**
     * Average load penalty in nanoseconds.
     */
    public double getAverageLoadPenaltyNanos() {
        return averageLoadPenaltyNanos;
    }

    public long getEvictionCount() {
        return evictionCount;
    }

    public long getInvalidateCount() {
        return invalidateCount;
    }

    /**
     * Last successful load timestamp in epoch milliseconds, or {@code -1} if absent.
     */
    public long getLastLoadSuccessTimeMs() {
        return lastLoadSuccessTimeMs;
    }

    /**
     * Last failed load timestamp in epoch milliseconds, or {@code -1} if absent.
     */
    public long getLastLoadFailureTimeMs() {
        return lastLoadFailureTimeMs;
    }

    /**
     * Latest load failure message, or empty string if no failure is recorded.
     */
    public String getLastError() {
        return lastError;
    }
}
