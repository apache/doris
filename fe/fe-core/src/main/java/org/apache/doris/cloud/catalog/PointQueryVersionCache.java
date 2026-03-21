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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Partition;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.VersionHelper;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A request-coalescing version cache for point queries in cloud mode.
 *
 * <p>When {@code enable_snapshot_point_query=true}, every point query needs to fetch
 * the partition's visible version from MetaService. Under high concurrency, this causes
 * N RPCs for N concurrent point queries on the same partition.</p>
 *
 * <p>This cache optimizes the version fetching by:
 * <ul>
 *   <li><b>Short TTL caching</b>: Partition versions are cached for a configurable duration
 *       ({@code point_query_version_cache_ttl_ms}, default 0 i.e. disabled). Within the TTL window,
 *       concurrent queries reuse the cached version.</li>
 *   <li><b>Request coalescing</b>: When the cache expires, only the first request issues
 *       the MetaService RPC. Concurrent requests for the same partition wait on the inflight
 *       result via a {@link CompletableFuture}.</li>
 * </ul>
 * </p>
 */
public class PointQueryVersionCache {
    private static final Logger LOG = LogManager.getLogger(PointQueryVersionCache.class);

    /**
     * Maximum number of partition entries in the cache.
     * When exceeded, expired entries are cleaned up first;
     * if still over capacity, the oldest entries are evicted.
     */
    @VisibleForTesting
    static final int DEFAULT_MAX_CACHE_SIZE = 1_000_000;

    /** Timeout in milliseconds when waiting for a coalesced (inflight) request. */
    private static final long COALESCING_TIMEOUT_MS = 10_000;

    private static volatile PointQueryVersionCache instance;

    /**
     * Cache entry holding the version and the timestamp when it was cached.
     */
    static class VersionEntry {
        final long version;
        final long cachedTimeMs;

        VersionEntry(long version, long cachedTimeMs) {
            this.version = version;
            this.cachedTimeMs = cachedTimeMs;
        }

        boolean isExpired(long ttlMs) {
            if (ttlMs <= 0) {
                return true;
            }
            return System.currentTimeMillis() - cachedTimeMs > ttlMs;
        }
    }

    // partitionId -> cached VersionEntry
    private final ConcurrentHashMap<Long, VersionEntry> cache = new ConcurrentHashMap<>();
    private final int maxCacheSize;

    // partitionId -> inflight RPC future (for request coalescing)
    private final ConcurrentHashMap<Long, CompletableFuture<Long>> inflightRequests = new ConcurrentHashMap<>();

    @VisibleForTesting
    public PointQueryVersionCache() {
        this(DEFAULT_MAX_CACHE_SIZE);
    }

    @VisibleForTesting
    public PointQueryVersionCache(int maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
    }

    public static PointQueryVersionCache getInstance() {
        if (instance == null) {
            synchronized (PointQueryVersionCache.class) {
                if (instance == null) {
                    instance = new PointQueryVersionCache();
                }
            }
        }
        return instance;
    }

    @VisibleForTesting
    public static void setInstance(PointQueryVersionCache cache) {
        instance = cache;
    }

    /**
     * Get the visible version for a partition, using TTL-based caching and request coalescing.
     *
     * @param partition  the cloud partition to get version for
     * @param ttlMs      TTL in milliseconds; 0 or negative disables caching
     * @return the visible version
     * @throws RpcException if the MetaService RPC fails
     */
    public long getVersion(CloudPartition partition, long ttlMs) throws RpcException {
        long partitionId = partition.getId();

        // If cache is disabled, fetch directly
        if (ttlMs <= 0) {
            return fetchVersionFromMs(partition);
        }

        // Check cache first
        VersionEntry entry = cache.get(partitionId);
        if (entry != null && !entry.isExpired(ttlMs)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("point query version cache hit, partition={}, version={}", partitionId, entry.version);
            }
            return entry.version;
        }

        // Cache miss or expired: use request coalescing
        return getVersionWithCoalescing(partition, partitionId, ttlMs);
    }

    private long getVersionWithCoalescing(CloudPartition partition, long partitionId, long ttlMs)
            throws RpcException {
        // Try to become the leader request for this partition
        CompletableFuture<Long> myFuture = new CompletableFuture<>();
        CompletableFuture<Long> existingFuture = inflightRequests.putIfAbsent(partitionId, myFuture);

        if (existingFuture != null) {
            // Another request is already in flight — wait for its result
            if (LOG.isDebugEnabled()) {
                LOG.debug("point query version coalescing, waiting for inflight request, partition={}",
                        partitionId);
            }
            try {
                return existingFuture.get(COALESCING_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RpcException("get version", "interrupted while waiting for coalesced request");
            } catch (TimeoutException e) {
                throw new RpcException("get version",
                        "timed out after " + COALESCING_TIMEOUT_MS + "ms waiting for coalesced request");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RpcException) {
                    throw (RpcException) cause;
                }
                throw new RpcException("get version", cause != null ? cause.getMessage() : e.getMessage());
            }
        }

        // We are the leader — fetch version from MetaService
        try {
            long version = fetchVersionFromMs(partition);
            // Update cache with monotonicity enforcement:
            // only advance the cached version, never regress it.
            long now = System.currentTimeMillis();
            cache.compute(partitionId, (key, existing) -> {
                if (existing == null || version >= existing.version) {
                    return new VersionEntry(version, now);
                }
                // Fetched version is older — keep the existing higher version
                // but refresh the timestamp so TTL is extended.
                LOG.warn("point query version cache: fetched version {} is older than cached version {}"
                        + " for partition {}, keeping cached version",
                        version, existing.version, key);
                return new VersionEntry(existing.version, now);
            });
            // Evict if cache is over capacity
            evictIfNeeded(ttlMs);
            // Also update the partition's cached version
            long cachedVersion = cache.get(partitionId).version;
            partition.setCachedVisibleVersion(cachedVersion, now);
            // Complete the future so waiting requests get the result
            myFuture.complete(cachedVersion);
            if (LOG.isDebugEnabled()) {
                LOG.debug("point query version fetched from MS, partition={}, version={}, cachedVersion={}",
                        partitionId, version, cachedVersion);
            }
            return cachedVersion;
        } catch (Exception e) {
            // Complete exceptionally so waiting requests also get the error
            myFuture.completeExceptionally(e);
            if (e instanceof RpcException) {
                throw (RpcException) e;
            }
            throw new RpcException("get version", e.getMessage());
        } finally {
            // Remove the inflight request entry
            inflightRequests.remove(partitionId, myFuture);
        }
    }

    /**
     * Fetch visible version from MetaService for a single partition.
     * This method is package-private to allow mocking in tests.
     */
    @VisibleForTesting
    protected long fetchVersionFromMs(CloudPartition partition) throws RpcException {
        Cloud.GetVersionRequest request = Cloud.GetVersionRequest.newBuilder()
                .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                .setDbId(partition.getDbId())
                .setTableId(partition.getTableId())
                .setPartitionId(partition.getId())
                .setBatchMode(false)
                .build();

        Cloud.GetVersionResponse resp = VersionHelper.getVersionFromMeta(request);
        if (resp.getStatus().getCode() == Cloud.MetaServiceCode.OK) {
            return resp.getVersion();
        } else if (resp.getStatus().getCode() == Cloud.MetaServiceCode.VERSION_NOT_FOUND) {
            return Partition.PARTITION_INIT_VERSION;
        } else {
            throw new RpcException("get version", "unexpected status " + resp.getStatus());
        }
    }

    /**
     * Evict entries if the cache exceeds {@link #maxCacheSize}.
     * First removes expired entries; if still over capacity, evicts the oldest entries.
     */
    private void evictIfNeeded(long ttlMs) {
        if (cache.size() <= maxCacheSize) {
            return;
        }
        // Phase 1: remove expired entries
        cache.entrySet().removeIf(e -> e.getValue().isExpired(ttlMs));
        if (cache.size() <= maxCacheSize) {
            return;
        }
        // Phase 2: evict oldest entries until we are within bounds
        int toEvict = cache.size() - maxCacheSize;
        List<Map.Entry<Long, VersionEntry>> entries = new ArrayList<>(cache.entrySet());
        entries.sort(Comparator.comparingLong(e -> e.getValue().cachedTimeMs));
        for (int i = 0; i < toEvict && i < entries.size(); i++) {
            cache.remove(entries.get(i).getKey(), entries.get(i).getValue());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("point query version cache eviction: evicted {} entries, cache size now {}",
                    toEvict, cache.size());
        }
    }

    /**
     * Clear all cached entries. Primarily for testing.
     */
    @VisibleForTesting
    public void clear() {
        cache.clear();
        inflightRequests.clear();
    }

    /**
     * Get the number of cached entries. Primarily for testing.
     */
    @VisibleForTesting
    public int cacheSize() {
        return cache.size();
    }

    /**
     * Get the max cache size. Primarily for testing.
     */
    @VisibleForTesting
    public int maxCacheSize() {
        return maxCacheSize;
    }
}
