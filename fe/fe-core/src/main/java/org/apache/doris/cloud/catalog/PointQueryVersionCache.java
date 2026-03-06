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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

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
 *       ({@code point_query_version_cache_ttl_ms}, default 500ms). Within the TTL window,
 *       concurrent queries reuse the cached version.</li>
 *   <li><b>Request coalescing</b>: When the cache expires, only the first request issues
 *       the MetaService RPC. Concurrent requests for the same partition wait on the inflight
 *       result via a {@link CompletableFuture}.</li>
 * </ul>
 * </p>
 */
public class PointQueryVersionCache {
    private static final Logger LOG = LogManager.getLogger(PointQueryVersionCache.class);

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

    // partitionId -> inflight RPC future (for request coalescing)
    private final ConcurrentHashMap<Long, CompletableFuture<Long>> inflightRequests = new ConcurrentHashMap<>();

    @VisibleForTesting
    public PointQueryVersionCache() {
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
                return existingFuture.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RpcException("get version", "interrupted while waiting for coalesced request");
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
            // Update cache
            cache.put(partitionId, new VersionEntry(version, System.currentTimeMillis()));
            // Also update the partition's cached version
            partition.setCachedVisibleVersion(version, System.currentTimeMillis());
            // Complete the future so waiting requests get the result
            myFuture.complete(version);
            if (LOG.isDebugEnabled()) {
                LOG.debug("point query version fetched from MS, partition={}, version={}",
                        partitionId, version);
            }
            return version;
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
}
