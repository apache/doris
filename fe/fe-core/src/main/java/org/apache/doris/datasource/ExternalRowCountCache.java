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

package org.apache.doris.datasource;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.BasicAsyncCacheLoader;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class ExternalRowCountCache {

    private static final Logger LOG = LogManager.getLogger(ExternalRowCountCache.class);
    private final AsyncLoadingCache<RowCountKey, Optional<Long>> rowCountCache;
    private final ConcurrentHashMap<LoadKey, Set<LoadFence>> inFlightLoads = new ConcurrentHashMap<>();
    // Serialize future publication with explicit invalidation. Invalidation marks matching in-flight loads
    // before removing their cache entries, so a refresh that finishes later cannot republish stale data.
    private final ReentrantReadWriteLock publicationLock = new ReentrantReadWriteLock();

    public ExternalRowCountCache(ExecutorService executor) {
        this(executor, null);
    }

    ExternalRowCountCache(ExecutorService executor, Ticker ticker) {
        this(executor, ticker, new RowCountCacheLoader());
    }

    ExternalRowCountCache(ExecutorService executor, Ticker ticker, RowCountCacheLoader loader) {
        // 1. set expireAfterWrite to 1 day, avoid too many entries
        // 2. set refreshAfterWrite to 10min(default), so that the cache will be refreshed after 10min
        CacheFactory rowCountCacheFactory = new CacheFactory(
                OptionalLong.of(Config.external_cache_expire_time_seconds_after_access),
                OptionalLong.of(Config.external_cache_refresh_time_minutes * 60),
                Config.max_external_table_row_count_cache_num,
                false,
                ticker);
        rowCountCache = rowCountCacheFactory.buildAsyncCache(
                new InvalidationAwareLoader(loader), executor);
    }

    @Getter
    public static class RowCountKey {
        private final long catalogId;
        private final long dbId;
        private final long tableId;

        public RowCountKey(long catalogId, long dbId, long tableId) {
            this.catalogId = catalogId;
            this.dbId = dbId;
            this.tableId = tableId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof RowCountKey)) {
                return false;
            }
            return ((RowCountKey) obj).tableId == this.tableId;
        }

        @Override
        public int hashCode() {
            return (int) tableId;
        }
    }

    public static class RowCountCacheLoader extends BasicAsyncCacheLoader<RowCountKey, Optional<Long>> {
        @Override
        protected Optional<Long> doLoad(RowCountKey rowCountKey) {
            return loadRowCount(rowCountKey, false);
        }
    }

    private final class InvalidationAwareLoader implements AsyncCacheLoader<RowCountKey, Optional<Long>> {
        private final RowCountCacheLoader delegate;

        private InvalidationAwareLoader(RowCountCacheLoader delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<Optional<Long>> asyncLoad(RowCountKey key, Executor executor) {
            return loadWithInvalidationFence(key, executor, () -> delegate.doLoad(key));
        }
    }

    private static final class LoadFence {
        private boolean invalidated;
    }

    // RowCountKey intentionally uses tableId as the Caffeine cache identity. In-flight loads need
    // the complete scope so catalog/database invalidation can fence a same-tableId replacement.
    private static final class LoadKey {
        private final long catalogId;
        private final long dbId;
        private final long tableId;

        private LoadKey(RowCountKey key) {
            catalogId = key.catalogId;
            dbId = key.dbId;
            tableId = key.tableId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof LoadKey)) {
                return false;
            }
            LoadKey other = (LoadKey) obj;
            return catalogId == other.catalogId && dbId == other.dbId && tableId == other.tableId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogId, dbId, tableId);
        }
    }

    private CompletableFuture<Optional<Long>> loadWithInvalidationFence(
            RowCountKey key, Executor executor, Supplier<Optional<Long>> loader) {
        LoadFence fence = new LoadFence();
        LoadKey loadKey = new LoadKey(key);
        publicationLock.readLock().lock();
        try {
            inFlightLoads.compute(loadKey, (ignored, fences) -> {
                Set<LoadFence> currentFences = fences == null ? ConcurrentHashMap.newKeySet() : fences;
                currentFences.add(fence);
                return currentFences;
            });
        } finally {
            publicationLock.readLock().unlock();
        }

        CompletableFuture<Optional<Long>> publishedFuture = new CompletableFuture<>();
        CompletableFuture<Optional<Long>> loadFuture;
        try {
            loadFuture = CompletableFuture.supplyAsync(loader, executor);
        } catch (RuntimeException e) {
            publicationLock.readLock().lock();
            try {
                removeInFlightLoad(loadKey, fence);
            } finally {
                publicationLock.readLock().unlock();
            }
            throw e;
        }
        loadFuture.whenComplete((value, throwable) -> {
            publicationLock.readLock().lock();
            try {
                if (throwable != null) {
                    publishedFuture.completeExceptionally(throwable);
                } else if (fence.invalidated) {
                    publishedFuture.complete(null);
                } else {
                    publishedFuture.complete(value);
                }
            } finally {
                removeInFlightLoad(loadKey, fence);
                publicationLock.readLock().unlock();
            }
        });
        return publishedFuture;
    }

    private void removeInFlightLoad(LoadKey key, LoadFence fence) {
        inFlightLoads.computeIfPresent(key, (ignored, fences) -> {
            fences.remove(fence);
            return fences.isEmpty() ? null : fences;
        });
    }

    int getInFlightLoadCountForTest() {
        publicationLock.readLock().lock();
        try {
            return inFlightLoads.values().stream().mapToInt(Set::size).sum();
        } finally {
            publicationLock.readLock().unlock();
        }
    }

    void refreshForTest(long catalogId, long dbId, long tableId) {
        rowCountCache.synchronous().refresh(new RowCountKey(catalogId, dbId, tableId));
    }

    static Optional<Long> loadRowCount(RowCountKey rowCountKey, boolean fillMetaCache) {
        try {
            ExternalTable table = (ExternalTable) StatisticsUtil.findTable(
                    rowCountKey.catalogId, rowCountKey.dbId, rowCountKey.tableId);
            return Optional.of(table.fetchRowCountWithMetaCache(fillMetaCache));
        } catch (Exception e) {
            String message = String.format("Failed to get table row count with catalogId %s, dbId %s, tableId %s. "
                            + "Reason %s",
                    rowCountKey.catalogId, rowCountKey.dbId, rowCountKey.tableId, e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.warn(message, e);
            } else {
                LOG.warn(message);
            }

            // Return Optional.empty() will cache this empty value in memory,
            // so we can't try to load the row count until the cache expire.
            // Throw an exception here will cause too much stack log in fe.out.
            // So we return null when exception happen.
            // Null may raise NPE in caller, but that is expected.
            // We catch that NPE and return a default value -1 without keep the value in cache,
            // so we can trigger the load function to fetch row count again next time in this exception case.
            return null;
        }
    }

    /**
     * Get cached row count for the given table. Return -1 if cached not loaded or table not exists.
     * Cached will be loaded async.
     * @param fillMetaCache whether loading the row count may fill external metadata caches
     * @return Cached row count or -1 if not exist
     */
    public long getCachedRowCount(long catalogId, long dbId, long tableId, boolean fillMetaCache) {
        RowCountKey key = new RowCountKey(catalogId, dbId, tableId);
        try {
            CompletableFuture<Optional<Long>> f;
            publicationLock.readLock().lock();
            try {
                f = fillMetaCache
                        ? rowCountCache.get(key, (rowCountKey, executor) -> loadWithInvalidationFence(
                                rowCountKey, executor, () -> loadRowCount(rowCountKey, true)))
                        : rowCountCache.get(key);
            } finally {
                publicationLock.readLock().unlock();
            }
            // Get row count synchronously by default.
            if (ConnectContext.get() == null
                    || ConnectContext.get().getSessionVariable().fetchHiveRowCountSync) {
                return f.get().orElse(TableIf.UNKNOWN_ROW_COUNT);
            } else {
                if (f.isDone()) {
                    return f.get().orElse(TableIf.UNKNOWN_ROW_COUNT);
                }
                LOG.info("Row count for table {}.{}.{} is still processing.", catalogId, dbId, tableId);
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning row count", e);
        }
        return TableIf.UNKNOWN_ROW_COUNT;
    }

    /**
     * Get cached row count for the given table if present. Return -1 if cached not loaded.
     * This method will not trigger async loading if cache is missing.
     * @return Cached row count or -1 if not exist
     */
    public long getCachedRowCountIfPresent(long catalogId, long dbId, long tableId) {
        RowCountKey key = new RowCountKey(catalogId, dbId, tableId);
        try {
            CompletableFuture<Optional<Long>> f;
            publicationLock.readLock().lock();
            try {
                f = rowCountCache.getIfPresent(key);
            } finally {
                publicationLock.readLock().unlock();
            }
            if (f == null) {
                return -1;
            } else if (f.isDone()) {
                return f.get().orElse(-1L);
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning row count if present", e);
        }
        return -1;
    }

    // Catalog/db invalidation is O(N): row-count keys are numeric ids, and Caffeine
    // does not support prefix invalidation by catalog or database id.
    void invalidateCatalog(long catalogId) {
        publicationLock.writeLock().lock();
        try {
            inFlightLoads.forEach((key, fences) -> {
                if (key.catalogId == catalogId) {
                    fences.forEach(fence -> fence.invalidated = true);
                }
            });
            rowCountCache.asMap().keySet().removeIf(key -> key.catalogId == catalogId);
        } finally {
            publicationLock.writeLock().unlock();
        }
    }

    void invalidateDb(long catalogId, long dbId) {
        publicationLock.writeLock().lock();
        try {
            inFlightLoads.forEach((key, fences) -> {
                if (key.catalogId == catalogId && key.dbId == dbId) {
                    fences.forEach(fence -> fence.invalidated = true);
                }
            });
            rowCountCache.asMap().keySet().removeIf(key -> key.catalogId == catalogId && key.dbId == dbId);
        } finally {
            publicationLock.writeLock().unlock();
        }
    }

    void invalidateTable(long catalogId, long dbId, long tableId) {
        publicationLock.writeLock().lock();
        try {
            RowCountKey key = new RowCountKey(catalogId, dbId, tableId);
            Set<LoadFence> fences = inFlightLoads.get(new LoadKey(key));
            if (fences != null) {
                fences.forEach(fence -> fence.invalidated = true);
            }
            rowCountCache.synchronous().invalidate(key);
        } finally {
            publicationLock.writeLock().unlock();
        }
    }

}
