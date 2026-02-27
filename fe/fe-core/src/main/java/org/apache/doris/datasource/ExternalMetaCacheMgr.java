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

import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.datasource.doris.DorisExternalMetaCacheMgr;
import org.apache.doris.datasource.metacache.MetaCache;
import org.apache.doris.datasource.metacache.UnifiedMetaCacheMgr;
import org.apache.doris.fs.FileSystemCache;
import org.apache.doris.system.Backend;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

/**
 * Cache meta of external catalog
 * 1. Meta for hive meta store, mainly for partition.
 * 2. Table Schema cache.
 * 3. Row count cache.
 */
public class ExternalMetaCacheMgr {
    private static final Logger LOG = LogManager.getLogger(ExternalMetaCacheMgr.class);

    /**
     * Executors for loading caches
     * 1. rowCountRefreshExecutor
     * For row count cache.
     * Row count cache is an async loading cache, and we can ignore the result
     * if cache missing or thread pool is full.
     * So use a separate executor for this cache.
     * <p>
     * 2.  commonRefreshExecutor
     * For other caches. Other caches are sync loading cache.
     * But commonRefreshExecutor will be used for async refresh.
     * That is, if cache entry is missing, the cache value will be loaded in caller thread, sychronously.
     * if cache entry need refresh, it will be reloaded in commonRefreshExecutor.
     * <p>
     * 3. fileListingExecutor
     * File listing is a heavy operation, so use a separate executor for it.
     * For fileCache, the refresh operation will still use commonRefreshExecutor to trigger refresh.
     * And fileListingExecutor will be used to list file.
     */
    private ExecutorService rowCountRefreshExecutor;
    private ExecutorService commonRefreshExecutor;
    private ExecutorService fileListingExecutor;
    // This executor is used to schedule the getting split tasks
    private ExecutorService scheduleExecutor;

    // all catalogs could share the same fsCache.
    private FileSystemCache fsCache;
    // all external table row count cache.
    private ExternalRowCountCache rowCountCache;
    private final DorisExternalMetaCacheMgr dorisExternalMetaCacheMgr;
    private final UnifiedMetaCacheMgr unifiedMetaCacheMgr;

    public ExternalMetaCacheMgr(boolean isCheckpointCatalog) {
        rowCountRefreshExecutor = newThreadPool(isCheckpointCatalog,
                Config.max_external_cache_loader_thread_pool_size,
                Config.max_external_cache_loader_thread_pool_size * 1000,
                "RowCountRefreshExecutor", 0, true);

        commonRefreshExecutor = newThreadPool(isCheckpointCatalog,
                Config.max_external_cache_loader_thread_pool_size,
                Config.max_external_cache_loader_thread_pool_size * 10000,
                "CommonRefreshExecutor", 10, true);

        // The queue size should be large enough,
        // because there may be thousands of partitions being queried at the same time.
        fileListingExecutor = newThreadPool(isCheckpointCatalog,
                Config.max_external_cache_loader_thread_pool_size,
                Config.max_external_cache_loader_thread_pool_size * 1000,
                "FileListingExecutor", 10, true);

        scheduleExecutor = newThreadPool(isCheckpointCatalog,
                Config.max_external_cache_loader_thread_pool_size,
                Config.max_external_cache_loader_thread_pool_size * 1000,
                "scheduleExecutor", 10, true);

        fsCache = new FileSystemCache();
        rowCountCache = new ExternalRowCountCache(rowCountRefreshExecutor);

        unifiedMetaCacheMgr = new UnifiedMetaCacheMgr(commonRefreshExecutor, fileListingExecutor);
        dorisExternalMetaCacheMgr = new DorisExternalMetaCacheMgr(commonRefreshExecutor);
    }

    private ExecutorService newThreadPool(boolean isCheckpointCatalog, int numThread, int queueSize,
            String poolName, int timeoutSeconds,
            boolean needRegisterMetric) {
        String executorNamePrefix = isCheckpointCatalog ? "Checkpoint" : "NotCheckpoint";
        String realPoolName = executorNamePrefix + poolName;
        // Business threads require a fixed size thread pool and use queues to store unprocessed tasks.
        // Checkpoint threads have almost no business and need to be released in a timely manner to avoid thread leakage
        if (isCheckpointCatalog) {
            return ThreadPoolManager.newDaemonCacheThreadPool(numThread, realPoolName, needRegisterMetric);
        } else {
            return ThreadPoolManager.newDaemonFixedThreadPool(numThread, queueSize, realPoolName, timeoutSeconds,
                    needRegisterMetric);
        }
    }

    public ExecutorService getFileListingExecutor() {
        return fileListingExecutor;
    }

    public ExecutorService getScheduleExecutor() {
        return scheduleExecutor;
    }

    public UnifiedMetaCacheMgr getUnifiedMetaCacheMgr() {
        return unifiedMetaCacheMgr;
    }

    public Optional<SchemaCacheValue> getSchema(ExternalTable table, long versionToken) {
        return unifiedMetaCacheMgr.getSchema(table, versionToken);
    }

    public FileSystemCache getFsCache() {
        return fsCache;
    }

    public ExternalRowCountCache getRowCountCache() {
        return rowCountCache;
    }

    public ImmutableMap<Long, Backend> getDorisBackends(long catalogId) {
        return dorisExternalMetaCacheMgr.getBackends(catalogId);
    }

    public void invalidateDorisBackends(long catalogId) {
        dorisExternalMetaCacheMgr.invalidateBackendCache(catalogId);
    }

    public void removeCache(ExternalCatalog catalog) {
        Objects.requireNonNull(catalog, "catalog cannot be null");
        long catalogId = catalog.getId();
        unifiedMetaCacheMgr.removeCatalogMetaCache(catalogId);
        dorisExternalMetaCacheMgr.removeCache(catalogId);
    }

    public void invalidate(ExternalTable table) {
        Objects.requireNonNull(table, "table cannot be null");
        unifiedMetaCacheMgr.invalidateTableCache(table);
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalid table cache for {}.{} in catalog {}", table.getRemoteDbName(),
                    table.getRemoteName(), table.getCatalog().getName());
        }
    }

    public void invalidate(ExternalDatabase<? extends ExternalTable> db) {
        Objects.requireNonNull(db, "database cannot be null");
        invalidate((ExternalCatalog) db.getCatalog(), db.getFullName());
    }

    public void invalidate(ExternalCatalog catalog, String dbName) {
        Objects.requireNonNull(catalog, "catalog cannot be null");
        String normalizedDbName = org.apache.doris.cluster.ClusterNamespace.getNameFromFullName(dbName);
        long catalogId = catalog.getId();
        unifiedMetaCacheMgr.invalidateDbCache(catalogId, normalizedDbName);
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalid db cache for {} in catalog {}", normalizedDbName, catalogId);
        }
    }

    public void invalidate(ExternalCatalog catalog) {
        Objects.requireNonNull(catalog, "catalog cannot be null");
        long catalogId = catalog.getId();
        unifiedMetaCacheMgr.invalidateCatalogCache(catalogId);
        dorisExternalMetaCacheMgr.invalidateCatalogCache(catalogId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalid catalog cache for {}", catalogId);
        }
    }

    public <T> MetaCache<T> buildMetaCache(String name,
            OptionalLong expireAfterAccessSec, OptionalLong refreshAfterWriteSec, long maxSize,
            CacheLoader<String, List<Pair<String, String>>> namesCacheLoader,
            CacheLoader<String, Optional<T>> metaObjCacheLoader,
            RemovalListener<String, Optional<T>> removalListener) {
        MetaCache<T> metaCache = new MetaCache<>(
                name, commonRefreshExecutor, expireAfterAccessSec, refreshAfterWriteSec,
                maxSize, namesCacheLoader, metaObjCacheLoader, removalListener);
        return metaCache;
    }

    public static Map<String, String> getCacheStats(CacheStats cacheStats, long estimatedSize) {
        Map<String, String> stats = Maps.newHashMap();
        stats.put("hit_ratio", String.valueOf(cacheStats.hitRate()));
        stats.put("hit_count", String.valueOf(cacheStats.hitCount()));
        stats.put("read_count", String.valueOf(cacheStats.hitCount() + cacheStats.missCount()));
        stats.put("eviction_count", String.valueOf(cacheStats.evictionCount()));
        stats.put("average_load_penalty", String.valueOf(cacheStats.averageLoadPenalty()));
        stats.put("estimated_size", String.valueOf(estimatedSize));
        return stats;
    }
}
