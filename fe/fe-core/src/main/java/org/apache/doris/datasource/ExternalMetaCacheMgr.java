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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.datasource.doris.DorisExternalMetaCache;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;
import org.apache.doris.datasource.hudi.HudiExternalMetaCache;
import org.apache.doris.datasource.iceberg.IcebergExternalMetaCache;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalMetaCache;
import org.apache.doris.datasource.metacache.AbstractExternalMetaCache;
import org.apache.doris.datasource.metacache.ExternalMetaCache;
import org.apache.doris.datasource.metacache.ExternalMetaCacheRegistry;
import org.apache.doris.datasource.metacache.ExternalMetaCacheRouteResolver;
import org.apache.doris.datasource.metacache.LegacyMetaCacheFactory;
import org.apache.doris.datasource.metacache.MetaCacheEntryDef;
import org.apache.doris.datasource.metacache.MetaCacheEntryInvalidation;
import org.apache.doris.datasource.metacache.MetaCacheEntryStats;
import org.apache.doris.datasource.paimon.PaimonExternalMetaCache;
import org.apache.doris.fs.FileSystemCache;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Cache meta of external catalog
 * 1. Meta for hive meta store, mainly for partition.
 * 2. Table Schema cache.
 * 3. Row count cache.
 */
public class ExternalMetaCacheMgr {
    private static final Logger LOG = LogManager.getLogger(ExternalMetaCacheMgr.class);
    private static final String ENTRY_SCHEMA = "schema";
    private static final String ENGINE_DEFAULT = "default";
    private static final String ENGINE_HIVE = "hive";
    private static final String ENGINE_HUDI = "hudi";
    private static final String ENGINE_ICEBERG = "iceberg";
    private static final String ENGINE_PAIMON = "paimon";
    private static final String ENGINE_MAXCOMPUTE = "maxcompute";
    private static final String ENGINE_DORIS = "doris";

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
    private final ExternalMetaCacheRegistry cacheRegistry;
    private final ExternalMetaCacheRouteResolver routeResolver;
    private final LegacyMetaCacheFactory legacyMetaCacheFactory;

    // all catalogs could share the same fsCache.
    private FileSystemCache fsCache;
    // all external table row count cache.
    private ExternalRowCountCache rowCountCache;

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
        cacheRegistry = new ExternalMetaCacheRegistry();
        routeResolver = new ExternalMetaCacheRouteResolver(cacheRegistry);
        legacyMetaCacheFactory = new LegacyMetaCacheFactory(commonRefreshExecutor);

        initEngineCaches();
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

    ExternalMetaCache engine(String engine) {
        return cacheRegistry.resolve(engine);
    }

    public HiveExternalMetaCache hive(long catalogId) {
        prepareCatalogByEngine(catalogId, ENGINE_HIVE);
        return (HiveExternalMetaCache) engine(ENGINE_HIVE);
    }

    public HudiExternalMetaCache hudi(long catalogId) {
        prepareCatalogByEngine(catalogId, ENGINE_HUDI);
        return (HudiExternalMetaCache) engine(ENGINE_HUDI);
    }

    public IcebergExternalMetaCache iceberg(long catalogId) {
        prepareCatalogByEngine(catalogId, ENGINE_ICEBERG);
        return (IcebergExternalMetaCache) engine(ENGINE_ICEBERG);
    }

    public PaimonExternalMetaCache paimon(long catalogId) {
        prepareCatalogByEngine(catalogId, ENGINE_PAIMON);
        return (PaimonExternalMetaCache) engine(ENGINE_PAIMON);
    }

    public MaxComputeExternalMetaCache maxCompute(long catalogId) {
        prepareCatalogByEngine(catalogId, ENGINE_MAXCOMPUTE);
        return (MaxComputeExternalMetaCache) engine(ENGINE_MAXCOMPUTE);
    }

    public DorisExternalMetaCache doris(long catalogId) {
        prepareCatalogByEngine(catalogId, ENGINE_DORIS);
        return (DorisExternalMetaCache) engine(ENGINE_DORIS);
    }

    public void prepareCatalog(long catalogId) {
        Map<String, String> catalogProperties = findCatalogProperties(catalogId);
        if (catalogProperties == null) {
            logMissingCatalogSkip(catalogId, "prepareCatalog");
            return;
        }
        routeCatalogEngines(catalogId, cache -> cache.initCatalog(catalogId, catalogProperties));
    }

    public void prepareCatalogByEngine(long catalogId, String engine) {
        Map<String, String> catalogProperties = findCatalogProperties(catalogId);
        if (catalogProperties == null) {
            logMissingCatalogSkip(catalogId, "prepareCatalogByEngine");
            return;
        }
        prepareCatalogByEngine(catalogId, engine, catalogProperties);
    }

    public void prepareCatalogByEngine(long catalogId, String engine, Map<String, String> catalogProperties) {
        Map<String, String> safeCatalogProperties = catalogProperties == null
                ? Maps.newHashMap()
                : Maps.newHashMap(catalogProperties);
        routeSpecifiedEngine(engine, cache -> cache.initCatalog(catalogId, safeCatalogProperties));
    }

    public void invalidateCatalog(long catalogId) {
        routeCatalogEngines(catalogId, cache -> safeInvalidate(
                cache, catalogId, "invalidateCatalog",
                () -> cache.invalidateCatalogEntries(catalogId)));
    }

    public void invalidateCatalogByEngine(long catalogId, String engine) {
        routeSpecifiedEngine(engine, cache -> safeInvalidate(
                cache, catalogId, "invalidateCatalogByEngine",
                () -> cache.invalidateCatalogEntries(catalogId)));
    }

    public void removeCatalog(long catalogId) {
        routeCatalogEngines(catalogId, cache -> safeInvalidate(
                cache, catalogId, "removeCatalog",
                () -> cache.invalidateCatalog(catalogId)));
    }

    public void removeCatalogByEngine(long catalogId, String engine) {
        routeSpecifiedEngine(engine, cache -> safeInvalidate(
                cache, catalogId, "removeCatalogByEngine",
                () -> cache.invalidateCatalog(catalogId)));
    }

    public void invalidateDb(long catalogId, String dbName) {
        routeCatalogEngines(catalogId, cache -> safeInvalidate(
                cache, catalogId, "invalidateDb", () -> cache.invalidateDb(catalogId, dbName)));
    }

    public void invalidateTable(long catalogId, String dbName, String tableName) {
        routeCatalogEngines(catalogId, cache -> safeInvalidate(
                cache, catalogId, "invalidateTable",
                () -> cache.invalidateTable(catalogId, dbName, tableName)));
    }

    public void invalidateTableByEngine(long catalogId, String engine, String dbName, String tableName) {
        routeSpecifiedEngine(engine, cache -> safeInvalidate(
                cache, catalogId, "invalidateTableByEngine",
                () -> cache.invalidateTable(catalogId, dbName, tableName)));
    }

    public void invalidatePartitions(long catalogId,
            String dbName, String tableName, List<String> partitions) {
        routeCatalogEngines(catalogId, cache -> safeInvalidate(
                cache, catalogId, "invalidatePartitions",
                () -> cache.invalidatePartitions(catalogId, dbName, tableName, partitions)));
    }

    public List<CatalogMetaCacheStats> getCatalogCacheStats(long catalogId) {
        List<CatalogMetaCacheStats> stats = new ArrayList<>();
        cacheRegistry.allCaches().forEach(externalMetaCache -> externalMetaCache.stats(catalogId)
                .forEach((entryName, entryStats) -> stats.add(
                        new CatalogMetaCacheStats(externalMetaCache.engine(), entryName, entryStats))));
        stats.sort(Comparator.comparing(CatalogMetaCacheStats::getEngineName)
                .thenComparing(CatalogMetaCacheStats::getEntryName));
        return stats;
    }

    public static final class CatalogMetaCacheStats {
        private final String engineName;
        private final String entryName;
        private final MetaCacheEntryStats entryStats;

        public CatalogMetaCacheStats(String engineName, String entryName, MetaCacheEntryStats entryStats) {
            this.engineName = Objects.requireNonNull(engineName, "engineName");
            this.entryName = Objects.requireNonNull(entryName, "entryName");
            this.entryStats = Objects.requireNonNull(entryStats, "entryStats");
        }

        public String getEngineName() {
            return engineName;
        }

        public String getEntryName() {
            return entryName;
        }

        public MetaCacheEntryStats getEntryStats() {
            return entryStats;
        }
    }

    private void initEngineCaches() {
        registerBuiltinEngineCaches();
    }

    private void registerBuiltinEngineCaches() {
        cacheRegistry.register(new DefaultExternalMetaCache(ENGINE_DEFAULT, commonRefreshExecutor));
        cacheRegistry.register(new HiveExternalMetaCache(commonRefreshExecutor, fileListingExecutor));
        cacheRegistry.register(new HudiExternalMetaCache(commonRefreshExecutor));
        cacheRegistry.register(new IcebergExternalMetaCache(commonRefreshExecutor));
        cacheRegistry.register(new PaimonExternalMetaCache(commonRefreshExecutor));
        cacheRegistry.register(new MaxComputeExternalMetaCache(commonRefreshExecutor));
        cacheRegistry.register(new DorisExternalMetaCache(commonRefreshExecutor));
    }

    private void routeCatalogEngines(long catalogId, Consumer<ExternalMetaCache> action) {
        routeResolver.resolveCatalogCaches(catalogId, getCatalog(catalogId)).forEach(action);
    }

    private void routeSpecifiedEngine(String engine, Consumer<ExternalMetaCache> action) {
        action.accept(this.engine(engine));
    }

    List<String> resolveCatalogEngineNamesForTest(@Nullable CatalogIf<?> catalog, long catalogId) {
        List<String> resolved = new ArrayList<>();
        routeResolver.resolveCatalogCaches(catalogId, catalog).forEach(cache -> resolved.add(cache.engine()));
        return new ArrayList<>(resolved);
    }

    private void safeInvalidate(ExternalMetaCache cache, long catalogId, String operation, Runnable action) {
        if (!cache.isCatalogInitialized(catalogId)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("skip {} for catalog {} on engine '{}' because cache entry is absent",
                        operation, catalogId, cache.engine());
            }
            return;
        }
        action.run();
    }

    @Nullable
    private Map<String, String> findCatalogProperties(long catalogId) {
        CatalogIf<?> catalog = getCatalog(catalogId);
        if (catalog == null) {
            return null;
        }
        if (catalog.getProperties() == null) {
            return Maps.newHashMap();
        }
        return Maps.newHashMap(catalog.getProperties());
    }

    private void logMissingCatalogSkip(long catalogId, String operation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("skip {} for catalog {} because catalog does not exist", operation, catalogId);
        }
    }

    @Nullable
    private CatalogIf<?> getCatalog(long catalogId) {
        if (Env.getCurrentEnv() == null || Env.getCurrentEnv().getCatalogMgr() == null) {
            return null;
        }
        return Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
    }

    @SuppressWarnings("unchecked")
    public Optional<SchemaCacheValue> getSchemaCacheValue(ExternalTable table, SchemaCacheKey key) {
        long catalogId = table.getCatalog().getId();
        String resolvedEngine = table.getMetaCacheEngine();
        prepareCatalogByEngine(catalogId, resolvedEngine);
        try {
            return ((ExternalMetaCache) engine(resolvedEngine)).getSchemaValue(catalogId, key);
        } catch (IllegalStateException e) {
            if (getCatalog(catalogId) != null) {
                throw e;
            }
            logMissingCatalogSkip(catalogId, "getSchemaCacheValue");
            return Optional.empty();
        }
    }

    public FileSystemCache getFsCache() {
        return fsCache;
    }

    public ExternalRowCountCache getRowCountCache() {
        return rowCountCache;
    }

    public void invalidateTableCache(ExternalTable dorisTable) {
        invalidateTable(dorisTable.getCatalog().getId(),
                dorisTable.getDbName(),
                dorisTable.getName());
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalid table cache for {}.{} in catalog {}", dorisTable.getRemoteDbName(),
                    dorisTable.getRemoteName(), dorisTable.getCatalog().getName());
        }
    }

    public LegacyMetaCacheFactory legacyMetaCacheFactory() {
        return legacyMetaCacheFactory;
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

    void replaceEngineCachesForTest(List<? extends ExternalMetaCache> caches) {
        cacheRegistry.resetForTest(caches);
    }

    /**
     * Fallback implementation of {@link AbstractExternalMetaCache} for engines that do not
     * provide dedicated cache entries.
     *
     * <p>Registered entries:
     * <ul>
     *   <li>{@code schema}: schema-only cache keyed by {@link SchemaCacheKey}</li>
     * </ul>
     *
     * <p>This class keeps compatibility for generic external engines and routes only schema
     * loading/invalidation. No engine-specific metadata (partitions/files/snapshots) is cached.
     */
    private static class DefaultExternalMetaCache extends AbstractExternalMetaCache {
        DefaultExternalMetaCache(String engine, ExecutorService refreshExecutor) {
            super(engine, refreshExecutor);
            registerEntry(MetaCacheEntryDef.of(
                    ENTRY_SCHEMA,
                    SchemaCacheKey.class,
                    SchemaCacheValue.class,
                    this::loadSchemaCacheValue,
                    defaultSchemaCacheSpec(),
                    MetaCacheEntryInvalidation.forTableIdentity(
                            key -> key.getNameMapping().getLocalDbName(),
                            key -> key.getNameMapping().getLocalTblName())));
        }

        @Override
        protected Map<String, String> catalogPropertyCompatibilityMap() {
            return singleCompatibilityMap(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, ENTRY_SCHEMA);
        }

        private SchemaCacheValue loadSchemaCacheValue(SchemaCacheKey key) {
            CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(key.getNameMapping().getCtlId());
            if (!(catalog instanceof ExternalCatalog)) {
                throw new CacheException("catalog %s is not external when loading schema cache",
                        null, key.getNameMapping().getCtlId());
            }
            ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
            return externalCatalog.getSchema(key).orElseThrow(() -> new CacheException(
                    "failed to load schema cache value for: %s.%s.%s",
                    null, key.getNameMapping().getCtlId(),
                    key.getNameMapping().getLocalDbName(),
                    key.getNameMapping().getLocalTblName()));
        }
    }
}
