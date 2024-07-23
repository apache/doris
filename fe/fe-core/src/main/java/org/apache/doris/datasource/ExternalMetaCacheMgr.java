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

import org.apache.doris.catalog.Type;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hudi.source.HudiPartitionMgr;
import org.apache.doris.datasource.hudi.source.HudiPartitionProcessor;
import org.apache.doris.datasource.iceberg.IcebergMetadataCache;
import org.apache.doris.datasource.iceberg.IcebergMetadataCacheMgr;
import org.apache.doris.datasource.maxcompute.MaxComputeMetadataCache;
import org.apache.doris.datasource.maxcompute.MaxComputeMetadataCacheMgr;
import org.apache.doris.datasource.metacache.MetaCache;
import org.apache.doris.fs.FileSystemCache;
import org.apache.doris.nereids.exceptions.NotSupportedException;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
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
    private ExecutorService scheduleExecutor;

    // catalog id -> HiveMetaStoreCache
    private final Map<Long, HiveMetaStoreCache> cacheMap = Maps.newConcurrentMap();
    // catalog id -> table schema cache
    private Map<Long, ExternalSchemaCache> schemaCacheMap = Maps.newHashMap();
    // hudi partition manager
    private final HudiPartitionMgr hudiPartitionMgr;
    // all catalogs could share the same fsCache.
    private FileSystemCache fsCache;
    // all external table row count cache.
    private ExternalRowCountCache rowCountCache;
    private final IcebergMetadataCacheMgr icebergMetadataCacheMgr;
    private final MaxComputeMetadataCacheMgr maxComputeMetadataCacheMgr;

    public ExternalMetaCacheMgr() {
        rowCountRefreshExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                Config.max_external_cache_loader_thread_pool_size,
                Config.max_external_cache_loader_thread_pool_size * 1000,
                "RowCountRefreshExecutor", 0, true);

        commonRefreshExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                Config.max_external_cache_loader_thread_pool_size,
                Config.max_external_cache_loader_thread_pool_size * 1000,
                "CommonRefreshExecutor", 10, true);

        // The queue size should be large enough,
        // because there may be thousands of partitions being queried at the same time.
        fileListingExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                Config.max_external_cache_loader_thread_pool_size,
                Config.max_external_cache_loader_thread_pool_size * 1000,
                "FileListingExecutor", 10, true);

        scheduleExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                Config.max_external_cache_loader_thread_pool_size,
                Config.max_external_cache_loader_thread_pool_size * 1000,
                "scheduleExecutor", 10, true);

        fsCache = new FileSystemCache();
        rowCountCache = new ExternalRowCountCache(rowCountRefreshExecutor);

        hudiPartitionMgr = new HudiPartitionMgr(commonRefreshExecutor);
        icebergMetadataCacheMgr = new IcebergMetadataCacheMgr(commonRefreshExecutor);
        maxComputeMetadataCacheMgr = new MaxComputeMetadataCacheMgr();
    }

    public ExecutorService getFileListingExecutor() {
        return fileListingExecutor;
    }

    public ExecutorService getScheduleExecutor() {
        return scheduleExecutor;
    }

    public HiveMetaStoreCache getMetaStoreCache(HMSExternalCatalog catalog) {
        HiveMetaStoreCache cache = cacheMap.get(catalog.getId());
        if (cache == null) {
            synchronized (cacheMap) {
                if (!cacheMap.containsKey(catalog.getId())) {
                    cacheMap.put(catalog.getId(),
                            new HiveMetaStoreCache(catalog, commonRefreshExecutor, fileListingExecutor));
                }
                cache = cacheMap.get(catalog.getId());
            }
        }
        return cache;
    }

    public ExternalSchemaCache getSchemaCache(ExternalCatalog catalog) {
        ExternalSchemaCache cache = schemaCacheMap.get(catalog.getId());
        if (cache == null) {
            synchronized (schemaCacheMap) {
                if (!schemaCacheMap.containsKey(catalog.getId())) {
                    schemaCacheMap.put(catalog.getId(), new ExternalSchemaCache(catalog, commonRefreshExecutor));
                }
                cache = schemaCacheMap.get(catalog.getId());
            }
        }
        return cache;
    }

    public HudiPartitionProcessor getHudiPartitionProcess(ExternalCatalog catalog) {
        return hudiPartitionMgr.getPartitionProcessor(catalog);
    }

    public IcebergMetadataCache getIcebergMetadataCache() {
        return icebergMetadataCacheMgr.getIcebergMetadataCache();
    }

    public MaxComputeMetadataCache getMaxComputeMetadataCache(long catalogId) {
        return maxComputeMetadataCacheMgr.getMaxComputeMetadataCache(catalogId);
    }

    public FileSystemCache getFsCache() {
        return fsCache;
    }

    public ExternalRowCountCache getRowCountCache() {
        return rowCountCache;
    }

    public void removeCache(long catalogId) {
        if (cacheMap.remove(catalogId) != null) {
            LOG.info("remove hive metastore cache for catalog {}", catalogId);
        }
        if (schemaCacheMap.remove(catalogId) != null) {
            LOG.info("remove schema cache for catalog {}", catalogId);
        }
        hudiPartitionMgr.removePartitionProcessor(catalogId);
        icebergMetadataCacheMgr.removeCache(catalogId);
        maxComputeMetadataCacheMgr.removeCache(catalogId);
    }

    public void invalidateTableCache(long catalogId, String dbName, String tblName) {
        dbName = ClusterNamespace.getNameFromFullName(dbName);
        ExternalSchemaCache schemaCache = schemaCacheMap.get(catalogId);
        if (schemaCache != null) {
            schemaCache.invalidateTableCache(dbName, tblName);
        }
        HiveMetaStoreCache metaCache = cacheMap.get(catalogId);
        if (metaCache != null) {
            metaCache.invalidateTableCache(dbName, tblName);
        }
        hudiPartitionMgr.cleanTablePartitions(catalogId, dbName, tblName);
        icebergMetadataCacheMgr.invalidateTableCache(catalogId, dbName, tblName);
        maxComputeMetadataCacheMgr.invalidateTableCache(catalogId, dbName, tblName);
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalid table cache for {}.{} in catalog {}", dbName, tblName, catalogId);
        }
    }

    public void invalidateDbCache(long catalogId, String dbName) {
        dbName = ClusterNamespace.getNameFromFullName(dbName);
        ExternalSchemaCache schemaCache = schemaCacheMap.get(catalogId);
        if (schemaCache != null) {
            schemaCache.invalidateDbCache(dbName);
        }
        HiveMetaStoreCache metaCache = cacheMap.get(catalogId);
        if (metaCache != null) {
            metaCache.invalidateDbCache(dbName);
        }
        hudiPartitionMgr.cleanDatabasePartitions(catalogId, dbName);
        icebergMetadataCacheMgr.invalidateDbCache(catalogId, dbName);
        maxComputeMetadataCacheMgr.invalidateDbCache(catalogId, dbName);
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalid db cache for {} in catalog {}", dbName, catalogId);
        }
    }

    public void invalidateCatalogCache(long catalogId) {
        ExternalSchemaCache schemaCache = schemaCacheMap.get(catalogId);
        if (schemaCache != null) {
            schemaCache.invalidateAll();
        }
        HiveMetaStoreCache metaCache = cacheMap.get(catalogId);
        if (metaCache != null) {
            metaCache.invalidateAll();
        }
        hudiPartitionMgr.cleanPartitionProcess(catalogId);
        icebergMetadataCacheMgr.invalidateCatalogCache(catalogId);
        maxComputeMetadataCacheMgr.invalidateCatalogCache(catalogId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalid catalog cache for {}", catalogId);
        }
    }

    public void addPartitionsCache(long catalogId, HMSExternalTable table, List<String> partitionNames) {
        String dbName = ClusterNamespace.getNameFromFullName(table.getDbName());
        HiveMetaStoreCache metaCache = cacheMap.get(catalogId);
        if (metaCache != null) {
            List<Type> partitionColumnTypes;
            try {
                partitionColumnTypes = table.getPartitionColumnTypes();
            } catch (NotSupportedException e) {
                LOG.warn("Ignore not supported hms table, message: {} ", e.getMessage());
                return;
            }
            metaCache.addPartitionsCache(dbName, table.getName(), partitionNames, partitionColumnTypes);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("add partition cache for {}.{} in catalog {}", dbName, table.getName(), catalogId);
        }
    }

    public void dropPartitionsCache(long catalogId, HMSExternalTable table, List<String> partitionNames) {
        String dbName = ClusterNamespace.getNameFromFullName(table.getDbName());
        HiveMetaStoreCache metaCache = cacheMap.get(catalogId);
        if (metaCache != null) {
            metaCache.dropPartitionsCache(dbName, table.getName(), partitionNames, true);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("drop partition cache for {}.{} in catalog {}", dbName, table.getName(), catalogId);
        }
    }

    public void invalidatePartitionsCache(long catalogId, String dbName, String tableName,
            List<String> partitionNames) {
        HiveMetaStoreCache metaCache = cacheMap.get(catalogId);
        if (metaCache != null) {
            dbName = ClusterNamespace.getNameFromFullName(dbName);
            for (String partitionName : partitionNames) {
                metaCache.invalidatePartitionCache(dbName, tableName, partitionName);
            }

        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalidate partition cache for {}.{} in catalog {}", dbName, tableName, catalogId);
        }
    }

    public <T> MetaCache<T> buildMetaCache(String name,
            OptionalLong expireAfterWriteSec, OptionalLong refreshAfterWriteSec, long maxSize,
            CacheLoader<String, List<String>> namesCacheLoader,
            CacheLoader<String, Optional<T>> metaObjCacheLoader,
            RemovalListener<String, Optional<T>> removalListener) {
        MetaCache<T> metaCache = new MetaCache<>(name, commonRefreshExecutor, expireAfterWriteSec, refreshAfterWriteSec,
                maxSize, namesCacheLoader, metaObjCacheLoader, removalListener);
        return metaCache;
    }
}
