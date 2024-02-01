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
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.fs.FileSystemCache;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.planner.external.hudi.HudiPartitionMgr;
import org.apache.doris.planner.external.hudi.HudiPartitionProcessor;
import org.apache.doris.planner.external.iceberg.IcebergMetadataCache;
import org.apache.doris.planner.external.iceberg.IcebergMetadataCacheMgr;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Cache meta of external catalog
 * 1. Meta for hive meta store, mainly for partition.
 * 2. Table Schema cahce.
 */
public class ExternalMetaCacheMgr {
    private static final Logger LOG = LogManager.getLogger(ExternalMetaCacheMgr.class);

    // catalog id -> HiveMetaStoreCache
    private final Map<Long, HiveMetaStoreCache> cacheMap = Maps.newConcurrentMap();
    // catalog id -> table schema cache
    private Map<Long, ExternalSchemaCache> schemaCacheMap = Maps.newHashMap();
    // hudi partition manager
    private final HudiPartitionMgr hudiPartitionMgr;
    private ExecutorService executor;
    // all catalogs could share the same fsCache.
    private FileSystemCache fsCache;
    private final IcebergMetadataCacheMgr icebergMetadataCacheMgr;
    private final MaxComputeMetadataCacheMgr maxComputeMetadataCacheMgr;

    public ExternalMetaCacheMgr() {
        executor = ThreadPoolManager.newDaemonFixedThreadPool(
                Config.max_external_cache_loader_thread_pool_size,
                Config.max_external_cache_loader_thread_pool_size * 1000,
                "ExternalMetaCacheMgr", 120, true);
        hudiPartitionMgr = HudiPartitionMgr.get(executor);
        fsCache = new FileSystemCache(executor);
        icebergMetadataCacheMgr = new IcebergMetadataCacheMgr();
        maxComputeMetadataCacheMgr = new MaxComputeMetadataCacheMgr();
    }

    public HiveMetaStoreCache getMetaStoreCache(HMSExternalCatalog catalog) {
        HiveMetaStoreCache cache = cacheMap.get(catalog.getId());
        if (cache == null) {
            synchronized (cacheMap) {
                if (!cacheMap.containsKey(catalog.getId())) {
                    cacheMap.put(catalog.getId(), new HiveMetaStoreCache(catalog, executor));
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
                    schemaCacheMap.put(catalog.getId(), new ExternalSchemaCache(catalog));
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
        LOG.debug("invalid table cache for {}.{} in catalog {}", dbName, tblName, catalogId);
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
        LOG.debug("invalid db cache for {} in catalog {}", dbName, catalogId);
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
        LOG.debug("invalid catalog cache for {}", catalogId);
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
        LOG.debug("add partition cache for {}.{} in catalog {}", dbName, table.getName(), catalogId);
    }

    public void dropPartitionsCache(long catalogId, HMSExternalTable table, List<String> partitionNames) {
        String dbName = ClusterNamespace.getNameFromFullName(table.getDbName());
        HiveMetaStoreCache metaCache = cacheMap.get(catalogId);
        if (metaCache != null) {
            metaCache.dropPartitionsCache(dbName, table.getName(), partitionNames, true);
        }
        LOG.debug("drop partition cache for {}.{} in catalog {}", dbName, table.getName(), catalogId);
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
        LOG.debug("invalidate partition cache for {}.{} in catalog {}", dbName, tableName, catalogId);
    }
}
