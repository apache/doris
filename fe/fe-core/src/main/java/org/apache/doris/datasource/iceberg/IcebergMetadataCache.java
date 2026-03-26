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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.iceberg.cache.IcebergManifestCache;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.mtmv.MTMVRelatedTableIf;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.view.View;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

public class IcebergMetadataCache {
    private static final Logger LOG = LogManager.getLogger(IcebergMetadataCache.class);
    private final ExecutorService executor;
    private final ExternalCatalog catalog;
    private LoadingCache<IcebergMetadataCacheKey, IcebergTableCacheValue> tableCache;
    private LoadingCache<IcebergMetadataCacheKey, View> viewCache;
    private IcebergManifestCache manifestCache;

    public IcebergMetadataCache(ExternalCatalog catalog, ExecutorService executor) {
        this.executor = executor;
        this.catalog = catalog;
        init();
    }

    public void init() {
        CacheSpec tableCacheSpec = resolveTableCacheSpec();
        CacheFactory tableCacheFactory = new CacheFactory(
                CacheSpec.toExpireAfterAccess(tableCacheSpec.getTtlSecond()),
                OptionalLong.empty(),
                tableCacheSpec.getCapacity(),
                true,
                null);
        this.tableCache = tableCacheFactory.buildCache(this::loadTableCacheValue, executor);
        this.viewCache = tableCacheFactory.buildCache(this::loadView, executor);

        CacheSpec manifestCacheSpec = resolveManifestCacheSpec();
        this.manifestCache = new IcebergManifestCache(manifestCacheSpec.getCapacity(),
                manifestCacheSpec.getTtlSecond());
    }

    private CacheSpec resolveTableCacheSpec() {
        return CacheSpec.fromProperties(catalog.getProperties(),
                IcebergExternalCatalog.ICEBERG_TABLE_CACHE_ENABLE, true,
                IcebergExternalCatalog.ICEBERG_TABLE_CACHE_TTL_SECOND,
                Config.external_cache_expire_time_seconds_after_access,
                IcebergExternalCatalog.ICEBERG_TABLE_CACHE_CAPACITY,
                Config.max_external_table_cache_num);
    }

    private CacheSpec resolveManifestCacheSpec() {
        return CacheSpec.fromProperties(catalog.getProperties(),
                IcebergExternalCatalog.ICEBERG_MANIFEST_CACHE_ENABLE,
                IcebergExternalCatalog.DEFAULT_ICEBERG_MANIFEST_CACHE_ENABLE,
                IcebergExternalCatalog.ICEBERG_MANIFEST_CACHE_TTL_SECOND,
                IcebergExternalCatalog.DEFAULT_ICEBERG_MANIFEST_CACHE_TTL_SECOND,
                IcebergExternalCatalog.ICEBERG_MANIFEST_CACHE_CAPACITY,
                IcebergExternalCatalog.DEFAULT_ICEBERG_MANIFEST_CACHE_CAPACITY);
    }

    public Table getIcebergTable(ExternalTable dorisTable) {
        IcebergMetadataCacheKey key = new IcebergMetadataCacheKey(dorisTable.getOrBuildNameMapping());
        return tableCache.get(key).getIcebergTable();
    }

    public Table getIcebergTable(IcebergMetadataCacheKey key) {
        return tableCache.get(key).getIcebergTable();
    }

    public IcebergSnapshotCacheValue getSnapshotCache(ExternalTable dorisTable) {
        IcebergMetadataCacheKey key = new IcebergMetadataCacheKey(dorisTable.getOrBuildNameMapping());
        IcebergTableCacheValue tableCacheValue = tableCache.get(key);
        return tableCacheValue.getSnapshotCacheValue(() -> loadSnapshot(dorisTable, tableCacheValue.getIcebergTable()));
    }

    public List<Snapshot> getSnapshotList(ExternalTable dorisTable) {
        Table icebergTable = getIcebergTable(dorisTable);
        List<Snapshot> snaps = Lists.newArrayList();
        Iterables.addAll(snaps, icebergTable.snapshots());
        return snaps;
    }

    public IcebergManifestCache getManifestCache() {
        return manifestCache;
    }

    @NotNull
    private IcebergTableCacheValue loadTableCacheValue(IcebergMetadataCacheKey key) {
        NameMapping nameMapping = key.nameMapping;
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(nameMapping.getCtlId());
        if (catalog == null) {
            throw new RuntimeException(String.format("Cannot find catalog %d when loading table %s/%s.",
                    nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName()));
        }
        IcebergMetadataOps ops;
        if (catalog instanceof HMSExternalCatalog) {
            ops = ((HMSExternalCatalog) catalog).getIcebergMetadataOps();
        } else if (catalog instanceof IcebergExternalCatalog) {
            ops = (IcebergMetadataOps) (((IcebergExternalCatalog) catalog).getMetadataOps());
        } else {
            throw new RuntimeException("Only support 'hms' and 'iceberg' type for iceberg table");
        }
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("load iceberg table {}", nameMapping, new Exception());
            }
            Table table = ((ExternalCatalog) catalog).getExecutionAuthenticator()
                    .execute(()
                            -> ops.loadTable(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName()));
            return new IcebergTableCacheValue(table);
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }

    }

    @NotNull
    private IcebergSnapshotCacheValue loadSnapshot(ExternalTable dorisTable, Table icebergTable) {
        if (!(dorisTable instanceof MTMVRelatedTableIf)) {
            throw new RuntimeException(String.format("Table %s.%s is not a valid MTMV related table.",
                    dorisTable.getDbName(), dorisTable.getName()));
        }

        try {
            MTMVRelatedTableIf table = (MTMVRelatedTableIf) dorisTable;
            IcebergSnapshot latestIcebergSnapshot = IcebergUtils.getLatestIcebergSnapshot(icebergTable);
            IcebergPartitionInfo icebergPartitionInfo;
            if (!table.isValidRelatedTable()) {
                icebergPartitionInfo = IcebergPartitionInfo.empty();
            } else {
                icebergPartitionInfo = IcebergUtils.loadPartitionInfo(dorisTable, icebergTable,
                        latestIcebergSnapshot.getSnapshotId(), latestIcebergSnapshot.getSchemaId());
            }
            return new IcebergSnapshotCacheValue(icebergPartitionInfo, latestIcebergSnapshot);
        } catch (AnalysisException e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    public void invalidateCatalogCache(long catalogId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalidate all iceberg table cache when invalidating catalog {}", catalogId);
        }
        // Invalidate all entries related to the catalog
        tableCache.invalidateAll();
        viewCache.invalidateAll();
        manifestCache.invalidateAll();
    }

    public void invalidateTableCache(ExternalTable dorisTable) {
        IcebergMetadataCacheKey key = IcebergMetadataCacheKey.of(dorisTable.getOrBuildNameMapping());
        IcebergTableCacheValue tableCacheValue = tableCache.getIfPresent(key);
        if (tableCacheValue != null) {
            invalidateTableCache(key, tableCacheValue);
        } else {
            invalidateTableCacheByLocalName(dorisTable);
        }
    }

    private void invalidateTableCache(IcebergMetadataCacheKey key, IcebergTableCacheValue tableCacheValue) {
        ManifestFiles.dropCache(tableCacheValue.getIcebergTable().io());
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalidate iceberg table cache {}", key.nameMapping, new Exception());
        }
        tableCache.invalidate(key);
        viewCache.invalidate(key);
    }

    private void invalidateTableCacheByLocalName(ExternalTable dorisTable) {
        String dbName = dorisTable.getDbName();
        String tblName = dorisTable.getName();
        tableCache.asMap().entrySet().stream()
                .filter(entry -> entry.getKey().nameMapping.getLocalDbName().equals(dbName)
                        && entry.getKey().nameMapping.getLocalTblName().equals(tblName))
                .forEach(entry -> invalidateTableCache(entry.getKey(), entry.getValue()));
        viewCache.asMap().keySet().stream()
                .filter(key -> key.nameMapping.getLocalDbName().equals(dbName)
                        && key.nameMapping.getLocalTblName().equals(tblName))
                .forEach(viewCache::invalidate);
    }

    public void invalidateDbCache(long catalogId, String dbName) {
        tableCache.asMap().entrySet().stream()
                .filter(entry -> entry.getKey().nameMapping.getLocalDbName().equals(dbName))
                .forEach(entry -> {
                    ManifestFiles.dropCache(entry.getValue().getIcebergTable().io());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("invalidate iceberg table cache {} when invalidating db cache",
                                entry.getKey().nameMapping, new Exception());
                    }
                    tableCache.invalidate(entry.getKey());
                });
        viewCache.asMap().keySet().stream()
                .filter(key -> key.nameMapping.getLocalDbName().equals(dbName))
                .forEach(viewCache::invalidate);
    }

    private static void initIcebergTableFileIO(Table table, Map<String, String> props) {
        Map<String, String> ioConf = new HashMap<>();
        table.properties().forEach((key, value) -> {
            if (key.startsWith("io.")) {
                ioConf.put(key, value);
            }
        });

        // This `initialize` method will directly override the properties as a whole,
        // so we need to merge the table's io-related properties with the doris's catalog-related properties
        props.putAll(ioConf);
        table.io().initialize(props);
    }

    static class IcebergMetadataCacheKey {
        NameMapping nameMapping;

        private IcebergMetadataCacheKey(NameMapping nameMapping) {
            this.nameMapping = nameMapping;
        }

        private static IcebergMetadataCacheKey of(NameMapping nameMapping) {
            return new IcebergMetadataCacheKey(nameMapping);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IcebergMetadataCacheKey that = (IcebergMetadataCacheKey) o;
            return nameMapping.equals(that.nameMapping);
        }

        @Override
        public int hashCode() {
            return nameMapping.hashCode();
        }
    }

    public Map<String, Map<String, String>> getCacheStats() {
        Map<String, Map<String, String>> res = Maps.newHashMap();
        res.put("iceberg_table_cache", ExternalMetaCacheMgr.getCacheStats(tableCache.stats(),
                tableCache.estimatedSize()));
        return res;
    }

    private View loadView(IcebergMetadataCacheKey key) {
        IcebergMetadataOps ops;
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(key.nameMapping.getCtlId());
        if (catalog instanceof IcebergExternalCatalog) {
            ops = (IcebergMetadataOps) (((IcebergExternalCatalog) catalog).getMetadataOps());
        } else {
            return null;
        }
        try {
            return ((ExternalCatalog) catalog).getExecutionAuthenticator().execute(() ->
                    ops.loadView(key.nameMapping.getRemoteDbName(), key.nameMapping.getRemoteTblName()));
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    public View getIcebergView(ExternalTable dorisTable) {
        IcebergMetadataCacheKey key = new IcebergMetadataCacheKey(dorisTable.getOrBuildNameMapping());
        return viewCache.get(key);
    }
}
