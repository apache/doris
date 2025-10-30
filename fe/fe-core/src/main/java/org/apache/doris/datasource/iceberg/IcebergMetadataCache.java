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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
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

    private final LoadingCache<IcebergMetadataCacheKey, List<Snapshot>> snapshotListCache;
    private final LoadingCache<IcebergMetadataCacheKey, Table> tableCache;
    private final LoadingCache<IcebergMetadataCacheKey, IcebergSnapshotCacheValue> snapshotCache;
    private final LoadingCache<IcebergMetadataCacheKey, View> viewCache;

    public IcebergMetadataCache(ExecutorService executor) {
        CacheFactory snapshotListCacheFactory = new CacheFactory(
                OptionalLong.of(Config.external_cache_expire_time_seconds_after_access),
                OptionalLong.of(Config.external_cache_refresh_time_minutes * 60),
                Config.max_external_table_cache_num,
                true,
                null);
        this.snapshotListCache = snapshotListCacheFactory.buildCache(key -> loadSnapshots(key), null, executor);

        CacheFactory tableCacheFactory = new CacheFactory(
                OptionalLong.of(Config.external_cache_expire_time_seconds_after_access),
                OptionalLong.of(Config.external_cache_refresh_time_minutes * 60),
                Config.max_external_table_cache_num,
                true,
                null);
        this.tableCache = tableCacheFactory.buildCache(key -> loadTable(key), null, executor);

        CacheFactory snapshotCacheFactory = new CacheFactory(
                OptionalLong.of(Config.external_cache_expire_time_seconds_after_access),
                OptionalLong.of(Config.external_cache_refresh_time_minutes * 60),
                Config.max_external_table_cache_num,
                true,
                null);
        this.snapshotCache = snapshotCacheFactory.buildCache(key -> loadSnapshot(key), null, executor);
        this.viewCache = tableCacheFactory.buildCache(key -> loadView(key), null, executor);
    }

    public Table getIcebergTable(ExternalTable dorisTable) {
        IcebergMetadataCacheKey key = new IcebergMetadataCacheKey(dorisTable.getOrBuildNameMapping());
        return tableCache.get(key);
    }

    public Table getIcebergTable(IcebergMetadataCacheKey key) {
        return tableCache.get(key);
    }

    public IcebergSnapshotCacheValue getSnapshotCache(ExternalTable dorisTable) {
        IcebergMetadataCacheKey key = new IcebergMetadataCacheKey(dorisTable.getOrBuildNameMapping());
        return snapshotCache.get(key);
    }

    @NotNull
    private List<Snapshot> loadSnapshots(IcebergMetadataCacheKey key) {
        Table icebergTable = getIcebergTable(key);
        List<Snapshot> snaps = Lists.newArrayList();
        Iterables.addAll(snaps, icebergTable.snapshots());
        return snaps;
    }

    @NotNull
    private Table loadTable(IcebergMetadataCacheKey key) {
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
            return ((ExternalCatalog) catalog).getExecutionAuthenticator().execute(()
                    -> ops.loadTable(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName()));
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }

    }

    @NotNull
    private IcebergSnapshotCacheValue loadSnapshot(IcebergMetadataCacheKey key) throws AnalysisException {
        NameMapping nameMapping = key.nameMapping;
        TableIf dorisTable = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(nameMapping.getCtlId())
                .getDbOrAnalysisException(nameMapping.getLocalDbName())
                .getTableOrAnalysisException(nameMapping.getLocalTblName());

        if (!(dorisTable instanceof MTMVRelatedTableIf)) {
            throw new AnalysisException(String.format("Table %s.%s is not a valid MTMV related table.",
                    nameMapping.getLocalDbName(), nameMapping.getLocalTblName()));
        }

        MTMVRelatedTableIf table = (MTMVRelatedTableIf) dorisTable;
        IcebergSnapshot lastedIcebergSnapshot = IcebergUtils.getLastedIcebergSnapshot((ExternalTable) table);
        IcebergPartitionInfo icebergPartitionInfo;
        if (!table.isValidRelatedTable()) {
            icebergPartitionInfo = IcebergPartitionInfo.empty();
        } else {
            icebergPartitionInfo = IcebergUtils.loadPartitionInfo((ExternalTable) table,
                    lastedIcebergSnapshot.getSnapshotId());
        }
        return new IcebergSnapshotCacheValue(icebergPartitionInfo, lastedIcebergSnapshot);
    }

    public void invalidateCatalogCache(long catalogId) {
        snapshotListCache.asMap().keySet().stream()
                .filter(key -> key.nameMapping.getCtlId() == catalogId)
                .forEach(snapshotListCache::invalidate);

        tableCache.asMap().entrySet().stream()
                .filter(entry -> entry.getKey().nameMapping.getCtlId() == catalogId)
                .forEach(entry -> {
                    ManifestFiles.dropCache(entry.getValue().io());
                    if (LOG.isDebugEnabled()) {
                        LOG.info("invalidate iceberg table cache {} when invalidating catalog cache",
                                entry.getKey().nameMapping, new Exception());
                    }
                    tableCache.invalidate(entry.getKey());
                });

        snapshotCache.asMap().keySet().stream()
                .filter(key -> key.nameMapping.getCtlId() == catalogId)
                .forEach(snapshotCache::invalidate);

        viewCache.asMap().entrySet().stream()
                .filter(entry -> entry.getKey().nameMapping.getCtlId() == catalogId)
                .forEach(entry -> viewCache.invalidate(entry.getKey()));
    }

    public void invalidateTableCache(ExternalTable dorisTable) {
        long catalogId = dorisTable.getCatalog().getId();
        String dbName = dorisTable.getDbName();
        String tblName = dorisTable.getName();
        snapshotListCache.asMap().keySet().stream()
                .filter(key -> key.nameMapping.getCtlId() == catalogId
                        && key.nameMapping.getLocalDbName().equals(dbName)
                        && key.nameMapping.getLocalTblName().equals(tblName))
                .forEach(snapshotListCache::invalidate);

        tableCache.asMap().entrySet().stream()
                .filter(entry -> {
                    IcebergMetadataCacheKey key = entry.getKey();
                    return key.nameMapping.getCtlId() == catalogId
                            && key.nameMapping.getLocalDbName().equals(dbName)
                            && key.nameMapping.getLocalTblName().equals(tblName);
                })
                .forEach(entry -> {
                    ManifestFiles.dropCache(entry.getValue().io());
                    if (LOG.isDebugEnabled()) {
                        LOG.info("invalidate iceberg table cache {}",
                                entry.getKey().nameMapping, new Exception());
                    }
                    tableCache.invalidate(entry.getKey());
                });

        snapshotCache.asMap().keySet().stream()
                .filter(key -> key.nameMapping.getCtlId() == catalogId
                        && key.nameMapping.getLocalDbName().equals(dbName)
                        && key.nameMapping.getLocalTblName().equals(tblName))
                .forEach(snapshotCache::invalidate);
        viewCache.asMap().entrySet().stream()
                .filter(entry -> {
                    IcebergMetadataCacheKey key = entry.getKey();
                    return key.nameMapping.getCtlId() == catalogId
                            && key.nameMapping.getLocalDbName().equals(dbName)
                            && key.nameMapping.getLocalTblName().equals(tblName);
                })
                .forEach(entry -> viewCache.invalidate(entry.getKey()));
    }

    public void invalidateDbCache(long catalogId, String dbName) {
        snapshotListCache.asMap().keySet().stream()
                .filter(key -> key.nameMapping.getCtlId() == catalogId
                        && key.nameMapping.getLocalDbName().equals(dbName))
                .forEach(snapshotListCache::invalidate);

        tableCache.asMap().entrySet().stream()
                .filter(entry -> {
                    IcebergMetadataCacheKey key = entry.getKey();
                    return key.nameMapping.getCtlId() == catalogId
                            && key.nameMapping.getLocalDbName().equals(dbName);
                })
                .forEach(entry -> {
                    ManifestFiles.dropCache(entry.getValue().io());
                    if (LOG.isDebugEnabled()) {
                        LOG.info("invalidate iceberg table cache {} when invalidating db cache",
                                entry.getKey().nameMapping, new Exception());
                    }
                    tableCache.invalidate(entry.getKey());
                });

        snapshotCache.asMap().keySet().stream()
                .filter(key -> key.nameMapping.getCtlId() == catalogId
                        && key.nameMapping.getLocalDbName().equals(dbName))
                .forEach(snapshotCache::invalidate);
        viewCache.asMap().entrySet().stream()
                .filter(entry -> {
                    IcebergMetadataCacheKey key = entry.getKey();
                    return key.nameMapping.getCtlId() == catalogId
                            && key.nameMapping.getLocalDbName().equals(dbName);
                })
                .forEach(entry -> viewCache.invalidate(entry.getKey()));
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
        res.put("iceberg_snapshot_list_cache", ExternalMetaCacheMgr.getCacheStats(snapshotListCache.stats(),
                snapshotListCache.estimatedSize()));
        res.put("iceberg_table_cache", ExternalMetaCacheMgr.getCacheStats(tableCache.stats(),
                tableCache.estimatedSize()));
        res.put("iceberg_snapshot_cache", ExternalMetaCacheMgr.getCacheStats(snapshotCache.stats(),
                snapshotCache.estimatedSize()));
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
