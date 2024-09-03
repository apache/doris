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
import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
import org.apache.doris.thrift.TIcebergMetadataParams;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

public class IcebergMetadataCache {

    private final LoadingCache<IcebergMetadataCacheKey, List<Snapshot>> snapshotListCache;
    private final LoadingCache<IcebergMetadataCacheKey, Table> tableCache;

    public IcebergMetadataCache(ExecutorService executor) {
        CacheFactory snapshotListCacheFactory = new CacheFactory(
                OptionalLong.of(28800L),
                OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60),
                Config.max_external_table_cache_num,
                true,
                null);
        this.snapshotListCache = snapshotListCacheFactory.buildCache(key -> loadSnapshots(key), null, executor);

        CacheFactory tableCacheFactory = new CacheFactory(
                OptionalLong.of(28800L),
                OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60),
                Config.max_external_table_cache_num,
                true,
                null);
        this.tableCache = tableCacheFactory.buildCache(key -> loadTable(key), null, executor);
    }

    public List<Snapshot> getSnapshotList(TIcebergMetadataParams params) throws UserException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(params.getCatalog());
        if (catalog == null) {
            throw new UserException("The specified catalog does not exist:" + params.getCatalog());
        }
        IcebergMetadataCacheKey key =
                IcebergMetadataCacheKey.of(catalog, params.getDatabase(), params.getTable());
        return snapshotListCache.get(key);
    }

    public Table getIcebergTable(CatalogIf catalog, String dbName, String tbName) {
        IcebergMetadataCacheKey key = IcebergMetadataCacheKey.of(catalog, dbName, tbName);
        return tableCache.get(key);
    }

    public Table getAndCloneTable(CatalogIf catalog, String dbName, String tbName) {
        Table restTable;
        synchronized (this) {
            Table table = getIcebergTable(catalog, dbName, tbName);
            restTable = SerializableTable.copyOf(table);
        }
        return restTable;
    }

    public Table getRemoteTable(CatalogIf catalog, String dbName, String tbName) {
        IcebergMetadataCacheKey key = IcebergMetadataCacheKey.of(catalog, dbName, tbName);
        return loadTable(key);
    }

    @NotNull
    private List<Snapshot> loadSnapshots(IcebergMetadataCacheKey key) {
        Table icebergTable = getIcebergTable(key.catalog, key.dbName, key.tableName);
        List<Snapshot> snaps = Lists.newArrayList();
        Iterables.addAll(snaps, icebergTable.snapshots());
        return snaps;
    }

    @NotNull
    private Table loadTable(IcebergMetadataCacheKey key) {
        Catalog icebergCatalog;
        if (key.catalog instanceof HMSExternalCatalog) {
            HMSExternalCatalog ctg = (HMSExternalCatalog) key.catalog;
            icebergCatalog = createIcebergHiveCatalog(
                    ctg.getHiveMetastoreUris(),
                    ctg.getCatalogProperty().getHadoopProperties(),
                    ctg.getProperties());
        } else if (key.catalog instanceof IcebergExternalCatalog) {
            icebergCatalog = ((IcebergExternalCatalog) key.catalog).getCatalog();
        } else {
            throw new RuntimeException("Only support 'hms' and 'iceberg' type for iceberg table");
        }
        Table icebergTable = HiveMetaStoreClientHelper.ugiDoAs(key.catalog.getId(),
                () -> icebergCatalog.loadTable(TableIdentifier.of(key.dbName, key.tableName)));
        initIcebergTableFileIO(icebergTable, key.catalog.getProperties());
        return icebergTable;
    }

    public void invalidateCatalogCache(long catalogId) {
        snapshotListCache.asMap().keySet().stream()
                .filter(key -> key.catalog.getId() == catalogId)
                .forEach(snapshotListCache::invalidate);

        tableCache.asMap().entrySet().stream()
                .filter(entry -> entry.getKey().catalog.getId() == catalogId)
                .forEach(entry -> {
                    ManifestFiles.dropCache(entry.getValue().io());
                    tableCache.invalidate(entry.getKey());
                });
    }

    public void invalidateTableCache(long catalogId, String dbName, String tblName) {
        snapshotListCache.asMap().keySet().stream()
                .filter(key -> key.catalog.getId() == catalogId && key.dbName.equals(dbName) && key.tableName.equals(
                        tblName))
                .forEach(snapshotListCache::invalidate);

        tableCache.asMap().entrySet().stream()
                .filter(entry -> {
                    IcebergMetadataCacheKey key = entry.getKey();
                    return key.catalog.getId() == catalogId && key.dbName.equals(dbName) && key.tableName.equals(
                            tblName);
                })
                .forEach(entry -> {
                    ManifestFiles.dropCache(entry.getValue().io());
                    tableCache.invalidate(entry.getKey());
                });
    }

    public void invalidateDbCache(long catalogId, String dbName) {
        snapshotListCache.asMap().keySet().stream()
                .filter(key -> key.catalog.getId() == catalogId && key.dbName.equals(dbName))
                .forEach(snapshotListCache::invalidate);

        tableCache.asMap().entrySet().stream()
                .filter(entry -> {
                    IcebergMetadataCacheKey key = entry.getKey();
                    return key.catalog.getId() == catalogId && key.dbName.equals(dbName);
                })
                .forEach(entry -> {
                    ManifestFiles.dropCache(entry.getValue().io());
                    tableCache.invalidate(entry.getKey());
                });
    }

    private Catalog createIcebergHiveCatalog(String uri, Map<String, String> hdfsConf, Map<String, String> props) {
        // set hdfs configure
        Configuration conf = DFSFileSystem.getHdfsConf(
                hdfsConf.getOrDefault(DFSFileSystem.PROP_ALLOW_FALLBACK_TO_SIMPLE_AUTH, "").isEmpty());
        for (Map.Entry<String, String> entry : hdfsConf.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(conf);

        if (props.containsKey(HMSExternalCatalog.BIND_BROKER_NAME)) {
            props.put(HMSProperties.HIVE_METASTORE_URIS, uri);
            props.put("uri", uri);
            hiveCatalog.initialize("hive", props);
        } else {
            Map<String, String> catalogProperties = new HashMap<>();
            catalogProperties.put(HMSProperties.HIVE_METASTORE_URIS, uri);
            catalogProperties.put("uri", uri);
            hiveCatalog.initialize("hive", catalogProperties);
        }
        return hiveCatalog;
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
        CatalogIf catalog;
        String dbName;
        String tableName;

        public IcebergMetadataCacheKey(CatalogIf catalog, String dbName, String tableName) {
            this.catalog = catalog;
            this.dbName = dbName;
            this.tableName = tableName;
        }

        static IcebergMetadataCacheKey of(CatalogIf catalog, String dbName, String tableName) {
            return new IcebergMetadataCacheKey(catalog, dbName, tableName);
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
            return catalog.getId() == that.catalog.getId()
                    && Objects.equals(dbName, that.dbName)
                    && Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalog.getId(), dbName, tableName);
        }
    }

    public Map<String, Map<String, String>> getCacheStats() {
        Map<String, Map<String, String>> res = Maps.newHashMap();
        res.put("iceberg_snapshot_cache", ExternalMetaCacheMgr.getCacheStats(snapshotListCache.stats(),
                snapshotListCache.estimatedSize()));
        res.put("iceberg_table_cache", ExternalMetaCacheMgr.getCacheStats(tableCache.stats(),
                tableCache.estimatedSize()));
        return res;
    }
}
