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
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.thrift.TIcebergMetadataParams;

import avro.shaded.com.google.common.collect.Lists;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class IcebergMetadataCache {

    private final Cache<IcebergMetadataCacheKey, List<Snapshot>> snapshotListCache;
    private final Cache<IcebergMetadataCacheKey, Table> tableCache;

    public IcebergMetadataCache() {
        this.snapshotListCache = CacheBuilder.newBuilder().maximumSize(Config.max_hive_table_cache_num)
            .expireAfterAccess(Config.external_cache_expire_time_minutes_after_access, TimeUnit.MINUTES)
            .build();

        this.tableCache = CacheBuilder.newBuilder().maximumSize(Config.max_hive_table_cache_num)
            .expireAfterAccess(Config.external_cache_expire_time_minutes_after_access, TimeUnit.MINUTES)
            .build();
    }

    public List<Snapshot> getSnapshotList(TIcebergMetadataParams params) throws UserException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(params.getCatalog());
        IcebergMetadataCacheKey key =
                IcebergMetadataCacheKey.of(catalog.getId(), params.getDatabase(), params.getTable());
        List<Snapshot> ifPresent = snapshotListCache.getIfPresent(key);
        if (ifPresent != null) {
            return ifPresent;
        }

        Table icebergTable = getIcebergTable(catalog, params.getDatabase(), params.getTable());
        List<Snapshot> snaps = Lists.newArrayList();
        Iterables.addAll(snaps, icebergTable.snapshots());
        snapshotListCache.put(key, snaps);
        return snaps;
    }

    public Table getIcebergTable(CatalogIf catalog, String dbName, String tbName) {
        IcebergMetadataCacheKey key = IcebergMetadataCacheKey.of(catalog.getId(), dbName, tbName);
        Table cacheTable = tableCache.getIfPresent(key);
        if (cacheTable != null) {
            return cacheTable;
        }

        Catalog icebergCatalog;
        if (catalog instanceof HMSExternalCatalog) {
            HMSExternalCatalog ctg = (HMSExternalCatalog) catalog;
            icebergCatalog = createIcebergHiveCatalog(
                    ctg.getHiveMetastoreUris(),
                    ctg.getCatalogProperty().getHadoopProperties(),
                    ctg.getProperties());
        } else if (catalog instanceof IcebergExternalCatalog) {
            icebergCatalog = ((IcebergExternalCatalog) catalog).getCatalog();
        } else {
            throw new RuntimeException("Only support 'hms' and 'iceberg' type for iceberg table");
        }
        Table icebergTable = HiveMetaStoreClientHelper.ugiDoAs(catalog.getId(),
                () -> icebergCatalog.loadTable(TableIdentifier.of(dbName, tbName)));
        initIcebergTableFileIO(icebergTable, catalog.getProperties());
        tableCache.put(key, icebergTable);
        return icebergTable;
    }

    public void invalidateCatalogCache(long catalogId) {
        snapshotListCache.asMap().keySet().stream()
            .filter(key -> key.catalogId == catalogId)
            .forEach(snapshotListCache::invalidate);

        tableCache.asMap().entrySet().stream()
                .filter(entry -> entry.getKey().catalogId == catalogId)
                .forEach(entry -> {
                    ManifestFiles.dropCache(entry.getValue().io());
                    tableCache.invalidate(entry.getKey());
                });
    }

    public void invalidateTableCache(long catalogId, String dbName, String tblName) {
        snapshotListCache.asMap().keySet().stream()
            .filter(key -> key.catalogId == catalogId && key.dbName.equals(dbName) && key.tableName.equals(tblName))
            .forEach(snapshotListCache::invalidate);

        tableCache.asMap().entrySet().stream()
                .filter(entry -> {
                    IcebergMetadataCacheKey key = entry.getKey();
                    return key.catalogId == catalogId && key.dbName.equals(dbName) && key.tableName.equals(tblName);
                })
                .forEach(entry -> {
                    ManifestFiles.dropCache(entry.getValue().io());
                    tableCache.invalidate(entry.getKey());
                });
    }

    public void invalidateDbCache(long catalogId, String dbName) {
        snapshotListCache.asMap().keySet().stream()
                .filter(key -> key.catalogId == catalogId && key.dbName.equals(dbName))
                .forEach(snapshotListCache::invalidate);

        tableCache.asMap().entrySet().stream()
                .filter(entry -> {
                    IcebergMetadataCacheKey key = entry.getKey();
                    return key.catalogId == catalogId && key.dbName.equals(dbName);
                })
                .forEach(entry -> {
                    ManifestFiles.dropCache(entry.getValue().io());
                    tableCache.invalidate(entry.getKey());
                });
    }

    private Catalog createIcebergHiveCatalog(String uri, Map<String, String> hdfsConf, Map<String, String> props) {
        // set hdfs configure
        Configuration conf = new HdfsConfiguration();
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
        long catalogId;
        String dbName;
        String tableName;

        public IcebergMetadataCacheKey(long catalogId, String dbName, String tableName) {
            this.catalogId = catalogId;
            this.dbName = dbName;
            this.tableName = tableName;
        }

        static IcebergMetadataCacheKey of(long catalogId, String dbName, String tableName) {
            return new IcebergMetadataCacheKey(
                catalogId,
                dbName,
                tableName
            );
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
            return catalogId == that.catalogId
                    && Objects.equals(dbName, that.dbName)
                    && Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogId, dbName, tableName);
        }
    }
}
