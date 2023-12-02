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

package org.apache.doris.planner.external.iceberg;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
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

        Table icebergTable = getIcebergTable(key, catalog, params.getDatabase(), params.getTable());
        List<Snapshot> snaps = Lists.newArrayList();
        Iterables.addAll(snaps, icebergTable.snapshots());
        snapshotListCache.put(key, snaps);
        return snaps;
    }

    public Table getIcebergTable(IcebergMetadataCacheKey key, CatalogIf catalog, String dbName, String tbName)
            throws UserException {
        Table cacheTable = tableCache.getIfPresent(key);
        if (cacheTable != null) {
            return cacheTable;
        }

        Table icebergTable;
        if (catalog instanceof HMSExternalCatalog) {
            HMSExternalCatalog ctg = (HMSExternalCatalog) catalog;
            icebergTable = createIcebergTable(
                ctg.getHiveMetastoreUris(),
                ctg.getCatalogProperty().getHadoopProperties(),
                dbName,
                tbName,
                ctg.getProperties());
        } else if (catalog instanceof IcebergExternalCatalog) {
            IcebergExternalCatalog extCatalog = (IcebergExternalCatalog) catalog;
            icebergTable = getIcebergTable(
                extCatalog.getCatalog(), extCatalog.getId(), dbName, tbName, extCatalog.getProperties());
        } else {
            throw new UserException("Only support 'hms' and 'iceberg' type for iceberg table");
        }
        tableCache.put(key, icebergTable);
        return icebergTable;
    }

    public Table getIcebergTable(IcebergSource icebergSource) throws MetaNotFoundException {
        return icebergSource.getIcebergTable();
    }

    public Table getIcebergTable(HMSExternalTable hmsTable) {
        IcebergMetadataCacheKey key = IcebergMetadataCacheKey.of(
                hmsTable.getCatalog().getId(),
                hmsTable.getDbName(),
                hmsTable.getName());
        Table table = tableCache.getIfPresent(key);
        if (table != null) {
            return table;
        }
        Table icebergTable = createIcebergTable(hmsTable);
        tableCache.put(key, icebergTable);

        return icebergTable;
    }

    public Table getIcebergTable(Catalog catalog, long catalogId, String dbName, String tbName,
            Map<String, String> props) {
        IcebergMetadataCacheKey key = IcebergMetadataCacheKey.of(
                catalogId,
                dbName,
                tbName);
        Table cacheTable = tableCache.getIfPresent(key);
        if (cacheTable != null) {
            return cacheTable;
        }

        Table table = HiveMetaStoreClientHelper.ugiDoAs(catalogId,
                () -> catalog.loadTable(TableIdentifier.of(dbName, tbName)));
        initIcebergTableFileIO(table, props);

        tableCache.put(key, table);

        return table;
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

    private Table createIcebergTable(String uri, Map<String, String> hdfsConf, String db, String tbl,
            Map<String, String> props) {
        // set hdfs configure
        Configuration conf = new HdfsConfiguration();
        for (Map.Entry<String, String> entry : hdfsConf.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }

        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(conf);

        if (props.containsKey(HMSExternalCatalog.BIND_BROKER_NAME)) {
            // Set Iceberg FileIO implementation as `IcebergBrokerIO` when Catalog binding broker is specified.
            props.put("io-impl", "org.apache.doris.datasource.iceberg.broker.IcebergBrokerIO");
            props.put(HMSProperties.HIVE_METASTORE_URIS, uri);
            props.put("uri", uri);
            hiveCatalog.initialize("hive", props);
        } else {
            Map<String, String> catalogProperties = new HashMap<>();
            catalogProperties.put(HMSProperties.HIVE_METASTORE_URIS, uri);
            catalogProperties.put("uri", uri);
            hiveCatalog.initialize("hive", catalogProperties);
        }
        Table table = HiveMetaStoreClientHelper.ugiDoAs(conf, () -> hiveCatalog.loadTable(TableIdentifier.of(db, tbl)));
        initIcebergTableFileIO(table, props);
        return table;
    }

    private Table createIcebergTable(HMSExternalTable hmsTable) {
        return createIcebergTable(hmsTable.getMetastoreUri(),
            hmsTable.getHadoopProperties(),
            hmsTable.getDbName(),
            hmsTable.getName(),
            hmsTable.getCatalogProperties());
    }

    private void initIcebergTableFileIO(Table table, Map<String, String> props) {
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
