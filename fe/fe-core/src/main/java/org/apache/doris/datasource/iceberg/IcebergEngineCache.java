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

import org.apache.doris.common.Config;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.metacache.CacheModule;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.EngineMetaCache;
import org.apache.doris.datasource.metacache.EnginePartitionInfo;
import org.apache.doris.datasource.metacache.EngineSnapshot;
import org.apache.doris.datasource.mvcc.MvccSnapshot;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Engine-level cache adapter for iceberg.
 */
public class IcebergEngineCache implements EngineMetaCache {
    public static final String ENGINE_TYPE = "iceberg";
    private static final String TABLE_MODULE_NAME = "table";
    private static final String MANIFEST_MODULE_NAME = "manifest";

    private final IcebergMetadataCache metadataCache;
    private List<CacheModule> cacheModules = ImmutableList.of();

    public IcebergEngineCache(IcebergMetadataCache metadataCache) {
        this.metadataCache = Objects.requireNonNull(metadataCache, "metadataCache cannot be null");
    }

    @Override
    public String getEngineType() {
        return ENGINE_TYPE;
    }

    @Override
    public void init(ExternalCatalog catalog) {
        ExternalCatalog nonNullCatalog = Objects.requireNonNull(catalog, "catalog cannot be null");
        this.cacheModules = ImmutableList.of(new IcebergTableModule(nonNullCatalog, metadataCache),
                new IcebergManifestModule(nonNullCatalog, metadataCache));
    }

    @Override
    public List<CacheModule> getCacheModules() {
        return cacheModules;
    }

    @Override
    public EnginePartitionInfo getPartitionInfo(ExternalTable table, Optional<MvccSnapshot> snapshot) {
        IcebergSnapshotCacheValue snapshotCacheValue = getSnapshotCacheValue(snapshot, table);
        return new IcebergPartition(snapshotCacheValue.getPartitionInfo());
    }

    @Override
    public EngineSnapshot getSnapshot(ExternalTable table, Optional<MvccSnapshot> snapshot) {
        IcebergSnapshotCacheValue snapshotCacheValue = getSnapshotCacheValue(snapshot, table);
        return new IcebergSnapshotMeta(snapshotCacheValue.getSnapshot());
    }

    @Override
    public SchemaCacheValue getSchema(ExternalTable table, long schemaVersion) {
        if (schemaVersion > 0) {
            return IcebergUtils.loadSchemaCacheValue(table, schemaVersion, table.isView())
                    .orElseThrow(() -> new IllegalArgumentException("Schema not found, table="
                            + table.getName() + ", schemaVersion=" + schemaVersion));
        }
        return getSnapshotCacheValue(Optional.empty(), table).getSchema();
    }

    public IcebergMetadataCache getMetadataCache() {
        return metadataCache;
    }

    private IcebergSnapshotCacheValue getSnapshotCacheValue(Optional<MvccSnapshot> snapshot, ExternalTable table) {
        if (snapshot.isPresent() && snapshot.get() instanceof IcebergMvccSnapshot) {
            return ((IcebergMvccSnapshot) snapshot.get()).getSnapshotCacheValue();
        }
        return metadataCache.getSnapshotCache(table);
    }

    public static final class IcebergPartition implements EnginePartitionInfo {
        private final IcebergPartitionInfo partitionInfo;

        public IcebergPartition(IcebergPartitionInfo partitionInfo) {
            this.partitionInfo = partitionInfo;
        }

        public IcebergPartitionInfo getPartitionInfo() {
            return partitionInfo;
        }
    }

    public static final class IcebergSnapshotMeta implements EngineSnapshot {
        private final IcebergSnapshot snapshot;

        public IcebergSnapshotMeta(IcebergSnapshot snapshot) {
            this.snapshot = snapshot;
        }

        public IcebergSnapshot getSnapshot() {
            return snapshot;
        }
    }

    private static class IcebergTableModule implements CacheModule {
        private final long catalogId;
        private final IcebergMetadataCache metadataCache;
        private final CacheSpec cacheSpec;

        IcebergTableModule(ExternalCatalog catalog, IcebergMetadataCache metadataCache) {
            this.catalogId = catalog.getId();
            this.metadataCache = metadataCache;
            this.cacheSpec = CacheSpec.fromUnifiedProperties(catalog.getProperties(),
                    ENGINE_TYPE, TABLE_MODULE_NAME, true,
                    Config.external_cache_expire_time_seconds_after_access, Config.max_external_table_cache_num);
        }

        @Override
        public String getName() {
            return TABLE_MODULE_NAME;
        }

        @Override
        public CacheSpec getSpec() {
            return cacheSpec;
        }

        @Override
        public void invalidateAll() {
            metadataCache.invalidateCatalogCache(catalogId);
        }

        @Override
        public void invalidateDb(String dbName) {
            metadataCache.invalidateDbCache(catalogId, dbName);
        }

        @Override
        public void invalidateTable(String dbName, String tableName) {
            metadataCache.invalidateTableCacheByLocalName(dbName, tableName);
        }

        @Override
        public CacheStats getStats() {
            return metadataCache.getTableCacheStats();
        }

        @Override
        public long getEstimatedSize() {
            return metadataCache.getTableCacheEstimatedSize();
        }
    }

    private static class IcebergManifestModule implements CacheModule {
        private final IcebergMetadataCache metadataCache;
        private final CacheSpec cacheSpec;

        IcebergManifestModule(ExternalCatalog catalog, IcebergMetadataCache metadataCache) {
            this.metadataCache = metadataCache;
            this.cacheSpec = CacheSpec.fromUnifiedProperties(catalog.getProperties(),
                    ENGINE_TYPE, MANIFEST_MODULE_NAME, IcebergExternalCatalog.DEFAULT_ICEBERG_MANIFEST_CACHE_ENABLE,
                    IcebergExternalCatalog.DEFAULT_ICEBERG_MANIFEST_CACHE_TTL_SECOND,
                    IcebergExternalCatalog.DEFAULT_ICEBERG_MANIFEST_CACHE_CAPACITY);
        }

        @Override
        public String getName() {
            return MANIFEST_MODULE_NAME;
        }

        @Override
        public CacheSpec getSpec() {
            return cacheSpec;
        }

        @Override
        public void invalidateAll() {
            metadataCache.getManifestCache().invalidateAll();
        }

        @Override
        public void invalidateDb(String dbName) {
            metadataCache.invalidateManifestCacheByDbName(dbName);
        }

        @Override
        public void invalidateTable(String dbName, String tableName) {
            metadataCache.invalidateManifestCacheByLocalName(dbName, tableName);
        }

        @Override
        public CacheStats getStats() {
            return metadataCache.getManifestCacheStats();
        }

        @Override
        public long getEstimatedSize() {
            return metadataCache.getManifestCacheEstimatedSize();
        }
    }
}
