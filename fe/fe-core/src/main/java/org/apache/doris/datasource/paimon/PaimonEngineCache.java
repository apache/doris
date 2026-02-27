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

package org.apache.doris.datasource.paimon;

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
 * Engine-level cache adapter for paimon.
 */
public class PaimonEngineCache implements EngineMetaCache {
    public static final String ENGINE_TYPE = "paimon";
    private static final String TABLE_MODULE_NAME = "table";

    private final PaimonMetadataCache metadataCache;
    private List<CacheModule> cacheModules = ImmutableList.of();

    public PaimonEngineCache(PaimonMetadataCache metadataCache) {
        this.metadataCache = Objects.requireNonNull(metadataCache, "metadataCache cannot be null");
    }

    @Override
    public String getEngineType() {
        return ENGINE_TYPE;
    }

    @Override
    public void init(ExternalCatalog catalog) {
        ExternalCatalog nonNullCatalog = Objects.requireNonNull(catalog, "catalog cannot be null");
        this.cacheModules = ImmutableList.of(new PaimonTableModule(nonNullCatalog, metadataCache));
    }

    @Override
    public List<CacheModule> getCacheModules() {
        return cacheModules;
    }

    @Override
    public EnginePartitionInfo getPartitionInfo(ExternalTable table, Optional<MvccSnapshot> snapshot) {
        PaimonSnapshotCacheValue snapshotCacheValue = getSnapshotCacheValue(snapshot, table);
        return new PaimonPartition(snapshotCacheValue.getPartitionInfo());
    }

    @Override
    public EngineSnapshot getSnapshot(ExternalTable table, Optional<MvccSnapshot> snapshot) {
        PaimonSnapshotCacheValue snapshotCacheValue = getSnapshotCacheValue(snapshot, table);
        return new PaimonSnapshotMeta(snapshotCacheValue.getSnapshot());
    }

    @Override
    public SchemaCacheValue getSchema(ExternalTable table, long schemaVersion) {
        return getMetadataCache().getPaimonSchemaCacheValue(table, schemaVersion);
    }

    public PaimonMetadataCache getMetadataCache() {
        return metadataCache;
    }

    private PaimonSnapshotCacheValue getSnapshotCacheValue(Optional<MvccSnapshot> snapshot, ExternalTable table) {
        if (snapshot.isPresent() && snapshot.get() instanceof PaimonMvccSnapshot) {
            return ((PaimonMvccSnapshot) snapshot.get()).getSnapshotCacheValue();
        }
        return metadataCache.getSnapshotCache(table);
    }

    public static final class PaimonPartition implements EnginePartitionInfo {
        private final PaimonPartitionInfo partitionInfo;

        public PaimonPartition(PaimonPartitionInfo partitionInfo) {
            this.partitionInfo = partitionInfo;
        }

        public PaimonPartitionInfo getPartitionInfo() {
            return partitionInfo;
        }
    }

    public static final class PaimonSnapshotMeta implements EngineSnapshot {
        private final PaimonSnapshot snapshot;

        public PaimonSnapshotMeta(PaimonSnapshot snapshot) {
            this.snapshot = snapshot;
        }

        public PaimonSnapshot getSnapshot() {
            return snapshot;
        }
    }

    private static class PaimonTableModule implements CacheModule {
        private final long catalogId;
        private final PaimonMetadataCache metadataCache;
        private final CacheSpec cacheSpec;

        PaimonTableModule(ExternalCatalog catalog, PaimonMetadataCache metadataCache) {
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
}
