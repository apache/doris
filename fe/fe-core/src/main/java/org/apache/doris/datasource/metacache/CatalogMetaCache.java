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

package org.apache.doris.datasource.metacache;

import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Per-catalog cache container. One catalog can own multiple engine caches.
 */
public class CatalogMetaCache {
    private final long catalogId;
    private final long schemaCacheTtlSecond;
    private final Map<String, EngineMetaCache> engineCaches = new ConcurrentHashMap<>();
    private final Object schemaCacheLock = new Object();
    private volatile LoadingCache<SchemaMetaCacheKey, Optional<SchemaCacheValue>> schemaCache;

    public CatalogMetaCache(ExternalCatalog catalog) {
        ExternalCatalog nonNullCatalog = Objects.requireNonNull(catalog, "catalog cannot be null");
        this.catalogId = nonNullCatalog.getId();
        this.schemaCacheTtlSecond = nonNullCatalog.getSchemaCacheTtlSecond();
    }

    public long getCatalogId() {
        return catalogId;
    }

    public EngineMetaCache getOrCreateEngineMetaCache(String engineType,
            Supplier<? extends EngineMetaCache> cacheSupplier, ExternalCatalog catalog) {
        String normalizedEngineType = normalizeEngineType(engineType);
        ExternalCatalog nonNullCatalog = Objects.requireNonNull(catalog, "catalog cannot be null");
        Objects.requireNonNull(cacheSupplier, "cacheSupplier cannot be null");
        return engineCaches.computeIfAbsent(normalizedEngineType, key -> {
            EngineMetaCache newCache = Objects.requireNonNull(cacheSupplier.get(), "cacheSupplier returned null");
            try {
                newCache.init(nonNullCatalog);
                return newCache;
            } catch (Exception e) {
                try {
                    newCache.invalidateCatalog();
                } catch (Exception cleanupException) {
                    e.addSuppressed(cleanupException);
                }
                throw new IllegalStateException(String.format("Failed to initialize engine cache: catalogId=%d,"
                        + " engineType=%s", catalogId, normalizedEngineType), e);
            }
        });
    }

    public EngineMetaCache getEngineMetaCache(String engineType) {
        return engineCaches.get(normalizeEngineType(engineType));
    }

    public EngineMetaCache removeEngineMetaCache(String engineType) {
        return engineCaches.remove(normalizeEngineType(engineType));
    }

    public List<EngineMetaCache> getEngineMetaCaches() {
        return ImmutableList.copyOf(engineCaches.values());
    }

    public Optional<SchemaCacheValue> getSchema(ExternalTable table, long versionToken) {
        Objects.requireNonNull(table, "table cannot be null");
        return getOrCreateSchemaCache().get(SchemaMetaCacheKey.of(table, versionToken));
    }

    public void invalidateCatalog() {
        engineCaches.values().forEach(EngineMetaCache::invalidateCatalog);
        invalidateSchemaCatalog();
    }

    public void invalidateDb(String dbName) {
        engineCaches.values().forEach(cache -> cache.invalidateDb(dbName));
        invalidateSchemaDb(dbName);
    }

    public void invalidateTable(ExternalTable table) {
        engineCaches.values().forEach(cache -> cache.invalidateTable(table));
        invalidateSchemaTable(table);
    }

    public void invalidateSchemaCatalog() {
        LoadingCache<SchemaMetaCacheKey, Optional<SchemaCacheValue>> localSchemaCache = schemaCache;
        if (localSchemaCache != null) {
            localSchemaCache.invalidateAll();
        }
    }

    public void invalidateSchemaDb(String dbName) {
        invalidateSchemaKeys(key -> Objects.equals(key.getDbName(), dbName));
    }

    public void invalidateSchemaTable(ExternalTable table) {
        String tableDbName = table.getDbName();
        String tableName = table.getName();
        invalidateSchemaKeys(key -> Objects.equals(key.getDbName(), tableDbName)
                && Objects.equals(key.getTableName(), tableName));
    }

    private static String normalizeEngineType(String engineType) {
        String trimmedEngineType = Objects.requireNonNull(engineType, "engineType cannot be null").trim();
        if (trimmedEngineType.isEmpty()) {
            throw new IllegalArgumentException("engineType cannot be empty");
        }
        return trimmedEngineType.toLowerCase(Locale.ROOT);
    }

    private LoadingCache<SchemaMetaCacheKey, Optional<SchemaCacheValue>> getOrCreateSchemaCache() {
        LoadingCache<SchemaMetaCacheKey, Optional<SchemaCacheValue>> localSchemaCache = schemaCache;
        if (localSchemaCache != null) {
            return localSchemaCache;
        }
        synchronized (schemaCacheLock) {
            localSchemaCache = schemaCache;
            if (localSchemaCache != null) {
                return localSchemaCache;
            }
            CacheFactory cacheFactory = new CacheFactory(
                    CacheSpec.toExpireAfterAccess(schemaCacheTtlSecond),
                    OptionalLong.of(Config.external_cache_refresh_time_minutes * 60),
                    Config.max_external_schema_cache_num,
                    false,
                    null);
            localSchemaCache = cacheFactory.buildCache(this::loadSchema);
            schemaCache = localSchemaCache;
            return localSchemaCache;
        }
    }

    private Optional<SchemaCacheValue> loadSchema(SchemaMetaCacheKey key) {
        Optional<SchemaCacheValue> schema = key.getTable().loadSchemaByVersion(key.getVersionToken());
        schema.ifPresent(SchemaCacheValue::validateSchema);
        return schema;
    }

    private void invalidateSchemaKeys(Predicate<SchemaMetaCacheKey> predicate) {
        LoadingCache<SchemaMetaCacheKey, Optional<SchemaCacheValue>> localSchemaCache = schemaCache;
        if (localSchemaCache == null) {
            return;
        }
        for (SchemaMetaCacheKey key : localSchemaCache.asMap().keySet()) {
            if (predicate.test(key)) {
                localSchemaCache.invalidate(key);
            }
        }
    }

    private static final class SchemaMetaCacheKey {
        private final long tableId;
        private final String engineType;
        private final String dbName;
        private final String tableName;
        private final long versionToken;
        private final ExternalTable table;

        private SchemaMetaCacheKey(long tableId, String engineType, String dbName, String tableName,
                long versionToken, ExternalTable table) {
            this.tableId = tableId;
            this.engineType = engineType;
            this.dbName = dbName;
            this.tableName = tableName;
            this.versionToken = versionToken;
            this.table = table;
        }

        static SchemaMetaCacheKey of(ExternalTable table, long versionToken) {
            String engineType = table.getType() == null ? "unknown" : table.getType().toEngineName();
            return new SchemaMetaCacheKey(
                    table.getId(),
                    normalizeEngineType(engineType),
                    table.getDbName(),
                    table.getName(),
                    versionToken,
                    table);
        }

        long getVersionToken() {
            return versionToken;
        }

        String getDbName() {
            return dbName;
        }

        String getTableName() {
            return tableName;
        }

        ExternalTable getTable() {
            return table;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof SchemaMetaCacheKey)) {
                return false;
            }
            SchemaMetaCacheKey that = (SchemaMetaCacheKey) obj;
            return tableId == that.tableId
                    && versionToken == that.versionToken
                    && Objects.equals(engineType, that.engineType)
                    && Objects.equals(dbName, that.dbName)
                    && Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, engineType, dbName, tableName, versionToken);
        }
    }
}
