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

import org.apache.doris.datasource.CatalogScopedCacheMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveEngineCache;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hudi.source.HudiCatalogCache;
import org.apache.doris.datasource.hudi.source.HudiEngineCache;
import org.apache.doris.datasource.iceberg.IcebergEngineCache;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMetadataCache;
import org.apache.doris.datasource.maxcompute.MaxComputeEngineCache;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.datasource.maxcompute.MaxComputeMetadataCache;
import org.apache.doris.datasource.paimon.PaimonEngineCache;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonMetadataCache;

import com.google.common.collect.ImmutableSet;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Global metadata cache manager that provides one unified entrance for all engines.
 */
public class UnifiedMetaCacheMgr {
    private final Map<Long, CatalogMetaCache> catalogMetaCaches = new ConcurrentHashMap<>();
    private final Map<String, Function<ExternalCatalog, ? extends EngineMetaCache>> engineFactories =
            new ConcurrentHashMap<>();
    private final CatalogScopedCacheMgr<HiveMetaStoreCache> hiveMetaStoreCacheMgr;
    private final CatalogScopedCacheMgr<IcebergMetadataCache> icebergMetadataCacheMgr;
    private final CatalogScopedCacheMgr<PaimonMetadataCache> paimonMetadataCacheMgr;
    private final CatalogScopedCacheMgr<HudiCatalogCache> hudiCatalogCacheMgr;
    private final CatalogScopedCacheMgr<MaxComputeMetadataCache> maxComputeMetadataCacheMgr;

    public UnifiedMetaCacheMgr(ExecutorService commonRefreshExecutor, ExecutorService fileListingExecutor) {
        Objects.requireNonNull(commonRefreshExecutor, "commonRefreshExecutor cannot be null");
        Objects.requireNonNull(fileListingExecutor, "fileListingExecutor cannot be null");

        hudiCatalogCacheMgr = new CatalogScopedCacheMgr<>(
                catalog -> HudiCatalogCache.fromCatalog(catalog, commonRefreshExecutor));
        maxComputeMetadataCacheMgr = new CatalogScopedCacheMgr<>(MaxComputeMetadataCache::fromCatalog);
        hiveMetaStoreCacheMgr = new CatalogScopedCacheMgr<>(
                catalog -> new HiveMetaStoreCache((HMSExternalCatalog) catalog,
                        commonRefreshExecutor, fileListingExecutor));
        icebergMetadataCacheMgr = new CatalogScopedCacheMgr<>(
                catalog -> new IcebergMetadataCache(catalog, commonRefreshExecutor));
        paimonMetadataCacheMgr = new CatalogScopedCacheMgr<>(
                catalog -> new PaimonMetadataCache(catalog, commonRefreshExecutor));

        registerEngineFactory(PaimonEngineCache.ENGINE_TYPE,
                catalog -> new PaimonEngineCache(paimonMetadataCacheMgr.getCache(catalog)));
        registerEngineFactory(IcebergEngineCache.ENGINE_TYPE,
                catalog -> new IcebergEngineCache(icebergMetadataCacheMgr.getCache(catalog)));
        registerEngineFactory(MaxComputeEngineCache.ENGINE_TYPE,
                catalog -> new MaxComputeEngineCache(
                        maxComputeMetadataCacheMgr.getCache(catalog)));
        registerEngineFactory(HiveEngineCache.ENGINE_TYPE,
                catalog -> new HiveEngineCache(hiveMetaStoreCacheMgr.getCache(catalog)));
        registerEngineFactory(HudiEngineCache.ENGINE_TYPE,
                catalog -> {
                    HudiCatalogCache hudiCatalogCache = hudiCatalogCacheMgr.getCache(catalog);
                    return new HudiEngineCache(
                            hudiCatalogCache.getPartitionProcessor(),
                            hudiCatalogCache.getFsViewProcessor(),
                            hudiCatalogCache.getMetaClientProcessor());
                });
    }

    public void registerEngineFactory(String engineType,
            Function<ExternalCatalog, ? extends EngineMetaCache> engineFactory) {
        String normalizedEngineType = normalizeEngineType(engineType);
        engineFactories.put(normalizedEngineType, Objects.requireNonNull(engineFactory, "engineFactory is null"));
    }

    public Function<ExternalCatalog, ? extends EngineMetaCache> removeEngineFactory(String engineType) {
        return engineFactories.remove(normalizeEngineType(engineType));
    }

    public Set<String> getRegisteredEngineTypes() {
        return ImmutableSet.copyOf(engineFactories.keySet());
    }

    public EngineMetaCache getOrCreateEngineMetaCache(ExternalCatalog catalog, String engineType) {
        ExternalCatalog nonNullCatalog = Objects.requireNonNull(catalog, "catalog cannot be null");
        String normalizedEngineType = normalizeEngineType(engineType);
        Function<ExternalCatalog, ? extends EngineMetaCache> engineFactory = engineFactories.get(normalizedEngineType);
        if (engineFactory == null) {
            throw new IllegalArgumentException("Engine cache factory is not registered: " + normalizedEngineType);
        }
        CatalogMetaCache catalogMetaCache = getOrCreateCatalogMetaCache(nonNullCatalog);
        return catalogMetaCache.getOrCreateEngineMetaCache(normalizedEngineType,
                () -> engineFactory.apply(nonNullCatalog), nonNullCatalog);
    }

    public <T extends EngineMetaCache> T getOrCreateEngineMetaCache(
            ExternalCatalog catalog, String engineType, Class<T> expectedClass) {
        return castEngineMetaCache(
                getOrCreateEngineMetaCache(catalog, engineType), expectedClass, normalizeEngineType(engineType));
    }

    public EngineMetaCache getOrCreateEngineMetaCache(ExternalTable table) {
        ExternalCatalog catalog = requireCatalog(table);
        return getOrCreateEngineMetaCache(catalog, resolveEngineType(table));
    }

    public <T extends EngineMetaCache> T getOrCreateEngineMetaCache(ExternalTable table, Class<T> expectedClass) {
        ExternalCatalog catalog = requireCatalog(table);
        String engineType = resolveEngineType(table);
        return castEngineMetaCache(
                getOrCreateEngineMetaCache(catalog, engineType), expectedClass, engineType);
    }

    public Optional<SchemaCacheValue> getSchema(ExternalTable table, long versionToken) {
        ExternalCatalog catalog = requireCatalog(table);
        CatalogMetaCache catalogMetaCache = getOrCreateCatalogMetaCache(catalog);
        return catalogMetaCache.getSchema(table, versionToken);
    }

    public EngineMetaCache getEngineMetaCache(long catalogId, String engineType) {
        CatalogMetaCache catalogMetaCache = catalogMetaCaches.get(catalogId);
        return catalogMetaCache == null ? null : catalogMetaCache.getEngineMetaCache(engineType);
    }

    public <T extends EngineMetaCache> T getEngineMetaCache(long catalogId, String engineType, Class<T> expectedClass) {
        EngineMetaCache engineMetaCache = getEngineMetaCache(catalogId, engineType);
        return engineMetaCache == null ? null
                : castEngineMetaCache(engineMetaCache, expectedClass, normalizeEngineType(engineType));
    }

    public CatalogMetaCache getCatalogMetaCache(long catalogId) {
        return catalogMetaCaches.get(catalogId);
    }

    public CatalogMetaCache getCatalogMetaCache(ExternalCatalog catalog) {
        return getCatalogMetaCache(Objects.requireNonNull(catalog, "catalog cannot be null").getId());
    }

    public CatalogMetaCache removeCatalogMetaCache(long catalogId) {
        CatalogMetaCache catalogMetaCache = catalogMetaCaches.remove(catalogId);
        if (catalogMetaCache != null) {
            catalogMetaCache.invalidateCatalog();
        }
        removeCatalogBackingCaches(catalogId);
        return catalogMetaCache;
    }

    public void invalidateCatalogCache(long catalogId) {
        withCatalogMetaCache(catalogId, CatalogMetaCache::invalidateCatalog);
    }

    public void invalidateDbCache(long catalogId, String dbName) {
        withCatalogMetaCache(catalogId, cache -> cache.invalidateDb(dbName));
    }

    public void invalidateTableCache(ExternalTable table) {
        CatalogMetaCache catalogMetaCache = getCatalogMetaCache(table);
        if (catalogMetaCache != null) {
            catalogMetaCache.invalidateTable(table);
        }
    }

    public void invalidateSchemaCatalog(long catalogId) {
        withCatalogMetaCache(catalogId, CatalogMetaCache::invalidateSchemaCatalog);
    }

    public void invalidateSchemaDb(long catalogId, String dbName) {
        withCatalogMetaCache(catalogId, cache -> cache.invalidateSchemaDb(dbName));
    }

    public void invalidateSchemaTable(ExternalTable table) {
        CatalogMetaCache catalogMetaCache = getCatalogMetaCache(table);
        if (catalogMetaCache != null) {
            catalogMetaCache.invalidateSchemaTable(table);
        }
    }

    private static String normalizeEngineType(String engineType) {
        String trimmedEngineType = Objects.requireNonNull(engineType, "engineType cannot be null").trim();
        if (trimmedEngineType.isEmpty()) {
            throw new IllegalArgumentException("engineType cannot be empty");
        }
        return trimmedEngineType.toLowerCase(Locale.ROOT);
    }

    private static <T extends EngineMetaCache> T castEngineMetaCache(
            EngineMetaCache engineMetaCache, Class<T> expectedClass, String engineType) {
        if (!expectedClass.isInstance(engineMetaCache)) {
            throw new IllegalStateException("Engine cache type mismatch for engine '" + engineType
                    + "', expected " + expectedClass.getSimpleName()
                    + " but was " + engineMetaCache.getClass().getSimpleName());
        }
        return expectedClass.cast(engineMetaCache);
    }

    private static String resolveEngineType(ExternalTable table) {
        if (table instanceof IcebergExternalTable) {
            return IcebergEngineCache.ENGINE_TYPE;
        }
        if (table instanceof PaimonExternalTable) {
            return PaimonEngineCache.ENGINE_TYPE;
        }
        if (table instanceof MaxComputeExternalTable) {
            return MaxComputeEngineCache.ENGINE_TYPE;
        }
        if (table instanceof HMSExternalTable) {
            switch (((HMSExternalTable) table).getDlaType()) {
                case HIVE:
                    return HiveEngineCache.ENGINE_TYPE;
                case ICEBERG:
                    return IcebergEngineCache.ENGINE_TYPE;
                case HUDI:
                    return HudiEngineCache.ENGINE_TYPE;
                default:
                    throw new IllegalArgumentException("Unsupported HMS table type: "
                            + ((HMSExternalTable) table).getDlaType() + ", table="
                            + table.getNameWithFullQualifiers());
            }
        }
        throw new IllegalArgumentException("Unsupported external table type for engine cache: "
                + table.getClass().getSimpleName() + ", table=" + table.getNameWithFullQualifiers());
    }

    private CatalogMetaCache getOrCreateCatalogMetaCache(ExternalCatalog catalog) {
        return catalogMetaCaches.computeIfAbsent(catalog.getId(), ignored -> new CatalogMetaCache(catalog));
    }

    private CatalogMetaCache getCatalogMetaCache(ExternalTable table) {
        return catalogMetaCaches.get(requireCatalog(table).getId());
    }

    private ExternalCatalog requireCatalog(ExternalTable table) {
        Objects.requireNonNull(table, "table cannot be null");
        return Objects.requireNonNull(table.getCatalog(), "table catalog cannot be null");
    }

    private void removeCatalogBackingCaches(long catalogId) {
        hiveMetaStoreCacheMgr.removeCache(catalogId);
        icebergMetadataCacheMgr.removeCache(catalogId);
        HudiCatalogCache hudiCatalogCache = hudiCatalogCacheMgr.removeCache(catalogId);
        if (hudiCatalogCache != null) {
            hudiCatalogCache.cleanUp();
        }
        MaxComputeMetadataCache maxComputeMetadataCache = maxComputeMetadataCacheMgr.removeCache(catalogId);
        if (maxComputeMetadataCache != null) {
            maxComputeMetadataCache.cleanUp();
        }
        PaimonMetadataCache paimonMetadataCache = paimonMetadataCacheMgr.removeCache(catalogId);
        if (paimonMetadataCache != null) {
            paimonMetadataCache.invalidateCatalogCache(catalogId);
        }
    }

    private void withCatalogMetaCache(long catalogId, Consumer<CatalogMetaCache> consumer) {
        CatalogMetaCache catalogMetaCache = catalogMetaCaches.get(catalogId);
        if (catalogMetaCache != null) {
            consumer.accept(catalogMetaCache);
        }
    }
}
