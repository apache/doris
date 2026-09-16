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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.MetaCacheSizeEstimators;
import org.apache.doris.connector.cache.ScopePath;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.SegmentsCache;

import java.time.Duration;
import java.util.Optional;

/**
 * Doris-owned replacement for the Paimon SDK {@code CachingCatalog}. Every catalog-level cache
 * ({@code tableCache}, {@code databaseCache}) lives in Doris's {@link CatalogMetaCache} framework
 * with a per-catalog scope, so {@code REFRESH TABLE}/{@code REFRESH DATABASE}/
 * {@code REFRESH CATALOG} invalidates them through the same registry path as every other
 * connector-owned cache. The per-{@link FileStoreTable} caches ({@code snapshotCache},
 * {@code statsCache}, {@code manifestCache}) are built from the same {@link CatalogOptions} that
 * {@code CachingCatalog} reads and attached on {@link #getTable}, preserving scan-time performance.
 *
 * <p><b>Why not the SDK's CachingCatalog?</b> Its {@code tableCache} freezes schema/snapshot
 * state at load time and exposes only per-table {@code invalidateTable(Identifier)} — no
 * db/catalog-level eviction. After an external same-name drop/recreate the stale frozen
 * {@link Table} survives every Doris-side {@code REFRESH}.
 */
final class PaimonMetaCacheCatalog extends DelegateCatalog {

    private final MetaCache<Identifier, Table> tableCache;
    private final MetaCache<String, Database> databaseCache;
    private final SegmentsCache<Path> manifestCache;
    private final Duration expireAfterAccess;
    private final Duration expireAfterWrite;
    private final int snapshotMaxNumPerTable;

    PaimonMetaCacheCatalog(Catalog wrapped, CatalogMetaCache metaCache, int tableCacheMaxSize,
            long tableCacheTtlSecond, Options catalogOptions) {
        super(wrapped);
        CacheSpec tableSpec = CacheSpec.ofConnectorTtl(tableCacheTtlSecond, tableCacheMaxSize);
        this.tableCache = metaCache.create(MetaCacheDefinition
                .<Identifier, Table>builder("paimon-table", tableSpec,
                        id -> ScopePath.table(id.getDatabaseName(), id.getObjectName()))
                .sizeEstimator(MetaCacheSizeEstimators.reflective())
                .build());
        CacheSpec dbSpec = CacheSpec.ofConnectorTtl(86400L, 100);
        this.databaseCache = metaCache.create(MetaCacheDefinition
                .<String, Database>builder("paimon-database", dbSpec,
                        ScopePath::database)
                .sizeEstimator(MetaCacheSizeEstimators.reflective())
                .build());

        this.manifestCache = buildManifestCache(catalogOptions);
        this.expireAfterAccess = catalogOptions.get(
                CatalogOptions.CACHE_EXPIRE_AFTER_ACCESS);
        this.expireAfterWrite = catalogOptions.get(
                CatalogOptions.CACHE_EXPIRE_AFTER_WRITE);
        this.snapshotMaxNumPerTable = catalogOptions.get(
                CatalogOptions.CACHE_SNAPSHOT_MAX_NUM_PER_TABLE);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        try {
            return tableCache.get(identifier, ignored -> {
                try {
                    return attachPerTableCaches(super.getTable(identifier));
                } catch (TableNotExistException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof TableNotExistException) {
                throw (TableNotExistException) e.getCause();
            }
            throw e;
        }
    }

    @Override
    public Database getDatabase(String name) throws DatabaseNotExistException {
        try {
            return databaseCache.get(name, ignored -> {
                try {
                    return super.getDatabase(name);
                } catch (DatabaseNotExistException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof DatabaseNotExistException) {
                throw (DatabaseNotExistException) e.getCause();
            }
            throw e;
        }
    }

    @Override
    public void invalidateTable(Identifier identifier) {
        tableCache.invalidateKey(identifier);
        super.invalidateTable(identifier);
    }

    @Override
    public CatalogLoader catalogLoader() {
        return wrapped.catalogLoader();
    }

    // ---- per-Table caches (SDK-level, mirroring CachingCatalog.putTableCache) ----

    private Table attachPerTableCaches(Table table) {
        if (!(table instanceof FileStoreTable)) {
            return table;
        }
        FileStoreTable storeTable = (FileStoreTable) table;
        storeTable.setSnapshotCache(Caffeine.newBuilder()
                .softValues()
                .expireAfterAccess(expireAfterAccess)
                .expireAfterWrite(expireAfterWrite)
                .maximumSize(snapshotMaxNumPerTable)
                .executor(Runnable::run)
                .build());
        storeTable.setStatsCache(Caffeine.newBuilder()
                .softValues()
                .expireAfterAccess(expireAfterAccess)
                .expireAfterWrite(expireAfterWrite)
                .maximumSize(5)
                .executor(Runnable::run)
                .build());
        storeTable.setManifestCache(manifestCache);
        return table;
    }

    private static SegmentsCache<Path> buildManifestCache(Options options) {
        MemorySize manifestMaxMemory = options.get(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY);
        long manifestCacheThreshold = options.get(
                CatalogOptions.CACHE_MANIFEST_SMALL_FILE_THRESHOLD).getBytes();
        Optional<MemorySize> maxMemory = options.getOptional(CatalogOptions.CACHE_MANIFEST_MAX_MEMORY);
        if (maxMemory.isPresent() && maxMemory.get().compareTo(manifestMaxMemory) > 0) {
            manifestMaxMemory = maxMemory.get();
            manifestCacheThreshold = Long.MAX_VALUE;
        }
        return SegmentsCache.create(manifestMaxMemory, manifestCacheThreshold);
    }
}

