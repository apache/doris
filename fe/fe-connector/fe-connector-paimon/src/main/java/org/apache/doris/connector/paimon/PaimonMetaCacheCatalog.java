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
import org.apache.doris.connector.cache.JvmSizeUtils;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.MetaCacheSizeEstimators;
import org.apache.doris.connector.cache.ScopePath;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.privilege.PrivilegedCatalog;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.utils.SegmentsCache;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Doris-owned replacement for Paimon's {@code CachingCatalog}. Table and database entries live in
 * {@link CatalogMetaCache}, so a Doris catalog/database/table invalidation fences every matching
 * in-flight load and cached value.
 *
 * <p>The cache retains raw table metadata. Paimon's privilege catalog is applied outside this
 * wrapper so every lookup receives a fresh checker instead of caching one authorization snapshot.
 *
 * <p>The user's {@code paimon.cache-enabled} and access/write expiry settings remain authoritative.
 * The Paimon SDK wrapper itself is disabled because a second hidden table cache cannot participate
 * in Doris invalidation. Under a Doris weight budget, mutable SDK snapshot/stats/manifest caches are
 * not attached: their post-publication growth cannot be reweighed by the enclosing budget.
 */
final class PaimonMetaCacheCatalog extends DelegateCatalog {

    private static final int DATABASE_CACHE_CAPACITY = 100;
    static final long TABLE_ENTRY_OVERHEAD_BYTES = JvmSizeUtils.saturatedAdd(
            JvmSizeUtils.instanceSize(ExpiringValue.class), JvmSizeUtils.instanceSize(AtomicLong.class));

    private final CatalogMetaCache metaCache;
    private final MetaCache<Identifier, ExpiringValue<Table>> tableCache;
    private final MetaCache<String, ExpiringValue<Database>> databaseCache;
    private final SegmentsCache<Path> manifestCache;
    private final long tableExpireAfterAccessNanos;
    private final long databaseExpireAfterAccessNanos;
    private final long expireAfterWriteNanos;
    private final int snapshotMaxNumPerTable;
    private final boolean attachSdkCaches;
    private final LongSupplier nanoTime;
    private final BiConsumer<String, Object> cacheMissObserver;

    static Catalog tryToCreate(Catalog wrapped, CatalogMetaCache metaCache, int tableCacheMaxSize,
            long tableCacheTtlSecond, Options catalogOptions, boolean cacheEnabled,
            boolean hasEnclosingWeightLimit) {
        return tryToCreate(wrapped, metaCache, tableCacheMaxSize, tableCacheTtlSecond,
                catalogOptions, cacheEnabled, hasEnclosingWeightLimit,
                catalog -> PrivilegedCatalog.tryToCreate(catalog, catalogOptions));
    }

    static Catalog tryToCreate(Catalog wrapped, CatalogMetaCache metaCache, int tableCacheMaxSize,
            long tableCacheTtlSecond, Options catalogOptions, boolean cacheEnabled,
            boolean hasEnclosingWeightLimit, Function<Catalog, Catalog> decorator) {
        PaimonMetaCacheCatalog cached = null;
        try {
            cached = new PaimonMetaCacheCatalog(wrapped, metaCache, tableCacheMaxSize,
                    tableCacheTtlSecond, catalogOptions, cacheEnabled, hasEnclosingWeightLimit,
                    System::nanoTime, (name, key) -> { });
            return decorator.apply(cached);
        } catch (RuntimeException | Error throwable) {
            if (cached != null) {
                try {
                    cached.unregisterCaches();
                } catch (RuntimeException | Error rollbackFailure) {
                    throwable.addSuppressed(rollbackFailure);
                }
            }
            try {
                wrapped.close();
            } catch (Exception | Error closeFailure) {
                throwable.addSuppressed(closeFailure);
            }
            throw throwable;
        }
    }

    PaimonMetaCacheCatalog(Catalog wrapped, CatalogMetaCache metaCache, int tableCacheMaxSize,
            long tableCacheTtlSecond, Options catalogOptions, boolean hasEnclosingWeightLimit,
            LongSupplier nanoTime) {
        this(wrapped, metaCache, tableCacheMaxSize, tableCacheTtlSecond, catalogOptions,
                true, hasEnclosingWeightLimit, nanoTime, (name, key) -> { });
    }

    PaimonMetaCacheCatalog(Catalog wrapped, CatalogMetaCache metaCache, int tableCacheMaxSize,
            long tableCacheTtlSecond, Options catalogOptions, boolean hasEnclosingWeightLimit,
            LongSupplier nanoTime, BiConsumer<String, Object> cacheMissObserver) {
        this(wrapped, metaCache, tableCacheMaxSize, tableCacheTtlSecond, catalogOptions,
                true, hasEnclosingWeightLimit, nanoTime, cacheMissObserver);
    }

    private PaimonMetaCacheCatalog(Catalog wrapped, CatalogMetaCache metaCache, int tableCacheMaxSize,
            long tableCacheTtlSecond, Options catalogOptions, boolean cacheEnabled,
            boolean hasEnclosingWeightLimit, LongSupplier nanoTime,
            BiConsumer<String, Object> cacheMissObserver) {
        super(wrapped);
        this.metaCache = metaCache;
        this.nanoTime = nanoTime;
        this.cacheMissObserver = cacheMissObserver;

        Duration expireAfterAccess = cacheEnabled
                ? catalogOptions.get(CatalogOptions.CACHE_EXPIRE_AFTER_ACCESS) : null;
        Duration expireAfterWrite = cacheEnabled
                ? catalogOptions.get(CatalogOptions.CACHE_EXPIRE_AFTER_WRITE) : null;
        if (cacheEnabled) {
            requirePositive(expireAfterAccess, CatalogOptions.CACHE_EXPIRE_AFTER_ACCESS.key());
            requirePositive(expireAfterWrite, CatalogOptions.CACHE_EXPIRE_AFTER_WRITE.key());
        }
        long paimonAccessNanos = cacheEnabled ? saturatedNanos(expireAfterAccess) : Long.MAX_VALUE;
        this.tableExpireAfterAccessNanos = cacheEnabled && tableCacheTtlSecond > 0
                ? Math.min(paimonAccessNanos, saturatedNanos(Duration.ofSeconds(tableCacheTtlSecond)))
                : paimonAccessNanos;
        this.databaseExpireAfterAccessNanos = paimonAccessNanos;
        this.expireAfterWriteNanos = cacheEnabled ? saturatedNanos(expireAfterWrite) : Long.MAX_VALUE;
        this.attachSdkCaches = cacheEnabled && !hasEnclosingWeightLimit;
        this.manifestCache = attachSdkCaches ? buildManifestCache(catalogOptions) : null;
        this.snapshotMaxNumPerTable = attachSdkCaches
                ? catalogOptions.get(CatalogOptions.CACHE_SNAPSHOT_MAX_NUM_PER_TABLE) : 0;

        CacheSpec tableSpec = CacheSpec.of(cacheEnabled,
                cacheEnabled && tableCacheTtlSecond > 0
                        ? CacheSpec.CACHE_NO_TTL : CacheSpec.CACHE_TTL_DISABLE_CACHE,
                tableCacheMaxSize);
        MetaCache<Identifier, ExpiringValue<Table>> createdTableCache = metaCache.create(
                MetaCacheDefinition
                        .<Identifier, ExpiringValue<Table>>builder("paimon-table", tableSpec,
                                id -> ScopePath.table(id.getDatabaseName(), id.getTableName()))
                        .sizeEstimator((id, value) -> PaimonCacheSizeEstimator.estimateTable(
                                id, value.value, TABLE_ENTRY_OVERHEAD_BYTES))
                        .build());
        try {
            CacheSpec dbSpec = CacheSpec.of(cacheEnabled, cacheEnabled
                    ? CacheSpec.CACHE_NO_TTL : CacheSpec.CACHE_TTL_DISABLE_CACHE, DATABASE_CACHE_CAPACITY);
            this.databaseCache = metaCache.create(MetaCacheDefinition
                    .<String, ExpiringValue<Database>>builder("paimon-database", dbSpec, ScopePath::database)
                    .sizeEstimator(MetaCacheSizeEstimators.reflective())
                    .build());
        } catch (RuntimeException | Error throwable) {
            try {
                metaCache.remove(createdTableCache);
            } catch (RuntimeException | Error rollbackFailure) {
                throwable.addSuppressed(rollbackFailure);
            }
            throw throwable;
        }
        this.tableCache = createdTableCache;
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        if (identifier.isSystemTable()) {
            Identifier origin = new Identifier(identifier.getDatabaseName(), identifier.getTableName(),
                    identifier.getBranchName(), null);
            Table originTable = getTable(origin);
            if (!(originTable instanceof FileStoreTable)) {
                return super.getTable(identifier);
            }
            Table systemTable = SystemTableLoader.load(identifier.getSystemTableName(),
                    (FileStoreTable) originTable);
            if (systemTable == null) {
                throw new TableNotExistException(identifier);
            }
            return systemTable;
        }

        while (true) {
            ExpiringValue<Table> cached = tableCache.getIfPresent(identifier);
            if (cached == null) {
                cacheMissObserver.accept("table", identifier);
                try {
                    cached = tableCache.get(identifier, ignored -> {
                        try {
                            Table loaded = attachPerTableCaches(super.getTable(identifier));
                            return new ExpiringValue<>(loaded, nanoTime.getAsLong());
                        } catch (TableNotExistException e) {
                            throw new CatalogLoadException(e);
                        }
                    });
                } catch (CatalogLoadException e) {
                    throw (TableNotExistException) e.getCause();
                }
            }
            if (cached.tryAccess(nanoTime.getAsLong(), tableExpireAfterAccessNanos,
                    expireAfterWriteNanos)) {
                return cached.value;
            }
            tableCache.compareAndSet(identifier, cached, null);
        }
    }

    @Override
    public Database getDatabase(String name) throws DatabaseNotExistException {
        while (true) {
            ExpiringValue<Database> cached = databaseCache.getIfPresent(name);
            if (cached == null) {
                cacheMissObserver.accept("database", name);
                try {
                    cached = databaseCache.get(name, ignored -> {
                        try {
                            return new ExpiringValue<>(super.getDatabase(name), nanoTime.getAsLong());
                        } catch (DatabaseNotExistException e) {
                            throw new CatalogLoadException(e);
                        }
                    });
                } catch (CatalogLoadException e) {
                    throw (DatabaseNotExistException) e.getCause();
                }
            }
            if (cached.tryAccess(nanoTime.getAsLong(), databaseExpireAfterAccessNanos,
                    expireAfterWriteNanos)) {
                return cached.value;
            }
            databaseCache.compareAndSet(name, cached, null);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        try {
            super.dropDatabase(name, ignoreIfNotExists, cascade);
        } finally {
            metaCache.invalidateDatabase(name);
        }
    }

    @Override
    public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
            throws DatabaseNotExistException {
        try {
            super.alterDatabase(name, changes, ignoreIfNotExists);
        } finally {
            metaCache.invalidateDatabase(name);
        }
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        try {
            super.dropTable(identifier, ignoreIfNotExists);
        } finally {
            invalidateTable(identifier);
        }
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        try {
            super.renameTable(fromTable, toTable, ignoreIfNotExists);
        } finally {
            invalidateTable(fromTable);
            invalidateTable(toTable);
        }
    }

    @Override
    public void alterTable(Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        try {
            super.alterTable(identifier, changes, ignoreIfNotExists);
        } finally {
            invalidateTable(identifier);
        }
    }

    @Override
    public void invalidateTable(Identifier identifier) {
        metaCache.invalidateTable(identifier.getDatabaseName(), identifier.getTableName());
        super.invalidateTable(identifier);
    }

    @Override
    public CatalogLoader catalogLoader() {
        return wrapped.catalogLoader();
    }

    private void unregisterCaches() {
        metaCache.remove(databaseCache);
        metaCache.remove(tableCache);
    }

    private Table attachPerTableCaches(Table table) {
        if (!attachSdkCaches || !(table instanceof FileStoreTable)) {
            return table;
        }
        FileStoreTable storeTable = (FileStoreTable) table;
        Duration expireAfterAccess = Duration.ofNanos(databaseExpireAfterAccessNanos);
        Duration expireAfterWrite = Duration.ofNanos(expireAfterWriteNanos);
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

    private static void requirePositive(Duration duration, String option) {
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("When '" + option
                    + "' is set to negative or 0, the catalog cache should be disabled.");
        }
    }

    private static long saturatedNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static final class ExpiringValue<V> {
        private final V value;
        private final long createdNanos;
        private final AtomicLong lastAccessNanos;

        private ExpiringValue(V value, long createdNanos) {
            this.value = value;
            this.createdNanos = createdNanos;
            this.lastAccessNanos = new AtomicLong(createdNanos);
        }

        private boolean tryAccess(long now, long expireAfterAccessNanos, long expireAfterWriteNanos) {
            while (true) {
                long lastAccess = lastAccessNanos.get();
                if (now - createdNanos >= expireAfterWriteNanos
                        || now - lastAccess >= expireAfterAccessNanos) {
                    return false;
                }
                if (now <= lastAccess) {
                    return true;
                }
                if (lastAccessNanos.compareAndSet(lastAccess, now)) {
                    return true;
                }
            }
        }
    }

    private static final class CatalogLoadException extends RuntimeException {
        private CatalogLoadException(Exception cause) {
            super(cause);
        }
    }
}
