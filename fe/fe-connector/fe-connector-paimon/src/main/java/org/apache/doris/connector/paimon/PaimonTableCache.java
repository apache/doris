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
import java.util.function.Supplier;

/**
 * Per-catalog cache of the paimon {@link Table} object, keyed by {@link Identifier} (db.table[.branch]),
 * replacing the Paimon SDK {@code CachingCatalog} tableCache.
 *
 * <p><b>Why Doris owns this cache.</b> {@code CachingCatalog} caches frozen {@link Table} objects (schema and
 * snapshot pointer pinned at load time) behind only a per-table {@code invalidateTable(Identifier)} API. After
 * an external same-name drop/recreate, {@code REFRESH TABLE}/{@code REFRESH DATABASE}/{@code REFRESH CATALOG}
 * clear Doris-side caches but the SDK keeps serving the stale {@code Table} — and the SDK exposes no
 * db/catalog-level eviction to fix that from the connector. Doris therefore forces {@code cache-enabled=false}
 * ({@link PaimonCatalogFactory#buildCatalogOptions}) and caches the {@code Table} here instead, registered in
 * the connector {@link CatalogMetaCache} with a per-table scope, so the framework's
 * table/database/catalog invalidation covers it exactly like every other Doris-owned cache.
 *
 * <p><b>Backing.</b> Reuses the shared {@link MetaCache} framework identically to
 * {@link PaimonLatestSnapshotCache}: a contextual, access-TTL entry whose per-key loader is supplied at
 * {@link #getOrLoad}, with manual miss-load on so the loader runs OUTSIDE Caffeine's compute lock
 * (single-flight per key). TTL is {@code meta.cache.paimon.table.ttl-second} — the same knob that governs the
 * snapshot cache: {@code <= 0} disables caching (every read goes live, matching the legacy "no-cache"
 * catalog).
 *
 * <p><b>SDK cache parity.</b> To preserve the performance layer {@code CachingCatalog} installed on each
 * {@link FileStoreTable} (snapshot/stat caches), the loader attaches the same per-table caches on load. The
 * per-catalog manifest {@link SegmentsCache} is kept on this cache and shared by every loaded table, exactly
 * as {@code CachingCatalog} shared its manifest cache, so scan-time manifest reads keep the same in-memory
 * hit rate without the SDK caching the {@code Table} object itself.
 */
final class PaimonTableCache {

    private final CatalogMetaCache owner;
    private final MetaCache<Identifier, Table> entry;
    private final SegmentsCache<Path> manifestCache;

    PaimonTableCache(long ttlSeconds, int maxSize) {
        this(CatalogMetaCache.unmanaged(), ttlSeconds, maxSize, new Options());
    }

    PaimonTableCache(CatalogMetaCache owner, long ttlSeconds, int maxSize, Options catalogOptions) {
        this.owner = owner;
        // "<= 0 disables" connector TTL contract, folded to CacheSpec's disable sentinel (CacheSpec.ofConnectorTtl).
        CacheSpec spec = CacheSpec.ofConnectorTtl(ttlSeconds, maxSize);
        this.entry = owner.create(MetaCacheDefinition
                .<Identifier, Table>builder("paimon-table", spec,
                        id -> ScopePath.table(id.getDatabaseName(), id.getObjectName()))
                .sizeEstimator(MetaCacheSizeEstimators.reflective())
                .build());
        this.manifestCache = buildManifestCache(catalogOptions);
    }

    /** Caching is on only when the TTL is positive; ttl-second &lt;= 0 means "always read live". */
    boolean isEnabled() {
        return entry.isEnabled();
    }

    /**
     * Returns the cached paimon {@link Table} for {@code identifier} if present and unexpired, else runs
     * {@code loader} (the live {@code catalog.getTable} read), attaches the SDK performance caches, caches
     * the result and returns it. When caching is disabled ({@link #isEnabled()} is false) {@code loader} runs
     * every call, the performance caches are still attached, and nothing is kept.
     */
    Table getOrLoad(Identifier identifier, Supplier<Table> loader) {
        return entry.get(identifier, ignored -> attachSdkCaches(loader.get()));
    }

    /** Drops the cached entry for one table so the next read goes live (REFRESH TABLE). */
    void invalidate(Identifier identifier) {
        owner.invalidateTable(identifier.getDatabaseName(), identifier.getObjectName());
    }

    /**
     * Drops every cached entry for one database so the next read of any of its tables goes live
     * (REFRESH DATABASE / a Doris-issued DROP DATABASE). Entries are scoped by
     * {@code ScopePath.table(db, table)} (see the {@link MetaCacheDefinition} builder), so a db match is
     * {@code databaseName} equality at the registry level.
     */
    void invalidateDb(String dbName) {
        owner.invalidateDatabase(dbName);
    }

    /** Drops all cached entries (REFRESH CATALOG). */
    void invalidateAll() {
        owner.invalidateCatalog();
    }

    /** Test-only: current number of cached entries (accurate map membership, not Caffeine's estimate). */
    int size() {
        return Math.toIntExact(entry.size());
    }

    /**
     * Mirrors {@code CachingCatalog.putTableCache}: snapshot/stat caches are per-{@link FileStoreTable}, the
     * manifest cache is the shared per-catalog {@link SegmentsCache} held by this cache instance. Only called
     * for FileStoreTable; system/branch wrappers are built over a base FileStoreTable on the FE side, so the
     * base's caches flow through regardless.
     */
    private Table attachSdkCaches(Table table) {
        if (!(table instanceof FileStoreTable)) {
            return table;
        }
        FileStoreTable storeTable = (FileStoreTable) table;
        storeTable.setSnapshotCache(Caffeine.newBuilder()
                .softValues()
                .expireAfterAccess(Duration.ofDays(1))
                .maximumSize(10)
                .executor(Runnable::run)
                .build());
        storeTable.setStatsCache(Caffeine.newBuilder()
                .softValues()
                .expireAfterAccess(Duration.ofDays(1))
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
