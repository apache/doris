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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.ScopePath;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Per-catalog cache of an iceberg manifest's parsed files, keyed by {@link IcebergManifestEntryKey}
 * (manifest path + content). Ported from the legacy fe-core {@code IcebergExternalMetaCache} manifest entry +
 * {@code IcebergManifestCacheLoader}, now backed by the shared {@link MetaCache} framework.
 *
 * <p>Consumed by {@link IcebergScanPlanProvider}'s manifest-level planning path (gated by
 * {@code meta.cache.iceberg.manifest.enable}, default off — the default scan path is the iceberg SDK
 * {@code planFiles()}). The external enable-gate lives in the scan provider (which decides whether to take the
 * full manifest-planning path); the compact equality-delete field-id projection is always reused per immutable
 * snapshot. Within one catalog the same manifest file is parsed once and shared across queries (and across
 * tables that reference it).
 *
 * <p><b>No TTL; capacity-bounded; cleared on REFRESH CATALOG.</b> This mirrors the legacy entry's
 * {@code contextualOnly(CacheSpec.of(false, CACHE_NO_TTL, 100_000))} default spec: a manifest's content is
 * immutable for a given path, so entries never go stale and need no TTL; a table-level invalidation (REFRESH
 * TABLE) intentionally keeps them ({@code IcebergConnector.invalidateTable} does not touch this cache, legacy
 * parity). It is cleared by {@link #invalidateAll()} on a REFRESH CATALOG (via
 * {@link IcebergConnector#invalidateAll()}, mirroring legacy catalog-wide {@code group.invalidateAll()}) and
 * dropped wholesale when the {@link IcebergConnector} is rebuilt (ADD/MODIFY CATALOG). Overflow is bounded by
 * Caffeine {@code maximumSize} eviction (re-reads are harmless — the value is immutable). The parse loader runs
 * OUTSIDE Caffeine's compute lock (manual miss-load, single-flight per key).
 */
final class IcebergManifestCache {

    /** Legacy effective capacity (fe-core {@code DEFAULT_MANIFEST_CACHE_CAPACITY}, asserted in its stats tests). */
    static final int DEFAULT_MANIFEST_CACHE_CAPACITY = 100_000;

    /** Leak backstop for the per-scan stats stash (see {@link #statsByQuery}); far above any live entry's life. */
    private static final long DEFAULT_STATS_TTL_SECONDS = 300L;

    private final CatalogMetaCache owner;
    private final MetaCache<IcebergManifestEntryKey, ManifestCacheValue> entry;
    private final MetaCache<SnapshotKey, Set<Integer>> equalityDeleteFieldIds;

    /** Immutable snapshot key for the compact equality-delete field-id projection. */
    private static final class SnapshotKey {
        private final String tableLocation;
        private final long snapshotId;

        private SnapshotKey(String tableLocation, long snapshotId) {
            this.tableLocation = tableLocation;
            this.snapshotId = snapshotId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SnapshotKey)) {
                return false;
            }
            SnapshotKey that = (SnapshotKey) o;
            return snapshotId == that.snapshotId && Objects.equals(tableLocation, that.tableLocation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableLocation, snapshotId);
        }
    }

    // Per-scan manifest-cache access tally, keyed by the statement's stable queryId
    // (ConnectorSession.getQueryId()), so VERBOSE EXPLAIN can report THIS scan's hits/misses/failures (the
    // "manifest cache:" line). The provider that PLANS the scan and the (transient, fresh-per-call) provider that
    // renders EXPLAIN are different instances, so per-scan state cannot live on the provider; the long-lived
    // per-catalog cache is their only shared survivor (the queryId keys this per-scan tally onto it).
    // {@link #takeStats} is the primary eviction (EXPLAIN drains its entry); a non-EXPLAIN query records but
    // never drains, so its leaked entry is aged out by the lazy TTL sweep in {@link #touch}.
    private final Map<String, ScanStats> statsByQuery = new ConcurrentHashMap<>();
    private final long statsTtlNanos;
    private final LongSupplier nanoClock;

    /** One in-flight statement's manifest-cache access tally, plus a touch stamp for the leak sweep. */
    private static final class ScanStats {
        long hits;
        long misses;
        long failures;
        volatile long lastTouchNanos;

        ScanStats(long nowNanos) {
            this.lastTouchNanos = nowNanos;
        }
    }

    IcebergManifestCache() {
        this(new CatalogMetaCache(), DEFAULT_MANIFEST_CACHE_CAPACITY);
    }

    IcebergManifestCache(CatalogMetaCache owner) {
        this(owner, DEFAULT_MANIFEST_CACHE_CAPACITY);
    }

    IcebergManifestCache(int maxSize) {
        this(new CatalogMetaCache(), maxSize);
    }

    private IcebergManifestCache(CatalogMetaCache owner, int maxSize) {
        this(owner, maxSize, DEFAULT_STATS_TTL_SECONDS, System::nanoTime);
    }

    /** Visible for testing: injectable stats TTL + clock so the leak sweep is deterministic without sleeping. */
    IcebergManifestCache(int maxSize, long statsTtlSeconds, LongSupplier nanoClock) {
        this(new CatalogMetaCache(), maxSize, statsTtlSeconds, nanoClock);
    }

    private IcebergManifestCache(
            CatalogMetaCache owner, int maxSize, long statsTtlSeconds, LongSupplier nanoClock) {
        this.owner = owner;
        // Always enabled, no expiry, capacity-bounded (CACHE_NO_TTL == -1 means "no expiration", enabled).
        CacheSpec spec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, Math.max(1, maxSize));
        this.entry = owner.create(MetaCacheDefinition
                .<IcebergManifestEntryKey, ManifestCacheValue>builder(
                        "iceberg-manifest", spec, ignored -> ScopePath.catalog())
                .build());
        this.equalityDeleteFieldIds = owner.create(MetaCacheDefinition
                .<SnapshotKey, Set<Integer>>builder(
                        "iceberg-equality-delete-field-ids", spec, ignored -> ScopePath.catalog())
                .build());
        this.statsTtlNanos = TimeUnit.SECONDS.toNanos(Math.max(1L, statsTtlSeconds));
        this.nanoClock = nanoClock;
    }

    /**
     * Returns the equality-delete field ids for one immutable snapshot. Unlike the optional full manifest-file
     * cache, this compact projection is always reused so scan properties do not re-read every delete manifest on
     * every query. Loader failures are deliberately not cached, preserving retry after transient storage errors.
     */
    Set<Integer> getOrLoadEqualityDeleteFieldIds(
            String tableLocation, long snapshotId, Supplier<Set<Integer>> loader) {
        SnapshotKey key = new SnapshotKey(tableLocation, snapshotId);
        return equalityDeleteFieldIds.get(key,
                ignored -> Collections.unmodifiableSet(new HashSet<>(loader.get())));
    }

    /**
     * Returns the parsed files for {@code manifest}, loading (and reading from storage) only on a miss. The
     * loader runs OUTSIDE Caffeine's compute lock (manual miss-load; single-flight per key), so a same-key
     * miss parses at most once.
     */
    ManifestCacheValue getManifestCacheValue(ManifestFile manifest, Table table) {
        IcebergManifestEntryKey key = IcebergManifestEntryKey.of(manifest);
        return entry.get(key, k -> loadManifestCacheValue(manifest, table, k.getContent()));
    }

    /**
     * Same as {@link #getManifestCacheValue(ManifestFile, Table)} but tallies this access as a hit or miss under
     * {@code queryId} (a hit == the entry was already cached BEFORE this load, matching the legacy
     * {@code getIfPresent(key) != null} probe). Within one statement the manifest-level plan processes manifests
     * sequentially, so the per-query counters need no synchronization. A blank queryId is not recorded.
     */
    ManifestCacheValue getManifestCacheValue(ManifestFile manifest, Table table, String queryId) {
        IcebergManifestEntryKey key = IcebergManifestEntryKey.of(manifest);
        boolean hit = entry.getIfPresent(key) != null;
        if (queryId != null && !queryId.isEmpty()) {
            ScanStats stats = touch(queryId);
            if (hit) {
                stats.hits++;
            } else {
                stats.misses++;
            }
        }
        return entry.get(key, k -> loadManifestCacheValue(manifest, table, k.getContent()));
    }

    /**
     * Records one manifest-level planning failure for {@code queryId} (the scan provider fell back to the SDK
     * scan). Mirrors the legacy {@code manifestCacheFailures} bump. A blank queryId is not recorded.
     */
    void recordFailure(String queryId) {
        if (queryId != null && !queryId.isEmpty()) {
            touch(queryId).failures++;
        }
    }

    /**
     * Returns and REMOVES this query's {@code {hits, misses, failures}} tally (zeros when nothing was recorded).
     * The remove is the primary eviction — VERBOSE EXPLAIN drains its own entry once it has rendered the line.
     */
    long[] takeStats(String queryId) {
        if (queryId == null || queryId.isEmpty()) {
            return new long[] {0L, 0L, 0L};
        }
        ScanStats stats = statsByQuery.remove(queryId);
        return stats == null
                ? new long[] {0L, 0L, 0L}
                : new long[] {stats.hits, stats.misses, stats.failures};
    }

    /** Fetch (or lazily create) this query's tally, opportunistically aging out leaked entries first. */
    private ScanStats touch(String queryId) {
        long now = nanoClock.getAsLong();
        ScanStats stats = statsByQuery.get(queryId);
        if (stats == null) {
            // First access of a not-yet-seen query: age out leaked non-EXPLAIN entries. Done OUTSIDE
            // computeIfAbsent (ConcurrentHashMap forbids mutating other mappings from within it).
            sweepExpiredStats(now);
            stats = statsByQuery.computeIfAbsent(queryId, k -> new ScanStats(now));
        }
        stats.lastTouchNanos = now;
        return stats;
    }

    /** Drops stats entries untouched for longer than the TTL — leaked non-EXPLAIN query tallies only. */
    private void sweepExpiredStats(long nowNanos) {
        statsByQuery.entrySet().removeIf(e -> nowNanos - e.getValue().lastTouchNanos >= statsTtlNanos);
    }

    private static ManifestCacheValue loadManifestCacheValue(ManifestFile manifest, Table table,
            ManifestContent content) {
        try {
            if (content == ManifestContent.DELETES) {
                return ManifestCacheValue.forDeleteFiles(loadDeleteFiles(manifest, table));
            }
            return ManifestCacheValue.forDataFiles(loadDataFiles(manifest, table));
        } catch (IOException e) {
            throw new RuntimeException("Failed to read iceberg manifest " + manifest.path(), e);
        }
    }

    private static List<DataFile> loadDataFiles(ManifestFile manifest, Table table) throws IOException {
        List<DataFile> dataFiles = new ArrayList<>();
        // .copy() is mandatory: the ManifestReader iterator reuses the same object across iterations.
        try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, table.io())) {
            for (DataFile dataFile : reader) {
                dataFiles.add(dataFile.copy());
            }
        }
        return dataFiles;
    }

    private static List<DeleteFile> loadDeleteFiles(ManifestFile manifest, Table table) throws IOException {
        List<DeleteFile> deleteFiles = new ArrayList<>();
        try (ManifestReader<DeleteFile> reader =
                ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
            for (DeleteFile deleteFile : reader) {
                deleteFiles.add(deleteFile.copy());
            }
        }
        return deleteFiles;
    }

    /**
     * REFRESH CATALOG hook: drop every cached manifest. Called by {@link IcebergConnector#invalidateAll()}
     * (catalog-wide invalidation); table-level invalidation (REFRESH TABLE) intentionally does not.
     */
    void invalidateAll() {
        owner.invalidateCatalog();
        statsByQuery.clear();
    }

    void clearStats() {
        statsByQuery.clear();
    }

    /** Test-only: current number of cached entries (accurate map membership, not Caffeine's estimate). */
    int size() {
        return Math.toIntExact(entry.size());
    }
}
