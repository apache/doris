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
import org.apache.doris.connector.cache.MetaCacheEntry;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

/**
 * Per-catalog cache of an iceberg manifest's parsed files, keyed by {@link IcebergManifestEntryKey}
 * (manifest path + content). Ported from the legacy fe-core {@code IcebergExternalMetaCache} manifest entry +
 * {@code IcebergManifestCacheLoader}, now backed by the shared {@link MetaCacheEntry} framework
 * (independent-copy meta-cache migration).
 *
 * <p>Consumed by {@link IcebergScanPlanProvider}'s manifest-level planning path (gated by
 * {@code meta.cache.iceberg.manifest.enable}, default off — the default scan path is the iceberg SDK
 * {@code planFiles()}). The external enable-gate lives in the scan provider (which decides whether to take the
 * manifest-planning path at all); this cache is unconditionally on when consulted. Within one catalog the same
 * manifest file is parsed once and shared across queries (and across tables that reference it).
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

    private final MetaCacheEntry<IcebergManifestEntryKey, ManifestCacheValue> entry;

    IcebergManifestCache() {
        this(DEFAULT_MANIFEST_CACHE_CAPACITY);
    }

    IcebergManifestCache(int maxSize) {
        // Always enabled, no expiry, capacity-bounded (CACHE_NO_TTL == -1 means "no expiration", enabled).
        CacheSpec spec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, Math.max(1, maxSize));
        this.entry = new MetaCacheEntry<>("iceberg-manifest", null, spec,
                ForkJoinPool.commonPool(), false, true, 0L, true);
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
        entry.invalidateAll();
    }

    /** Test-only: current number of cached entries (accurate map membership, not Caffeine's estimate). */
    int size() {
        int[] count = {0};
        entry.forEach((key, value) -> count[0]++);
        return count[0];
    }
}
