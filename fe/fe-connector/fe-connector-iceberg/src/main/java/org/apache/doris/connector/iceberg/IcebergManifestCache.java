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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-catalog cache of an iceberg manifest's parsed files, keyed by {@link IcebergManifestEntryKey}
 * (manifest path + content). Ported from the legacy fe-core {@code IcebergExternalMetaCache} manifest entry +
 * {@code IcebergManifestCacheLoader} (the connector cannot import fe-core, so this is a PORT, not a reuse).
 *
 * <p>Consumed by {@link IcebergScanPlanProvider}'s manifest-level planning path (gated by
 * {@code meta.cache.iceberg.manifest.enable}, default off — the default scan path is the iceberg SDK
 * {@code planFiles()}). Within one catalog the same manifest file is parsed once and shared across queries
 * (and across tables that reference it).
 *
 * <p><b>No TTL; capacity-bounded; cleared on REFRESH CATALOG.</b> This mirrors the legacy entry's
 * registered {@code CacheSpec.of(false, CACHE_NO_TTL, 100_000)}: a manifest's content is immutable for a given
 * path, so entries never go stale and need no TTL; a table-level invalidation (REFRESH TABLE) intentionally
 * keeps them ({@code IcebergConnector.invalidateTable} does not touch this cache, legacy
 * {@code testInvalidateTableKeepsManifestCache} parity). It is cleared by {@link #invalidateAll()} on a
 * REFRESH CATALOG (via {@link IcebergConnector#invalidateAll()}, mirroring legacy catalog-wide
 * {@code group.invalidateAll()}) and dropped wholesale when the {@link IcebergConnector} is rebuilt
 * (ADD/MODIFY CATALOG). A best-effort size bound flushes on overflow (re-reads are harmless — the value is
 * immutable).
 */
final class IcebergManifestCache {

    /** Legacy effective capacity (fe-core {@code DEFAULT_MANIFEST_CACHE_CAPACITY}, asserted in its stats tests). */
    static final int DEFAULT_MANIFEST_CACHE_CAPACITY = 100_000;

    private final Map<IcebergManifestEntryKey, ManifestCacheValue> cache = new ConcurrentHashMap<>();
    private final int maxSize;

    IcebergManifestCache() {
        this(DEFAULT_MANIFEST_CACHE_CAPACITY);
    }

    IcebergManifestCache(int maxSize) {
        this.maxSize = Math.max(1, maxSize);
    }

    /**
     * Returns the parsed files for {@code manifest}, loading (and reading from storage) only on a miss. The
     * loader runs OUTSIDE any lock (no manifest I/O under a lock; not {@code computeIfAbsent}); a concurrent
     * same-key miss may parse twice — harmless because the value is immutable and identical.
     */
    ManifestCacheValue getManifestCacheValue(ManifestFile manifest, Table table) {
        IcebergManifestEntryKey key = IcebergManifestEntryKey.of(manifest);
        ManifestCacheValue hit = cache.get(key);
        if (hit != null) {
            return hit;
        }
        ManifestCacheValue loaded = loadManifestCacheValue(manifest, table, key.getContent());
        if (cache.size() >= maxSize) {
            cache.clear();
        }
        ManifestCacheValue prev = cache.putIfAbsent(key, loaded);
        return prev != null ? prev : loaded;
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
        cache.clear();
    }

    /** Test-only: current number of cached entries. */
    int size() {
        return cache.size();
    }
}
