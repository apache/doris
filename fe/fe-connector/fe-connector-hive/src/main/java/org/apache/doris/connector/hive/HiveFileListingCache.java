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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.MetaCacheEntry;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;

import org.apache.hadoop.fs.UnsupportedFileSystemException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;

/**
 * The hive connector's own directory-listing cache — the second, separate cache layer of the D2 scan-side cache
 * (the metastore-metadata layer is {@link org.apache.doris.connector.hms.CachingHmsClient}). It memoizes the
 * (expensive) {@code FileSystem.listStatus} of each partition directory, keyed by {@code (db, table, location)}.
 *
 * <p><b>Why this exists.</b> Legacy fe-core kept directory listings in the engine-side
 * {@code HiveExternalMetaCache}'s {@code file} entry; once a hive catalog becomes plugin-driven that cache stops
 * routing to it, so without this connector-owned cache every scan (and every periodic row-count refresh) would
 * re-list every partition directory from the filesystem. Trino keeps the equivalent {@code CachingDirectoryLister}
 * as a layer separate from its metastore cache — D2 mirrors that split.</p>
 *
 * <p><b>Who reads it.</b> {@link HiveScanPlanProvider} (the scan hot path) and
 * {@link HiveConnectorMetadata#estimateDataSizeByListingFiles} (the row-count estimate) — both go through the SAME
 * instance, held as a {@code final} field on the per-catalog {@link HiveConnector} (the only object that outlives a
 * single query; the scan provider / metadata are rebuilt per call). A scan therefore warms the estimate and vice
 * versa.</p>
 *
 * <p><b>TCCL.</b> The entry is contextual-only + manual-miss + no auto-refresh, so the loader runs synchronously on
 * the CALLING thread — which is already pinned to the plugin classloader (the scan thread by
 * {@code PluginDrivenScanNode.onPluginClassLoader}; the stats thread by {@code estimateDataSizeByListingFiles}
 * itself). A background refresh thread would NOT inherit that pin and Hadoop's {@code FileSystem} reflection would
 * fail to resolve its impl.</p>
 *
 * <p><b>Failures are not cached, and are split by blast radius.</b> The loader never caches a failed load (matching
 * {@code MetaCacheEntry}'s null-is-a-miss / exception-propagates contract). A SYSTEMIC filesystem-resolution failure
 * ({@link FileSystem#forLocation} unresolvable scheme/storage, or a lazily-surfaced {@code "No FileSystem for
 * scheme"} — it fails for every partition of the table) is thrown as a plain {@link DorisConnectorException} and the
 * scan path lets it propagate to fail the query loud. A LOCAL per-directory failure ({@link FileSystem#list}: this
 * partition missing/unreadable) is thrown as {@link HiveDirectoryListingException} and the scan path skips just that
 * partition with a warning (the pre-cache tolerance). The estimate path degrades to {@code -1} on either. This keeps
 * a broken storage config from silently returning an empty scan (legacy failed {@code FileSystem.get} loud) while
 * preserving one-bad-partition resilience.</p>
 *
 * <p><b>Dormant.</b> {@code "hms"} is not in {@code SPI_READY_TYPES}, so no live catalog builds a
 * {@link HiveConnector}, so this cache is never instantiated for a live catalog until the flip. Byte-neutral for
 * every other connector.</p>
 */
public class HiveFileListingCache {

    /** Engine token for the {@code meta.cache.<engine>.<entry>.*} property namespace. */
    static final String ENGINE = "hive";
    /** {@code meta.cache.hive.file.*} — cached directory listings. */
    static final String ENTRY_FILE = "file";

    /**
     * Legacy fe-core catalog knob ({@code HMSExternalCatalog.FILE_META_CACHE_TTL_SECOND}) remapped onto this
     * cache's namespaced {@code meta.cache.hive.file.ttl-second} for backward compatibility. See the constructor.
     */
    static final String LEGACY_FILE_META_CACHE_TTL_SECOND = "file.meta.cache.ttl-second";

    // Legacy fe-core Config values, mirrored locally (the connector never touches fe-core Config):
    //   TTL      = Config.external_cache_expire_time_seconds_after_access (86400s = 24h)
    //   capacity = Config.max_external_file_cache_num                     (10000)
    static final long DEFAULT_TTL_SECOND = 86400L;
    static final long DEFAULT_FILE_CAPACITY = 10000L;

    /**
     * Catalog property controlling whether partition directories are listed recursively (descend into
     * sub-directories). Default {@code true} — legacy {@code HiveExternalMetaCache.getFileCache} defaulted the
     * same. When {@code false}, a table whose data lives in sub-directories silently loses those rows.
     */
    static final String RECURSIVE_DIRECTORIES_PROPERTY = "hive.recursive_directories";

    /**
     * The raw directory lister: the engine-injected Doris {@link FileSystem} in production
     * ({@link #listFromFileSystem}), a fake in unit tests. Injected so the cache's hit/miss/invalidation behaviour
     * is testable without a live filesystem (mirrors {@code HiveConnectorMetadata.estimateDataSize} injecting its
     * {@code ToLongFunction} size source). The {@code fs} is borrowed from {@code ConnectorContext.getFileSystem}
     * (engine-owned, per-catalog, must NOT be closed by the connector).
     */
    @FunctionalInterface
    interface DirectoryLister {
        List<HiveFileStatus> list(String location, FileSystem fs);
    }

    private final MetaCacheEntry<FileListingKey, List<HiveFileStatus>> cache;
    private final DirectoryLister lister;

    public HiveFileListingCache(Map<String, String> properties) {
        this(properties, defaultLister(properties));
    }

    /**
     * The production {@link DirectoryLister}: {@link #listFromFileSystem} with the catalog's
     * {@code hive.recursive_directories} flag (default {@code true}) baked in. The flag is a per-catalog
     * constant, so capturing it here makes every consumer of the shared cache (scan, size estimate, stats
     * sampling) recurse consistently without a hot-path signature change.
     */
    private static DirectoryLister defaultLister(Map<String, String> properties) {
        Map<String, String> props = properties == null ? Collections.emptyMap() : properties;
        boolean recursive = Boolean.parseBoolean(
                props.getOrDefault(RECURSIVE_DIRECTORIES_PROPERTY, "true"));
        return (location, fs) -> listFromFileSystem(location, fs, recursive);
    }

    HiveFileListingCache(Map<String, String> properties, DirectoryLister lister) {
        Map<String, String> props = properties == null ? Collections.emptyMap() : properties;
        // Translate the legacy fe-core catalog knob file.meta.cache.ttl-second into the namespaced key this cache
        // reads (mirrors HiveExternalMetaCache.catalogPropertyCompatibilityMap: FILE_META_CACHE_TTL_SECOND ->
        // ENTRY_FILE ttl). Without this, an "hms" catalog that set the legacy key silently kept the default 24h
        // file cache after the SPI cutover, so e.g. file.meta.cache.ttl-second=0 no longer disabled the listing
        // cache and a newly-written file in an already-listed partition stayed invisible until REFRESH.
        props = CacheSpec.applyCompatibilityMap(props,
                Collections.singletonMap(LEGACY_FILE_META_CACHE_TTL_SECOND,
                        CacheSpec.metaCacheTtlKey(ENGINE, ENTRY_FILE)));
        CacheSpec spec = CacheSpec.fromProperties(props, ENGINE, ENTRY_FILE,
                CacheSpec.of(true, DEFAULT_TTL_SECOND, DEFAULT_FILE_CAPACITY));
        // Contextual-only + manual-miss so the slow listStatus runs on the caller (TCCL-pinned) thread outside
        // Caffeine's sync compute lock, deduplicated by a striped lock — mirrors CachingHmsClient's entries.
        this.cache = new MetaCacheEntry<>("hive.file", null, spec, ForkJoinPool.commonPool(), false, true, 0L, true);
        this.lister = Objects.requireNonNull(lister, "lister can not be null");
    }

    /**
     * Lists the data files under {@code location} (recursively into non-hidden sub-directories when the catalog's
     * {@code hive.recursive_directories} is set, default {@code true}; directories and {@code _}/{@code .}
     * -prefixed hidden files removed), served from the cache. Keyed by {@code (db, table, location)} so
     * {@link #invalidateTable} can drop exactly one table's entries. The loader runs on the calling thread; an I/O
     * failure throws {@link DorisConnectorException} (and is NOT cached). The returned list is shared by reference
     * (immutable elements) — callers must treat it as read-only, the codebase-wide metadata-cache convention.
     */
    public List<HiveFileStatus> listDataFiles(String dbName, String tableName, String location, FileSystem fs) {
        return listDataFiles(dbName, tableName, location, Collections.emptyList(), fs);
    }

    /**
     * As {@link #listDataFiles(String, String, String, FileSystem)}, but tags the entry with the partition's
     * VALUES so {@link #invalidatePartitions} can drop exactly that partition — mirroring legacy
     * {@code HiveExternalMetaCache}'s per-partition file-cache invalidation keyed by {@code tableId +
     * partitionValues}. The values are part of the cache key, so the scan and size-estimate paths must pass the
     * SAME values for a given partition (both derive them from {@code HmsPartitionInfo.getValues()}); an
     * unpartitioned table passes an empty list.
     */
    public List<HiveFileStatus> listDataFiles(String dbName, String tableName, String location,
            List<String> partitionValues, FileSystem fs) {
        return cache.get(new FileListingKey(dbName, tableName, location, partitionValues),
                key -> lister.list(key.location, fs));
    }

    /** Drops every cached listing for one table. Backs {@code REFRESH TABLE}. */
    public void invalidateTable(String dbName, String tableName) {
        cache.invalidateIf(key -> key.matches(dbName, tableName));
    }

    /**
     * Drops the cached listings for exactly the given partitions of one table, matched by partition VALUES.
     * Mirrors legacy {@code HiveExternalMetaCache}'s per-partition file-cache invalidation (its {@code tableId +
     * partitionValues} predicate). The match is on values carried in the key — which the caller derives purely
     * from the partition NAME ({@code HiveWriteUtils.toPartitionValues}) — so it needs NO partition-metadata
     * lookup; that is precisely why an evicted partition-metadata entry can no longer leave a stale file listing
     * (the #65334 failure mode). Backs a partition add/drop/alter refresh.
     */
    public void invalidatePartitions(String dbName, String tableName, Set<List<String>> partitionValues) {
        if (partitionValues.isEmpty()) {
            return;
        }
        cache.invalidateIf(key -> key.matches(dbName, tableName)
                && partitionValues.contains(key.partitionValues));
    }

    /** Drops every cached listing for one database (all its tables). Backs {@code REFRESH DATABASE}. */
    public void invalidateDb(String dbName) {
        cache.invalidateIf(key -> key.matchesDb(dbName));
    }

    /** Drops the whole file-listing cache. Backs {@code REFRESH CATALOG}. */
    public void invalidateAll() {
        cache.invalidateAll();
    }

    /** Current number of cached directory listings — for unit tests only (mirrors iceberg manifestCache.size()). */
    long size() {
        long[] count = {0L};
        cache.forEach((key, value) -> count[0]++);
        return count[0];
    }

    /**
     * The production {@link DirectoryLister}: a LITERAL listing through the engine-injected Doris
     * {@link FileSystem} (a per-catalog {@code SpiSwitchingFileSystem}), filtering out directories and
     * {@code _}/{@code .}-prefixed hidden files (byte-parity with the pre-cache filters in
     * {@code HiveScanPlanProvider.listAndSplitFiles} and {@code HiveConnectorMetadata.sumCachedFileSizes}). When
     * {@code recursive} (from {@code hive.recursive_directories}, default {@code true}) it descends into non-hidden
     * sub-directories (see {@link #collectFiles}). A zero-length data file is kept (the scan splitter skips it; the
     * size estimate adds 0) so both consumers keep their exact prior behaviour.
     *
     * <p><b>Two-boundary failure split (byte-parity with the pre-cache {@code FileSystem.get}/{@code listStatus}
     * split):</b> {@link FileSystem#forLocation} does the scheme/storage resolution + concrete-FS construction (no
     * I/O) — a failure here (unresolvable scheme, no {@code StorageProperties}, factory error) is SYSTEMIC, wrapped
     * loud in a plain {@link DorisConnectorException}. {@link FileSystem#list} then does the actual directory
     * listing — a failure here is LOCAL (this partition missing/unreadable), wrapped in the skippable
     * {@link HiveDirectoryListingException}, EXCEPT a lazily-surfaced {@code "No FileSystem for scheme"}
     * ({@link UnsupportedFileSystemException}, the migration's own failure class — a missing engine-side FS impl,
     * affecting every partition) which is re-classified SYSTEMIC/loud by {@link #isSystemicResolutionFailure} so a
     * broken deployment fails the query instead of silently returning an empty scan. Neither is ever cached.
     *
     * <p><b>Literal listing:</b> {@code fs.list(loc)} (not {@code listFiles(loc)}) is used deliberately — the
     * per-scheme filesystems ({@code DFSFileSystem}, {@code S3CompatibleFileSystem}) override {@code listFiles} with
     * a glob-aware branch that would treat a location containing {@code [}/{@code *}/{@code ?} as a pattern; the old
     * {@code listStatus} never glob-expanded, and a hive location can legitimately contain those characters.
     */
    static List<HiveFileStatus> listFromFileSystem(String location, FileSystem fs) {
        return listFromFileSystem(location, fs, false);
    }

    static List<HiveFileStatus> listFromFileSystem(String location, FileSystem fs, boolean recursive) {
        if (fs == null) {
            // No engine filesystem for this catalog (empty storage): a SYSTEMIC config error affecting every
            // partition. Fail loud (the scan path does NOT skip this), never a silent empty scan.
            throw new DorisConnectorException("No filesystem configured for " + location);
        }
        Location loc = Location.of(location);
        FileSystem resolved;
        try {
            resolved = fs.forLocation(loc);
        } catch (IOException | RuntimeException e) {
            // Scheme/storage resolution or concrete-FS construction failed: a SYSTEMIC storage-config error
            // affecting every partition of the table. (RuntimeException is also caught: the FileSystem factory may
            // report a misconfiguration as an unchecked exception, which must still wrap uniformly as the loud
            // systemic type rather than escape untyped.)
            throw new DorisConnectorException("Failed to resolve filesystem for " + location, e);
        }
        try {
            List<HiveFileStatus> files = new ArrayList<>();
            collectFiles(resolved, loc, recursive, files);
            return files;
        } catch (IOException e) {
            if (isSystemicResolutionFailure(e)) {
                // A lazily-surfaced "No FileSystem for scheme X": the engine-side FS impl for this scheme is missing
                // (broken packaging) — SYSTEMIC, affecting every partition. Fail loud, matching the pre-cache
                // FileSystem.get behavior (this is the exact error class FIX-HIVEFS exists to keep loud).
                throw new DorisConnectorException("Failed to resolve filesystem for " + location, e);
            }
            // Listing THIS partition directory (or one of its sub-directories) failed (missing / unreadable /
            // transient): a LOCAL failure the scan path tolerates by skipping the partition with a warning
            // (pre-cache parity). Distinct exception type so only this is skipped, while systemic failures stay loud.
            throw new HiveDirectoryListingException("Failed to list files under " + location, e);
        }
    }

    /**
     * Collects the visible data files under {@code dir} into {@code out}, filtering directories and
     * {@code _}/{@code .}-prefixed hidden files. When {@code recursive}, descends into every NON-hidden
     * sub-directory; a hidden sub-directory ({@code _temporary} / {@code .hive-staging}) is skipped — exact net
     * parity with legacy {@code HiveExternalMetaCache}'s full-path {@code containsHiddenPath} filter (the connector
     * filters only the leaf {@link FileEntry#name()}, so descending into a hidden dir would surface staging files
     * legacy suppresses). A listing failure at any level throws {@link IOException} up to the single classifier in
     * {@link #listFromFileSystem}, so a sub-directory failure gets the same systemic/local verdict as the top.
     * Descent reuses the already-resolved {@code resolved} (a sub-directory shares scheme/authority).
     */
    private static void collectFiles(FileSystem resolved, Location dir, boolean recursive,
            List<HiveFileStatus> out) throws IOException {
        try (FileIterator it = resolved.list(dir)) {
            while (it.hasNext()) {
                FileEntry entry = it.next();
                String name = entry.name();
                if (entry.isDirectory()) {
                    if (recursive && !isHidden(name)) {
                        collectFiles(resolved, entry.location(), true, out);
                    }
                    continue;
                }
                if (isHidden(name)) {
                    continue;
                }
                out.add(new HiveFileStatus(entry.location().uri(), entry.length(),
                        entry.modificationTime()));
            }
        }
    }

    private static boolean isHidden(String name) {
        return name.startsWith("_") || name.startsWith(".");
    }

    /**
     * Whether {@code t} (or any exception in its cause chain — robust to {@code authenticator.doAs} wrapping)
     * is a scheme-not-registered failure ({@link UnsupportedFileSystemException} / {@code "No FileSystem for
     * scheme"}). Such a failure is a deterministic, whole-table storage/packaging error and must stay LOUD, unlike
     * a per-directory listing failure.
     */
    private static boolean isSystemicResolutionFailure(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof UnsupportedFileSystemException) {
                return true;
            }
            String msg = c.getMessage();
            if (msg != null && msg.contains("No FileSystem for scheme")) {
                return true;
            }
            if (c.getCause() == c) {
                break;
            }
        }
        return false;
    }

    /**
     * Cache key: (db, table, location, partitionValues). db+table let {@link #invalidateTable} select one table's
     * entries; db alone lets {@link #invalidateDb} select one database's entries; the partition values let
     * {@link #invalidatePartitions} select exactly one partition's entries (legacy per-partition parity). The
     * values co-vary with the location (a partition has one location), so including both keeps the scan and
     * size-estimate paths sharing the same entry while making per-partition invalidation possible.
     */
    static final class FileListingKey {
        private final String dbName;
        private final String tableName;
        private final String location;
        private final List<String> partitionValues;

        FileListingKey(String dbName, String tableName, String location, List<String> partitionValues) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.location = location;
            this.partitionValues = partitionValues == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(partitionValues));
        }

        boolean matches(String db, String table) {
            return Objects.equals(dbName, db) && Objects.equals(tableName, table);
        }

        boolean matchesDb(String db) {
            return Objects.equals(dbName, db);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FileListingKey)) {
                return false;
            }
            FileListingKey that = (FileListingKey) o;
            return Objects.equals(dbName, that.dbName)
                    && Objects.equals(tableName, that.tableName)
                    && Objects.equals(location, that.location)
                    && Objects.equals(partitionValues, that.partitionValues);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName, location, partitionValues);
        }
    }
}
