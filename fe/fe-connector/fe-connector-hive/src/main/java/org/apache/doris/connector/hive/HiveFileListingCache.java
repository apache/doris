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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 * <p><b>Failures are not cached.</b> The loader throws {@link DorisConnectorException} on any I/O error (never
 * caching a transient failure, matching {@code MetaCacheEntry}'s null-is-a-miss / exception-propagates contract).
 * The scan path catches it and skips the partition with a warning (the pre-cache tolerance of a missing/unreadable
 * partition directory); the estimate path lets it propagate and degrades to {@code -1}.</p>
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

    // Legacy fe-core Config values, mirrored locally (the connector never touches fe-core Config):
    //   TTL      = Config.external_cache_expire_time_seconds_after_access (86400s = 24h)
    //   capacity = Config.max_external_file_cache_num                     (10000)
    static final long DEFAULT_TTL_SECOND = 86400L;
    static final long DEFAULT_FILE_CAPACITY = 10000L;

    /**
     * The raw directory lister: the real Hadoop filesystem in production ({@link #listFromFileSystem}), a fake in
     * unit tests. Injected so the cache's hit/miss/invalidation behaviour is testable without a live filesystem
     * (mirrors {@code HiveConnectorMetadata.estimateDataSize} injecting its {@code ToLongFunction} size source).
     */
    @FunctionalInterface
    interface DirectoryLister {
        List<HiveFileStatus> list(String location, Configuration conf);
    }

    private final MetaCacheEntry<FileListingKey, List<HiveFileStatus>> cache;
    private final DirectoryLister lister;

    public HiveFileListingCache(Map<String, String> properties) {
        this(properties, HiveFileListingCache::listFromFileSystem);
    }

    HiveFileListingCache(Map<String, String> properties, DirectoryLister lister) {
        Map<String, String> props = properties == null ? Collections.emptyMap() : properties;
        CacheSpec spec = CacheSpec.fromProperties(props, ENGINE, ENTRY_FILE,
                CacheSpec.of(true, DEFAULT_TTL_SECOND, DEFAULT_FILE_CAPACITY));
        // Contextual-only + manual-miss so the slow listStatus runs on the caller (TCCL-pinned) thread outside
        // Caffeine's sync compute lock, deduplicated by a striped lock — mirrors CachingHmsClient's entries.
        this.cache = new MetaCacheEntry<>("hive.file", null, spec, ForkJoinPool.commonPool(), false, true, 0L, true);
        this.lister = Objects.requireNonNull(lister, "lister can not be null");
    }

    /**
     * Lists the data files directly under {@code location} (non-recursive; directories and {@code _}/{@code .}
     * -prefixed hidden files removed), served from the cache. Keyed by {@code (db, table, location)} so
     * {@link #invalidateTable} can drop exactly one table's entries. The loader runs on the calling thread; an I/O
     * failure throws {@link DorisConnectorException} (and is NOT cached). The returned list is shared by reference
     * (immutable elements) — callers must treat it as read-only, the codebase-wide metadata-cache convention.
     */
    public List<HiveFileStatus> listDataFiles(String dbName, String tableName, String location, Configuration conf) {
        return cache.get(new FileListingKey(dbName, tableName, location),
                key -> lister.list(key.location, conf));
    }

    /** Drops every cached listing for one table. Backs {@code REFRESH TABLE}. */
    public void invalidateTable(String dbName, String tableName) {
        cache.invalidateIf(key -> key.matches(dbName, tableName));
    }

    /** Drops the whole file-listing cache. Backs {@code REFRESH CATALOG}. */
    public void invalidateAll() {
        cache.invalidateAll();
    }

    /**
     * The production {@link DirectoryLister}: a real, non-recursive {@code FileSystem.listStatus}, filtering out
     * directories and {@code _}/{@code .}-prefixed hidden files (byte-parity with the pre-cache filters in
     * {@code HiveScanPlanProvider.listAndSplitFiles} and {@code HiveConnectorMetadata.sumFileSizesUnder}). A zero
     * -length data file is kept (the scan splitter skips it; the size estimate adds 0) so both consumers keep
     * their exact prior behaviour. Any {@link IOException} (unresolvable filesystem or unreadable directory) is
     * wrapped in a {@link DorisConnectorException} so the entry is never cached and the caller decides how to
     * degrade.
     */
    static List<HiveFileStatus> listFromFileSystem(String location, Configuration conf) {
        try {
            Path path = new Path(location);
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            FileStatus[] statuses = fs.listStatus(path);
            List<HiveFileStatus> files = new ArrayList<>(statuses.length);
            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    continue;
                }
                String fileName = status.getPath().getName();
                if (fileName.startsWith("_") || fileName.startsWith(".")) {
                    continue;
                }
                files.add(new HiveFileStatus(status.getPath().toString(), status.getLen(),
                        status.getModificationTime()));
            }
            return files;
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to list files under " + location, e);
        }
    }

    /** Cache key: (db, table, location). db+table let {@link #invalidateTable} select one table's entries. */
    static final class FileListingKey {
        private final String dbName;
        private final String tableName;
        private final String location;

        FileListingKey(String dbName, String tableName, String location) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.location = location;
        }

        boolean matches(String db, String table) {
            return Objects.equals(dbName, db) && Objects.equals(tableName, table);
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
                    && Objects.equals(location, that.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName, location);
        }
    }
}
