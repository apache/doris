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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.MetaCacheEntry;

import com.aliyun.odps.Partition;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;

/**
 * The MaxCompute connector's own partition-listing cache — a structural copy of the hive connector's
 * {@code HiveFileListingCache}, backed by the shared
 * {@code fe-connector-cache} framework ({@link CacheSpec} + {@link MetaCacheEntry}). It memoizes the (expensive)
 * per-table ODPS partition listing ({@code structureHelper.getPartitions}), keyed by {@code (db, table)} — the
 * ODPS project is constant per catalog, so it is NOT part of the key.
 *
 * <p><b>Why this exists.</b> Legacy fe-core kept partition listings in the engine-side external meta cache; once
 * a MaxCompute catalog becomes plugin-driven that cache stops routing to it, so without this connector-owned
 * cache every {@code SHOW PARTITIONS} / partition-pruning / partition-values call would re-list every partition
 * from ODPS. The three metadata consumers ({@code listPartitions}, {@code listPartitionNames},
 * {@code listPartitionValues}) share ONE instance, held as a {@code final} field on the per-catalog
 * {@link MaxComputeDorisConnector} (the only object that outlives a single query; the metadata is rebuilt per
 * call), so a partition read warms the others.
 *
 * <p><b>Read-only convention.</b> The cached {@code List<Partition>} is shared by reference. The consumers only
 * read local partition accessors ({@code getPartitionSpec()}, {@code spec.keys()/get()}, {@code toString()}),
 * never a per-partition lazy reload, so the shared list is safe as long as callers treat it as read-only (the
 * codebase-wide metadata-cache convention).
 *
 * <p><b>TCCL.</b> The entry is contextual-only + manual-miss + no auto-refresh, so the loader runs synchronously
 * on the CALLING thread — keeping TCCL/classloading byte-identical to today's uncached ODPS call (a background
 * refresh thread would not inherit the caller's pin). Mirrors {@code CachingHmsClient} / {@code
 * HiveFileListingCache}'s entry construction.
 *
 * <p><b>Failures are not cached.</b> The loader never caches a failed load (matching {@link MetaCacheEntry}'s
 * null-is-a-miss / exception-propagates contract), so a transient ODPS failure does not poison the listing for
 * the whole TTL.
 */
public class MaxComputePartitionCache {

    /** Engine token for the {@code meta.cache.<engine>.<entry>.*} property namespace. */
    static final String ENGINE = "max_compute";
    /** {@code meta.cache.max_compute.partition.*} — cached partition listings. */
    static final String ENTRY_PARTITION = "partition";

    // Legacy MaxCompute partition-cache Config values, mirrored locally (the connector never touches fe-core
    // Config). These are the knobs the DELETED MaxComputeExternalMetaCache.partitionValuesEntry actually used —
    //   TTL      = Config.external_cache_refresh_time_minutes * 60 (default 10 min * 60 = 600s)
    //   capacity = Config.max_hive_partition_table_cache_num       (default 10000)
    // NOT the hive file-listing cache's knobs (external_cache_expire_time_seconds_after_access = 24h /
    // max_external_file_cache_num): a MaxCompute table's partition set must re-list ~10 min after last access,
    // matching legacy, so a partition added directly in ODPS becomes visible without an explicit REFRESH.
    static final long DEFAULT_TTL_SECOND = 600L;
    static final long DEFAULT_PARTITION_CAPACITY = 10000L;

    /**
     * The raw partition lister: {@code structureHelper.getPartitions(odps, db, table)} in production, a fake in
     * unit tests. Injected so the cache's hit/miss/invalidation behaviour is testable without a live ODPS client
     * (mirrors the hive connector's {@code HiveFileListingCache.DirectoryLister}).
     */
    @FunctionalInterface
    interface PartitionLister {
        List<Partition> list(String dbName, String tableName);
    }

    private final MetaCacheEntry<PartitionKey, List<Partition>> cache;
    private final PartitionLister lister;

    MaxComputePartitionCache(Map<String, String> properties, PartitionLister lister) {
        Map<String, String> props = properties == null ? Collections.emptyMap() : properties;
        CacheSpec spec = CacheSpec.fromProperties(props, ENGINE, ENTRY_PARTITION,
                CacheSpec.of(true, DEFAULT_TTL_SECOND, DEFAULT_PARTITION_CAPACITY));
        // Contextual-only + manual-miss so the slow getPartitions runs on the caller (TCCL-pinned) thread outside
        // Caffeine's sync compute lock, deduplicated by a striped lock — mirrors CachingHmsClient's entries.
        this.cache = new MetaCacheEntry<>("max_compute.partition", null, spec, ForkJoinPool.commonPool(),
                false, true, 0L, true);
        this.lister = Objects.requireNonNull(lister, "lister can not be null");
    }

    /**
     * Returns all partitions of {@code (dbName, tableName)}, served from the cache. Keyed by {@code (db, table)}
     * so {@link #invalidateTable} can drop exactly one table's entry. The loader runs on the calling thread; a
     * failure propagates and is NOT cached. The returned list is shared by reference — callers must treat it as
     * read-only (the codebase-wide metadata-cache convention).
     */
    public List<Partition> getPartitions(String dbName, String tableName) {
        return cache.get(new PartitionKey(dbName, tableName),
                key -> lister.list(key.dbName, key.tableName));
    }

    /** Drops the cached partition listing for one table. Backs {@code REFRESH TABLE}. */
    public void invalidateTable(String dbName, String tableName) {
        cache.invalidateIf(key -> key.matches(dbName, tableName));
    }

    /** Drops every cached partition listing for one database (all its tables). Backs {@code REFRESH DATABASE}. */
    public void invalidateDb(String dbName) {
        cache.invalidateIf(key -> key.matchesDb(dbName));
    }

    /** Drops the whole partition cache. Backs {@code REFRESH CATALOG}. */
    public void invalidateAll() {
        cache.invalidateAll();
    }

    /** Current number of cached partition listings — for unit tests only (mirrors HiveFileListingCache.size()). */
    long size() {
        long[] count = {0L};
        cache.forEach((key, value) -> count[0]++);
        return count[0];
    }

    /**
     * Cache key: (db, table). db+table let {@link #invalidateTable} select one table's entry; db alone lets
     * {@link #invalidateDb} select one database's entries.
     */
    static final class PartitionKey {
        private final String dbName;
        private final String tableName;

        PartitionKey(String dbName, String tableName) {
            this.dbName = dbName;
            this.tableName = tableName;
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
            if (!(o instanceof PartitionKey)) {
                return false;
            }
            PartitionKey that = (PartitionKey) o;
            return Objects.equals(dbName, that.dbName)
                    && Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName);
        }
    }
}
