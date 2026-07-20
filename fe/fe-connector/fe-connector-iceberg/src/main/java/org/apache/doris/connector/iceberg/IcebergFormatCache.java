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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

/**
 * Per-catalog cache of an iceberg table's inferred write file-format name (PERF-03), keyed by
 * {@code (TableIdentifier, snapshotId)}. Kills the per-query revival of the #64134 heavy fallback: when a table
 * carries neither the {@code write-format} nickname nor {@code write.format.default} (migrated tables, and any
 * table whose writer never set the property — parquet is an implicit default, not stored),
 * {@link IcebergScanPlanProvider#getScanNodeProperties} resolves the scan-level {@code file_format_type} through
 * {@link IcebergWriterHelper#getFileFormat}, which falls back to an unfiltered {@code table.newScan().planFiles()}
 * — a whole-table manifest scan (remote FileIO) — just to read the FIRST data file's format. That fallback was
 * re-run on every query/EXPLAIN with no cross-query reuse; this cache collapses it to one scan per
 * {@code (table, snapshot)}.
 *
 * <p><b>Snapshot-keyed, so always correct.</b> A snapshot is immutable, so its first data file's format is a
 * pure function of the key; a new commit yields a new snapshot id (a new key -&gt; a live scan). Within the TTL
 * the snapshot id itself is held stable by {@link IcebergLatestSnapshotCache}, which is what makes the key stable
 * across queries. The key snapshot is the table's {@code currentSnapshot().snapshotId()} — exactly what the
 * inference reads (it scans {@code table.newScan()}, never the handle's time-travel pin), matching legacy
 * {@code IcebergUtils.getFileFormat} and PERF-02's partition cache verbatim.
 *
 * <p><b>Only the inference fallback is cached.</b> The two cheap property probes ({@code write-format},
 * {@code write.format.default}) stay outside this cache — the caller consults it only when both miss.
 *
 * <p><b>No credential gate</b> (unlike {@link IcebergTableCache}, like {@link IcebergPartitionCache}): the cached
 * value is a bare format-name {@link String} with no {@code FileIO} / credential, so it is safe to share across
 * users and is built unconditionally (only the TTL knob disables it).
 *
 * <p>Backed identically to {@link IcebergPartitionCache}: a contextual, access-TTL {@link MetaCacheEntry} with
 * manual miss-load, so the inference runs OUTSIDE Caffeine's compute lock and a failed scan's exception
 * propagates verbatim and is NOT cached (the next query retries — legacy parity). TTL is
 * {@code meta.cache.iceberg.table.ttl-second}; {@code <= 0} disables (read live). Lives on the long-lived
 * per-catalog {@link IcebergConnector}; a REFRESH CATALOG rebuilds the connector and thus the cache.
 */
final class IcebergFormatCache {

    /** Immutable composite key: a table's inferred format is distinct per pinned snapshot id. */
    static final class Key {
        final TableIdentifier id;
        final long snapshotId;

        Key(TableIdentifier id, long snapshotId) {
            this.id = id;
            this.snapshotId = snapshotId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Key)) {
                return false;
            }
            Key that = (Key) o;
            return snapshotId == that.snapshotId && Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, snapshotId);
        }
    }

    private final MetaCacheEntry<Key, String> entry;

    IcebergFormatCache(long ttlSeconds, int maxSize) {
        // Mirror IcebergPartitionCache: translate the connector's "<= 0 disables" contract to CacheSpec's ttl == 0
        // (disabled) rather than passing a negative value through (which CacheSpec reads as "no expiration").
        CacheSpec spec = ttlSeconds > 0
                ? CacheSpec.of(true, ttlSeconds, maxSize)
                : CacheSpec.of(true, CacheSpec.CACHE_TTL_DISABLE_CACHE, maxSize);
        this.entry = new MetaCacheEntry<>("iceberg-format", null, spec,
                ForkJoinPool.commonPool(), false, true, 0L, true);
    }

    /** Caching is on only when the TTL is positive; ttl-second &lt;= 0 means "always infer live". */
    boolean isEnabled() {
        return entry.stats().isEffectiveEnabled();
    }

    /**
     * Returns the cached format name for {@code key} if present and unexpired, else runs {@code loader} (the live
     * whole-table format inference), caches and returns it. Disabled cache -&gt; {@code loader} every call. The
     * loader runs OUTSIDE Caffeine's compute lock (single-flight per key) and its exception propagates unwrapped
     * and is NOT cached.
     */
    String getOrLoad(Key key, Supplier<String> loader) {
        return entry.get(key, ignored -> loader.get());
    }

    /** Drops every cached snapshot entry for one table so the next read infers live (REFRESH TABLE). */
    void invalidate(TableIdentifier id) {
        entry.invalidateIf(key -> key.id.equals(id));
    }

    /** Drops every cached entry for one database (REFRESH DATABASE / DROP DATABASE); db match = namespace equality. */
    void invalidateDb(String dbName) {
        Namespace ns = Namespace.of(dbName);
        entry.invalidateIf(key -> key.id.namespace().equals(ns));
    }

    /** Drops all cached entries (REFRESH CATALOG). */
    void invalidateAll() {
        entry.invalidateAll();
    }

    /** Test-only: current number of cached entries (accurate map membership, not Caffeine's estimate). */
    int size() {
        int[] count = {0};
        entry.forEach((key, value) -> count[0]++);
        return count[0];
    }

    /** Test-only: how many times the live loader (the whole-table format inference) actually ran — the metric gate. */
    long loadCountForTest() {
        return entry.stats().getLoadSuccessCount();
    }
}
