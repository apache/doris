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

import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

/**
 * Per-catalog cache of an iceberg table's LATEST snapshot, keyed by {@link TableIdentifier} (db.table).
 *
 * <p>Mirrors the paimon connector's {@code PaimonLatestSnapshotCache}: restores the legacy
 * {@code IcebergExternalMetaCache} table-cache semantics that the SPI cutover dropped.
 * Within the TTL an iceberg catalog serves a STABLE (possibly stale) latest snapshot across queries, so a
 * query-begin pin ({@link IcebergConnectorMetadata#beginQuerySnapshot}) reads the SAME snapshot until the
 * entry expires or is invalidated by {@code REFRESH TABLE}/{@code REFRESH CATALOG}.
 *
 * <p><b>Value carries BOTH snapshotId and schemaId (the single iceberg-specific deviation from the paimon
 * {@code long}-only mirror).</b> {@code beginQuerySnapshot} pins the snapshot id <i>and</i> the LATEST schema
 * id ({@code table.schema().schemaId()} — not {@code currentSnapshot().schemaId()}, mirroring legacy
 * {@code IcebergUtils.getLatestIcebergSnapshot}). A schema-only {@code ALTER} bumps the latest schema id
 * without producing a new snapshot, so the two ids must be captured atomically — otherwise two live reads
 * within one pin could observe a snapshotId/schemaId skew. The value type therefore ports legacy
 * {@code IcebergSnapshot}'s {@code (snapshotId, schemaId)} shape.
 *
 * <p>Backed by the shared {@link MetaCacheEntry} framework (independent-copy meta-cache migration): a
 * contextual, access-TTL entry whose per-query loader is supplied at {@link #getOrLoad}. TTL is
 * {@code meta.cache.iceberg.table.ttl-second}: {@code <= 0} disables caching (every read goes live, matching
 * the legacy "no-cache" catalog); a positive value is Caffeine {@code expireAfterAccess} with a
 * {@code maxSize} capacity (real LRU eviction, replacing the former clear-on-overflow). Manual miss-load is
 * on so the loader runs OUTSIDE Caffeine's compute lock (single-flight per key). Lives on the long-lived
 * per-catalog {@link IcebergConnector}; a REFRESH CATALOG rebuilds the connector and thus the cache.
 */
final class IcebergLatestSnapshotCache {

    /** Immutable atomic pin = the latest snapshot id plus the latest schema id (port of legacy IcebergSnapshot). */
    static final class CachedSnapshot {
        final long snapshotId;
        final long schemaId;

        CachedSnapshot(long snapshotId, long schemaId) {
            this.snapshotId = snapshotId;
            this.schemaId = schemaId;
        }
    }

    private final MetaCacheEntry<TableIdentifier, CachedSnapshot> entry;

    IcebergLatestSnapshotCache(long ttlSeconds, int maxSize) {
        // ttl-second <= 0 disables caching (always read live); a positive ttl is access-based expiry with the
        // given capacity. CacheSpec treats ttl == -1 as "no expiration (enabled)" and ttl == 0 as "disabled",
        // so translate the connector's "<= 0 disables" contract to ttl == 0 rather than passing a negative
        // value straight through (which would otherwise flip -1 into a never-expiring cache).
        CacheSpec spec = ttlSeconds > 0
                ? CacheSpec.of(true, ttlSeconds, maxSize)
                : CacheSpec.of(true, CacheSpec.CACHE_TTL_DISABLE_CACHE, maxSize);
        this.entry = new MetaCacheEntry<>("iceberg-latest-snapshot", null, spec,
                ForkJoinPool.commonPool(), false, true, 0L, true);
    }

    /** Caching is on only when the TTL is positive; ttl-second &lt;= 0 means "always read live". */
    boolean isEnabled() {
        return entry.stats().isEffectiveEnabled();
    }

    /**
     * Returns the cached latest snapshot for {@code identifier} if present and unexpired, else runs
     * {@code loader} (the live {@code currentSnapshot()} + latest-schema read), caches and returns it. When
     * caching is disabled ({@link #isEnabled()} is false) {@code loader} runs every call and nothing is cached.
     * A hit refreshes the entry's expiry (access-based). The loader runs OUTSIDE Caffeine's compute lock
     * (single-flight per key); a disabled entry bypasses the cache entirely and always loads.
     */
    CachedSnapshot getOrLoad(TableIdentifier identifier, Supplier<CachedSnapshot> loader) {
        return entry.get(identifier, ignored -> loader.get());
    }

    /** Drops the cached entry for one table so the next read goes live (REFRESH TABLE). */
    void invalidate(TableIdentifier identifier) {
        entry.invalidateKey(identifier);
    }

    /**
     * Drops every cached entry for one database so the next read of any of its tables goes live
     * (REFRESH DATABASE / a Doris-issued DROP DATABASE). Entries are keyed by
     * {@code TableIdentifier.of(db, table)} (single-level namespace = {@code [db]}, see
     * {@code IcebergConnectorMetadata.beginQuerySnapshot}), so a db match is namespace equality.
     */
    void invalidateDb(String dbName) {
        Namespace ns = Namespace.of(dbName);
        entry.invalidateIf(id -> id.namespace().equals(ns));
    }

    /** Drops all cached entries. */
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
