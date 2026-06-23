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

import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
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
 * <p>TTL is {@code meta.cache.iceberg.table.ttl-second}: {@code <= 0} disables caching (every read goes live,
 * matching the legacy "no-cache" catalog). Expiry is access-based. A best-effort size bound flushes wholesale
 * on overflow (re-reads are harmless — the value is the live latest snapshot). Lives on the long-lived
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

    private static final class Entry {
        final CachedSnapshot value;
        volatile long expireAtNanos;

        Entry(CachedSnapshot value, long expireAtNanos) {
            this.value = value;
            this.expireAtNanos = expireAtNanos;
        }
    }

    private final Map<TableIdentifier, Entry> cache = new ConcurrentHashMap<>();
    private final long ttlNanos;
    private final int maxSize;
    private final LongSupplier nanoClock;

    IcebergLatestSnapshotCache(long ttlSeconds, int maxSize) {
        this(ttlSeconds, maxSize, System::nanoTime);
    }

    /** Visible for testing: injectable clock so TTL expiry is deterministic without sleeping. */
    IcebergLatestSnapshotCache(long ttlSeconds, int maxSize, LongSupplier nanoClock) {
        this.ttlNanos = ttlSeconds <= 0 ? 0L : TimeUnit.SECONDS.toNanos(ttlSeconds);
        this.maxSize = Math.max(1, maxSize);
        this.nanoClock = nanoClock;
    }

    /** Caching is on only when the TTL is positive; ttl-second &lt;= 0 means "always read live". */
    boolean isEnabled() {
        return ttlNanos > 0;
    }

    /**
     * Returns the cached latest snapshot for {@code identifier} if present and unexpired, else runs
     * {@code loader} (the live {@code currentSnapshot()} + latest-schema read), caches and returns it. When
     * caching is disabled ({@link #isEnabled()} is false) {@code loader} runs every call and nothing is cached.
     * A hit refreshes the entry's expiry (access-based). The loader runs OUTSIDE any lock; a concurrent
     * same-key miss may load twice (harmless — the value is the current live snapshot).
     */
    CachedSnapshot getOrLoad(TableIdentifier identifier, Supplier<CachedSnapshot> loader) {
        if (ttlNanos <= 0) {
            return loader.get();
        }
        long now = nanoClock.getAsLong();
        Entry hit = cache.get(identifier);
        if (hit != null && now - hit.expireAtNanos < 0) {
            hit.expireAtNanos = now + ttlNanos;
            return hit.value;
        }
        CachedSnapshot loaded = loader.get();
        if (cache.size() >= maxSize) {
            cache.clear();
        }
        cache.put(identifier, new Entry(loaded, now + ttlNanos));
        return loaded;
    }

    /** Drops the cached entry for one table so the next read goes live (REFRESH TABLE). */
    void invalidate(TableIdentifier identifier) {
        cache.remove(identifier);
    }

    /** Drops all cached entries. */
    void invalidateAll() {
        cache.clear();
    }

    /** Test-only: current number of cached entries. */
    int size() {
        return cache.size();
    }
}
