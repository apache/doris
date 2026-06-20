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

import org.apache.paimon.catalog.Identifier;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Per-catalog cache of a paimon table's LATEST snapshot id, keyed by {@link Identifier} (db.table).
 *
 * <p>Restores the legacy {@code PaimonExternalMetaCache} table-cache semantics that the SPI cutover dropped
 * (FIX-4 / CI 973411 test_paimon_table_meta_cache): within the TTL a paimon catalog serves a STABLE
 * (possibly stale) latest-snapshot id across queries, so a query-begin pin
 * ({@link PaimonConnectorMetadata#beginQuerySnapshot}) reads the SAME snapshot until the entry expires or is
 * invalidated by {@code REFRESH TABLE}/{@code REFRESH CATALOG}. The id flows through
 * {@code applySnapshot} -&gt; {@code scan.snapshot-id} -&gt; {@code Table.copy}, so an external write made
 * after the pin is not visible until refresh.
 *
 * <p>TTL is {@code meta.cache.paimon.table.ttl-second}: {@code <= 0} disables caching (every read goes live,
 * matching the legacy "no-cache" catalog). Expiry is access-based (legacy used Caffeine
 * {@code expireAfterAccess(external_cache_expire_time_seconds_after_access)}). The keyspace (tables in one
 * catalog) is naturally small; a best-effort size bound flushes wholesale on overflow (re-reads are harmless
 * — the value is the live latest id). Lives on the long-lived per-catalog {@link PaimonConnector}, mirroring
 * {@link PaimonSchemaAtMemo}; a REFRESH CATALOG rebuilds the connector and thus the cache.
 */
final class PaimonLatestSnapshotCache {

    private static final class Entry {
        final long snapshotId;
        volatile long expireAtNanos;

        Entry(long snapshotId, long expireAtNanos) {
            this.snapshotId = snapshotId;
            this.expireAtNanos = expireAtNanos;
        }
    }

    private final Map<Identifier, Entry> cache = new ConcurrentHashMap<>();
    private final long ttlNanos;
    private final int maxSize;
    private final LongSupplier nanoClock;

    PaimonLatestSnapshotCache(long ttlSeconds, int maxSize) {
        this(ttlSeconds, maxSize, System::nanoTime);
    }

    /** Visible for testing: injectable clock so TTL expiry is deterministic without sleeping. */
    PaimonLatestSnapshotCache(long ttlSeconds, int maxSize, LongSupplier nanoClock) {
        this.ttlNanos = ttlSeconds <= 0 ? 0L : TimeUnit.SECONDS.toNanos(ttlSeconds);
        this.maxSize = Math.max(1, maxSize);
        this.nanoClock = nanoClock;
    }

    /** Caching is on only when the TTL is positive; ttl-second &lt;= 0 means "always read live". */
    boolean isEnabled() {
        return ttlNanos > 0;
    }

    /**
     * Returns the cached latest-snapshot id for {@code identifier} if present and unexpired, else runs
     * {@code loader} (the live {@code latestSnapshotId} read), caches the result and returns it. When caching
     * is disabled ({@link #isEnabled()} is false) {@code loader} runs every call and nothing is cached. A hit
     * refreshes the entry's expiry (access-based). The loader runs OUTSIDE any lock; a concurrent same-key
     * miss may load twice (harmless — the value is the current live id), mirroring {@link PaimonSchemaAtMemo}.
     */
    long getOrLoad(Identifier identifier, LongSupplier loader) {
        if (ttlNanos <= 0) {
            return loader.getAsLong();
        }
        long now = nanoClock.getAsLong();
        Entry hit = cache.get(identifier);
        if (hit != null && now - hit.expireAtNanos < 0) {
            hit.expireAtNanos = now + ttlNanos;
            return hit.snapshotId;
        }
        long loaded = loader.getAsLong();
        if (cache.size() >= maxSize) {
            cache.clear();
        }
        cache.put(identifier, new Entry(loaded, now + ttlNanos));
        return loaded;
    }

    /** Drops the cached entry for one table so the next read goes live (REFRESH TABLE). */
    void invalidate(Identifier identifier) {
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
