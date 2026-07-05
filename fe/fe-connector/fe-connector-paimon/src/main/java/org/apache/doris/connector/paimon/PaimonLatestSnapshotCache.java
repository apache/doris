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

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.MetaCacheEntry;

import org.apache.paimon.catalog.Identifier;

import java.util.concurrent.ForkJoinPool;
import java.util.function.LongSupplier;

/**
 * Per-catalog cache of a paimon table's LATEST snapshot id, keyed by {@link Identifier} (db.table).
 *
 * <p>Restores the legacy {@code PaimonExternalMetaCache} table-cache semantics that the SPI cutover dropped
 * (test_paimon_table_meta_cache): within the TTL a paimon catalog serves a STABLE (possibly stale)
 * latest-snapshot id across queries, so a query-begin pin
 * ({@link PaimonConnectorMetadata#beginQuerySnapshot}) reads the SAME snapshot until the entry expires or is
 * invalidated by {@code REFRESH TABLE}/{@code REFRESH CATALOG}. The id flows through
 * {@code applySnapshot} -&gt; {@code scan.snapshot-id} -&gt; {@code Table.copy}, so an external write made
 * after the pin is not visible until refresh.
 *
 * <p>Backed by the shared {@link MetaCacheEntry} framework (independent-copy meta-cache migration): a
 * contextual, access-TTL entry whose per-query loader is supplied at {@link #getOrLoad}. TTL is
 * {@code meta.cache.paimon.table.ttl-second}: {@code <= 0} disables caching (every read goes live, matching
 * the legacy "no-cache" catalog); a positive value is Caffeine {@code expireAfterAccess} with a
 * {@code maxSize} capacity (real LRU eviction, replacing the former clear-on-overflow). Manual miss-load is
 * on so the loader runs OUTSIDE Caffeine's compute lock (single-flight per key). Lives on the long-lived
 * per-catalog {@link PaimonConnector}, mirroring {@link PaimonSchemaAtMemo}; a REFRESH CATALOG rebuilds the
 * connector and thus the cache.
 */
final class PaimonLatestSnapshotCache {

    private final MetaCacheEntry<Identifier, Long> entry;

    PaimonLatestSnapshotCache(long ttlSeconds, int maxSize) {
        // ttl-second <= 0 disables caching (always read live); a positive ttl is access-based expiry with the
        // given capacity. CacheSpec treats ttl == -1 as "no expiration (enabled)" and ttl == 0 as "disabled",
        // so translate the connector's "<= 0 disables" contract to ttl == 0 rather than passing a negative
        // value straight through (which would otherwise flip -1 into a never-expiring cache).
        CacheSpec spec = ttlSeconds > 0
                ? CacheSpec.of(true, ttlSeconds, maxSize)
                : CacheSpec.of(true, CacheSpec.CACHE_TTL_DISABLE_CACHE, maxSize);
        this.entry = new MetaCacheEntry<>("paimon-latest-snapshot", null, spec,
                ForkJoinPool.commonPool(), false, true, 0L, true);
    }

    /** Caching is on only when the TTL is positive; ttl-second &lt;= 0 means "always read live". */
    boolean isEnabled() {
        return entry.stats().isEffectiveEnabled();
    }

    /**
     * Returns the cached latest-snapshot id for {@code identifier} if present and unexpired, else runs
     * {@code loader} (the live {@code latestSnapshotId} read), caches the result and returns it. When caching
     * is disabled ({@link #isEnabled()} is false) {@code loader} runs every call and nothing is cached. A hit
     * refreshes the entry's expiry (access-based). The loader runs OUTSIDE Caffeine's compute lock
     * (single-flight per key); a disabled entry bypasses the cache entirely and always loads.
     */
    long getOrLoad(Identifier identifier, LongSupplier loader) {
        return entry.get(identifier, ignored -> loader.getAsLong());
    }

    /** Drops the cached entry for one table so the next read goes live (REFRESH TABLE). */
    void invalidate(Identifier identifier) {
        entry.invalidateKey(identifier);
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
