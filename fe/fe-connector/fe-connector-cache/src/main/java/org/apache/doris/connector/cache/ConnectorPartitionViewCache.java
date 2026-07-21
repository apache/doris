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

package org.apache.doris.connector.cache;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

/**
 * GENERIC (engine-agnostic) cross-query cache of a connector's derived partition view — "cache A" of the
 * external-partition-derived-cache design (design doc {@code 2026-07-20-external-partition-derived-cache-design.md}
 * §5). It is the generic version of {@code org.apache.doris.connector.iceberg.IcebergPartitionCache}: same
 * construction pattern (a contextual-only, manual-miss-load {@link MetaCacheEntry}), same {@link CacheSpec} wiring,
 * same invalidation style — but keyed by the engine-agnostic {@link PartitionViewCacheKey} and holding an opaque
 * value {@code V} instead of an iceberg-specific raw-partition list, so it has no engine-specific imports and can be
 * shared by every connector (iceberg/paimon/hive/maxcompute, wired in later subsystems — this class has NO
 * consumers yet).
 *
 * <p><b>Config</b>: {@code meta.cache.<engine>.partition_view.(enable|ttl-second|capacity)}, default ON / 86400s /
 * 1000 entries (matching {@code IcebergPartitionCache}'s {@code DEFAULT_TABLE_CACHE_CAPACITY}). {@code enable=false}
 * / {@code ttl-second=0} / {@code capacity=0} each disable the cache (see {@link CacheSpec#isCacheEnabled}): {@link
 * #get} then calls the loader on every call, matching {@code IcebergPartitionCache}'s disabled-cache bypass.
 *
 * <p><b>Concurrency</b>: mirrors {@code IcebergPartitionCache} / {@code MaxComputePartitionCache} exactly — the
 * entry is contextual-only (no built-in loader; the caller supplies one per {@link #get} call) with manual miss
 * load, so a slow remote enumeration runs OUTSIDE Caffeine's compute lock (deduplicated per key by a striped lock)
 * on the calling thread, and its exception propagates to the caller unwrapped without poisoning the cache.
 */
public final class ConnectorPartitionViewCache<V> {

    /** {@code meta.cache.<engine>.partition_view.*} — the property-namespace entry name for this cache. */
    static final String ENTRY_PARTITION_VIEW = "partition_view";
    /** Default TTL: 24h, matching the design doc's stated default and sibling caches' 24h TTL. */
    static final long DEFAULT_TTL_SECOND = 86400L;
    /** Default capacity, matching {@code IcebergPartitionCache}'s {@code DEFAULT_TABLE_CACHE_CAPACITY}. */
    static final long DEFAULT_CAPACITY = 1000L;

    private final MetaCacheEntry<PartitionViewCacheKey, V> entry;

    /**
     * @param engine engine token for the {@code meta.cache.<engine>.partition_view.*} property namespace, e.g.
     *               {@code "iceberg"}/{@code "paimon"}/{@code "hive"}/{@code "max_compute"}.
     * @param props  the catalog properties; drives the {@link CacheSpec} (enable/ttl-second/capacity). May be
     *               {@code null}, treated as empty (defaults apply).
     */
    public ConnectorPartitionViewCache(String engine, Map<String, String> props) {
        Objects.requireNonNull(engine, "engine can not be null");
        Map<String, String> properties = props == null ? Collections.emptyMap() : props;
        CacheSpec spec = CacheSpec.fromProperties(properties, engine, ENTRY_PARTITION_VIEW,
                CacheSpec.of(true, DEFAULT_TTL_SECOND, DEFAULT_CAPACITY));
        // contextual-only (loader == null, supplied per-call by get()) + manual-miss-load, no auto-refresh --
        // identical shape to IcebergPartitionCache / MaxComputePartitionCache's entry construction.
        this.entry = new MetaCacheEntry<>(engine + "." + ENTRY_PARTITION_VIEW, null, spec,
                ForkJoinPool.commonPool(), false, true, 0L, true);
    }

    /** Caching is on only when the resolved {@link CacheSpec} is effectively enabled (see {@link #entry}'s spec). */
    public boolean isEnabled() {
        return entry.stats().isEffectiveEnabled();
    }

    /**
     * Returns the cached value for {@code key} if present, else runs {@code loader}, caches and returns it.
     * Disabled cache -&gt; {@code loader} runs on every call. The loader runs OUTSIDE Caffeine's compute lock
     * (single-flight per key) and its exception propagates unwrapped; a failed load is never cached.
     */
    public V get(PartitionViewCacheKey key, Supplier<V> loader) {
        Objects.requireNonNull(loader, "loader can not be null");
        return entry.get(key, ignored -> loader.get());
    }

    /** Evicts every cached snapshot/schema entry of one (db, table). Backs REFRESH TABLE / table-level invalidation. */
    public void invalidateTable(String db, String table) {
        entry.invalidateIf(key -> key.matches(db, table));
    }

    /** Evicts every cached entry of one db (all its tables). Backs REFRESH DATABASE / db-level invalidation. */
    public void invalidateDb(String db) {
        entry.invalidateIf(key -> key.matchesDb(db));
    }

    /** Evicts every cached entry. Backs REFRESH CATALOG. */
    public void invalidateAll() {
        entry.invalidateAll();
    }

    /** Test-only: current number of cached entries (accurate map membership, not Caffeine's estimate). */
    long size() {
        int[] count = {0};
        entry.forEach((key, value) -> count[0]++);
        return count[0];
    }
}
