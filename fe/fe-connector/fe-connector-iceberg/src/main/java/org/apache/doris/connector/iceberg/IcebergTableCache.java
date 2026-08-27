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

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Per-catalog cache of the RAW iceberg {@link Table} object, keyed by {@link TableIdentifier} (db.table)
 * (PERF-01). This restores the OTHER half of the legacy {@code IcebergExternalMetaCache} that the SPI cutover
 * dropped: {@link IcebergLatestSnapshotCache} kept only the {@code (snapshotId, schemaId)} pin, so every SPI
 * read entry ({@code getColumnHandles}, {@code getTableStatistics}, the scan provider's {@code resolveTable},
 * ...) re-loaded the table from the remote catalog (a metastore RPC + a {@code metadata.json} read). This cache
 * lets consecutive queries — and the analysis/planning phases of one query, whose handles have distinct memo
 * lineages — reuse a single loaded table, exactly as the legacy with-cache catalog did.
 *
 * <p><b>Backing.</b> Reuses the shared {@link MetaCacheEntry} framework identically to
 * {@link IcebergLatestSnapshotCache}: a contextual, access-TTL entry whose per-key loader is supplied at
 * {@link #getOrLoad}, with manual miss-load on so the loader runs OUTSIDE Caffeine's compute lock
 * (single-flight per key) and propagates its exception verbatim (a concurrent-drop
 * {@code NoSuchTableException} reaches the caller unwrapped, preserving each read entry's own degradation).
 * TTL is {@code meta.cache.iceberg.table.ttl-second} — the same knob that governs the snapshot cache: a
 * value {@code <= 0} disables caching (every read goes live), a positive value is Caffeine
 * {@code expireAfterAccess} with a {@code maxSize} capacity. Lives on the long-lived per-catalog
 * {@link IcebergConnector}; a REFRESH CATALOG rebuilds the connector and thus the cache.
 *
 * <p><b>Values are RAW tables.</b> The scan provider applies {@code wrapTableForScan} (the Kerberos
 * {@code doAs} FileIO wrap) per call on the way out, so no per-request authenticator is ever frozen into a
 * shared entry.
 *
 * <p><b>Credential isolation.</b> A raw table carries its FileIO's credentials, so this cross-query layer is
 * built ONLY when the connector's credentials are query-independent — it is left disabled (the connector
 * passes {@code null}) for {@code iceberg.rest.session=user} (per-user delegated FileIO) and REST
 * vended-credentials (server-vended tokens expire within the query, and iceberg keeps them fresh by reloading
 * the table each query). See {@code IcebergConnector}.
 */
final class IcebergTableCache {

    private final MetaCacheEntry<TableIdentifier, TableOwner> entry;
    private final Function<Table, Runnable> cleanupFactory;
    private final IcebergCatalogResourceTracker resourceTracker;
    private final AtomicBoolean closed = new AtomicBoolean();

    IcebergTableCache(long ttlSeconds, int maxSize) {
        this(ttlSeconds, maxSize, table -> () -> { }, null);
    }

    IcebergTableCache(long ttlSeconds, int maxSize, Function<Table, Runnable> cleanupFactory) {
        this(ttlSeconds, maxSize, cleanupFactory, null);
    }

    IcebergTableCache(long ttlSeconds, int maxSize, Function<Table, Runnable> cleanupFactory,
            IcebergCatalogResourceTracker resourceTracker) {
        this.cleanupFactory = cleanupFactory;
        this.resourceTracker = resourceTracker;
        // "<= 0 disables" connector TTL contract, folded to CacheSpec's disable sentinel (CacheSpec.ofConnectorTtl).
        CacheSpec spec = CacheSpec.ofConnectorTtl(ttlSeconds, maxSize);
        this.entry = new MetaCacheEntry<>("iceberg-table", null, spec,
                ForkJoinPool.commonPool(), false, true, 0L, true,
                (identifier, owner) -> owner.release());
    }

    /** Caching is on only when the TTL is positive; ttl-second &lt;= 0 means "always read live". */
    boolean isEnabled() {
        return entry.stats().isEffectiveEnabled();
    }

    /**
     * Returns the cached table for {@code identifier} if present and unexpired, else runs {@code loader} (the
     * live remote {@code loadTable}), caches and returns it. When caching is disabled ({@link #isEnabled()} is
     * false) {@code loader} runs every call and nothing is cached. A hit refreshes the entry's expiry
     * (access-based). The loader runs OUTSIDE Caffeine's compute lock (single-flight per key) and its exception
     * propagates unwrapped.
     */
    TableLease borrow(TableIdentifier identifier, Supplier<Table> loader) {
        while (true) {
            if (closed.get()) {
                throw new IllegalStateException("Iceberg table cache is already closed");
            }
            TableOwner[] loadedHere = {null};
            TableOwner owner = entry.get(identifier, ignored -> {
                IcebergCatalogResourceTracker.LoadGuard guard =
                        resourceTracker == null ? null : resourceTracker.beginLoad();
                try {
                    Table table = loader.get();
                    Runnable tableCleanup = cleanupFactory.apply(table);
                    IcebergCatalogResourceTracker.ResourceLease catalogLease =
                            guard == null ? null : guard.promote();
                    Runnable cleanup = () -> {
                        try {
                            tableCleanup.run();
                        } finally {
                            if (catalogLease != null) {
                                catalogLease.close();
                            }
                        }
                    };
                    TableOwner loaded = new TableOwner(table, cleanup, true);
                    loadedHere[0] = loaded;
                    return loaded;
                } finally {
                    if (guard != null) {
                        guard.close();
                    }
                }
            });
            try {
                if (closed.get()) {
                    // close() may have invalidated the cache while this loader was outside the cache lock.
                    // Remove a late publication before releasing the loader reference below.
                    entry.invalidateKey(identifier);
                    throw new IllegalStateException("Iceberg table cache is already closed");
                }
                TableLease lease = owner.tryBorrow();
                if (lease != null) {
                    return lease;
                }
                // Removal won the race between lookup and retain. Retry against the current cache generation.
            } finally {
                if (loadedHere[0] != null) {
                    loadedHere[0].release();
                }
            }
        }
    }

    /** Test-only convenience for cache membership tests that do not model a live statement borrower. */
    Table getOrLoad(TableIdentifier identifier, Supplier<Table> loader) {
        try (TableLease lease = borrow(identifier, loader)) {
            return lease.table();
        }
    }

    /** Drops the cached entry for one table so the next read goes live (REFRESH TABLE). */
    void invalidate(TableIdentifier identifier) {
        entry.invalidateKey(identifier);
    }

    /**
     * Drops every cached entry for one database so the next read of any of its tables goes live
     * (REFRESH DATABASE / a Doris-issued DROP DATABASE). Entries are keyed by
     * {@code TableIdentifier.of(db, table)} (single-level namespace = {@code [db]}), so a db match is
     * namespace equality — mirroring {@link IcebergLatestSnapshotCache#invalidateDb}.
     */
    void invalidateDb(String dbName) {
        Namespace ns = Namespace.of(dbName);
        entry.invalidateIf(id -> id.namespace().equals(ns));
    }

    /** Drops all cached entries. */
    void invalidateAll() {
        entry.invalidateAll();
    }

    /** Seals new borrows and retires every cache owner, including loaders that publish after invalidation. */
    void close() {
        if (closed.compareAndSet(false, true)) {
            entry.invalidateAll();
        }
    }

    /** Test-only: current number of cached entries (accurate map membership, not Caffeine's estimate). */
    int size() {
        int[] count = {0};
        entry.forEach((key, value) -> count[0]++);
        return count[0];
    }

    static final class TableLease implements AutoCloseable {
        private final TableOwner owner;
        private final AtomicBoolean closed = new AtomicBoolean();

        private TableLease(TableOwner owner) {
            this.owner = owner;
        }

        Table table() {
            return owner.table;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                owner.release();
            }
        }
    }

    private static final class TableOwner {
        private final Table table;
        private final Runnable cleanup;
        // A newly loaded value starts with a cache reference and a temporary loader reference. The temporary
        // reference bridges publication/discard to the first borrow, including invalidation-before-publication.
        private final AtomicInteger references;

        private TableOwner(Table table, Runnable cleanup, boolean loading) {
            this.table = table;
            this.cleanup = cleanup;
            this.references = new AtomicInteger(loading ? 2 : 1);
        }

        private TableLease tryBorrow() {
            int current = references.get();
            while (current != 0) {
                if (references.compareAndSet(current, current + 1)) {
                    return new TableLease(this);
                }
                current = references.get();
            }
            return null;
        }

        private void release() {
            int remaining = references.decrementAndGet();
            if (remaining == 0) {
                cleanup.run();
            } else if (remaining < 0) {
                throw new IllegalStateException("Iceberg table owner released too many times: " + table.name());
            }
        }
    }
}
