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
 * Per-catalog cache of an iceberg table's {@code comment} property (PERF-05), keyed by {@link TableIdentifier}.
 * Kills the per-table remote {@code loadTable} that {@code information_schema.tables} / {@code SHOW TABLE STATUS}
 * pays for EVERY table just to read its comment ({@code FrontendServiceImpl.listTableStatus} loops per table and
 * unconditionally calls {@code getComment} -&gt; {@link IcebergConnectorMetadata#getTableComment} -&gt;
 * {@code loadTable}).
 *
 * <p><b>Scope: REST vended-credentials catalogs ONLY</b> (see {@link IcebergConnector}). Plain catalogs already
 * reuse the raw {@link IcebergTableCache} (PERF-01) for the comment path, so this cache is not built for them (no
 * redundancy). A {@code iceberg.rest.session=user} catalog is DELIBERATELY excluded: there the {@code loadTable}
 * call itself carries the querying user's per-request authorization, and a connector-shared comment cache would
 * serve one user's comment to another whose delegated token was never validated for that table (a metadata
 * disclosure). Vended-credentials uses a single static catalog identity, so every user loads the same table and
 * sharing a comment is safe.
 *
 * <p><b>No credential gate</b> (unlike {@link IcebergTableCache}, like {@link IcebergPartitionCache} /
 * {@link IcebergFormatCache}): the cached value is a bare comment {@link String} with no {@code FileIO} /
 * credential. A comment changes only via external DDL, picked up through the REFRESH invalidate hooks. TTL is
 * {@code meta.cache.iceberg.table.ttl-second}; {@code <= 0} disables (read live), so a no-cache catalog serves a
 * fresh comment even when this object is built. Backed identically to {@link IcebergTableCache}: a contextual,
 * access-TTL {@link MetaCacheEntry} with manual miss-load, so the remote load runs OUTSIDE Caffeine's compute
 * lock and its exception (e.g. the view-handle {@code NoSuchTableException}) propagates verbatim and a failed
 * load is not cached. Lives on the long-lived per-catalog {@link IcebergConnector}; a REFRESH CATALOG rebuilds it.
 */
final class IcebergCommentCache {

    private final MetaCacheEntry<TableIdentifier, String> entry;

    IcebergCommentCache(long ttlSeconds, int maxSize) {
        // "<= 0 disables" connector TTL contract, folded to CacheSpec's disable sentinel (CacheSpec.ofConnectorTtl).
        // Load-bearing here: a vended no-cache catalog builds this object but must NOT cache comments
        // (operator "no meta cache" intent).
        CacheSpec spec = CacheSpec.ofConnectorTtl(ttlSeconds, maxSize);
        this.entry = new MetaCacheEntry<>("iceberg-comment", null, spec,
                ForkJoinPool.commonPool(), false, true, 0L, true);
    }

    /** Caching is on only when the TTL is positive; ttl-second &lt;= 0 means "always read the comment live". */
    boolean isEnabled() {
        return entry.stats().isEffectiveEnabled();
    }

    /**
     * Returns the cached comment for {@code identifier} if present and unexpired, else runs {@code loader} (the
     * live remote {@code loadTable} + property read), caches and returns it. Disabled cache -&gt; {@code loader}
     * every call. The loader runs OUTSIDE Caffeine's compute lock (single-flight per key) and its exception
     * propagates unwrapped and is NOT cached (the caller degrades a thrown comment load to {@code ""}).
     */
    String getOrLoad(TableIdentifier identifier, Supplier<String> loader) {
        return entry.get(identifier, ignored -> loader.get());
    }

    /** Drops the cached comment for one table so the next read goes live (REFRESH TABLE). */
    void invalidate(TableIdentifier identifier) {
        entry.invalidateKey(identifier);
    }

    /** Drops every cached comment for one database (REFRESH DATABASE / DROP DATABASE); match = namespace equality. */
    void invalidateDb(String dbName) {
        Namespace ns = Namespace.of(dbName);
        entry.invalidateIf(id -> id.namespace().equals(ns));
    }

    /** Drops all cached comments (REFRESH CATALOG). */
    void invalidateAll() {
        entry.invalidateAll();
    }

    /** Test-only: current number of cached entries (accurate map membership, not Caffeine's estimate). */
    int size() {
        int[] count = {0};
        entry.forEach((key, value) -> count[0]++);
        return count[0];
    }

    /** Test-only: how many times the live loader (the remote comment load) actually ran — the metric gate. */
    long loadCountForTest() {
        return entry.stats().getLoadSuccessCount();
    }
}
