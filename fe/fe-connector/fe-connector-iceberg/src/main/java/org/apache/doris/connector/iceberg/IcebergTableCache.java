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

    private final MetaCacheEntry<TableIdentifier, Table> entry;

    IcebergTableCache(long ttlSeconds, int maxSize) {
        // "<= 0 disables" connector TTL contract, folded to CacheSpec's disable sentinel (CacheSpec.ofConnectorTtl).
        CacheSpec spec = CacheSpec.ofConnectorTtl(ttlSeconds, maxSize);
        this.entry = new MetaCacheEntry<>("iceberg-table", null, spec,
                ForkJoinPool.commonPool(), false, true, 0L, true);
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
    Table getOrLoad(TableIdentifier identifier, Supplier<Table> loader) {
        return entry.get(identifier, ignored -> loader.get());
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

    /** Test-only: current number of cached entries (accurate map membership, not Caffeine's estimate). */
    int size() {
        int[] count = {0};
        entry.forEach((key, value) -> count[0]++);
        return count[0];
    }
}
