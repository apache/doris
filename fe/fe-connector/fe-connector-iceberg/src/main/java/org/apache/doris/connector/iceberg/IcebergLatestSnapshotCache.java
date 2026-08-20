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
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccPartitionView;

import org.apache.iceberg.catalog.TableIdentifier;

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
 * <p><b>Value carries snapshotId, schemaId and the resolved-empty partition style.</b>
 * {@code beginQuerySnapshot} pins the snapshot id <i>and</i> the LATEST schema id
 * ({@code table.schema().schemaId()} — not {@code currentSnapshot().schemaId()}, mirroring legacy
 * {@code IcebergUtils.getLatestIcebergSnapshot}). A schema-only {@code ALTER} bumps the latest schema id
 * without producing a new snapshot, so the ids must be captured atomically. When the snapshot id is negative,
 * the partition style must come from that same metadata generation; otherwise later spec evolution could make
 * an empty scan expose live partition metadata.
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

    /** Immutable atomic pin for the latest snapshot/schema and its resolved-empty partition style. */
    static final class CachedSnapshot {
        final long snapshotId;
        final long schemaId;
        final ConnectorMvccPartitionView.Style emptyPartitionStyle;

        CachedSnapshot(long snapshotId, long schemaId) {
            this(snapshotId, schemaId, ConnectorMvccPartitionView.Style.UNPARTITIONED);
        }

        CachedSnapshot(long snapshotId, long schemaId,
                ConnectorMvccPartitionView.Style emptyPartitionStyle) {
            this.snapshotId = snapshotId;
            this.schemaId = schemaId;
            this.emptyPartitionStyle = emptyPartitionStyle;
        }
    }

    private final CatalogMetaCache owner;
    private final MetaCache<TableIdentifier, CachedSnapshot> entry;

    IcebergLatestSnapshotCache(long ttlSeconds, int maxSize) {
        this(new CatalogMetaCache(), ttlSeconds, maxSize);
    }

    IcebergLatestSnapshotCache(CatalogMetaCache owner, long ttlSeconds, int maxSize) {
        this.owner = owner;
        // "<= 0 disables" connector TTL contract, folded to CacheSpec's disable sentinel (CacheSpec.ofConnectorTtl).
        CacheSpec spec = CacheSpec.ofConnectorTtl(ttlSeconds, maxSize);
        this.entry = owner.create(MetaCacheDefinition
                .<TableIdentifier, CachedSnapshot>builder(
                        "iceberg-latest-snapshot", spec, IcebergLatestSnapshotCache::scope)
                .build());
    }

    /** Caching is on only when the TTL is positive; ttl-second &lt;= 0 means "always read live". */
    boolean isEnabled() {
        return entry.isEnabled();
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
        owner.invalidateTable(identifier.namespace().toString(), identifier.name());
    }

    /**
     * Drops every cached entry for one database so the next read of any of its tables goes live
     * (REFRESH DATABASE / a Doris-issued DROP DATABASE). Entries are keyed by
     * {@code TableIdentifier.of(db, table)} (single-level namespace = {@code [db]}, see
     * {@code IcebergConnectorMetadata.beginQuerySnapshot}), so a db match is namespace equality.
     */
    void invalidateDb(String dbName) {
        owner.invalidateDatabase(dbName);
    }

    /** Drops all cached entries. */
    void invalidateAll() {
        owner.invalidateCatalog();
    }

    /** Test-only: current number of cached entries (accurate map membership, not Caffeine's estimate). */
    int size() {
        return Math.toIntExact(entry.size());
    }

    private static ScopePath scope(TableIdentifier identifier) {
        return ScopePath.table(identifier.namespace().toString(), identifier.name());
    }
}
