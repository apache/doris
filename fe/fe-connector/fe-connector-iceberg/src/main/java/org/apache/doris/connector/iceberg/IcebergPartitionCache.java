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
import org.apache.doris.connector.iceberg.IcebergPartitionUtils.IcebergRawPartition;

import org.apache.iceberg.catalog.TableIdentifier;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Per-catalog cache of an iceberg table's raw partition list (PERF-02), keyed by {@code (TableIdentifier,
 * snapshotId, schemaId, specId)}. Restores the partition-info half of the legacy
 * {@code IcebergExternalMetaCache} that the SPI
 * cutover dropped: the analysis-phase PARTITIONS metadata-table scan
 * ({@link IcebergPartitionUtils#loadRawPartitionsUncached}, which the iceberg SDK materializes by reading EVERY
 * data+delete manifest of the snapshot) was re-run per query and re-run 4~6 times per MTMV refresh, with no
 * cross-query reuse. The three consumers ({@code buildMvccPartitionView} for the MVCC/MTMV partition view,
 * {@code listPartitions} for {@code selectedPartitionNum}, {@code listPartitionNames} for SHOW PARTITIONS) all
 * funnel through it, so they share a single scan per metadata generation.
 *
 * <p><b>Metadata-generation-keyed.</b> A data commit yields a new snapshot id, while schema/spec IDs fence
 * metadata-only evolution that changes Iceberg's unified partition struct without changing the snapshot.
 *
 * <p><b>No credential gate</b> (unlike {@link IcebergTableCache}): the cached value is pure metadata (partition
 * names, values, transforms, timestamps, snapshot ids) and carries no {@code FileIO} / credential, so it is
 * safe to share across users and is built unconditionally (only the TTL knob disables it).
 *
 * <p>Backed identically to {@link IcebergLatestSnapshotCache}: a contextual, access-TTL {@link MetaCache}
 * with manual miss-load, so the scan runs OUTSIDE Caffeine's compute lock and its exception (e.g. the
 * dropped-partition-source-column {@link org.apache.iceberg.exceptions.ValidationException} that
 * {@code listPartitions} degrades on) propagates verbatim and a failed scan is not cached. TTL is
 * {@code meta.cache.iceberg.table.ttl-second}; {@code <= 0} disables (read live). Lives on the long-lived
 * per-catalog {@link IcebergConnector}; a REFRESH CATALOG rebuilds the connector and thus the cache.
 */
final class IcebergPartitionCache {

    /** Immutable composite key: partition projection depends on the snapshot and current schema/spec metadata. */
    static final class Key {
        final TableIdentifier id;
        final long snapshotId;
        final int schemaId;
        final int specId;

        Key(TableIdentifier id, long snapshotId, int schemaId, int specId) {
            this.id = id;
            this.snapshotId = snapshotId;
            this.schemaId = schemaId;
            this.specId = specId;
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
            return snapshotId == that.snapshotId
                    && schemaId == that.schemaId
                    && specId == that.specId
                    && Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, snapshotId, schemaId, specId);
        }
    }

    private final CatalogMetaCache owner;
    private final MetaCache<Key, List<IcebergRawPartition>> entry;

    IcebergPartitionCache(long ttlSeconds, int maxSize) {
        this(new CatalogMetaCache(), ttlSeconds, maxSize);
    }

    IcebergPartitionCache(CatalogMetaCache owner, long ttlSeconds, int maxSize) {
        this.owner = owner;
        // "<= 0 disables" connector TTL contract, folded to CacheSpec's disable sentinel (CacheSpec.ofConnectorTtl).
        CacheSpec spec = CacheSpec.ofConnectorTtl(ttlSeconds, maxSize);
        this.entry = owner.create(MetaCacheDefinition
                .<Key, List<IcebergRawPartition>>builder("iceberg-partition", spec, IcebergPartitionCache::scope)
                .build());
    }

    /** Caching is on only when the TTL is positive; ttl-second &lt;= 0 means "always scan live". */
    boolean isEnabled() {
        return entry.isEnabled();
    }

    /**
     * Returns the cached raw partition list for {@code key} if present and unexpired, else runs {@code loader}
     * (the live PARTITIONS scan), caches and returns it. Disabled cache -&gt; {@code loader} every call. The
     * loader runs OUTSIDE Caffeine's compute lock (single-flight per key) and its exception propagates unwrapped.
     */
    List<IcebergRawPartition> getOrLoad(Key key, Supplier<List<IcebergRawPartition>> loader) {
        return entry.get(key, ignored -> loader.get());
    }

    /** Drops every cached snapshot entry for one table so the next read scans live (REFRESH TABLE). */
    void invalidate(TableIdentifier id) {
        owner.invalidateTable(id.namespace().toString(), id.name());
    }

    /** Drops every cached entry for one database (REFRESH DATABASE / DROP DATABASE); db match = namespace equality. */
    void invalidateDb(String dbName) {
        owner.invalidateDatabase(dbName);
    }

    /** Drops all cached entries (REFRESH CATALOG). */
    void invalidateAll() {
        owner.invalidateCatalog();
    }

    /** Test-only: current number of cached entries (accurate map membership, not Caffeine's estimate). */
    int size() {
        return Math.toIntExact(entry.size());
    }

    /** Test-only: how many times the live loader (the PARTITIONS scan) actually ran — the metric gate. */
    long loadCountForTest() {
        return entry.loadSuccessCount();
    }

    private static ScopePath scope(Key key) {
        return ScopePath.table(key.id.namespace().toString(), key.id.name());
    }
}
