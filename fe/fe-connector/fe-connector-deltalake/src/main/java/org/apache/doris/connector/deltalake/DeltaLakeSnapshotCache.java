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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.JvmSizeUtils;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.MetaCacheSizeEstimate;
import org.apache.doris.connector.cache.MetaCacheSizeEstimators;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.engine.Engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Catalog-owned immutable snapshots, isolated by user and fenced by the shared refresh registry. */
final class DeltaLakeSnapshotCache {
    static final String LATEST = "latest";
    static final String SNAPSHOT = "snapshot";
    private static final long DEFAULT_SNAPSHOT_BYTES = 256L << 20;
    private final MetaCache<TableKey, Latest> latest;
    private final MetaCache<SnapshotKey, CachedSnapshot> snapshots;
    private final long latestTtlNanos;
    private final LongSupplier nanoTime;

    DeltaLakeSnapshotCache(CatalogMetaCache owner, Map<String, String> properties) {
        this(owner, properties, System::nanoTime);
    }

    DeltaLakeSnapshotCache(CatalogMetaCache owner, Map<String, String> properties, LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
        CacheSpec latestSpec = spec(properties, LATEST);
        latestTtlNanos = latestSpec.getTtlSecond() > 0
                ? TimeUnit.SECONDS.toNanos(latestSpec.getTtlSecond()) : Long.MAX_VALUE;
        // The framework's normal TTL is access-based. Apply fixed expiry below using the load-completion
        // timestamp, while retaining its single-flight loads, invalidation fences and capacity governance.
        long physicalTtl = latestSpec.getTtlSecond() == 0 ? 0 : CacheSpec.CACHE_NO_TTL;
        CacheSpec physicalSpec = latestSpec.getMaxWeight().isPresent()
                ? CacheSpec.ofWeight(latestSpec.isEnable(), physicalTtl, latestSpec.getCapacity(),
                        latestSpec.getMaxWeight().getAsLong())
                : CacheSpec.of(latestSpec.isEnable(), physicalTtl, latestSpec.getCapacity());
        latest = owner.create(MetaCacheDefinition.<TableKey, Latest>builder("deltalake-latest", physicalSpec,
                        key -> ScopePath.table(key.database, key.table))
                .sizeEstimator(MetaCacheSizeEstimators.reflective()).build());
        snapshots = owner.create(MetaCacheDefinition.<SnapshotKey, CachedSnapshot>builder("deltalake-snapshot",
                        spec(properties, SNAPSHOT), key -> ScopePath.table(key.table.database, key.table.table))
                .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(JvmSizeUtils.saturatedAdd(
                        value.bytes, key.estimatedBytes())))
                .build());
    }

    static CacheSpec spec(Map<String, String> properties, String entry) {
        Map<String, String> configured = new HashMap<>(properties);
        if (SNAPSHOT.equals(entry)) {
            configured.putIfAbsent("meta.cache.deltalake.snapshot.max-weight", String.valueOf(DEFAULT_SNAPSHOT_BYTES));
        }
        return CacheSpec.fromProperties(configured, "deltalake", entry,
                CacheSpec.of(true, LATEST.equals(entry) ? 30 : 1800, 1000));
    }

    static void validate(Map<String, String> properties) {
        for (String entry : new String[] {LATEST, SNAPSHOT}) {
            String prefix = "meta.cache.deltalake." + entry;
            CacheSpec.checkBooleanProperty(properties.get(prefix + ".enable"), prefix + ".enable");
            CacheSpec.checkLongProperty(properties.get(prefix + ".ttl-second"), -1, prefix + ".ttl-second");
            CacheSpec.checkLongProperty(properties.get(prefix + ".capacity"), 0, prefix + ".capacity");
        }
        CacheSpec.checkWeightProperties(properties, "deltalake", LATEST, SNAPSHOT);
    }

    Snapshot latest(ConnectorSession session, String database, String table, String path, Engine engine) {
        if (session == null) {
            // Metadata callers without a user identity must not reuse another caller's cached result.
            return loadLatest(path, engine);
        }
        return latest(new TableKey(session.getUser(), database, table, path),
                () -> loadLatest(path, engine),
                version -> TableManager.loadSnapshot(path).atVersion(version).build(engine));
    }

    Snapshot latest(TableKey key, Supplier<Snapshot> loader, VersionLoader versionLoader) {
        while (true) {
            Snapshot[] loaded = {null};
            Latest reference = latest.get(key, ignored -> {
                Snapshot snapshot = loader.get();
                DeltaLakeFeatureCheck.check(snapshot);
                loaded[0] = snapshot;
                SnapshotKey version = new SnapshotKey(key, DeltaKernelCompat.tableId(snapshot), snapshot.getVersion());
                snapshots.get(version, unused -> cached(snapshot));
                return new Latest(version.tableId, version.version, nanoTime.getAsLong());
            });
            if (latestTtlNanos != Long.MAX_VALUE && nanoTime.getAsLong() - reference.loadedAt >= latestTtlNanos) {
                latest.compareAndSet(key, reference, null);
                continue;
            }
            if (loaded[0] != null) {
                // A disabled or weight-rejected snapshot cache must not force the miss caller to load twice.
                return loaded[0];
            }
            SnapshotKey version = new SnapshotKey(key, reference.tableId, reference.version);
            return snapshots.get(version, ignored -> {
                Snapshot snapshot = versionLoader.load(reference.version);
                if (!reference.tableId.equals(DeltaKernelCompat.tableId(snapshot))) {
                    throw new DorisConnectorException("Delta table was recreated while its version was cached: "
                            + key.path);
                }
                DeltaLakeFeatureCheck.check(snapshot);
                return cached(snapshot);
            }).snapshot;
        }
    }

    private static Snapshot loadLatest(String path, Engine engine) {
        Snapshot snapshot = TableManager.loadSnapshot(path).build(engine);
        DeltaLakeFeatureCheck.check(snapshot);
        return snapshot;
    }

    private static CachedSnapshot cached(Snapshot snapshot) {
        return new CachedSnapshot(snapshot, DeltaKernelCompat.snapshotRetainedBytes(snapshot));
    }

    interface VersionLoader {
        Snapshot load(long version);
    }

    static final class TableKey {
        private final String user;
        private final String database;
        private final String table;
        private final String path;

        TableKey(String user, String database, String table, String path) {
            this.user = Objects.requireNonNull(user);
            this.database = database;
            this.table = table;
            this.path = path;
        }

        private long estimatedBytes() {
            return 128L + JvmSizeUtils.stringSize(user) + JvmSizeUtils.stringSize(database)
                    + JvmSizeUtils.stringSize(table) + JvmSizeUtils.stringSize(path);
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof TableKey)) {
                return false;
            }
            TableKey other = (TableKey) object;
            return user.equals(other.user) && database.equals(other.database)
                    && table.equals(other.table) && path.equals(other.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(user, database, table, path);
        }
    }

    private static final class SnapshotKey {
        private final TableKey table;
        private final String tableId;
        private final long version;

        private SnapshotKey(TableKey table, String tableId, long version) {
            this.table = table;
            this.tableId = tableId;
            this.version = version;
        }

        private long estimatedBytes() {
            return table.estimatedBytes() + JvmSizeUtils.stringSize(tableId) + 64;
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof SnapshotKey)) {
                return false;
            }
            SnapshotKey other = (SnapshotKey) object;
            return table.equals(other.table) && tableId.equals(other.tableId) && version == other.version;
        }

        @Override
        public int hashCode() {
            return Objects.hash(table, tableId, version);
        }
    }

    private static final class Latest {
        private final String tableId;
        private final long version;
        private final long loadedAt;

        private Latest(String tableId, long version, long loadedAt) {
            this.tableId = tableId;
            this.version = version;
            this.loadedAt = loadedAt;
        }
    }

    private static final class CachedSnapshot {
        private final Snapshot snapshot;
        private final long bytes;

        private CachedSnapshot(Snapshot snapshot, long bytes) {
            this.snapshot = snapshot;
            this.bytes = bytes;
        }
    }
}
