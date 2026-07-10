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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.MetaCacheEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

/**
 * A caching {@link HmsClient} decorator: it wraps another {@code HmsClient} (in production the pooled
 * {@link ThriftHmsClient}) and serves the three scan-hot-path read methods from a bounded, TTL-expiring
 * cache, delegating every other method verbatim.
 *
 * <p><b>Why this exists.</b> Today the hive connector caches nothing — {@code getTable},
 * {@code listPartitionNames} and {@code getPartitions} are fresh Thrift RPCs on every scan. Legacy fe-core
 * kept these in the engine-side {@code HiveExternalMetaCache}, which stops routing to a hive catalog once
 * it becomes a plugin-driven ({@code SPI_READY}) catalog. This decorator re-homes that caching inside the
 * connector (Trino {@code CachingHiveMetastore} shape), so the connector stays performance-neutral vs
 * legacy after the cutover. Because the {@code HmsClient} is also held by the hudi/iceberg siblings from
 * this same module, the decorator is reusable by them later.</p>
 *
 * <p><b>What it caches (4 methods)</b>, each on its own {@link MetaCacheEntry} configured from catalog
 * properties {@code meta.cache.hive.<entry>.(enable|ttl-second|capacity)} (defaults mirror the legacy
 * fe-core {@code Config} values — the connector is {@code Config}-free):</p>
 * <ul>
 *   <li>{@code getTable} — keyed by {@code (db, table)} → {@link HmsTableInfo}.</li>
 *   <li>{@code listPartitionNames} — keyed by {@code (db, table, maxParts)} → partition-name list. Real
 *       callers pass the unbounded {@code maxParts}, so this is effectively one entry per table; keeping
 *       {@code maxParts} in the key keeps a bounded request from ever being served a fuller list.</li>
 *   <li>{@code getPartitions} — keyed by {@code (db, table, requested-name-list)} → partition list. This is
 *       RPC-argument granularity: a different requested set is a separate entry (no cross-request
 *       assembly). {@link HmsPartitionInfo} carries {@code transient_lastDdlTime} in its parameters, which
 *       a later step reads through this cache for the table max-modify-time.</li>
 *   <li>{@code getTableColumnStatistics} — keyed by {@code (db, table, requested-column-list)} → the
 *       (possibly sparse or empty) stats list. Same RPC-argument granularity; the empty-list "no stats"
 *       result is a legitimate cached value (only {@code null} loads are skipped). This is the planner
 *       column-stats fast path, off the scan hot path, so it caches at low priority but on the same
 *       machinery as the rest.</li>
 * </ul>
 *
 * <p><b>Pass-through.</b> Every other read, plus every write / DDL / ACID method, is passed straight
 * through to the delegate. A later invalidation step arms {@link #flush(String, String)} /
 * {@link #flushAll()} onto {@code REFRESH TABLE} / {@code REFRESH CATALOG}. This decorator does NOT
 * self-invalidate around writes — coarse REFRESH + TTL bound staleness.</p>
 *
 * <p><b>Cache-value safety.</b> {@code HmsTableInfo} / {@code HmsPartitionInfo} / {@code HmsColumnStatistics}
 * are immutable (all fields final, collections unmodifiable), so caching them by reference is safe. The
 * three list-returning methods cache and return the delegate's outer {@code List} container by reference and
 * do NOT defensively copy it — its elements are immutable but the container is shared, so callers must treat
 * a returned collection as read-only (the codebase-wide metadata-cache convention). Null loads are never
 * cached (the framework treats {@code null} as a miss), and a loader exception ({@link HmsClientException})
 * propagates to the caller and is not cached.</p>
 *
 * <p><b>Dormant.</b> Nothing wraps a client with this decorator yet — {@code HiveConnector} still returns a
 * raw {@code ThriftHmsClient}, and {@code "hms"} is not in {@code SPI_READY_TYPES}, so no live catalog
 * builds a {@code HiveConnector} at all. Wiring the decorator into the client and the freshness probes is a
 * later step; this class is fully unit-testable in isolation now.</p>
 */
public class CachingHmsClient implements HmsClient {

    /** Engine token for the {@code meta.cache.<engine>.<entry>.*} property namespace. */
    static final String ENGINE = "hive";
    /** {@code meta.cache.hive.table.*} — cached {@link HmsTableInfo}. */
    static final String ENTRY_TABLE = "table";
    /** {@code meta.cache.hive.partition_names.*} — cached partition-name lists. */
    static final String ENTRY_PARTITION_NAMES = "partition_names";
    /** {@code meta.cache.hive.partition.*} — cached partition-object lists. */
    static final String ENTRY_PARTITION = "partition";
    /** {@code meta.cache.hive.column_stats.*} — cached column-statistics lists. */
    static final String ENTRY_COLUMN_STATS = "column_stats";

    // Legacy fe-core Config values, mirrored locally (the connector never touches fe-core Config):
    //   TTL       = Config.external_cache_expire_time_seconds_after_access (86400s = 24h), shared by all entries
    //   table cap = Config.max_external_schema_cache_num          (per-table metadata sizing)
    //   names cap = Config.max_hive_partition_table_cache_num     (per-table partition-name lists)
    //   part cap  = Config.max_hive_partition_cache_num           (partition objects)
    //   stats cap = Config.max_external_schema_cache_num          (per-table, no legacy hive cache; reuse table sizing)
    static final long DEFAULT_TTL_SECOND = 86400L;
    static final long DEFAULT_TABLE_CAPACITY = 10000L;
    static final long DEFAULT_PARTITION_NAMES_CAPACITY = 10000L;
    static final long DEFAULT_PARTITION_CAPACITY = 100000L;
    static final long DEFAULT_COLUMN_STATS_CAPACITY = 10000L;

    private final HmsClient delegate;
    private final MetaCacheEntry<TableKey, HmsTableInfo> tableCache;
    private final MetaCacheEntry<PartitionNamesKey, List<String>> partitionNamesCache;
    private final MetaCacheEntry<PartitionsKey, List<HmsPartitionInfo>> partitionsCache;
    private final MetaCacheEntry<ColumnStatsKey, List<HmsColumnStatistics>> columnStatsCache;

    public CachingHmsClient(HmsClient delegate, Map<String, String> properties) {
        this.delegate = Objects.requireNonNull(delegate, "delegate can not be null");
        Map<String, String> props = properties == null ? Collections.emptyMap() : properties;
        this.tableCache = newEntry("hive.table", props, ENTRY_TABLE, DEFAULT_TABLE_CAPACITY);
        this.partitionNamesCache =
                newEntry("hive.partition_names", props, ENTRY_PARTITION_NAMES, DEFAULT_PARTITION_NAMES_CAPACITY);
        this.partitionsCache = newEntry("hive.partition", props, ENTRY_PARTITION, DEFAULT_PARTITION_CAPACITY);
        this.columnStatsCache =
                newEntry("hive.column_stats", props, ENTRY_COLUMN_STATS, DEFAULT_COLUMN_STATS_CAPACITY);
    }

    private static <K, V> MetaCacheEntry<K, V> newEntry(String name, Map<String, String> props,
            String entry, long defaultCapacity) {
        CacheSpec spec = CacheSpec.fromProperties(props, ENGINE, entry,
                CacheSpec.of(true, DEFAULT_TTL_SECOND, defaultCapacity));
        // Contextual-only + manual-miss load so a slow HMS RPC runs outside Caffeine's sync compute lock
        // (deduplicated by a striped lock instead), mirroring PaimonLatestSnapshotCache / IcebergLatestSnapshotCache.
        return new MetaCacheEntry<>(name, null, spec, ForkJoinPool.commonPool(), false, true, 0L, true);
    }

    // ========== Cached reads ==========

    @Override
    public HmsTableInfo getTable(String dbName, String tableName) {
        return tableCache.get(new TableKey(dbName, tableName),
                key -> delegate.getTable(key.dbName, key.tableName));
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
        return partitionNamesCache.get(new PartitionNamesKey(dbName, tableName, maxParts),
                key -> delegate.listPartitionNames(key.dbName, key.tableName, key.maxParts));
    }

    @Override
    public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
        return partitionsCache.get(new PartitionsKey(dbName, tableName, partNames),
                key -> delegate.getPartitions(key.dbName, key.tableName, key.partNames));
    }

    @Override
    public List<HmsColumnStatistics> getTableColumnStatistics(String dbName, String tableName,
            List<String> columns) {
        return columnStatsCache.get(new ColumnStatsKey(dbName, tableName, columns),
                key -> delegate.getTableColumnStatistics(key.dbName, key.tableName, key.columns));
    }

    // ========== Coarse invalidation (wired onto REFRESH TABLE / REFRESH CATALOG in a later step) ==========

    /** Drop every cached entry for one table. Backs {@code REFRESH TABLE}. */
    public void flush(String dbName, String tableName) {
        tableCache.invalidateKey(new TableKey(dbName, tableName));
        partitionNamesCache.invalidateIf(key -> key.matches(dbName, tableName));
        partitionsCache.invalidateIf(key -> key.matches(dbName, tableName));
        columnStatsCache.invalidateIf(key -> key.matches(dbName, tableName));
    }

    /** Drop the whole cache. Backs {@code REFRESH CATALOG}. */
    public void flushAll() {
        tableCache.invalidateAll();
        partitionNamesCache.invalidateAll();
        partitionsCache.invalidateAll();
        columnStatsCache.invalidateAll();
    }

    // ========== Pass-through: everything else is delegated verbatim ==========

    @Override
    public List<String> listDatabases() {
        return delegate.listDatabases();
    }

    @Override
    public HmsDatabaseInfo getDatabase(String dbName) {
        return delegate.getDatabase(dbName);
    }

    @Override
    public List<String> listTables(String dbName) {
        return delegate.listTables(dbName);
    }

    @Override
    public boolean tableExists(String dbName, String tableName) {
        return delegate.tableExists(dbName, tableName);
    }

    @Override
    public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
        return delegate.getDefaultColumnValues(dbName, tableName);
    }

    @Override
    public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
        return delegate.getPartition(dbName, tableName, values);
    }

    @Override
    public void createDatabase(HmsCreateDatabaseRequest request) {
        delegate.createDatabase(request);
    }

    @Override
    public void dropDatabase(String dbName) {
        delegate.dropDatabase(dbName);
    }

    @Override
    public void createTable(HmsCreateTableRequest request) {
        delegate.createTable(request);
    }

    @Override
    public void dropTable(String dbName, String tableName) {
        delegate.dropTable(dbName, tableName);
    }

    @Override
    public void truncateTable(String dbName, String tableName, List<String> partitions) {
        delegate.truncateTable(dbName, tableName, partitions);
    }

    @Override
    public void addPartitions(String dbName, String tableName, List<HmsPartitionWithStatistics> partitions) {
        delegate.addPartitions(dbName, tableName, partitions);
    }

    @Override
    public void updateTableStatistics(String dbName, String tableName,
            Function<HmsPartitionStatistics, HmsPartitionStatistics> update) {
        delegate.updateTableStatistics(dbName, tableName, update);
    }

    @Override
    public void updatePartitionStatistics(String dbName, String tableName, String partitionName,
            Function<HmsPartitionStatistics, HmsPartitionStatistics> update) {
        delegate.updatePartitionStatistics(dbName, tableName, partitionName, update);
    }

    @Override
    public boolean dropPartition(String dbName, String tableName, List<String> partitionValues,
            boolean deleteData) {
        return delegate.dropPartition(dbName, tableName, partitionValues, deleteData);
    }

    @Override
    public boolean partitionExists(String dbName, String tableName, List<String> partitionValues) {
        return delegate.partitionExists(dbName, tableName, partitionValues);
    }

    @Override
    public long openTxn(String user) {
        return delegate.openTxn(user);
    }

    @Override
    public void commitTxn(long txnId) {
        delegate.commitTxn(txnId);
    }

    @Override
    public Map<String, String> getValidWriteIds(String fullTableName, long currentTransactionId) {
        return delegate.getValidWriteIds(fullTableName, currentTransactionId);
    }

    @Override
    public void acquireSharedLock(String queryId, long txnId, String user, String dbName,
            String tableName, List<String> partitionNames, long timeoutMs) {
        delegate.acquireSharedLock(queryId, txnId, user, dbName, tableName, partitionNames, timeoutMs);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    // ========== Cache keys ==========
    // All keys carry (db, table) so flush(db, table) can select every entry for one table.

    static final class TableKey {
        private final String dbName;
        private final String tableName;

        TableKey(String dbName, String tableName) {
            this.dbName = dbName;
            this.tableName = tableName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableKey)) {
                return false;
            }
            TableKey that = (TableKey) o;
            return Objects.equals(dbName, that.dbName) && Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName);
        }
    }

    static final class PartitionNamesKey {
        private final String dbName;
        private final String tableName;
        private final int maxParts;

        PartitionNamesKey(String dbName, String tableName, int maxParts) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.maxParts = maxParts;
        }

        boolean matches(String db, String table) {
            return Objects.equals(dbName, db) && Objects.equals(tableName, table);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PartitionNamesKey)) {
                return false;
            }
            PartitionNamesKey that = (PartitionNamesKey) o;
            return maxParts == that.maxParts
                    && Objects.equals(dbName, that.dbName)
                    && Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName, maxParts);
        }
    }

    static final class PartitionsKey {
        private final String dbName;
        private final String tableName;
        // Order-sensitive, defensively copied: same requested list (same order) → hit; a different set or
        // order → separate entry. Simple and correct (no partition-name reconstruction from values).
        private final List<String> partNames;

        PartitionsKey(String dbName, String tableName, List<String> partNames) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.partNames = partNames == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(partNames));
        }

        boolean matches(String db, String table) {
            return Objects.equals(dbName, db) && Objects.equals(tableName, table);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PartitionsKey)) {
                return false;
            }
            PartitionsKey that = (PartitionsKey) o;
            return Objects.equals(dbName, that.dbName)
                    && Objects.equals(tableName, that.tableName)
                    && Objects.equals(partNames, that.partNames);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName, partNames);
        }
    }

    static final class ColumnStatsKey {
        private final String dbName;
        private final String tableName;
        // Order-sensitive, defensively copied (same as PartitionsKey): the value is exactly the (sparse or
        // empty) stats list for this requested column set.
        private final List<String> columns;

        ColumnStatsKey(String dbName, String tableName, List<String> columns) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.columns = columns == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(columns));
        }

        boolean matches(String db, String table) {
            return Objects.equals(dbName, db) && Objects.equals(tableName, table);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ColumnStatsKey)) {
                return false;
            }
            ColumnStatsKey that = (ColumnStatsKey) o;
            return Objects.equals(dbName, that.dbName)
                    && Objects.equals(tableName, that.tableName)
                    && Objects.equals(columns, that.columns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName, columns);
        }
    }
}
