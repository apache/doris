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
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.ScopePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * A caching {@link HmsClient} decorator: it wraps another {@code HmsClient} (in production the pooled
 * {@link ThriftHmsClient}) and serves the three scan-hot-path read methods from a bounded, TTL-expiring
 * cache, delegating every other method verbatim.
 *
 * <p><b>Why this exists.</b> Without this decorator the hive connector would cache nothing — {@code getTable},
 * {@code listPartitionNames} and {@code getPartitions} would be fresh Thrift RPCs on every scan. Legacy fe-core
 * kept these in the engine-side {@code HiveExternalMetaCache}, which stops routing to a hive catalog once
 * it becomes a plugin-driven ({@code SPI_READY}) catalog. This decorator re-homes that caching inside the
 * connector (Trino {@code CachingHiveMetastore} shape), so the connector stays performance-neutral vs
 * legacy after the cutover. Because the {@code HmsClient} is also held by the hudi/iceberg siblings from
 * this same module, the decorator is reusable by them later.</p>
 *
 * <p><b>What it caches (4 methods)</b>, each on its own framework cache configured from catalog
 * properties {@code meta.cache.hive.<entry>.(enable|ttl-second|capacity)} (defaults mirror the legacy
 * fe-core {@code Config} values — the connector is {@code Config}-free):</p>
 * <ul>
 *   <li>{@code getTable} — keyed by {@code (db, table)} → {@link HmsTableInfo}.</li>
 *   <li>{@code listPartitionNames} — keyed by {@code (db, table, maxParts)} → partition-name list. Real
 *       callers pass the unbounded {@code maxParts}, so this is effectively one entry per table; keeping
 *       {@code maxParts} in the key keeps a bounded request from ever being served a fuller list.</li>
 *   <li>{@code getPartitions} — one entry PER PARTITION, keyed by {@code (db, table, partition-values)} →
 *       {@link HmsPartitionInfo}. A bulk request looks up each requested name (parsed to its values) and
 *       fetches only the misses in a single delegate call, storing each returned partition under its OWN
 *       values — so overlapping requests SHARE partition entries and the capacity bounds partition OBJECTS
 *       (legacy {@code HiveExternalMetaCache} / Trino {@code CachingHiveMetastore} shape), not request-lists.
 *       {@link HmsPartitionInfo} carries {@code transient_lastDdlTime} in its parameters, which a later step
 *       reads through this cache for the table max-modify-time.</li>
 *   <li>{@code getTableColumnStatistics} — keyed by {@code (db, table, requested-column-list)} → the
 *       (possibly sparse or empty) stats list. Same RPC-argument granularity; the empty-list "no stats"
 *       result is a legitimate cached value (only {@code null} loads are skipped). This is the planner
 *       column-stats fast path, off the scan hot path, so it caches at low priority but on the same
 *       machinery as the rest.</li>
 * </ul>
 *
 * <p><b>Pass-through.</b> Every other read, plus every write / DDL / ACID method, is passed straight
 * through to the delegate. Cache invalidation belongs to the connector's shared {@link CatalogMetaCache}
 * owner, not to this read decorator. This decorator does NOT self-invalidate around writes — coarse REFRESH
 * + TTL bound staleness.</p>
 *
 * <p><b>Cache-value safety.</b> {@code HmsTableInfo} / {@code HmsPartitionInfo} / {@code HmsColumnStatistics}
 * are immutable (all fields final, collections unmodifiable), so caching them by reference is safe. The
 * three list-returning methods cache and return the delegate's outer {@code List} container by reference and
 * do NOT defensively copy it — its elements are immutable but the container is shared, so callers must treat
 * a returned collection as read-only (the codebase-wide metadata-cache convention). Null loads are never
 * cached (the framework treats {@code null} as a miss), and a loader exception ({@link HmsClientException})
 * propagates to the caller and is not cached.</p>
 *
 * <p><b>Live since the hms flip.</b> {@code HiveConnector.createClient} wraps the pooled
 * {@code ThriftHmsClient} in this decorator (see {@code HiveConnector.wrapWithCache}), so every hmsClient read
 * in {@code HiveConnectorMetadata} — including the freshness probes — is cache-backed. Fully unit-testable in
 * isolation.</p>
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
    private final MetaCache<TableKey, HmsTableInfo> tableCache;
    private final MetaCache<PartitionNamesKey, List<String>> partitionNamesCache;
    private final MetaCache<PartitionKey, HmsPartitionInfo> partitionsCache;
    private final MetaCache<ColumnStatsKey, List<HmsColumnStatistics>> columnStatsCache;
    private final ConcurrentMap<PartitionKey, PartitionLoadBatch> inFlightPartitionLoads =
            new ConcurrentHashMap<>();

    public CachingHmsClient(CatalogMetaCache owner, HmsClient delegate, Map<String, String> properties) {
        Objects.requireNonNull(owner, "owner can not be null");
        this.delegate = Objects.requireNonNull(delegate, "delegate can not be null");
        Map<String, String> props = applyLegacyTtlCompatibility(
                properties == null ? Collections.emptyMap() : properties);
        this.tableCache = newEntry(owner, "hive-table", props, ENTRY_TABLE, DEFAULT_TABLE_CAPACITY,
                key -> ScopePath.table(key.dbName, key.tableName));
        this.partitionNamesCache = newEntry(owner, "hive-partition-names", props, ENTRY_PARTITION_NAMES,
                DEFAULT_PARTITION_NAMES_CAPACITY,
                key -> ScopePath.partitionCollection(key.dbName, key.tableName));
        this.partitionsCache = newEntry(owner, "hive-partition", props, ENTRY_PARTITION,
                DEFAULT_PARTITION_CAPACITY,
                key -> ScopePath.partition(key.dbName, key.tableName, key.values));
        this.columnStatsCache = newEntry(owner, "hive-column-stats", props, ENTRY_COLUMN_STATS,
                DEFAULT_COLUMN_STATS_CAPACITY,
                key -> ScopePath.table(key.dbName, key.tableName));
    }

    private static <K, V> MetaCache<K, V> newEntry(CatalogMetaCache owner, String name,
            Map<String, String> props, String entry, long defaultCapacity, Function<K, ScopePath> scopeResolver) {
        CacheSpec spec = CacheSpec.fromProperties(props, ENGINE, entry,
                CacheSpec.of(true, DEFAULT_TTL_SECOND, defaultCapacity));
        return owner.create(MetaCacheDefinition.<K, V>builder(name, spec, scopeResolver).build());
    }

    /** Legacy fe-core catalog knob ({@code ExternalCatalog.SCHEMA_CACHE_TTL_SECOND}) for the table/schema cache. */
    static final String LEGACY_SCHEMA_CACHE_TTL_SECOND = "schema.cache.ttl-second";
    /** Legacy fe-core knob ({@code HMSExternalCatalog.PARTITION_CACHE_TTL_SECOND}) for the partition-list cache. */
    static final String LEGACY_PARTITION_CACHE_TTL_SECOND = "partition.cache.ttl-second";

    /**
     * Translate the legacy fe-core catalog TTL knobs into this client's namespaced entry keys, mirroring
     * {@code HiveExternalMetaCache.catalogPropertyCompatibilityMap} so an existing "hms" catalog that set the old
     * keys keeps working after the SPI cutover:
     * <ul>
     *   <li>{@code schema.cache.ttl-second}    &rarr; {@code meta.cache.hive.table.ttl-second} (schema/table meta,
     *       backs DESC)</li>
     *   <li>{@code partition.cache.ttl-second} &rarr; {@code meta.cache.hive.partition_names.ttl-second} (the
     *       partition-name list — legacy's {@code partition_values} entry; disabling it makes a newly-added
     *       partition visible without REFRESH)</li>
     * </ul>
     * Only the TTL is remapped (the sole knob the legacy keys exposed); {@code enable}/{@code capacity} have no
     * legacy equivalent. If both the legacy and namespaced keys are present, the namespaced key wins
     * ({@link CacheSpec#applyCompatibilityMap} contract).
     */
    private static Map<String, String> applyLegacyTtlCompatibility(Map<String, String> props) {
        Map<String, String> compat = new HashMap<>();
        compat.put(LEGACY_SCHEMA_CACHE_TTL_SECOND, CacheSpec.metaCacheTtlKey(ENGINE, ENTRY_TABLE));
        compat.put(LEGACY_PARTITION_CACHE_TTL_SECOND, CacheSpec.metaCacheTtlKey(ENGINE, ENTRY_PARTITION_NAMES));
        return CacheSpec.applyCompatibilityMap(props, compat);
    }

    // ========== Cached reads ==========

    @Override
    public HmsTableInfo getTable(String dbName, String tableName) {
        return tableCache.get(new TableKey(dbName, tableName),
                key -> delegate.getTable(key.dbName, key.tableName));
    }

    @Override
    public HmsTableInfo getTableFresh(String dbName, String tableName) {
        // Fresh (cache-bypassing) table read for SHOW CREATE TABLE, which must reflect the latest remote schema
        // even while DESC (served from the schema cache backed by this tableCache) still shows a stale one. This
        // neither READS nor WRITES tableCache: reading would serve the stale table this method exists to avoid,
        // writing would let a non-cache path repopulate off-band (mirrors listPartitionNamesFresh).
        return delegate.getTableFresh(dbName, tableName);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
        return partitionNamesCache.get(new PartitionNamesKey(dbName, tableName, maxParts),
                key -> delegate.listPartitionNames(key.dbName, key.tableName, key.maxParts));
    }

    @Override
    public List<String> listPartitionNamesFresh(String dbName, String tableName, int maxParts) {
        // Fresh (cache-bypassing) listing for SHOW PARTITIONS / the partitions metadata TVF — legacy read the raw
        // pooled client, never the metadata cache. This neither READS nor WRITES partitionNamesCache: reading would
        // serve the stale list this method exists to avoid, and writing would let a non-cache path repopulate the
        // cache off-band. The query-pruning path stays on the cached listPartitionNames (use_meta_cache contract).
        return delegate.listPartitionNames(dbName, tableName, maxParts);
    }

    @Override
    public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
        return getPartitionsWithStats(dbName, tableName, partNames).getPartitions();
    }

    @Override
    public HmsPartitionBatchResult getPartitionsWithStats(
            String dbName, String tableName, List<String> partNames) {
        return getPartitionsWithStats(dbName, tableName, partNames, false);
    }

    @Override
    public List<HmsPartitionInfo> getExistingPartitions(
            String dbName, String tableName, List<String> partNames) {
        return getExistingPartitionsWithStats(dbName, tableName, partNames).getPartitions();
    }

    @Override
    public HmsPartitionBatchResult getExistingPartitionsWithStats(
            String dbName, String tableName, List<String> partNames) {
        return getPartitionsWithStats(dbName, tableName, partNames, true);
    }

    private HmsPartitionBatchResult getPartitionsWithStats(
            String dbName, String tableName, List<String> partNames, boolean allowMissing) {
        long logicalStartNanos = System.nanoTime();
        if (partNames == null || partNames.isEmpty()) {
            HmsPartitionBatchStats stats = HmsPartitionBatchStats.builder()
                    .logicalElapsedNanos(System.nanoTime() - logicalStartNanos)
                    .build();
            return new HmsPartitionBatchResult(Collections.emptyList(), stats);
        }
        HmsPartitionRequest request = HmsPartitionRequest.builder()
                .database(dbName)
                .table(tableName)
                .partitionNames(partNames)
                .build();
        // Keep the existing cache policy: aggregate every miss into one logical delegate request and publish
        // only after that request succeeds. Reassemble from partition identities afterwards because a mixed
        // hit/miss request must preserve the caller's exact order even when HMS returns a different order.
        List<List<String>> requestedValues = new ArrayList<>(partNames.size());
        Map<List<String>, HmsPartitionInfo> resultByIdentity = new HashMap<>();
        List<HmsPartitionIdentity.ParsedPartitionName> misses = new ArrayList<>();
        for (HmsPartitionIdentity.ParsedPartitionName partition : request.getPartitions()) {
            List<String> values = partition.getValues();
            requestedValues.add(values);
            HmsPartitionInfo hit = partitionsCache.getIfPresent(new PartitionKey(dbName, tableName, values));
            if (hit != null) {
                resultByIdentity.put(values, hit);
            } else {
                misses.add(partition);
            }
        }
        PartitionStatsAccumulator physicalStats = new PartitionStatsAccumulator();
        if (!misses.isEmpty()) {
            try {
                if (partitionsCache.isEnabled()) {
                    loadMissingPartitions(dbName, tableName, allowMissing, misses,
                            resultByIdentity, physicalStats);
                } else {
                    loadOwnedPartitions(dbName, tableName, allowMissing, misses,
                            resultByIdentity, physicalStats, null);
                }
            } catch (HmsClientException e) {
                HmsPartitionBatchStats failedStats = e.getPartitionBatchStats();
                if (failedStats != null) {
                    physicalStats.add(failedStats);
                    e.withPartitionBatchStats(physicalStats.build(
                            partNames.size(), System.nanoTime() - logicalStartNanos));
                }
                throw e;
            }
        }
        List<HmsPartitionInfo> result = new ArrayList<>(partNames.size());
        for (int i = 0; i < requestedValues.size(); i++) {
            List<String> values = requestedValues.get(i);
            if (allowMissing && !resultByIdentity.containsKey(values)) {
                continue;
            }
            HmsPartitionInfo partition = resultByIdentity.get(values);
            if (partition == null) {
                throw HmsPartitionResultException.builder(partNames.size(), resultByIdentity.size())
                        .missing(request.getPartitions().get(i).getName())
                        .build();
            }
            result.add(partition);
        }
        HmsPartitionBatchStats stats = physicalStats.build(
                partNames.size(), System.nanoTime() - logicalStartNanos);
        return new HmsPartitionBatchResult(result, stats);
    }

    private void loadMissingPartitions(String dbName, String tableName, boolean allowMissing,
            List<HmsPartitionIdentity.ParsedPartitionName> initialMisses,
            Map<List<String>, HmsPartitionInfo> resultByIdentity,
            PartitionStatsAccumulator physicalStats) {
        List<HmsPartitionIdentity.ParsedPartitionName> pending = initialMisses;
        while (!pending.isEmpty()) {
            // Elect one owner per missing identity. One caller can own a batch and wait on identities owned by
            // another caller, so partially overlapping requests still issue one transport load per identity.
            PartitionLoadBatch ownedBatch = new PartitionLoadBatch();
            List<PartitionLoadRegistration> owned = new ArrayList<>();
            Map<PartitionLoadBatch, List<PartitionLoadRegistration>> waiting = new IdentityHashMap<>();
            try {
                for (HmsPartitionIdentity.ParsedPartitionName partition : pending) {
                    registerPartitionLoad(dbName, tableName, partition, ownedBatch,
                            resultByIdentity, owned, waiting);
                }
                afterPartitionLoadRegistrationForTest();
                if (!owned.isEmpty()) {
                    loadOwnedPartitions(dbName, tableName, allowMissing, registrationsToPartitions(owned),
                            resultByIdentity, physicalStats, ownedBatch);
                }
                ownedBatch.complete(null);
            } catch (RuntimeException | Error failure) {
                ownedBatch.complete(failure);
                releaseWaitingBatches(waiting.keySet());
                throw failure;
            } finally {
                releaseOwnedPartitionLoads(ownedBatch);
            }
            List<HmsPartitionIdentity.ParsedPartitionName> retries = new ArrayList<>();
            consumeWaitingBatches(waiting, resultByIdentity, retries);
            pending = retries;
        }
    }

    void afterPartitionLoadRegistrationForTest() {
    }

    int inFlightPartitionLoadCountForTest() {
        return inFlightPartitionLoads.size();
    }

    private void registerPartitionLoad(String dbName, String tableName,
            HmsPartitionIdentity.ParsedPartitionName partition, PartitionLoadBatch ownedBatch,
            Map<List<String>, HmsPartitionInfo> resultByIdentity,
            List<PartitionLoadRegistration> owned,
            Map<PartitionLoadBatch, List<PartitionLoadRegistration>> waiting) {
        PartitionKey key = new PartitionKey(dbName, tableName, partition.getValues());
        HmsPartitionInfo hit = partitionsCache.getIfPresent(key);
        if (hit != null) {
            resultByIdentity.put(partition.getValues(), hit);
            return;
        }
        while (true) {
            PartitionLoadBatch existing = inFlightPartitionLoads.putIfAbsent(key, ownedBatch);
            if (existing == null) {
                ownedBatch.claimedKeys.add(key);
                hit = partitionsCache.getIfPresent(key);
                if (hit == null) {
                    owned.add(new PartitionLoadRegistration(partition, key));
                } else {
                    resultByIdentity.put(partition.getValues(), hit);
                    ownedBatch.claimedKeys.remove(key);
                    inFlightPartitionLoads.remove(key, ownedBatch);
                }
                return;
            }
            List<PartitionLoadRegistration> registrations = waiting.get(existing);
            if (registrations != null) {
                registrations.add(new PartitionLoadRegistration(partition, key));
                return;
            }
            if (existing.tryRegisterWaiter()) {
                waiting.computeIfAbsent(existing, ignored -> new ArrayList<>())
                        .add(new PartitionLoadRegistration(partition, key));
                return;
            }
            inFlightPartitionLoads.remove(key, existing);
        }
    }

    private void loadOwnedPartitions(String dbName, String tableName, boolean allowMissing,
            List<HmsPartitionIdentity.ParsedPartitionName> owned,
            Map<List<String>, HmsPartitionInfo> resultByIdentity,
            PartitionStatsAccumulator physicalStats, PartitionLoadBatch ownedBatch) {
        List<String> names = new ArrayList<>(owned.size());
        for (HmsPartitionIdentity.ParsedPartitionName partition : owned) {
            names.add(partition.getName());
        }
        MetaCache.BulkLoad<PartitionKey, HmsPartitionInfo> load =
                partitionsCache.beginBulkLoad(ScopePath.table(dbName, tableName));
        if (ownedBatch != null) {
            ownedBatch.setLoad(load);
        }
        try {
            HmsPartitionBatchResult loadedResult = allowMissing
                    ? delegate.getExistingPartitionsWithStats(dbName, tableName, names)
                    : delegate.getPartitionsWithStats(dbName, tableName, names);
            physicalStats.add(loadedResult.getStats());
            List<HmsPartitionInfo> loaded = loadedResult.getPartitions();
            validateLoadedPartitions(owned, loaded, allowMissing);
            for (HmsPartitionInfo info : loaded) {
                PartitionKey key = new PartitionKey(dbName, tableName, info.getValues());
                load.publish(key, info);
                resultByIdentity.put(info.getValues(), info);
                if (ownedBatch != null) {
                    ownedBatch.resolvedPartitions.put(key, info);
                }
            }
        } finally {
            if (ownedBatch == null) {
                load.close();
            }
        }
    }

    private static void validateLoadedPartitions(List<HmsPartitionIdentity.ParsedPartitionName> requested,
            List<HmsPartitionInfo> loaded, boolean allowMissing) {
        if (loaded == null || (!allowMissing && loaded.size() != requested.size())) {
            throw new HmsClientException("HMS partition delegate violated its exact-result contract");
        }
        int requestIndex = 0;
        for (HmsPartitionInfo info : loaded) {
            if (info == null) {
                throw new HmsClientException("HMS partition delegate returned a null partition");
            }
            while (allowMissing && requestIndex < requested.size()
                    && !requested.get(requestIndex).getValues().equals(info.getValues())) {
                requestIndex++;
            }
            if (requestIndex >= requested.size()
                    || !requested.get(requestIndex).getValues().equals(info.getValues())) {
                throw new HmsClientException("HMS partition delegate violated request order at index "
                        + requestIndex);
            }
            requestIndex++;
        }
    }

    private static List<HmsPartitionIdentity.ParsedPartitionName> registrationsToPartitions(
            List<PartitionLoadRegistration> registrations) {
        List<HmsPartitionIdentity.ParsedPartitionName> partitions = new ArrayList<>(registrations.size());
        for (PartitionLoadRegistration registration : registrations) {
            partitions.add(registration.partition);
        }
        return partitions;
    }

    private void releaseOwnedPartitionLoads(PartitionLoadBatch ownedBatch) {
        for (PartitionKey key : ownedBatch.claimedKeys) {
            inFlightPartitionLoads.remove(key, ownedBatch);
        }
        ownedBatch.releaseOwner();
    }

    private static void releaseWaitingBatches(Set<PartitionLoadBatch> batches) {
        for (PartitionLoadBatch batch : batches) {
            batch.releaseWaiter();
        }
    }

    private void consumeWaitingBatches(
            Map<PartitionLoadBatch, List<PartitionLoadRegistration>> waiting,
            Map<List<String>, HmsPartitionInfo> resultByIdentity,
            List<HmsPartitionIdentity.ParsedPartitionName> retries) {
        List<Map.Entry<PartitionLoadBatch, List<PartitionLoadRegistration>>> entries =
                new ArrayList<>(waiting.entrySet());
        for (int i = 0; i < entries.size(); i++) {
            try {
                Map.Entry<PartitionLoadBatch, List<PartitionLoadRegistration>> entry = entries.get(i);
                consumeWaitingBatch(entry.getKey(), entry.getValue(), resultByIdentity, retries);
            } catch (RuntimeException | Error failure) {
                for (int remaining = i + 1; remaining < entries.size(); remaining++) {
                    entries.get(remaining).getKey().releaseWaiter();
                }
                throw failure;
            }
        }
    }

    private void consumeWaitingBatch(PartitionLoadBatch batch,
            List<PartitionLoadRegistration> registrations,
            Map<List<String>, HmsPartitionInfo> resultByIdentity,
            List<HmsPartitionIdentity.ParsedPartitionName> retries) {
        try {
            Throwable failure = batch.await();
            if (failure != null && !isRetryableSharedFailure(failure, batch, registrations)) {
                rethrow(failure);
            }
            for (PartitionLoadRegistration registration : registrations) {
                HmsPartitionInfo hit = partitionsCache.getIfPresent(registration.key);
                if (hit != null) {
                    resultByIdentity.put(hit.getValues(), hit);
                } else if (failure == null && batch.isCurrent(registration.key)) {
                    HmsPartitionInfo resolved = batch.resolvedPartitions.get(registration.key);
                    if (resolved != null) {
                        resultByIdentity.put(resolved.getValues(), resolved);
                    }
                } else {
                    retries.add(registration.partition);
                }
            }
        } finally {
            batch.releaseWaiter();
        }
    }

    private static boolean isRetryableSharedFailure(Throwable failure, PartitionLoadBatch ownerBatch,
            List<PartitionLoadRegistration> waiterRegistrations) {
        if (!(failure instanceof HmsPartitionResultException)) {
            return false;
        }
        return ownerBatch.claimedKeys.size() != waiterRegistrations.size()
                || waiterRegistrations.stream().anyMatch(
                        registration -> !ownerBatch.claimedKeys.contains(registration.key));
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new HmsClientException("HMS in-flight partition load failed", failure);
    }

    @Override
    public List<HmsColumnStatistics> getTableColumnStatistics(String dbName, String tableName,
            List<String> columns) {
        return columnStatsCache.get(new ColumnStatsKey(dbName, tableName, columns),
                key -> delegate.getTableColumnStatistics(key.dbName, key.tableName, key.columns));
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
    public long acquireExclusiveTableLock(String queryId, String user, String dbName,
            String tableName, long timeoutMs) {
        return delegate.acquireExclusiveTableLock(queryId, user, dbName, tableName, timeoutMs);
    }

    @Override
    public void releaseLock(long lockId) {
        delegate.releaseLock(lockId);
    }

    @Override
    public void heartbeatLock(long lockId) {
        delegate.heartbeatLock(lockId);
    }

    @Override
    public long getCurrentNotificationEventId() {
        return delegate.getCurrentNotificationEventId();
    }

    @Override
    public List<HmsNotificationEvent> getNextNotification(long lastEventId, int maxEvents) {
        return delegate.getNextNotification(lastEventId, maxEvents);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    // ========== Cache keys ==========
    // All keys carry (db, table) so the connector owner can invalidate every entry for one table.

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

    private static final class PartitionStatsAccumulator {
        private int transportInvocations;
        private long transportItems;
        private int largestBatchSize;
        private int smallestBatchSize;
        private int fallbackCount;
        private long transportElapsedNanos;
        private long maxTransportElapsedNanos;

        private void add(HmsPartitionBatchStats stats) {
            transportInvocations += stats.getTransportInvocations();
            transportItems += stats.getTransportItems();
            largestBatchSize = Math.max(largestBatchSize, stats.getLargestBatchSize());
            if (stats.getSmallestBatchSize() > 0) {
                smallestBatchSize = smallestBatchSize == 0
                        ? stats.getSmallestBatchSize()
                        : Math.min(smallestBatchSize, stats.getSmallestBatchSize());
            }
            fallbackCount += stats.getFallbackCount();
            transportElapsedNanos += stats.getTransportElapsedNanos();
            maxTransportElapsedNanos = Math.max(
                    maxTransportElapsedNanos, stats.getMaxTransportElapsedNanos());
        }

        private HmsPartitionBatchStats build(int requestedItems, long logicalElapsedNanos) {
            return HmsPartitionBatchStats.builder()
                    .requestedItems(requestedItems)
                    .transportInvocations(transportInvocations)
                    .transportItems(transportItems)
                    .largestBatchSize(largestBatchSize)
                    .smallestBatchSize(smallestBatchSize)
                    .fallbackCount(fallbackCount)
                    .logicalElapsedNanos(logicalElapsedNanos)
                    .transportElapsedNanos(transportElapsedNanos)
                    .maxTransportElapsedNanos(maxTransportElapsedNanos)
                    .build();
        }
    }

    private static final class PartitionLoadRegistration {
        private final HmsPartitionIdentity.ParsedPartitionName partition;
        private final PartitionKey key;

        private PartitionLoadRegistration(
                HmsPartitionIdentity.ParsedPartitionName partition, PartitionKey key) {
            this.partition = partition;
            this.key = key;
        }
    }

    private static final class PartitionLoadBatch {
        private final CompletableFuture<Throwable> completion = new CompletableFuture<>();
        private final Set<PartitionKey> claimedKeys = ConcurrentHashMap.newKeySet();
        private final Map<PartitionKey, HmsPartitionInfo> resolvedPartitions = new ConcurrentHashMap<>();
        // Kept open until every registered waiter consumes the result. Its publication fence distinguishes
        // harmless capacity eviction from invalidation, allowing waiters to reuse an evicted-but-current value.
        private MetaCache.BulkLoad<PartitionKey, HmsPartitionInfo> load;
        private boolean acceptingWaiters = true;
        private int waiters;
        private boolean ownerReleased;

        private synchronized boolean tryRegisterWaiter() {
            if (!acceptingWaiters) {
                return false;
            }
            waiters++;
            return true;
        }

        private synchronized void setLoad(MetaCache.BulkLoad<PartitionKey, HmsPartitionInfo> load) {
            this.load = load;
        }

        private void complete(Throwable failure) {
            synchronized (this) {
                acceptingWaiters = false;
            }
            completion.complete(failure);
        }

        private Throwable await() {
            try {
                return completion.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new HmsClientException("HMS in-flight partition load wait was interrupted", e);
            } catch (ExecutionException e) {
                throw new AssertionError("partition load completion must carry failures as values", e);
            }
        }

        private synchronized boolean isCurrent(PartitionKey key) {
            return load.isCurrent(key);
        }

        private synchronized void releaseWaiter() {
            waiters--;
            closeLoadIfReleased();
        }

        private synchronized void releaseOwner() {
            ownerReleased = true;
            closeLoadIfReleased();
        }

        private void closeLoadIfReleased() {
            if (ownerReleased && waiters == 0 && load != null) {
                load.close();
                load = null;
            }
        }
    }

    static final class PartitionKey {
        private final String dbName;
        private final String tableName;
        // ONE partition's ordered values (defensively copied). The cache stores one entry per partition keyed
        // by these values (legacy / Trino per-partition shape), so the capacity bounds partition OBJECTS and
        // overlapping requests share entries rather than duplicating partitions across request-list keys.
        private final List<String> values;

        PartitionKey(String dbName, String tableName, List<String> values) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.values = values == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(values));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PartitionKey)) {
                return false;
            }
            PartitionKey that = (PartitionKey) o;
            return Objects.equals(dbName, that.dbName)
                    && Objects.equals(tableName, that.tableName)
                    && Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName, values);
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
