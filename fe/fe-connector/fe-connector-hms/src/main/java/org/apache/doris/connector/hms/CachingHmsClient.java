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
import org.apache.doris.connector.spi.ConnectorMetadataAccessEvent;
import org.apache.doris.connector.spi.ConnectorMetadataAccessObserver;
import org.apache.doris.connector.spi.ConnectorMetadataAccessSource;
import org.apache.doris.connector.spi.ConnectorSession;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;

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
 * <p><b>What it caches (4 methods)</b>, each on its own {@link MetaCacheEntry} configured from catalog
 * properties {@code meta.cache.hive.<entry>.(enable|ttl-second|capacity)} (defaults mirror the legacy
 * fe-core {@code Config} values — the connector is {@code Config}-free):</p>
 * <ul>
 *   <li>{@code getTable} — keyed by {@code (db, table)} → {@link HmsTableInfo}.</li>
 *   <li>{@code listPartitionNames} — keyed by {@code (db, table, maxParts)} → partition-name list. Real
 *       callers pass the unbounded {@code maxParts}, so this is effectively one entry per table; keeping
 *       {@code maxParts} in the key keeps a bounded request from ever being served a fuller list.</li>
 *   <li>{@code getPartitions} — one entry PER PARTITION, keyed by {@code (db, table, partition-values)} →
 *       {@link HmsPartitionInfo}. A bulk request looks up each requested name (parsed to its values) and
 *       fetches only bounded windows of misses, storing each returned partition under its OWN values — so
 *       overlapping requests SHARE partition entries, the in-flight footprint is bounded, and the capacity
 *       bounds partition OBJECTS (legacy {@code HiveExternalMetaCache} / Trino
 *       {@code CachingHiveMetastore} shape), not request-lists.
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
 * through to the delegate. A later invalidation step arms {@link #flush(String, String)} /
 * {@link #flushDb(String)} / {@link #flushAll()} onto {@code REFRESH TABLE} / {@code REFRESH DATABASE} /
 * {@code REFRESH CATALOG}. This decorator does NOT
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
 * <p><b>Live since the hms flip.</b> {@code HiveConnector.createClient} wraps the pooled
 * {@code ThriftHmsClient} in this decorator (see {@code HiveConnector.wrapWithCache}), so every hmsClient read
 * in {@code HiveConnectorMetadata} — including the freshness probes — is cache-backed. Fully unit-testable in
 * isolation.</p>
 */
public class CachingHmsClient implements HmsClient {

    private static final Logger LOG = LogManager.getLogger(CachingHmsClient.class);
    private static final long PARTITION_LOAD_WAIT_CHECK_MILLIS = 100L;
    private static final int PARTITION_STATE_LOCK_STRIPES = 128;
    private static final int DEFAULT_MAX_CONCURRENT_PARTITION_LOADS = 8;
    private static final String PARTITION_INFLIGHT_WAIT_OPERATION = "hms.partition_inflight_wait";
    private static final String PARTITION_LOAD_SLOT_WAIT_OPERATION = "hms.partition_load_slot_wait";

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
    private final MetaCacheEntry<PartitionKey, HmsPartitionInfo> partitionsCache;
    private final MetaCacheEntry<ColumnStatsKey, List<HmsColumnStatistics>> columnStatsCache;
    private final ConnectorMetadataAccessObserver metadataAccessObserver;
    private final int partitionLoadWindowSize;
    private final PartitionLoadSlotLimiter partitionLoadSlots;
    private final ConcurrentMap<PartitionKey, PartitionLoadBatch> inFlightPartitionLoads =
            new ConcurrentHashMap<>();
    private final ReentrantLock[] partitionStateLocks = new ReentrantLock[PARTITION_STATE_LOCK_STRIPES];

    public CachingHmsClient(HmsClient delegate, Map<String, String> properties) {
        this(delegate, properties, DEFAULT_MAX_CONCURRENT_PARTITION_LOADS, ConnectorMetadataAccessObserver.NOOP);
    }

    public CachingHmsClient(HmsClient delegate, Map<String, String> properties, int maxConcurrentPartitionLoads) {
        this(delegate, properties, maxConcurrentPartitionLoads, ConnectorMetadataAccessObserver.NOOP);
    }

    public CachingHmsClient(HmsClient delegate, Map<String, String> properties, int maxConcurrentPartitionLoads,
            ConnectorMetadataAccessObserver metadataAccessObserver) {
        this.delegate = Objects.requireNonNull(delegate, "delegate can not be null");
        this.metadataAccessObserver = Objects.requireNonNull(metadataAccessObserver, "metadataAccessObserver");
        if (maxConcurrentPartitionLoads < 0) {
            throw new IllegalArgumentException("maxConcurrentPartitionLoads must be >= 0");
        }
        Map<String, String> props = applyLegacyTtlCompatibility(
                properties == null ? Collections.emptyMap() : properties);
        this.tableCache = newEntry("hive.table", props, ENTRY_TABLE, DEFAULT_TABLE_CAPACITY);
        this.partitionNamesCache =
                newEntry("hive.partition_names", props, ENTRY_PARTITION_NAMES, DEFAULT_PARTITION_NAMES_CAPACITY);
        this.partitionsCache = newEntry("hive.partition", props, ENTRY_PARTITION, DEFAULT_PARTITION_CAPACITY);
        this.columnStatsCache =
                newEntry("hive.column_stats", props, ENTRY_COLUMN_STATS, DEFAULT_COLUMN_STATS_CAPACITY);
        this.partitionLoadWindowSize = new HmsClientConfig(props, 0).getPartitionBatchSize();
        this.partitionLoadSlots = new PartitionLoadSlotLimiter(
                maxConcurrentPartitionLoads == 0 ? 1 : maxConcurrentPartitionLoads);
        for (int i = 0; i < partitionStateLocks.length; i++) {
            partitionStateLocks[i] = new ReentrantLock();
        }
    }

    private static <K, V> MetaCacheEntry<K, V> newEntry(String name, Map<String, String> props,
            String entry, long defaultCapacity) {
        CacheSpec spec = CacheSpec.fromProperties(props, ENGINE, entry,
                CacheSpec.of(true, DEFAULT_TTL_SECOND, defaultCapacity));
        return new MetaCacheEntry<>(name, null, spec, ForkJoinPool.commonPool(), false, true, 0L, true);
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
        return getPartitions(null, ConnectorMetadataAccessSource.UNKNOWN, dbName, tableName, partNames);
    }

    @Override
    public List<HmsPartitionInfo> getPartitions(ConnectorSession session, ConnectorMetadataAccessSource source,
            String dbName, String tableName, List<String> partNames) {
        return getPartitions(HmsPartitionRequest.from(session, source, dbName, tableName, partNames));
    }

    @Override
    public List<HmsPartitionInfo> getPartitions(HmsPartitionRequest request) {
        String dbName = request.getDbName();
        String tableName = request.getTableName();
        List<String> partNames = request.getPartitionNames();
        List<HmsPartitionIdentity.ParsedPartitionName> partitions = request.getPartitions();
        if (partNames.isEmpty()) {
            return Collections.emptyList();
        }
        long startNanos = System.nanoTime();
        boolean success = false;
        try {
            // Serve per-partition entries and fetch bounded miss windows so overlapping requests share objects.
            Map<List<String>, HmsPartitionInfo> resultByIdentity = new LinkedHashMap<>();
            List<HmsPartitionIdentity.ParsedPartitionName> missPartitions = null;
            for (int i = 0; i < partitions.size(); i++) {
                HmsPartitionIdentity.ParsedPartitionName partition = partitions.get(i);
                List<String> values = partition.getValues();
                HmsPartitionInfo hit =
                        partitionsCache.getIfPresent(new PartitionKey(dbName, tableName, values));
                if (hit != null) {
                    resultByIdentity.put(values, hit);
                } else {
                    if (missPartitions == null) {
                        missPartitions = new ArrayList<>();
                    }
                    missPartitions.add(partition);
                }
            }
            if (missPartitions != null) {
                loadMissingPartitions(request, missPartitions, resultByIdentity);
            }
            List<HmsPartitionInfo> result = new ArrayList<>(partNames.size());
            for (int i = 0; i < partitions.size(); i++) {
                HmsPartitionIdentity.ParsedPartitionName requested = partitions.get(i);
                HmsPartitionInfo partition = resultByIdentity.get(requested.getValues());
                if (partition == null) {
                    throw HmsPartitionResultException.builder(partNames.size(), resultByIdentity.size())
                            .missing(requested.getName())
                            .build();
                }
                result.add(partition);
            }
            success = true;
            return result;
        } finally {
            ConnectorMetadataAccessEvent event = request.logicalAccessEvent(
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos), success);
            recordSafely("catalog metrics", metadataAccessObserver, event);
            recordSafely("query profile", request.getMetadataAccessObserver(), event);
        }
    }

    private void loadMissingPartitions(HmsPartitionRequest request,
            List<HmsPartitionIdentity.ParsedPartitionName> initialMisses,
            Map<List<String>, HmsPartitionInfo> resultByIdentity) {
        for (int offset = 0; offset < initialMisses.size(); offset += partitionLoadWindowSize) {
            int end = Math.min(offset + partitionLoadWindowSize, initialMisses.size());
            List<HmsPartitionIdentity.ParsedPartitionName> window = initialMisses.subList(offset, end);
            if (partitionsCache.isEffectiveEnabled()) {
                loadMissingPartitionWindow(request, window, resultByIdentity);
            } else {
                acquirePartitionLoadSlot(request, window.size());
                try {
                    loadAndCacheMissingPartitions(
                            request, window, partitionsCache.invalidationGeneration(), resultByIdentity);
                } finally {
                    partitionLoadSlots.release();
                }
            }
        }
    }

    private void loadMissingPartitionWindow(HmsPartitionRequest request,
            List<HmsPartitionIdentity.ParsedPartitionName> initialMisses,
            Map<List<String>, HmsPartitionInfo> resultByIdentity) {
        List<HmsPartitionIdentity.ParsedPartitionName> pending = initialMisses;
        while (!pending.isEmpty()) {
            PartitionLoadBatch ownedBatch = new PartitionLoadBatch();
            List<PartitionLoadRegistration> owned = new ArrayList<>();
            Map<PartitionLoadBatch, List<PartitionLoadRegistration>> waiting = new IdentityHashMap<>();
            acquirePartitionLoadSlot(request, pending.size());
            try {
                try {
                    for (int i = 0; i < pending.size(); i++) {
                        registerPartitionLoad(request, pending.get(i), ownedBatch, resultByIdentity, owned, waiting);
                    }
                    afterPartitionLoadRegistrationForTest();
                    if (!owned.isEmpty()) {
                        loadOwnedPartitions(request, ownedBatch, owned, resultByIdentity);
                    }
                    ownedBatch.future.complete(PartitionLoadOutcome.success());
                } catch (RuntimeException | Error e) {
                    ownedBatch.future.complete(PartitionLoadOutcome.failure(e));
                    throw e;
                } finally {
                    releaseOwnedPartitionLoads(ownedBatch);
                }
            } finally {
                // A pure waiter releases HMS capacity before waiting, so unrelated cold loads can proceed.
                partitionLoadSlots.release();
            }
            List<HmsPartitionIdentity.ParsedPartitionName> retries = new ArrayList<>();
            for (Map.Entry<PartitionLoadBatch, List<PartitionLoadRegistration>> entry : waiting.entrySet()) {
                consumeWaitingBatch(request, entry.getKey(), entry.getValue(), resultByIdentity, retries);
            }
            pending = retries;
        }
    }

    void afterPartitionLoadRegistrationForTest() {
    }

    void beforePartitionCacheInvalidationForTest() {
    }

    int inFlightPartitionLoadCountForTest() {
        return inFlightPartitionLoads.size();
    }

    private void registerPartitionLoad(HmsPartitionRequest request,
            HmsPartitionIdentity.ParsedPartitionName partition,
            PartitionLoadBatch ownedBatch, Map<List<String>, HmsPartitionInfo> resultByIdentity,
            List<PartitionLoadRegistration> owned,
            Map<PartitionLoadBatch, List<PartitionLoadRegistration>> waiting) {
        List<String> values = partition.getValues();
        PartitionKey key = new PartitionKey(request.getDbName(), request.getTableName(), values);
        ReentrantLock stateLock = partitionStateLock(request.getDbName(), request.getTableName());
        stateLock.lock();
        try {
            HmsPartitionInfo hit = partitionsCache.getIfPresent(key);
            if (hit != null) {
                resultByIdentity.put(values, hit);
                return;
            }
            while (true) {
                PartitionLoadBatch existing = inFlightPartitionLoads.putIfAbsent(key, ownedBatch);
                if (existing == null) {
                    break;
                }
                if (!existing.isInvalidated(key)) {
                    waiting.computeIfAbsent(existing, ignored -> new ArrayList<>())
                            .add(new PartitionLoadRegistration(partition, key));
                    return;
                }
                if (inFlightPartitionLoads.replace(key, existing, ownedBatch)) {
                    break;
                }
            }
            ownedBatch.claimedKeys.add(key);
            hit = partitionsCache.getIfPresent(key);
            if (hit != null) {
                ownedBatch.resolvedPartitions.put(key, hit);
                resultByIdentity.put(values, hit);
                ownedBatch.claimedKeys.remove(key);
                inFlightPartitionLoads.remove(key, ownedBatch);
                return;
            }
            owned.add(new PartitionLoadRegistration(partition, key));
        } finally {
            stateLock.unlock();
        }
    }

    private void loadOwnedPartitions(HmsPartitionRequest request, PartitionLoadBatch ownedBatch,
            List<PartitionLoadRegistration> owned, Map<List<String>, HmsPartitionInfo> resultByIdentity) {
        List<HmsPartitionIdentity.ParsedPartitionName> ownedPartitions = new ArrayList<>(owned.size());
        Map<List<String>, PartitionLoadRegistration> ownedByIdentity = new HashMap<>();
        for (PartitionLoadRegistration registration : owned) {
            ownedPartitions.add(registration.partition);
            ownedByIdentity.put(registration.key.values, registration);
        }
        HmsPartitionRequest missRequest = copiedPartitionRequest(request, ownedPartitions)
                .partitionChunkConsumer((chunkNames, chunkPartitions) -> publishOwnedPartitions(
                        request, chunkPartitions, resultByIdentity, ownedBatch, ownedByIdentity))
                .build();
        List<HmsPartitionInfo> loaded = delegate.getPartitions(missRequest);
        if (ownedByIdentity.isEmpty()) {
            return;
        }
        if (ownedByIdentity.size() != ownedPartitions.size()) {
            throw new HmsClientException("HMS delegate invoked the partition chunk consumer for only part of "
                    + "the request: requested=" + ownedPartitions.size()
                    + ", unpublished=" + ownedByIdentity.size());
        }
        List<HmsPartitionInfo> validated = HmsPartitionBatchLoader.validateParsedAndOrder(
                ownedPartitions, loaded);
        publishOwnedPartitions(request, validated, resultByIdentity, ownedBatch, ownedByIdentity);
    }

    private void releaseOwnedPartitionLoads(PartitionLoadBatch ownedBatch) {
        for (PartitionKey key : ownedBatch.claimedKeys) {
            inFlightPartitionLoads.remove(key, ownedBatch);
        }
    }

    private void loadAndCacheMissingPartitions(HmsPartitionRequest request,
            List<HmsPartitionIdentity.ParsedPartitionName> misses,
            long generation, Map<List<String>, HmsPartitionInfo> resultByIdentity) {
        Set<List<String>> unpublishedIdentities = new LinkedHashSet<>();
        for (HmsPartitionIdentity.ParsedPartitionName miss : misses) {
            unpublishedIdentities.add(miss.getValues());
        }
        HmsPartitionRequest missRequest = copiedPartitionRequest(request, misses)
                .partitionChunkConsumer((chunkNames, chunkPartitions) ->
                        publishUncachedPartitions(request, chunkPartitions, generation,
                                resultByIdentity, unpublishedIdentities))
                .build();
        List<HmsPartitionInfo> loaded = delegate.getPartitions(missRequest);
        if (unpublishedIdentities.isEmpty()) {
            return;
        }
        if (unpublishedIdentities.size() != misses.size()) {
            throw new HmsClientException("HMS delegate invoked the partition chunk consumer for only part of "
                    + "the request: requested=" + misses.size()
                    + ", unpublished=" + unpublishedIdentities.size());
        }
        List<HmsPartitionInfo> validated = HmsPartitionBatchLoader.validateParsedAndOrder(
                misses, loaded);
        publishUncachedPartitions(request, validated, generation, resultByIdentity, unpublishedIdentities);
    }

    private void publishUncachedPartitions(HmsPartitionRequest request, List<HmsPartitionInfo> loaded,
            long generation, Map<List<String>, HmsPartitionInfo> resultByIdentity,
            Set<List<String>> unpublishedIdentities) {
        for (int i = 0; i < loaded.size(); i++) {
            HmsPartitionInfo info = loaded.get(i);
            if (!unpublishedIdentities.remove(info.getValues())) {
                throw new HmsClientException(
                        "HMS chunk consumer published an unowned partition: " + info.getValues());
            }
            PartitionKey key = new PartitionKey(request.getDbName(), request.getTableName(), info.getValues());
            partitionsCache.putIfNotInvalidatedSince(generation, key, info);
            resultByIdentity.put(info.getValues(), info);
        }
    }

    private static HmsPartitionRequest.Builder copiedPartitionRequest(
            HmsPartitionRequest request, List<HmsPartitionIdentity.ParsedPartitionName> partitions) {
        return HmsPartitionRequest.builder()
                .database(request.getDbName())
                .table(request.getTableName())
                .partitions(partitions)
                .source(request.getSource())
                .metadataAccessObserver(request.getMetadataAccessObserver())
                .shareBatchExecutionWith(request);
    }

    private void publishOwnedPartitions(HmsPartitionRequest request,
            List<HmsPartitionInfo> loaded, Map<List<String>, HmsPartitionInfo> resultByIdentity,
            PartitionLoadBatch ownedBatch, Map<List<String>, PartitionLoadRegistration> ownedByIdentity) {
        for (int i = 0; i < loaded.size(); i++) {
            HmsPartitionInfo info = loaded.get(i);
            PartitionLoadRegistration registration = ownedByIdentity.remove(info.getValues());
            if (registration == null) {
                throw new HmsClientException(
                        "HMS chunk consumer published an unowned partition: " + info.getValues());
            }
            // Shared stripes order invalidation and publication without suppressing unrelated-table results.
            ReentrantLock stateLock = partitionStateLock(request.getDbName(), request.getTableName());
            stateLock.lock();
            try {
                if (!ownedBatch.isInvalidated(registration.key)) {
                    partitionsCache.put(registration.key, info);
                }
                ownedBatch.resolvedPartitions.put(registration.key, info);
            } finally {
                stateLock.unlock();
            }
            resultByIdentity.put(info.getValues(), info);
        }
    }

    private void consumeWaitingBatch(HmsPartitionRequest request, PartitionLoadBatch batch,
            List<PartitionLoadRegistration> registrations, Map<List<String>, HmsPartitionInfo> resultByIdentity,
            List<HmsPartitionIdentity.ParsedPartitionName> retries) {
        long startNanos = System.nanoTime();
        boolean success = false;
        try {
            boolean retrying = false;
            for (PartitionLoadRegistration registration : registrations) {
                awaitPartitionLoad(batch, registration.key);
                if (batch.isInvalidated(registration.key)) {
                    inFlightPartitionLoads.remove(registration.key, batch);
                    retries.add(registration.partition);
                    retrying = true;
                    continue;
                }
                HmsPartitionInfo partition = batch.resolvedPartitions.get(registration.key);
                if (partition != null) {
                    resultByIdentity.put(partition.getValues(), partition);
                    continue;
                }
                PartitionLoadOutcome outcome = batch.future.getNow(null);
                Throwable ownerFailure = Objects.requireNonNull(
                        Objects.requireNonNull(outcome,
                                "partition load is unresolved but its completion is not available").failure,
                        "completed partition load has neither a result nor a failure");
                if (!isRetryableSharedFailure(ownerFailure, batch, registrations)) {
                    rethrow(ownerFailure);
                }
                // Only an exception published by the owner reaches this branch.
                inFlightPartitionLoads.remove(registration.key, batch);
                retries.add(registration.partition);
                retrying = true;
            }
            success = !retrying;
        } finally {
            recordPartitionWait(request, registrations.size(), startNanos, success);
        }
    }

    private static void awaitPartitionLoad(PartitionLoadBatch batch, PartitionKey key) {
        while (true) {
            if (batch.isInvalidated(key)
                    || batch.resolvedPartitions.containsKey(key) || batch.future.isDone()) {
                return;
            }
            try {
                batch.future.get(PARTITION_LOAD_WAIT_CHECK_MILLIS, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                // A chunk may publish this partition before the owner finishes the complete request.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new HmsClientException("HMS in-flight partition load wait was interrupted", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                rethrow(cause);
                throw new AssertionError("unreachable");
            }
        }
    }

    private static boolean isRetryableSharedFailure(Throwable failure, PartitionLoadBatch ownerBatch,
            List<PartitionLoadRegistration> waiterRegistrations) {
        if (!(failure instanceof HmsPartitionResultException)) {
            return false;
        }
        boolean samePartitionSet = ownerBatch.claimedKeys.size() == waiterRegistrations.size()
                && waiterRegistrations.stream().allMatch(
                        registration -> ownerBatch.claimedKeys.contains(registration.key));
        return !samePartitionSet;
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

    private void recordPartitionWait(HmsPartitionRequest request, int requestedItems,
            long startNanos, boolean success) {
        recordPartitionCoordinationWait(
                request, PARTITION_INFLIGHT_WAIT_OPERATION, requestedItems, startNanos, success);
    }

    private void recordPartitionCoordinationWait(HmsPartitionRequest request, String operation,
            int requestedItems, long startNanos, boolean success) {
        ConnectorMetadataAccessEvent event = ConnectorMetadataAccessEvent.builder()
                .operation(operation)
                .source(request.getSource().name())
                .requestedItems(requestedItems)
                .logicalElapsedMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos))
                .success(success)
                .build();
        recordSafely("catalog metrics", metadataAccessObserver, event);
        recordSafely("query profile", request.getMetadataAccessObserver(), event);
    }

    private static void recordSafely(String sinkName, ConnectorMetadataAccessObserver sink,
            ConnectorMetadataAccessEvent event) {
        try {
            sink.record(event);
        } catch (RuntimeException e) {
            LOG.warn("Failed to record HMS partition metadata access in {}", sinkName, e);
        }
    }

    private void acquirePartitionLoadSlot(HmsPartitionRequest request, int requestedItems) {
        PartitionLoadSlotWaiter waiter = partitionLoadSlots.tryAcquireOrEnqueue();
        if (waiter == null) {
            return;
        }
        beforePartitionLoadSlotWaitForTest();
        long startNanos = System.nanoTime();
        boolean success = false;
        boolean acquired = false;
        try {
            partitionLoadSlots.await(waiter);
            acquired = true;
            success = true;
        } finally {
            try {
                recordPartitionCoordinationWait(request, PARTITION_LOAD_SLOT_WAIT_OPERATION,
                        requestedItems, startNanos, success);
            } catch (Error e) {
                // The caller's release-finally is established only after this method returns. Preserve fail-loud
                // Error semantics without leaking a slot that the slow path already acquired.
                if (acquired) {
                    partitionLoadSlots.release();
                }
                throw e;
            }
        }
    }

    /** Test seam for deterministic load-slot waiter coordination. */
    void beforePartitionLoadSlotWaitForTest() {
    }

    /** Preserves FIFO position for partition-load admission. */
    private static final class PartitionLoadSlotLimiter {
        private final Deque<PartitionLoadSlotWaiter> waiters = new ArrayDeque<>();
        private int availableSlots;

        private PartitionLoadSlotLimiter(int availableSlots) {
            this.availableSlots = availableSlots;
        }

        private synchronized PartitionLoadSlotWaiter tryAcquireOrEnqueue() {
            if (availableSlots > 0 && waiters.isEmpty()) {
                availableSlots--;
                return null;
            }
            PartitionLoadSlotWaiter waiter = new PartitionLoadSlotWaiter();
            waiters.addLast(waiter);
            return waiter;
        }

        private void await(PartitionLoadSlotWaiter waiter) {
            boolean acquired = false;
            try {
                while (true) {
                    synchronized (this) {
                        if (waiters.peekFirst() == waiter && availableSlots > 0) {
                            waiters.removeFirst();
                            availableSlots--;
                            acquired = true;
                            notifyAll();
                            return;
                        }
                        wait();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new HmsClientException("HMS partition load slot wait was interrupted", e);
            } finally {
                if (!acquired) {
                    synchronized (this) {
                        waiters.remove(waiter);
                        notifyAll();
                    }
                }
            }
        }

        private synchronized void release() {
            availableSlots++;
            notifyAll();
        }
    }

    private static final class PartitionLoadSlotWaiter {
    }

    private static final class PartitionLoadRegistration {
        private final HmsPartitionIdentity.ParsedPartitionName partition;
        private final PartitionKey key;

        private PartitionLoadRegistration(HmsPartitionIdentity.ParsedPartitionName partition, PartitionKey key) {
            this.partition = partition;
            this.key = key;
        }
    }

    private static final class PartitionLoadBatch {
        private final CompletableFuture<PartitionLoadOutcome> future = new CompletableFuture<>();
        private final Set<PartitionKey> claimedKeys = new LinkedHashSet<>();
        private final ConcurrentMap<PartitionKey, HmsPartitionInfo> resolvedPartitions = new ConcurrentHashMap<>();
        private final AtomicBoolean fullyInvalidated = new AtomicBoolean();
        private final Set<PartitionKey> invalidatedKeys = ConcurrentHashMap.newKeySet();

        private boolean isInvalidated(PartitionKey key) {
            return fullyInvalidated.get() || invalidatedKeys.contains(key);
        }

        private void invalidate(PartitionKey key) {
            invalidatedKeys.add(key);
        }

        private void invalidateAll() {
            fullyInvalidated.set(true);
        }
    }

    private static final class PartitionLoadOutcome {
        private final Throwable failure;

        private PartitionLoadOutcome(Throwable failure) {
            this.failure = failure;
        }

        private static PartitionLoadOutcome success() {
            return new PartitionLoadOutcome(null);
        }

        private static PartitionLoadOutcome failure(Throwable failure) {
            return new PartitionLoadOutcome(Objects.requireNonNull(failure, "failure"));
        }
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
        ReentrantLock stateLock = partitionStateLock(dbName, tableName);
        stateLock.lock();
        try {
            invalidateInFlightPartitionLoads(key -> key.matches(dbName, tableName), true);
            beforePartitionCacheInvalidationForTest();
            partitionsCache.invalidateIf(key -> key.matches(dbName, tableName));
        } finally {
            stateLock.unlock();
        }
        tableCache.invalidateKey(new TableKey(dbName, tableName));
        partitionNamesCache.invalidateIf(key -> key.matches(dbName, tableName));
        columnStatsCache.invalidateIf(key -> key.matches(dbName, tableName));
    }

    /**
     * Per-partition invalidation for a partition add/drop/alter refresh, mirroring legacy
     * {@code HiveExternalMetaCache}'s per-partition metadata invalidation. Drops exactly the given partitions
     * from the partition-metadata cache (keyed by values) and re-fetches the partition-NAME list (its membership
     * may have changed on add/drop, so it must be refreshed whole). Deliberately does NOT touch {@code tableCache}
     * or {@code columnStatsCache} — legacy did not invalidate the table object or its column statistics on a
     * partition-level refresh.
     */
    public void invalidatePartitions(String dbName, String tableName, Set<List<String>> partitionValues) {
        ReentrantLock stateLock = partitionStateLock(dbName, tableName);
        stateLock.lock();
        try {
            invalidateInFlightPartitionLoads(
                    key -> key.matchesPartitions(dbName, tableName, partitionValues), false);
            if (!partitionValues.isEmpty()) {
                partitionsCache.invalidateIf(key -> key.matchesPartitions(dbName, tableName, partitionValues));
            }
        } finally {
            stateLock.unlock();
        }
        partitionNamesCache.invalidateIf(key -> key.matches(dbName, tableName));
    }

    /** Drop every cached entry for one database (all its tables). Backs {@code REFRESH DATABASE}. */
    public void flushDb(String dbName) {
        lockAllPartitionStateStripes();
        try {
            invalidateInFlightPartitionLoads(key -> key.matchesDb(dbName), true);
            partitionsCache.invalidateIf(key -> key.matchesDb(dbName));
        } finally {
            unlockAllPartitionStateStripes();
        }
        tableCache.invalidateIf(key -> key.matchesDb(dbName));
        partitionNamesCache.invalidateIf(key -> key.matchesDb(dbName));
        columnStatsCache.invalidateIf(key -> key.matchesDb(dbName));
    }

    /** Drop the whole cache. Backs {@code REFRESH CATALOG}. */
    public void flushAll() {
        lockAllPartitionStateStripes();
        try {
            invalidateInFlightPartitionLoads(key -> true, true);
            partitionsCache.invalidateAll();
        } finally {
            unlockAllPartitionStateStripes();
        }
        tableCache.invalidateAll();
        partitionNamesCache.invalidateAll();
        columnStatsCache.invalidateAll();
    }

    private void invalidateInFlightPartitionLoads(Predicate<PartitionKey> predicate, boolean wholeBatch) {
        inFlightPartitionLoads.forEach((key, batch) -> {
            if (predicate.test(key)) {
                if (wholeBatch) {
                    batch.invalidateAll();
                } else {
                    batch.invalidate(key);
                }
            }
        });
    }

    private ReentrantLock partitionStateLock(String dbName, String tableName) {
        int hash = 31 * dbName.hashCode() + tableName.hashCode();
        return partitionStateLocks[(hash & Integer.MAX_VALUE) % partitionStateLocks.length];
    }

    private void lockAllPartitionStateStripes() {
        for (ReentrantLock lock : partitionStateLocks) {
            lock.lock();
        }
    }

    private void unlockAllPartitionStateStripes() {
        for (int i = partitionStateLocks.length - 1; i >= 0; i--) {
            partitionStateLocks[i].unlock();
        }
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
    // All keys carry (db, table) so flush(db, table) can select every entry for one table.

    static final class TableKey {
        private final String dbName;
        private final String tableName;

        TableKey(String dbName, String tableName) {
            this.dbName = dbName;
            this.tableName = tableName;
        }

        boolean matchesDb(String db) {
            return Objects.equals(dbName, db);
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

        boolean matchesDb(String db) {
            return Objects.equals(dbName, db);
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

        boolean matches(String db, String table) {
            return Objects.equals(dbName, db) && Objects.equals(tableName, table);
        }

        boolean matchesDb(String db) {
            return Objects.equals(dbName, db);
        }

        /** This partition (its db, table and values) is one of {@code valueSet}. Backs per-partition invalidation. */
        boolean matchesPartitions(String db, String table, Set<List<String>> valueSet) {
            return matches(db, table) && valueSet.contains(values);
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

        boolean matches(String db, String table) {
            return Objects.equals(dbName, db) && Objects.equals(tableName, table);
        }

        boolean matchesDb(String db) {
            return Objects.equals(dbName, db);
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
