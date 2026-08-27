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
import org.apache.doris.connector.spi.ConnectorOperationAbortedException;
import org.apache.doris.connector.spi.ConnectorOperationControl;
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
    private final int partitionLoadWindowSize;
    private final PartitionLoadSlotLimiter partitionLoadSlots;
    private final ConcurrentMap<PartitionKey, PartitionLoadBatch> inFlightPartitionLoads =
            new ConcurrentHashMap<>();
    private final ReentrantLock[] partitionStateLocks = new ReentrantLock[PARTITION_STATE_LOCK_STRIPES];

    public CachingHmsClient(HmsClient delegate, Map<String, String> properties) {
        this(delegate, properties, DEFAULT_MAX_CONCURRENT_PARTITION_LOADS);
    }

    /**
     * Creates a cache whose concurrent cold-load windows are bounded by the underlying HMS client capacity.
     * A non-pooled raw client reports zero and is conservatively limited to one cold window.
     */
    public CachingHmsClient(HmsClient delegate, Map<String, String> properties, int maxConcurrentPartitionLoads) {
        this.delegate = Objects.requireNonNull(delegate, "delegate can not be null");
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
        // Keep the coordination footprint bounded by the same configured ceiling as the raw HMS loader. This
        // window is not a transport policy: every delegated request still goes through the one raw chunk/fallback
        // implementation, including when a degradable failure makes the effective wire batch smaller.
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
        // Contextual-only + manual-miss load so a slow HMS RPC runs outside Caffeine's sync compute lock.
        // Partition-object misses are deduplicated per identity by inFlightPartitionLoads; other entries retain
        // the cache framework's existing loading behavior.
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
        return getPartitions(null, HmsPartitionAccessSource.UNKNOWN, dbName, tableName, partNames);
    }

    @Override
    public List<HmsPartitionInfo> getPartitions(ConnectorSession session, HmsPartitionAccessSource source,
            String dbName, String tableName, List<String> partNames) {
        return getPartitions(HmsPartitionRequest.from(session, source, dbName, tableName, partNames));
    }

    @Override
    public List<HmsPartitionInfo> getPartitions(HmsPartitionRequest request) {
        String dbName = request.getDbName();
        String tableName = request.getTableName();
        List<String> partNames = request.getPartitionNames();
        if (partNames.isEmpty()) {
            return Collections.emptyList();
        }
        checkOperationActive(request.getEffectiveOperationControl());
        // Per-partition assembly (Trino CachingHiveMetastore / legacy HiveExternalMetaCache shape): serve each
        // requested partition from its own entry and fetch only bounded windows of misses, so
        // overlapping requests share partition objects and the capacity bounds partition OBJECTS, not
        // request-lists. Correctness is independent of name-parse fidelity: a LOOKUP is keyed by the requested
        // name parsed to values, while a STORE is keyed by the partition's OWN values. The delegate result is
        // validated per physical chunk before publication and the final list is reconstructed in exact request
        // order.
        Map<List<String>, HmsPartitionInfo> resultByIdentity = new LinkedHashMap<>();
        List<String> missNames = null;
        for (int i = 0; i < partNames.size(); i++) {
            checkOperationActivePeriodically(request.getEffectiveOperationControl(), i);
            String name = partNames.get(i);
            List<String> values = HmsPartitionIdentity.fromName(name);
            HmsPartitionInfo hit =
                    partitionsCache.getIfPresent(new PartitionKey(dbName, tableName, values));
            if (hit != null) {
                resultByIdentity.put(values, hit);
            } else {
                if (missNames == null) {
                    missNames = new ArrayList<>();
                }
                missNames.add(name);
            }
        }
        if (missNames != null) {
            loadMissingPartitions(request, missNames, resultByIdentity);
        }
        List<HmsPartitionInfo> result = new ArrayList<>(partNames.size());
        for (int i = 0; i < partNames.size(); i++) {
            checkOperationActivePeriodically(request.getEffectiveOperationControl(), i);
            String name = partNames.get(i);
            List<String> identity = HmsPartitionIdentity.fromName(name);
            HmsPartitionInfo partition = resultByIdentity.get(identity);
            if (partition == null) {
                throw HmsPartitionResultException.builder(partNames.size(), resultByIdentity.size())
                        .missing(name)
                        .build();
            }
            result.add(partition);
        }
        checkOperationActive(request.getEffectiveOperationControl());
        return result;
    }

    private void loadMissingPartitions(HmsPartitionRequest request, List<String> initialMissNames,
            Map<List<String>, HmsPartitionInfo> resultByIdentity) {
        if (!partitionsCache.isEffectiveEnabled()) {
            loadAndCacheMissingPartitions(
                    request, initialMissNames, partitionsCache.invalidationGeneration(), resultByIdentity);
            return;
        }
        for (int offset = 0; offset < initialMissNames.size(); offset += partitionLoadWindowSize) {
            checkOperationActive(request.getEffectiveOperationControl());
            int end = Math.min(offset + partitionLoadWindowSize, initialMissNames.size());
            loadMissingPartitionWindow(request, initialMissNames.subList(offset, end), resultByIdentity);
        }
    }

    private void loadMissingPartitionWindow(HmsPartitionRequest request, List<String> initialMissNames,
            Map<List<String>, HmsPartitionInfo> resultByIdentity) {
        ConnectorOperationControl operationControl = request.getEffectiveOperationControl();
        List<String> pendingNames = initialMissNames;
        while (!pendingNames.isEmpty()) {
            checkOperationActive(operationControl);
            PartitionLoadBatch ownedBatch = new PartitionLoadBatch();
            List<PartitionLoadRegistration> owned = new ArrayList<>();
            Map<PartitionLoadBatch, List<PartitionLoadRegistration>> waiting = new IdentityHashMap<>();
            acquirePartitionLoadSlot(request, pendingNames.size());
            try {
                try {
                    for (int i = 0; i < pendingNames.size(); i++) {
                        checkOperationActivePeriodically(operationControl, i);
                        registerPartitionLoad(
                                request, pendingNames.get(i), ownedBatch, resultByIdentity, owned, waiting);
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
                // A pure waiter does not consume HMS capacity. Release the owner-registration/load budget before
                // waiting so one slow identity cannot block unrelated cold loads while pool clients are idle.
                partitionLoadSlots.release();
            }
            List<String> retryNames = new ArrayList<>();
            for (Map.Entry<PartitionLoadBatch, List<PartitionLoadRegistration>> entry : waiting.entrySet()) {
                consumeWaitingBatch(request, entry.getKey(), entry.getValue(), resultByIdentity, retryNames);
            }
            pendingNames = retryNames;
        }
    }

    /** Test seam for observing registrations without changing the production coordination contract. */
    void afterPartitionLoadRegistrationForTest() {
    }

    int inFlightPartitionLoadCountForTest() {
        return inFlightPartitionLoads.size();
    }

    private void registerPartitionLoad(HmsPartitionRequest request, String partitionName,
            PartitionLoadBatch ownedBatch, Map<List<String>, HmsPartitionInfo> resultByIdentity,
            List<PartitionLoadRegistration> owned,
            Map<PartitionLoadBatch, List<PartitionLoadRegistration>> waiting) {
        List<String> values = HmsPartitionIdentity.fromName(partitionName);
        PartitionKey key = new PartitionKey(request.getDbName(), request.getTableName(), values);
        ReentrantLock stateLock = partitionStateLock(request.getDbName(), request.getTableName());
        acquirePartitionStateLock(stateLock, request.getEffectiveOperationControl());
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
                            .add(new PartitionLoadRegistration(partitionName, key));
                    return;
                }
                if (inFlightPartitionLoads.replace(key, existing, ownedBatch)) {
                    break;
                }
            }
            ownedBatch.claimedKeys.add(key);
            // Close the cache-check/register race: a previous owner may have filled the cache and removed its
            // future after our first cache check but before this putIfAbsent.
            hit = partitionsCache.getIfPresent(key);
            if (hit != null) {
                ownedBatch.resolvedPartitions.put(key, hit);
                resultByIdentity.put(values, hit);
                ownedBatch.claimedKeys.remove(key);
                inFlightPartitionLoads.remove(key, ownedBatch);
                return;
            }
            owned.add(new PartitionLoadRegistration(partitionName, key));
        } finally {
            stateLock.unlock();
        }
    }

    private void loadOwnedPartitions(HmsPartitionRequest request, PartitionLoadBatch ownedBatch,
            List<PartitionLoadRegistration> owned, Map<List<String>, HmsPartitionInfo> resultByIdentity) {
        List<String> ownedNames = new ArrayList<>(owned.size());
        Map<List<String>, PartitionLoadRegistration> ownedByIdentity = new HashMap<>();
        for (PartitionLoadRegistration registration : owned) {
            ownedNames.add(registration.partitionName);
            ownedByIdentity.put(registration.key.values, registration);
        }
        HmsPartitionRequest missRequest = copiedPartitionRequest(request, ownedNames)
                .partitionChunkConsumer((chunkNames, chunkPartitions, effectiveControl) -> publishOwnedPartitions(
                        request, chunkPartitions, resultByIdentity, ownedBatch, ownedByIdentity, effectiveControl))
                .build();
        List<HmsPartitionInfo> loaded = delegate.getPartitions(missRequest);
        ConnectorOperationControl effectiveControl = request.getEffectiveOperationControl();
        checkOperationActive(effectiveControl);
        if (ownedByIdentity.isEmpty()) {
            return;
        }
        if (ownedByIdentity.size() != ownedNames.size()) {
            throw new HmsClientException("HMS delegate invoked the partition chunk consumer for only part of "
                    + "the request: requested=" + ownedNames.size() + ", unpublished=" + ownedByIdentity.size());
        }
        // Compatibility fallback for legacy/test delegates that implement the request overload without invoking
        // its chunk consumer. The production raw client always takes the zero-extra-validation branch above.
        List<HmsPartitionInfo> validated = HmsPartitionBatchLoader.validateAndOrder(
                ownedNames, loaded, effectiveControl);
        publishOwnedPartitions(
                request, validated, resultByIdentity, ownedBatch, ownedByIdentity, effectiveControl);
    }

    private void releaseOwnedPartitionLoads(PartitionLoadBatch ownedBatch) {
        for (PartitionKey key : ownedBatch.claimedKeys) {
            inFlightPartitionLoads.remove(key, ownedBatch);
        }
    }

    private void loadAndCacheMissingPartitions(HmsPartitionRequest request, List<String> missNames,
            long generation, Map<List<String>, HmsPartitionInfo> resultByIdentity) {
        Set<List<String>> unpublishedIdentities = new LinkedHashSet<>();
        for (String missName : missNames) {
            unpublishedIdentities.add(HmsPartitionIdentity.fromName(missName));
        }
        HmsPartitionRequest missRequest = copiedPartitionRequest(request, missNames)
                .partitionChunkConsumer((chunkNames, chunkPartitions, effectiveControl) ->
                        publishUncachedPartitions(request, chunkPartitions, generation,
                                resultByIdentity, unpublishedIdentities, effectiveControl))
                .build();
        List<HmsPartitionInfo> loaded = delegate.getPartitions(missRequest);
        ConnectorOperationControl effectiveControl = request.getEffectiveOperationControl();
        checkOperationActive(effectiveControl);
        if (unpublishedIdentities.isEmpty()) {
            return;
        }
        if (unpublishedIdentities.size() != missNames.size()) {
            throw new HmsClientException("HMS delegate invoked the partition chunk consumer for only part of "
                    + "the request: requested=" + missNames.size()
                    + ", unpublished=" + unpublishedIdentities.size());
        }
        List<HmsPartitionInfo> validated = HmsPartitionBatchLoader.validateAndOrder(
                missNames, loaded, effectiveControl);
        publishUncachedPartitions(request, validated, generation,
                resultByIdentity, unpublishedIdentities, effectiveControl);
    }

    private void publishUncachedPartitions(HmsPartitionRequest request, List<HmsPartitionInfo> loaded,
            long generation, Map<List<String>, HmsPartitionInfo> resultByIdentity,
            Set<List<String>> unpublishedIdentities, ConnectorOperationControl effectiveControl) {
        for (int i = 0; i < loaded.size(); i++) {
            checkOperationActivePeriodically(effectiveControl, i);
            HmsPartitionInfo info = loaded.get(i);
            if (!unpublishedIdentities.remove(info.getValues())) {
                throw new HmsClientException(
                        "HMS chunk consumer published an unowned partition: " + info.getValues());
            }
            PartitionKey key = new PartitionKey(request.getDbName(), request.getTableName(), info.getValues());
            partitionsCache.putIfNotInvalidatedSince(generation, key, info);
            resultByIdentity.put(info.getValues(), info);
        }
        checkOperationActive(effectiveControl);
    }

    private static HmsPartitionRequest.Builder copiedPartitionRequest(
            HmsPartitionRequest request, List<String> partitionNames) {
        return HmsPartitionRequest.builder()
                .database(request.getDbName())
                .table(request.getTableName())
                .partitionNames(partitionNames)
                .source(request.getSource())
                .operationControl(request.getOperationControl())
                .metadataAccessObserver(request.getMetadataAccessObserver())
                .shareBatchExecutionWith(request);
    }

    private void publishOwnedPartitions(HmsPartitionRequest request,
            List<HmsPartitionInfo> loaded, Map<List<String>, HmsPartitionInfo> resultByIdentity,
            PartitionLoadBatch ownedBatch, Map<List<String>, PartitionLoadRegistration> ownedByIdentity,
            ConnectorOperationControl effectiveControl) {
        for (int i = 0; i < loaded.size(); i++) {
            checkOperationActivePeriodically(effectiveControl, i);
            HmsPartitionInfo info = loaded.get(i);
            PartitionLoadRegistration registration = ownedByIdentity.remove(info.getValues());
            if (registration == null) {
                throw new HmsClientException(
                        "HMS chunk consumer published an unowned partition: " + info.getValues());
            }
            // Every partition-cache invalidation takes the same table state lock (flushDb/flushAll take all
            // stripes). Therefore a relevant refresh either invalidates this key before this critical section,
            // making isInvalidated true, or runs after the put and removes it. Direct publication under that lock
            // also avoids a refresh of an unrelated table suppressing this valid result.
            ReentrantLock stateLock = partitionStateLock(request.getDbName(), request.getTableName());
            acquirePartitionStateLock(stateLock, effectiveControl);
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
        checkOperationActive(effectiveControl);
    }

    private void consumeWaitingBatch(HmsPartitionRequest request, PartitionLoadBatch batch,
            List<PartitionLoadRegistration> registrations, Map<List<String>, HmsPartitionInfo> resultByIdentity,
            List<String> retryNames) {
        long startNanos = System.nanoTime();
        boolean success = false;
        try {
            boolean retrying = false;
            for (PartitionLoadRegistration registration : registrations) {
                awaitPartitionLoad(batch, registration.key, request.getEffectiveOperationControl());
                if (batch.isInvalidated(registration.key)) {
                    inFlightPartitionLoads.remove(registration.key, batch);
                    retryNames.add(registration.partitionName);
                    retrying = true;
                    continue;
                }
                HmsPartitionInfo partition = batch.resolvedPartitions.get(registration.key);
                if (partition != null) {
                    resultByIdentity.put(partition.getValues(), partition);
                    continue;
                }
                PartitionLoadOutcome outcome = batch.future.getNow(null);
                checkOperationActive(request.getEffectiveOperationControl());
                Throwable ownerFailure = Objects.requireNonNull(
                        Objects.requireNonNull(outcome,
                                "partition load is unresolved but its completion is not available").failure,
                        "completed partition load has neither a result nor a failure");
                if (!isRetryableSharedFailure(ownerFailure)) {
                    rethrow(ownerFailure);
                }
                // Only an exception published by the OWNER reaches this branch. Cancellation, deadline and
                // interruption of the waiting request itself escape directly from awaitPartitionLoad and must
                // never remove or replace a normally-running owner's future.
                checkOperationActive(request.getEffectiveOperationControl());
                inFlightPartitionLoads.remove(registration.key, batch);
                retryNames.add(registration.partitionName);
                retrying = true;
            }
            success = !retrying;
        } finally {
            recordPartitionWait(request, registrations.size(), startNanos, success);
        }
    }

    private static void awaitPartitionLoad(
            PartitionLoadBatch batch, PartitionKey key, ConnectorOperationControl control) {
        while (true) {
            long remainingMillis = checkOperationActive(control);
            if (batch.isInvalidated(key)
                    || batch.resolvedPartitions.containsKey(key) || batch.future.isDone()) {
                return;
            }
            long waitMillis = remainingMillis == Long.MAX_VALUE
                    ? PARTITION_LOAD_WAIT_CHECK_MILLIS
                    : Math.min(remainingMillis, PARTITION_LOAD_WAIT_CHECK_MILLIS);
            try {
                batch.future.get(waitMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                // Re-check the waiting request's cancellation and deadline at a bounded interval.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectorOperationAbortedException(
                        ConnectorOperationAbortedException.Reason.CANCELLED,
                        "HMS in-flight partition load wait was interrupted");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                rethrow(cause);
                throw new AssertionError("unreachable");
            }
        }
    }

    private static boolean isRetryableSharedFailure(Throwable failure) {
        return failure instanceof ConnectorOperationAbortedException
                || failure instanceof HmsPartitionResultException;
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

    private static void recordPartitionWait(HmsPartitionRequest request, int requestedItems,
            long startNanos, boolean success) {
        recordPartitionCoordinationWait(
                request, PARTITION_INFLIGHT_WAIT_OPERATION, requestedItems, startNanos, success);
    }

    private static void recordPartitionCoordinationWait(HmsPartitionRequest request, String operation,
            int requestedItems, long startNanos, boolean success) {
        ConnectorMetadataAccessEvent event = ConnectorMetadataAccessEvent.builder()
                .operation(operation)
                .source(request.getSource().name())
                .requestedItems(requestedItems)
                .logicalElapsedMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos))
                .success(success)
                .build();
        try {
            request.getMetadataAccessObserver().record(event);
        } catch (RuntimeException e) {
            LOG.warn("Failed to record HMS partition coordination wait in query profile", e);
        }
    }

    private static long checkOperationActive(ConnectorOperationControl control) {
        control.checkActive();
        long remainingMillis = control.remainingTimeMillis();
        if (remainingMillis <= 0) {
            throw new ConnectorOperationAbortedException(
                    ConnectorOperationAbortedException.Reason.DEADLINE_EXCEEDED,
                    "HMS in-flight partition load deadline exceeded");
        }
        return remainingMillis;
    }

    private static void checkOperationActivePeriodically(ConnectorOperationControl control, int index) {
        if ((index & 1023) == 0) {
            checkOperationActive(control);
        }
    }

    private static void acquirePartitionStateLock(
            ReentrantLock lock, ConnectorOperationControl operationControl) {
        while (true) {
            long remainingMillis = checkOperationActive(operationControl);
            long waitMillis = remainingMillis == Long.MAX_VALUE
                    ? PARTITION_LOAD_WAIT_CHECK_MILLIS
                    : Math.min(remainingMillis, PARTITION_LOAD_WAIT_CHECK_MILLIS);
            try {
                if (lock.tryLock(waitMillis, TimeUnit.MILLISECONDS)) {
                    try {
                        checkOperationActive(operationControl);
                        return;
                    } catch (RuntimeException | Error e) {
                        lock.unlock();
                        throw e;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectorOperationAbortedException(
                        ConnectorOperationAbortedException.Reason.CANCELLED,
                        "HMS partition state lock wait was interrupted");
            }
        }
    }

    private void acquirePartitionLoadSlot(HmsPartitionRequest request, int requestedItems) {
        ConnectorOperationControl operationControl = request.getEffectiveOperationControl();
        checkOperationActive(operationControl);
        PartitionLoadSlotWaiter waiter = partitionLoadSlots.tryAcquireOrEnqueue();
        if (waiter == null) {
            checkAcquiredPartitionLoadSlot(operationControl);
            return;
        }
        beforePartitionLoadSlotWaitForTest();
        long startNanos = System.nanoTime();
        boolean success = false;
        boolean acquired = false;
        try {
            partitionLoadSlots.await(waiter, operationControl);
            checkAcquiredPartitionLoadSlot(operationControl);
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

    private void checkAcquiredPartitionLoadSlot(ConnectorOperationControl operationControl) {
        try {
            checkOperationActive(operationControl);
        } catch (RuntimeException | Error e) {
            partitionLoadSlots.release();
            throw e;
        }
    }

    /** Test seam for deterministic load-slot waiter coordination. */
    void beforePartitionLoadSlotWaitForTest() {
    }

    /** Preserves FIFO position while still polling each waiter's own cancellation and deadline. */
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

        private void await(PartitionLoadSlotWaiter waiter, ConnectorOperationControl operationControl) {
            boolean acquired = false;
            try {
                while (true) {
                    long remainingMillis = checkOperationActive(operationControl);
                    long waitMillis = remainingMillis == Long.MAX_VALUE
                            ? PARTITION_LOAD_WAIT_CHECK_MILLIS
                            : Math.min(remainingMillis, PARTITION_LOAD_WAIT_CHECK_MILLIS);
                    synchronized (this) {
                        if (waiters.peekFirst() == waiter && availableSlots > 0) {
                            waiters.removeFirst();
                            availableSlots--;
                            acquired = true;
                            notifyAll();
                            return;
                        }
                        wait(waitMillis);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectorOperationAbortedException(
                        ConnectorOperationAbortedException.Reason.CANCELLED,
                        "HMS partition load slot wait was interrupted");
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
        private final String partitionName;
        private final PartitionKey key;

        private PartitionLoadRegistration(String partitionName, PartitionKey key) {
            this.partitionName = partitionName;
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
        } finally {
            stateLock.unlock();
        }
        partitionsCache.invalidateIf(key -> key.matches(dbName, tableName));
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
        } finally {
            stateLock.unlock();
        }
        if (!partitionValues.isEmpty()) {
            partitionsCache.invalidateIf(key -> key.matchesPartitions(dbName, tableName, partitionValues));
        }
        partitionNamesCache.invalidateIf(key -> key.matches(dbName, tableName));
    }

    /** Drop every cached entry for one database (all its tables). Backs {@code REFRESH DATABASE}. */
    public void flushDb(String dbName) {
        lockAllPartitionStateStripes();
        try {
            invalidateInFlightPartitionLoads(key -> key.matchesDb(dbName), true);
        } finally {
            unlockAllPartitionStateStripes();
        }
        partitionsCache.invalidateIf(key -> key.matchesDb(dbName));
        tableCache.invalidateIf(key -> key.matchesDb(dbName));
        partitionNamesCache.invalidateIf(key -> key.matchesDb(dbName));
        columnStatsCache.invalidateIf(key -> key.matchesDb(dbName));
    }

    /** Drop the whole cache. Backs {@code REFRESH CATALOG}. */
    public void flushAll() {
        lockAllPartitionStateStripes();
        try {
            invalidateInFlightPartitionLoads(key -> true, true);
        } finally {
            unlockAllPartitionStateStripes();
        }
        partitionsCache.invalidateAll();
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
