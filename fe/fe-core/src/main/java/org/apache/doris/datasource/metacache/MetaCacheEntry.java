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

package org.apache.doris.datasource.metacache;

import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.metacache.ExternalMetaCacheBudgetManager.AdmissionReservation;
import org.apache.doris.datasource.metacache.ExternalMetaCacheBudgetManager.EntryBudget;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Unified cache entry abstraction.
 * It stores one logical cache dataset and provides optional lazy loading,
 * key/predicate/full invalidation, and lightweight runtime stats.
 */
public class MetaCacheEntry<K, V> {
    private static final Logger LOG = LogManager.getLogger(MetaCacheEntry.class);
    // Use striped locks to deduplicate slow external loads without managing per-key lock lifecycle.
    private static final int LOAD_LOCK_STRIPES = 128;
    private static final int LOCAL_EVICTION_BATCH_SIZE = 16;
    private static final int REMOVAL_CLEANUP_BATCH_SIZE = 256;
    private static final long WEIGHT_REJECT_LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1L);
    // Direct Caffeine callbacks must not wait for admissionLock. A daemon drains one coalesced
    // generation map per physical entry after callbacks return; cleanup tasks never capture values.
    private static final ExecutorService REMOVAL_CLEANUP_EXECUTOR = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "external-meta-cache-removal-cleanup");
        thread.setDaemon(true);
        return thread;
    });
    // Conservative retained cost outside the estimator-owned key/value graph: Caffeine's data
    // node and policy links plus the reservation ConcurrentHashMap node, record and token. This
    // deliberately overestimates common compressed-oops layouts; calibrate downward only with JOL.
    static final long FIXED_ENTRY_ACCOUNTING_OVERHEAD_BYTES = 512L;

    private final String name;
    @Nullable
    private final Function<K, V> loader;
    private final CacheSpec cacheSpec;
    private final boolean effectiveEnabled;
    private final boolean autoRefresh;
    private final ExecutorService refreshExecutor;
    @Nullable
    private final MetaCacheSizeEstimator<K, V> sizeEstimator;
    @Nullable
    private final EntryBudget entryBudget;
    @Nullable
    private final MetaCacheEntryReplacementListener<K, V> replacementListener;
    private final boolean weightBounded;
    // Entries with publication-time work use the same generation-fenced refresh protocol even
    // before a max-weight is configured. This keeps estimation and dependency retirement on every
    // load/refresh path instead of letting Caffeine publish values behind those hooks.
    private final boolean generationFencedRefresh;
    // Keep the loading cache for refreshAfterWrite and the legacy sync-load path when the feature is disabled.
    private final LoadingCache<K, V> loadingData;
    // Use the plain cache view for manual miss load so slow I/O does not happen in Caffeine's sync load path.
    private final Cache<K, V> data;
    // Protect one key stripe at a time to deduplicate concurrent miss loads with bounded lock count.
    private final Object[] loadLocks = new Object[LOAD_LOCK_STRIPES];
    // Serialize weighted cache mutation with reservation ownership changes.
    private final Object admissionLock = new Object();
    // Ownership records deliberately contain no V reference. A weighted cache's Caffeine soft
    // reference must be the only cache-owned path to its value, while generation fencing keeps
    // delayed removal callbacks from releasing a replacement reservation.
    private final Map<K, ReservationRecord> reservations = new ConcurrentHashMap<>();
    private final Map<K, RefreshRecord> refreshRecords = new ConcurrentHashMap<>();
    private final Map<K, Long> pendingRemovalGenerations = new ConcurrentHashMap<>();
    private final AtomicBoolean removalCleanupScheduled = new AtomicBoolean(false);
    private final Map<K, Boolean> refreshesInFlight = new ConcurrentHashMap<>();
    // A state exists only while a miss/refresh for the key is in flight. Mutations advance that
    // state's epoch, fencing stale publication without retaining every key ever observed.
    private final Map<K, KeyMutationState> keyMutationStates = new ConcurrentHashMap<>();
    private final AtomicLong invalidateCount = new AtomicLong(0);
    // Full invalidation is the only cross-key fence. Ordinary mutations use the per-key state.
    private final AtomicLong fullInvalidationGeneration = new AtomicLong(0);
    // Primitive owner id lets queued refresh work fence a reservation without retaining its value.
    private final AtomicLong reservationGeneration = new AtomicLong(0);
    // Track load statistics outside Caffeine because manual miss loads bypass the built-in load counters.
    private final AtomicLong loadSuccessCount = new AtomicLong(0);
    private final AtomicLong loadFailureCount = new AtomicLong(0);
    private final AtomicLong totalLoadTimeNanos = new AtomicLong(0);
    private final AtomicLong lastLoadSuccessTimeMs = new AtomicLong(-1L);
    private final AtomicLong lastLoadFailureTimeMs = new AtomicLong(-1L);
    private final AtomicReference<String> lastError = new AtomicReference<>("");
    private final AtomicLong weightAdmissionRejectedCount = new AtomicLong(0L);
    private final AtomicLong localEvictionCount = new AtomicLong(0L);
    private final AtomicLong localEvictionWeight = new AtomicLong(0L);
    // Exact byte weight released by Caffeine-driven evictions (size, expiry, soft collection).
    // Caffeine's own eviction weight is clamped to the int weigher, so it is not used for stats.
    private final AtomicLong automaticEvictionWeight = new AtomicLong(0L);
    // Generation reported evicted for a key, consumed by the fenced cleanup of that generation.
    private final Map<K, Long> pendingEvictionGenerations = new ConcurrentHashMap<>();
    private final AtomicReference<String> lastWeightRejectReason = new AtomicReference<>("");
    private final AtomicLong lastWeightRejectLogTimeMs = new AtomicLong(0L);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MetaCacheEntry(String name, Function<K, V> loader, CacheSpec cacheSpec, ExecutorService refreshExecutor) {
        this(name, loader, cacheSpec, refreshExecutor, true, false);
    }

    public MetaCacheEntry(String name, Function<K, V> loader, CacheSpec cacheSpec, ExecutorService refreshExecutor,
            boolean autoRefresh) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, false);
    }

    public MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, contextualOnly, null, null);
    }

    public MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly,
            @Nullable MetaCacheSizeEstimator<K, V> sizeEstimator, @Nullable EntryBudget entryBudget) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, contextualOnly,
                sizeEstimator, entryBudget, null);
    }

    public MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly,
            @Nullable MetaCacheSizeEstimator<K, V> sizeEstimator, @Nullable EntryBudget entryBudget,
            @Nullable MetaCacheEntryReplacementListener<K, V> replacementListener) {
        this.name = name;
        if (contextualOnly) {
            if (loader != null) {
                throw new IllegalArgumentException("contextual-only entry loader must be null");
            }
            if (autoRefresh) {
                throw new IllegalArgumentException("contextual-only entry can not enable auto refresh");
            }
        } else {
            Objects.requireNonNull(loader, "loader can not be null");
        }
        this.loader = loader;
        this.cacheSpec = Objects.requireNonNull(cacheSpec, "cacheSpec can not be null");
        this.autoRefresh = autoRefresh;
        this.refreshExecutor = Objects.requireNonNull(refreshExecutor, "refreshExecutor can not be null");
        this.sizeEstimator = sizeEstimator;
        this.entryBudget = entryBudget;
        this.replacementListener = replacementListener;
        this.weightBounded = this.cacheSpec.isWeightBounded();
        this.generationFencedRefresh = autoRefresh
                && (sizeEstimator != null || replacementListener != null);
        if (weightBounded && (sizeEstimator == null || entryBudget == null)) {
            throw new IllegalArgumentException("weighted cache entry requires both estimator and budget: " + name);
        }
        if (weightBounded) {
            entryBudget.setReclaimer(this::reclaimForPeer);
        }
        this.effectiveEnabled = this.cacheSpec.isCacheEnabled();
        OptionalLong expireAfterAccessSec =
                effectiveEnabled ? CacheSpec.toExpireAfterAccess(this.cacheSpec.getTtlSecond()) : OptionalLong.empty();
        OptionalLong refreshAfterWriteSec =
                effectiveEnabled && autoRefresh && !weightBounded && !generationFencedRefresh
                        ? OptionalLong.of(Config.external_cache_refresh_time_minutes * 60)
                        : OptionalLong.empty();
        long maxSize = effectiveEnabled ? this.cacheSpec.getCapacity() : 0L;
        Weigher<K, V> cacheWeigher = weightBounded ? this::weigh : null;
        CacheFactory cacheFactory = new CacheFactory(
                expireAfterAccessSec,
                refreshAfterWriteSec,
                maxSize,
                weightBounded ? OptionalLong.of(effectiveEnabled ? this.cacheSpec.getMaxWeight().getAsLong() : 0L)
                        : OptionalLong.empty(),
                cacheWeigher,
                true,
                null);
        if (weightBounded) {
            cacheFactory.withSoftValues();
        }
        if (weightBounded || generationFencedRefresh) {
            // Direct notification avoids queuing REPLACED values. The listener itself is lock-free
            // and delegates only current-owner cleanup, so it is safe under Caffeine's eviction lock.
            this.loadingData = cacheFactory.buildCacheWithSyncRemovalListener(
                    this::loadFromDefaultLoader, this::onRemoval);
        } else {
            this.loadingData = cacheFactory.buildCache(this::loadFromDefaultLoader, refreshExecutor);
        }
        this.data = loadingData;
        // Initialize striped locks eagerly to keep the hot path allocation-free.
        for (int i = 0; i < loadLocks.length; i++) {
            loadLocks[i] = new Object();
        }
    }

    public String name() {
        return name;
    }

    public V get(K key) {
        if (closed.get()) {
            return loadAndTrack(key, this::applyDefaultLoader);
        }
        if (!isManualMissLoadEnabled()) {
            return loadingData.get(key);
        }
        return getWithManualLoad(key, this::applyDefaultLoader);
    }

    public V get(K key, Function<K, V> missLoader) {
        Function<K, V> loadFunction = Objects.requireNonNull(missLoader, "missLoader can not be null");
        if (closed.get()) {
            return loadAndTrack(key, loadFunction);
        }
        if (!isManualMissLoadEnabled()) {
            return loadingData.get(key, typedKey -> loadAndTrack(typedKey, loadFunction));
        }
        return getWithManualLoad(key, loadFunction);
    }

    public V getIfPresent(K key) {
        if (!effectiveEnabled || closed.get()) {
            return null;
        }
        V value = data.getIfPresent(key);
        if (value != null) {
            maybeRefreshManagedValue(key, value);
        }
        return value;
    }

    /** Return the current value without recording a user-visible cache request. */
    public V peekIfPresent(K key) {
        if (!effectiveEnabled || closed.get()) {
            return null;
        }
        return data.asMap().get(key);
    }

    /**
     * Fence loads and refreshes that started before an event, but only while the expected value is
     * still current. Publication-managed entries retain the known-good value and advance the key's
     * mutation epoch. Other count-based entries must invalidate because their legacy
     * Caffeine-managed refresh path does not participate in that generation protocol.
     */
    public boolean fenceInFlightLoadIfSame(K key, V expectedCurrent) {
        Objects.requireNonNull(expectedCurrent, "expectedCurrent can not be null");
        synchronized (admissionLock) {
            if (!effectiveEnabled || closed.get() || data.asMap().get(key) != expectedCurrent) {
                return false;
            }
            advanceKeyMutation(key);
            if (!weightBounded && !generationFencedRefresh) {
                if (!data.asMap().remove(key, expectedCurrent)) {
                    return false;
                }
                invalidateCount.incrementAndGet();
            }
            return true;
        }
    }

    public void put(K key, V value) {
        if (!effectiveEnabled || closed.get()) {
            return;
        }
        if (weightBounded) {
            admitWeightedValue(key, value, null, false, null, -1L, true);
        } else {
            synchronized (admissionLock) {
                if (!closed.get()) {
                    advanceKeyMutation(key);
                    putNonWeightedValue(key, value);
                }
            }
        }
    }

    /** Result of an atomic compare-and-replace operation. */
    public enum ReplaceResult {
        REPLACED,
        NOT_CURRENT,
        REJECTED,
        DISABLED
    }

    /**
     * Replace one cached value only when it is still the expected identity.
     *
     * <p>Weighted entries perform the identity check, budget resize and Caffeine write under the
     * same admission lock. Callers can therefore distinguish a concurrent update from admission
     * rejection and avoid retaining a value they already know is stale.
     */
    public ReplaceResult tryReplace(K key, V expectedCurrent, V newValue) {
        Objects.requireNonNull(expectedCurrent, "expectedCurrent can not be null");
        Objects.requireNonNull(newValue, "newValue can not be null");
        if (!effectiveEnabled || closed.get()) {
            return ReplaceResult.DISABLED;
        }
        if (weightBounded) {
            return toReplaceResult(admitWeightedValue(
                    key, newValue, expectedCurrent, true, null, -1L, true));
        }
        synchronized (admissionLock) {
            AtomicReference<ReplaceResult> result = new AtomicReference<>(ReplaceResult.NOT_CURRENT);
            AtomicReference<RefreshRecord> published = new AtomicReference<>();
            data.asMap().computeIfPresent(key, (ignored, current) -> {
                if (closed.get()) {
                    result.set(ReplaceResult.DISABLED);
                    return current;
                }
                if (current != expectedCurrent) {
                    return current;
                }
                advanceKeyMutation(key);
                published.set(publishRefreshRecord(key));
                result.set(ReplaceResult.REPLACED);
                return newValue;
            });
            RefreshRecord record = published.get();
            if (record != null && refreshRecords.get(key) == record
                    && data.asMap().get(key) == newValue) {
                record.published = true;
            }
            if (result.get() == ReplaceResult.REPLACED && data.asMap().get(key) == newValue) {
                notifyReplacement(key, expectedCurrent, newValue);
            }
            return result.get();
        }
    }

    /** Remove a key only if it still maps to the expected value identity. */
    public boolean invalidateKeyIfSame(K key, V expectedCurrent) {
        Objects.requireNonNull(expectedCurrent, "expectedCurrent can not be null");
        if (!weightBounded) {
            synchronized (admissionLock) {
                AtomicBoolean removed = new AtomicBoolean(false);
                data.asMap().computeIfPresent(key, (ignored, current) -> {
                    if (current != expectedCurrent) {
                        return current;
                    }
                    advanceKeyMutation(key);
                    invalidateCount.incrementAndGet();
                    refreshRecords.remove(key);
                    removed.set(true);
                    return null;
                });
                return removed.get();
            }
        }
        synchronized (admissionLock) {
            V current = data.asMap().get(key);
            ReservationRecord record = reservations.get(key);
            if (current != expectedCurrent || record == null || !record.published) {
                return false;
            }
            advanceKeyMutation(key);
            if (!data.asMap().remove(key, current)) {
                return false;
            }
            releaseReservation(key, record.generation);
            invalidateCount.incrementAndGet();
            return true;
        }
    }

    public void invalidateKey(K key) {
        synchronized (admissionLock) {
            advanceKeyMutation(key);
            if (weightBounded) {
                ReservationRecord record = reservations.get(key);
                V removed = data.asMap().remove(key);
                if (removed != null) {
                    invalidateCount.incrementAndGet();
                }
                if (record != null && data.asMap().get(key) == null) {
                    releaseReservation(key, record.generation);
                }
            } else {
                V removed = data.asMap().remove(key);
                if (removed != null) {
                    refreshRecords.remove(key);
                    invalidateCount.incrementAndGet();
                }
            }
        }
    }

    public void invalidateIf(Predicate<K> predicate) {
        synchronized (admissionLock) {
            Set<K> candidates = new HashSet<>(data.asMap().keySet());
            candidates.addAll(keyMutationStates.keySet());
            if (weightBounded) {
                candidates.addAll(reservations.keySet());
            } else if (generationFencedRefresh) {
                candidates.addAll(refreshRecords.keySet());
            }
            for (K key : candidates) {
                if (predicate.test(key)) {
                    advanceKeyMutation(key);
                    if (weightBounded) {
                        ReservationRecord record = reservations.get(key);
                        V removed = data.asMap().remove(key);
                        if (removed != null) {
                            invalidateCount.incrementAndGet();
                        }
                        if (record != null && data.asMap().get(key) == null) {
                            releaseReservation(key, record.generation);
                        }
                    } else {
                        V removed = data.asMap().remove(key);
                        refreshRecords.remove(key);
                        if (removed != null) {
                            invalidateCount.incrementAndGet();
                        }
                    }
                }
            }
        }
    }

    public void invalidateAll() {
        synchronized (admissionLock) {
            fullInvalidationGeneration.incrementAndGet();
            if (weightBounded) {
                long size = data.estimatedSize();
                beforeWeightedInvalidateAllForTest();
                data.invalidateAll();
                reservations.values().forEach(record -> record.reservation.release());
                reservations.clear();
                pendingRemovalGenerations.clear();
                pendingEvictionGenerations.clear();
                invalidateCount.addAndGet(size);
            } else {
                long size = data.estimatedSize();
                data.invalidateAll();
                refreshRecords.clear();
                pendingRemovalGenerations.clear();
                pendingEvictionGenerations.clear();
                invalidateCount.addAndGet(size);
            }
        }
    }

    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        invalidateAll();
        if (entryBudget != null) {
            entryBudget.close();
        }
    }

    public void forEach(BiConsumer<K, V> consumer) {
        data.asMap().forEach(consumer);
    }

    public MetaCacheEntryStats stats() {
        CacheStats cacheStats = loadingData.stats();
        long successCount = loadSuccessCount.get();
        long failureCount = loadFailureCount.get();
        long totalLoadTime = totalLoadTimeNanos.get();
        long totalLoadCount = successCount + failureCount;
        return new MetaCacheEntryStats(
                cacheSpec.isEnable(),
                effectiveEnabled,
                autoRefresh,
                cacheSpec.getTtlSecond(),
                cacheSpec.getCapacity(),
                data.estimatedSize(),
                cacheStats.requestCount(),
                cacheStats.hitCount(),
                cacheStats.missCount(),
                cacheStats.hitRate(),
                successCount,
                failureCount,
                totalLoadTime,
                totalLoadCount == 0 ? 0D : (double) totalLoadTime / totalLoadCount,
                MetaCacheWeightUtils.saturatedAdd(
                        cacheStats.evictionCount(), localEvictionCount.get()),
                invalidateCount.get(),
                lastLoadSuccessTimeMs.get(),
                lastLoadFailureTimeMs.get(),
                lastError.get(),
                weightBounded,
                weightBounded ? cacheSpec.getMaxWeight().getAsLong() : -1L,
                weightBounded ? entryBudget.getUsedWeight() : -1L,
                weightBounded ? MetaCacheWeightUtils.saturatedAdd(
                        automaticEvictionWeight.get(), localEvictionWeight.get()) : -1L,
                weightBounded ? weightAdmissionRejectedCount.get() : -1L,
                weightBounded ? entryBudget.getCatalogMaxWeight() : -1L,
                weightBounded ? entryBudget.getCatalogUsedWeight() : -1L,
                weightBounded ? entryBudget.getGlobalMaxWeight() : -1L,
                weightBounded ? entryBudget.getGlobalUsedWeight() : -1L,
                weightBounded ? lastWeightRejectReason.get() : "");
    }

    public boolean isWeightBounded() {
        return weightBounded;
    }

    private AdmissionResult admitWeightedValue(
            K key, V value, @Nullable V expectedCurrent, boolean requireExpected,
            @Nullable KeyMutationToken expectedMutation, long expectedReservationGeneration,
            boolean advanceMutationOnAdmission) {
        if (closed.get()) {
            return AdmissionResult.DISABLED;
        }
        MetaCacheSizeEstimate estimate;
        try {
            estimate = Objects.requireNonNull(sizeEstimator.estimate(key, value), "size estimate");
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (RuntimeException e) {
            rejectWeight("invalid_estimate");
            return AdmissionResult.REJECTED;
        }
        if (!estimate.isComplete()) {
            rejectWeight(estimate.getIncompleteReason());
            return AdmissionResult.REJECTED;
        }

        long estimatedPayloadBytes = estimate.getBytes();
        // A retained non-null key/value plus Caffeine node can never consume zero bytes. Treat a
        // complete zero as an estimator contract violation so an omitted formula cannot bypass
        // every quota and admit an unbounded number of zero-weight entries.
        if (estimatedPayloadBytes == 0L) {
            rejectWeight("invalid_estimate");
            return AdmissionResult.REJECTED;
        }
        long newWeight = MetaCacheWeightUtils.saturatedAdd(
                estimatedPayloadBytes, FIXED_ENTRY_ACCOUNTING_OVERHEAD_BYTES);
        synchronized (admissionLock) {
            if (closed.get()) {
                return AdmissionResult.DISABLED;
            }
            if (expectedMutation != null && !isKeyMutationCurrent(key, expectedMutation)) {
                return AdmissionResult.NOT_CURRENT;
            }
            V oldValue = data.asMap().get(key);
            ReservationRecord record = reservations.get(key);
            if (expectedReservationGeneration >= 0L
                    && (record == null || record.generation != expectedReservationGeneration)) {
                return AdmissionResult.NOT_CURRENT;
            }
            if (requireExpected && oldValue != expectedCurrent) {
                return AdmissionResult.NOT_CURRENT;
            }
            if (oldValue == null && record != null) {
                // The previous generation was already removed by Caffeine; if that removal was
                // an eviction whose asynchronous cleanup has not run yet, account it here.
                if (pendingEvictionGenerations.remove(key, record.generation)) {
                    automaticEvictionWeight.accumulateAndGet(
                            record.weight, MetaCacheWeightUtils::saturatedAdd);
                }
                reservations.remove(key, record);
                record.reservation.release();
                record = null;
            }
            if (oldValue != null && (record == null || !record.published)) {
                rejectWeight("missing_reservation");
                return AdmissionResult.REJECTED;
            }

            if (record == null) {
                Optional<AdmissionReservation> reservation = reserveWithLocalEviction(key, newWeight);
                if (!reservation.isPresent()) {
                    rejectWeight("budget_exceeded");
                    return AdmissionResult.REJECTED;
                }
                ReservationRecord newRecord = new ReservationRecord(
                        newWeight, reservation.get(), nextReservationGeneration());
                if (advanceMutationOnAdmission) {
                    advanceKeyMutation(key);
                }
                reservations.put(key, newRecord);
                try {
                    beforeWeightedCachePutForTest(key, value);
                    data.put(key, value);
                    if (reservations.get(key) == newRecord && data.asMap().get(key) == value) {
                        newRecord.published = true;
                        notifyReplacement(key, null, value);
                    }
                    return AdmissionResult.ADMITTED;
                } catch (RuntimeException | Error e) {
                    reservations.remove(key, newRecord);
                    newRecord.reservation.release();
                    throw e;
                }
            }

            ReservationRecord previousRecord = record;
            long reservedWeight = Math.max(previousRecord.weight, newWeight);
            if (!resizeWithLocalEviction(key, previousRecord.reservation, reservedWeight)) {
                rejectWeight("budget_exceeded");
                return AdmissionResult.REJECTED;
            }
            if (advanceMutationOnAdmission) {
                advanceKeyMutation(key);
            }
            ReservationRecord newRecord = new ReservationRecord(
                    newWeight, previousRecord.reservation, nextReservationGeneration());
            reservations.put(key, newRecord);
            try {
                beforeWeightedCachePutForTest(key, value);
                data.put(key, value);
                boolean retained = reservations.get(key) == newRecord && data.asMap().get(key) == value;
                if (retained) {
                    newRecord.published = true;
                }
                if (retained && reservedWeight != newWeight && !newRecord.reservation.tryResize(newWeight)) {
                    throw new IllegalStateException("failed to release cache replacement reservation delta");
                }
                if (retained) {
                    notifyReplacement(key, oldValue, value);
                }
                return AdmissionResult.ADMITTED;
            } catch (RuntimeException | Error e) {
                if (reservations.replace(key, newRecord, previousRecord)) {
                    if (data.asMap().get(key) == null) {
                        reservations.remove(key, previousRecord);
                        previousRecord.reservation.release();
                    } else if (!previousRecord.reservation.tryResize(previousRecord.weight)) {
                        throw new IllegalStateException("failed to roll back cache replacement reservation", e);
                    }
                }
                throw e;
            }
        }
    }

    private Optional<AdmissionReservation> reserveWithLocalEviction(K incomingKey, long bytes) {
        if (bytes > entryBudget.getEffectiveMaxWeight()) {
            return Optional.empty();
        }
        Optional<AdmissionReservation> reservation = entryBudget.tryReserve(bytes);
        while (!reservation.isPresent()) {
            int evicted = evictLocalColdest(incomingKey, LOCAL_EVICTION_BATCH_SIZE);
            if (evicted == 0) {
                entryBudget.requestPeerReclaim(bytes);
                break;
            }
            reservation = entryBudget.tryReserve(bytes);
        }
        return reservation;
    }

    private boolean resizeWithLocalEviction(K incomingKey, AdmissionReservation reservation, long newBytes) {
        if (newBytes > entryBudget.getEffectiveMaxWeight()) {
            return false;
        }
        if (reservation.tryResize(newBytes)) {
            return true;
        }
        while (true) {
            int evicted = evictLocalColdest(incomingKey, LOCAL_EVICTION_BATCH_SIZE);
            if (evicted == 0) {
                entryBudget.requestPeerReclaim(Math.max(0L, newBytes - reservation.getBytes()));
                return false;
            }
            if (reservation.tryResize(newBytes)) {
                return true;
            }
        }
    }

    private int evictLocalColdest(K incomingKey, int limit) {
        if (!data.policy().eviction().isPresent()) {
            return 0;
        }
        Map<K, V> coldest = data.policy().eviction().get().coldest(limit);
        int evicted = 0;
        for (Map.Entry<K, V> candidate : coldest.entrySet()) {
            if (Objects.equals(candidate.getKey(), incomingKey)) {
                continue;
            }
            V current = data.asMap().get(candidate.getKey());
            ReservationRecord record = reservations.get(candidate.getKey());
            long evictedWeight = record != null && record.published && current != null ? record.weight : 0L;
            if (current == candidate.getValue() && data.asMap().remove(candidate.getKey(), current)) {
                if (record != null) {
                    releaseReservation(candidate.getKey(), record.generation);
                }
                localEvictionCount.incrementAndGet();
                localEvictionWeight.accumulateAndGet(evictedWeight, MetaCacheWeightUtils::saturatedAdd);
                evicted++;
            }
        }
        return evicted;
    }

    private long reclaimForPeer(long targetBytes) {
        if (targetBytes <= 0L || closed.get()) {
            return 0L;
        }
        synchronized (admissionLock) {
            long before = entryBudget.getUsedWeight();
            long reclaimed = 0L;
            while (reclaimed < targetBytes
                    && evictLocalColdest(null, LOCAL_EVICTION_BATCH_SIZE) > 0) {
                reclaimed = Math.max(0L, before - entryBudget.getUsedWeight());
            }
            return reclaimed;
        }
    }

    private int weigh(K key, V value) {
        ReservationRecord record = reservations.get(key);
        // Every supported write path installs the reservation record before calling data.put.
        // Missing ownership is an invariant violation, so fail closed without invoking an O(n)
        // estimator from Caffeine's hot weigher callback.
        long weight = record == null ? Integer.MAX_VALUE : record.weight;
        return weight >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) weight;
    }

    private void onRemoval(@Nullable K key, @Nullable V value, RemovalCause cause) {
        if (key == null) {
            return;
        }
        if (!weightBounded && !generationFencedRefresh) {
            return;
        }
        if (closed.get()) {
            return;
        }
        // Replacement transfers the existing reservation to the newly published generation. A
        // soft-value collection instead reports a null value with COLLECTED and must release it.
        if (cause == RemovalCause.REPLACED) {
            return;
        }
        if (Thread.holdsLock(admissionLock)) {
            // Other removals have already removed the Caffeine mapping and can release their owner
            // inline. A stale callback cannot release a replacement while its mapping is visible.
            if (data.asMap().get(key) == null) {
                if (weightBounded) {
                    ReservationRecord record = reservations.get(key);
                    if (record != null) {
                        if (cause.wasEvicted()) {
                            automaticEvictionWeight.accumulateAndGet(
                                    record.weight, MetaCacheWeightUtils::saturatedAdd);
                        }
                        releaseReservation(key, record.generation);
                    }
                } else {
                    RefreshRecord record = refreshRecords.get(key);
                    if (record != null) {
                        releaseRefreshRecord(key, record.generation);
                    }
                }
            }
            return;
        }
        beforeRemovalOwnerSnapshotForTest(key);
        long ownerGeneration = currentOwnerGeneration(key);
        if (ownerGeneration >= 0L) {
            beforeRemovalReleaseForTest(key);
            if (closed.get()) {
                return;
            }
            if (cause.wasEvicted()) {
                pendingEvictionGenerations.merge(key, ownerGeneration, Math::max);
            }
            pendingRemovalGenerations.merge(key, ownerGeneration, Math::max);
            if (closed.get()) {
                pendingRemovalGenerations.remove(key, ownerGeneration);
                pendingEvictionGenerations.remove(key, ownerGeneration);
                return;
            }
            scheduleRemovalCleanup();
        }
    }

    private void scheduleRemovalCleanup() {
        if (closed.get()) {
            return;
        }
        if (removalCleanupScheduled.compareAndSet(false, true)) {
            try {
                REMOVAL_CLEANUP_EXECUTOR.execute(this::drainRemovalCleanups);
            } catch (RejectedExecutionException e) {
                removalCleanupScheduled.set(false);
                LOG.warn("Failed to schedule removal cleanup for external metadata cache entry {}", name, e);
            }
        }
    }

    private void drainRemovalCleanups() {
        try {
            int processed = 0;
            for (Map.Entry<K, Long> cleanup : pendingRemovalGenerations.entrySet()) {
                if (processed++ >= REMOVAL_CLEANUP_BATCH_SIZE) {
                    break;
                }
                K key = cleanup.getKey();
                long generation = cleanup.getValue();
                // Claim before cleanup. If the same generation is reported again while cleanup
                // runs, its notification creates a new pending item instead of being lost when
                // this worker finishes.
                if (!pendingRemovalGenerations.remove(key, generation)) {
                    continue;
                }
                boolean evicted = pendingEvictionGenerations.remove(key, generation);
                if (!evicted) {
                    // A newer generation superseded the evicted one; its eviction can no longer
                    // be attributed, so drop the stale marker instead of retaining the key.
                    pendingEvictionGenerations.remove(key);
                }
                try {
                    cleanupRemovedReservation(key, generation, evicted);
                } catch (RuntimeException e) {
                    // Restore the generation for retry. The finally block requeues one bounded
                    // drain instead of permanently wedging this entry's scheduled flag.
                    pendingRemovalGenerations.merge(key, generation, Math::max);
                    if (evicted) {
                        pendingEvictionGenerations.merge(key, generation, Math::max);
                    }
                    LOG.warn("Failed to clean a removal reservation for external metadata cache entry {}",
                            name, e);
                }
            }
        } finally {
            removalCleanupScheduled.set(false);
            if (!closed.get() && !pendingRemovalGenerations.isEmpty()) {
                // One bounded task per turn prevents a hot entry from monopolizing the process-wide
                // cleanup executor; a later task is queued behind already scheduled catalogs.
                scheduleRemovalCleanup();
            }
        }
    }

    private void cleanupRemovedReservation(K key, long expectedReservationGeneration, boolean evicted) {
        beforeRemovalCleanupLockForTest(key);
        synchronized (admissionLock) {
            if (weightBounded) {
                ReservationRecord record = reservations.get(key);
                if (record != null && record.generation == expectedReservationGeneration
                        && data.asMap().get(key) == null
                        && reservations.remove(key, record)) {
                    if (evicted) {
                        // The exact reservation, not Caffeine's int-clamped weigher value.
                        automaticEvictionWeight.accumulateAndGet(
                                record.weight, MetaCacheWeightUtils::saturatedAdd);
                    }
                    record.reservation.release();
                }
            } else {
                RefreshRecord record = refreshRecords.get(key);
                if (record != null && record.generation == expectedReservationGeneration
                        && data.asMap().get(key) == null) {
                    refreshRecords.remove(key, record);
                }
            }
        }
        afterRemovalCleanupForTest(key);
    }

    private boolean releaseReservation(K key, long expectedGeneration) {
        ReservationRecord record = reservations.get(key);
        if (record != null && record.generation == expectedGeneration && reservations.remove(key, record)) {
            record.reservation.release();
            return true;
        }
        return false;
    }

    private void releaseRefreshRecord(K key, long expectedGeneration) {
        RefreshRecord record = refreshRecords.get(key);
        if (record != null && record.generation == expectedGeneration) {
            refreshRecords.remove(key, record);
        }
    }

    private long currentOwnerGeneration(K key) {
        if (weightBounded) {
            ReservationRecord record = reservations.get(key);
            return record == null ? -1L : record.generation;
        }
        RefreshRecord record = refreshRecords.get(key);
        return record == null ? -1L : record.generation;
    }

    private void putNonWeightedValue(K key, V value) {
        V previousValue = data.asMap().get(key);
        RefreshRecord previous = refreshRecords.get(key);
        RefreshRecord next = publishRefreshRecord(key);
        try {
            beforeNonWeightedCachePutForTest(key, value);
            data.put(key, value);
            if (next != null && refreshRecords.get(key) == next && data.asMap().get(key) == value) {
                next.published = true;
            }
            if (data.asMap().get(key) == value) {
                notifyReplacement(key, previousValue, value);
            }
        } catch (RuntimeException | Error e) {
            if (next != null) {
                if (previous == null) {
                    refreshRecords.remove(key, next);
                } else {
                    refreshRecords.replace(key, next, previous);
                }
            }
            throw e;
        }
    }

    @Nullable
    private RefreshRecord publishRefreshRecord(K key) {
        if (!generationFencedRefresh) {
            return null;
        }
        RefreshRecord record = new RefreshRecord(nextReservationGeneration());
        refreshRecords.put(key, record);
        return record;
    }

    private void notifyReplacement(K key, @Nullable V previousValue, V currentValue) {
        if (replacementListener == null || previousValue == currentValue) {
            return;
        }
        try {
            replacementListener.onReplacement(key, previousValue, currentValue);
        } catch (RuntimeException e) {
            LOG.warn("Failed to retire dependencies after replacing external metadata cache entry {}", name, e);
        }
    }

    private long nextReservationGeneration() {
        return reservationGeneration.incrementAndGet();
    }

    private void rejectWeight(String reason) {
        weightAdmissionRejectedCount.incrementAndGet();
        String normalizedReason = reason == null || reason.isEmpty() ? "unknown" : reason;
        lastWeightRejectReason.set(normalizedReason);
        long now = System.currentTimeMillis();
        long previous = lastWeightRejectLogTimeMs.get();
        if (now - previous >= WEIGHT_REJECT_LOG_INTERVAL_MS
                && lastWeightRejectLogTimeMs.compareAndSet(previous, now)) {
            LOG.warn("Rejected external metadata cache admission for entry {}: reason={}, entryUsed={}, "
                            + "entryMax={}, catalogUsed={}, catalogMax={}, globalUsed={}, globalMax={}",
                    name, normalizedReason, entryBudget.getUsedWeight(), entryBudget.getEffectiveMaxWeight(),
                    entryBudget.getCatalogUsedWeight(), entryBudget.getCatalogMaxWeight(),
                    entryBudget.getGlobalUsedWeight(), entryBudget.getGlobalMaxWeight());
        }
    }

    private void maybeRefreshManagedValue(K key, V currentValue) {
        if (closed.get() || !generationFencedRefresh || loader == null) {
            return;
        }
        if (!weightBounded) {
            maybeRefreshNonWeightedValue(key, currentValue);
            return;
        }
        ReservationRecord record = reservations.get(key);
        long refreshNanos = TimeUnit.MINUTES.toNanos(Config.external_cache_refresh_time_minutes);
        if (record == null || !record.published || data.asMap().get(key) != currentValue || refreshNanos <= 0
                || System.nanoTime() - record.writeNanos < refreshNanos
                || refreshesInFlight.putIfAbsent(key, Boolean.TRUE) != null) {
            return;
        }
        submitWeightedRefresh(key, record.generation, beginKeyMutation(key));
    }

    private void maybeRefreshNonWeightedValue(K key, V currentValue) {
        RefreshRecord record = refreshRecords.get(key);
        long refreshNanos = TimeUnit.MINUTES.toNanos(Config.external_cache_refresh_time_minutes);
        if (record == null || !record.published || data.asMap().get(key) != currentValue || refreshNanos <= 0
                || System.nanoTime() - record.writeNanos < refreshNanos
                || refreshesInFlight.putIfAbsent(key, Boolean.TRUE) != null) {
            return;
        }
        submitNonWeightedRefresh(key, record.generation, beginKeyMutation(key));
    }

    private void submitNonWeightedRefresh(
            K key, long expectedRefreshGeneration, KeyMutationToken expectedMutation) {
        try {
            refreshExecutor.execute(() -> {
                try {
                    if (!isRefreshRecordCurrent(key, expectedRefreshGeneration, expectedMutation)) {
                        return;
                    }
                    V refreshed = loadAndTrack(key, this::applyDefaultLoader);
                    if (refreshed == null) {
                        return;
                    }
                    synchronized (admissionLock) {
                        if (!isRefreshRecordCurrent(key, expectedRefreshGeneration, expectedMutation)) {
                            return;
                        }
                        advanceKeyMutation(key);
                        putNonWeightedValue(key, refreshed);
                    }
                } catch (RuntimeException e) {
                    LOG.warn("Failed to refresh external metadata cache entry {} for key {}; "
                            + "retaining the previous value", name, key, e);
                } finally {
                    endKeyMutation(key, expectedMutation);
                    refreshesInFlight.remove(key);
                }
            });
        } catch (RejectedExecutionException e) {
            endKeyMutation(key, expectedMutation);
            refreshesInFlight.remove(key);
        }
    }

    private boolean isRefreshRecordCurrent(
            K key, long expectedRefreshGeneration, KeyMutationToken expectedMutation) {
        if (closed.get() || !isKeyMutationCurrent(key, expectedMutation)) {
            return false;
        }
        RefreshRecord record = refreshRecords.get(key);
        return record != null && record.generation == expectedRefreshGeneration
                && record.published && data.asMap().get(key) != null;
    }

    private void submitWeightedRefresh(
            K key, long expectedReservationGeneration, KeyMutationToken expectedMutation) {
        try {
            refreshExecutor.execute(() -> {
                try {
                    if (!isReservationCurrent(key, expectedReservationGeneration, expectedMutation)) {
                        return;
                    }
                    V refreshed = loadAndTrack(key, this::applyDefaultLoader);
                    if (refreshed != null && isKeyMutationCurrent(key, expectedMutation)) {
                        admitWeightedValue(
                                key, refreshed, null, false, expectedMutation,
                                expectedReservationGeneration, true);
                        // Admission rejection leaves the already reserved, known-good generation
                        // in place. A larger refresh must not turn a transient quota shortage into
                        // a forced cache miss for every subsequent reader.
                    }
                } catch (RuntimeException e) {
                    LOG.warn("Failed to refresh external metadata cache entry {} for key {}; "
                            + "retaining the previous value", name, key, e);
                } finally {
                    endKeyMutation(key, expectedMutation);
                    refreshesInFlight.remove(key);
                }
            });
        } catch (RejectedExecutionException e) {
            endKeyMutation(key, expectedMutation);
            refreshesInFlight.remove(key);
        }
    }

    private boolean isReservationCurrent(
            K key, long expectedReservationGeneration, KeyMutationToken expectedMutation) {
        if (closed.get() || !isKeyMutationCurrent(key, expectedMutation)) {
            return false;
        }
        ReservationRecord record = reservations.get(key);
        return record != null && record.generation == expectedReservationGeneration
                && record.published && data.asMap().get(key) != null;
    }

    // Read the config dynamically so existing cache entries follow runtime config updates.
    private boolean isManualMissLoadEnabled() {
        return weightBounded || generationFencedRefresh || Config.enable_external_meta_cache_manual_miss_load;
    }

    // Execute slow miss loads outside Caffeine's sync load path and suppress stale write-back after invalidation.
    private V getWithManualLoad(K key, Function<K, V> loadFunction) {
        if (!effectiveEnabled || closed.get()) {
            // Disabled and closed entries may still serve the caller, but can not retain the loaded value.
            return loadAndTrack(key, loadFunction);
        }

        V value = data.getIfPresent(key);
        if (value != null) {
            maybeRefreshManagedValue(key, value);
            return value;
        }

        synchronized (loadLock(key)) {
            if (!effectiveEnabled || closed.get()) {
                return loadAndTrack(key, loadFunction);
            }
            value = data.asMap().get(key);
            if (value != null) {
                maybeRefreshManagedValue(key, value);
                return value;
            }

            KeyMutationToken mutation = beginKeyMutation(key);
            try {
                V loaded = loadAndTrack(key, loadFunction);
                if (!isKeyMutationCurrent(key, mutation)) {
                    return loaded;
                }

                // Keep null results uncached so manual miss load matches LoadingCache null-return behavior.
                if (loaded == null) {
                    return null;
                }

                // Leave a narrow hook for tests to pause exactly before the cache put race window.
                beforeManualCachePutForTest(key, loaded);
                if (closed.get() || !isKeyMutationCurrent(key, mutation)) {
                    return loaded;
                }
                if (weightBounded) {
                    admitWeightedValue(key, loaded, null, false, mutation, -1L, false);
                } else {
                    synchronized (admissionLock) {
                        if (closed.get() || !isKeyMutationCurrent(key, mutation)) {
                            return loaded;
                        }
                        beforeNonWeightedManualCachePutForTest(key, loaded);
                        putNonWeightedValue(key, loaded);
                    }
                }
                return loaded;
            } finally {
                endKeyMutation(key, mutation);
            }
        }
    }

    // Map keys to a fixed lock stripe set to bound memory usage while keeping same-key deduplication.
    private Object loadLock(K key) {
        int hash = key == null ? 0 : key.hashCode();
        return loadLocks[(hash & Integer.MAX_VALUE) % loadLocks.length];
    }

    // Let tests pause between the first generation check and data.put without affecting production behavior.
    void beforeManualCachePutForTest(K key, V loaded) {
    }

    // Let tests pause after the final generation check while holding the admission lock.
    void beforeNonWeightedManualCachePutForTest(K key, V loaded) {
    }

    // Called inside Caffeine's direct removal callback; tests use it to force the eviction-lock race.
    void beforeRemovalReleaseForTest(K key) {
    }

    // Let tests pause a callback after Caffeine removal but before reservation-owner lookup.
    void beforeRemovalOwnerSnapshotForTest(K key) {
    }

    // Called after reservation ownership is published and before Caffeine receives the value.
    void beforeWeightedCachePutForTest(K key, V value) {
    }

    // Called after refresh ownership is published and before Caffeine receives a count-bounded value.
    void beforeNonWeightedCachePutForTest(K key, V value) {
    }

    // Called after one asynchronous generation-conditional removal cleanup has finished.
    void afterRemovalCleanupForTest(K key) {
    }

    // Called immediately before the asynchronous drain tries to acquire admissionLock.
    void beforeRemovalCleanupLockForTest(K key) {
    }

    // Let tests establish admissionLock -> Caffeine eviction-lock ordering deterministically.
    void beforeWeightedInvalidateAllForTest() {
    }

    // Invoke the direct-listener branch while holding the mutation lock.
    void notifyRemovalUnderAdmissionLockForTest(K key, V value, RemovalCause cause) {
        synchronized (admissionLock) {
            onRemoval(key, value, cause);
        }
    }

    // Enqueue the same generation-only refresh task without waiting for the production interval.
    void triggerRefreshForTest(K key) {
        V current = data.asMap().get(key);
        if (current == null || refreshesInFlight.putIfAbsent(key, Boolean.TRUE) != null) {
            return;
        }
        if (weightBounded) {
            ReservationRecord record = reservations.get(key);
            if (record != null && record.published) {
                submitWeightedRefresh(key, record.generation, beginKeyMutation(key));
                return;
            }
        } else if (generationFencedRefresh) {
            RefreshRecord record = refreshRecords.get(key);
            if (record != null && record.published) {
                submitNonWeightedRefresh(key, record.generation, beginKeyMutation(key));
                return;
            }
        }
        refreshesInFlight.remove(key);
    }

    private V loadFromDefaultLoader(K key) {
        return loadAndTrack(key, this::applyDefaultLoader);
    }

    // Resolve the default loader separately so the manual path can share tracking without double counting.
    private V applyDefaultLoader(K key) {
        if (loader == null) {
            throw new UnsupportedOperationException(
                    String.format("Entry '%s' requires a contextual miss loader.", name));
        }
        return loader.apply(key);
    }

    // Track load outcomes locally because manual miss loads do not contribute to Caffeine load statistics.
    private V loadAndTrack(K key, Function<K, V> loadFunction) {
        long startNanos = System.nanoTime();
        try {
            V value = loadFunction.apply(key);
            loadSuccessCount.incrementAndGet();
            totalLoadTimeNanos.addAndGet(System.nanoTime() - startNanos);
            lastLoadSuccessTimeMs.set(System.currentTimeMillis());
            return value;
        } catch (RuntimeException | Error e) {
            loadFailureCount.incrementAndGet();
            totalLoadTimeNanos.addAndGet(System.nanoTime() - startNanos);
            lastLoadFailureTimeMs.set(System.currentTimeMillis());
            lastError.set(e.toString());
            throw e;
        }
    }

    private KeyMutationToken beginKeyMutation(K key) {
        synchronized (admissionLock) {
            KeyMutationState state = keyMutationStates.computeIfAbsent(key, ignored -> new KeyMutationState());
            state.inFlight++;
            return new KeyMutationToken(state, state.generation, fullInvalidationGeneration.get());
        }
    }

    private void advanceKeyMutation(K key) {
        // Callers already serialize cache mutation with admissionLock. Keeping the helper lock-free
        // prevents accidental deadlock if it is used by a listener reached under that same lock.
        KeyMutationState state = keyMutationStates.get(key);
        if (state != null) {
            state.generation++;
        }
    }

    private boolean isKeyMutationCurrent(K key, KeyMutationToken token) {
        return token.fullInvalidationGeneration == fullInvalidationGeneration.get()
                && token.state == keyMutationStates.get(key)
                && token.generation == token.state.generation;
    }

    private void endKeyMutation(K key, KeyMutationToken token) {
        synchronized (admissionLock) {
            if (--token.state.inFlight == 0) {
                keyMutationStates.remove(key, token.state);
            }
        }
    }

    private static final class ReservationRecord {
        private final long weight;
        private final long writeNanos;
        private final AdmissionReservation reservation;
        private final long generation;
        private volatile boolean published;

        private ReservationRecord(long weight, AdmissionReservation reservation, long generation) {
            this.weight = weight;
            this.reservation = reservation;
            this.writeNanos = System.nanoTime();
            this.generation = generation;
        }
    }

    private static final class RefreshRecord {
        private final long writeNanos;
        private final long generation;
        private volatile boolean published;

        private RefreshRecord(long generation) {
            this.writeNanos = System.nanoTime();
            this.generation = generation;
        }
    }

    private static final class KeyMutationState {
        private volatile long generation;
        private int inFlight;
    }

    private static final class KeyMutationToken {
        private final KeyMutationState state;
        private final long generation;
        private final long fullInvalidationGeneration;

        private KeyMutationToken(
                KeyMutationState state, long generation, long fullInvalidationGeneration) {
            this.state = state;
            this.generation = generation;
            this.fullInvalidationGeneration = fullInvalidationGeneration;
        }
    }

    private static ReplaceResult toReplaceResult(AdmissionResult result) {
        switch (result) {
            case ADMITTED:
                return ReplaceResult.REPLACED;
            case NOT_CURRENT:
                return ReplaceResult.NOT_CURRENT;
            case REJECTED:
                return ReplaceResult.REJECTED;
            case DISABLED:
            default:
                return ReplaceResult.DISABLED;
        }
    }

    private enum AdmissionResult {
        ADMITTED,
        NOT_CURRENT,
        REJECTED,
        DISABLED
    }
}
