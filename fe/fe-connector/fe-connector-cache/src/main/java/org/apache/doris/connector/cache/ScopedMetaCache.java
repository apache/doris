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

package org.apache.doris.connector.cache;

import org.apache.doris.connector.cache.ScopedMetaCacheRegistry.CacheAddress;
import org.apache.doris.connector.cache.ScopedMetaCacheRegistry.PublicationState;
import org.apache.doris.connector.cache.ScopedMetaCacheRegistry.ScopeLease;
import org.apache.doris.connector.cache.ScopedMetaCacheRegistry.ScopeSnapshot;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * One physical Caffeine cache participating in a {@link ScopedMetaCacheRegistry}.
 *
 * <p>Every value is wrapped with both its hierarchical scope-state identities and an exact-key state. Hierarchical
 * invalidation can therefore detach a whole catalog/database/table/partition subtree, while exact-key invalidation
 * fences only one physical cache key. Removal listeners conditionally remove the exact wrapper from its original
 * scope bucket and key node, so delayed callbacks cannot delete a replacement.
 */
public final class ScopedMetaCache<K, V> implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(ScopedMetaCache.class);
    private static final long FIXED_ENTRY_ACCOUNTING_OVERHEAD_BYTES = 512L;
    private static final long WEIGHT_REJECT_WARN_INTERVAL_NANOS = Duration.ofMinutes(1).toNanos();
    private static final Runnable NO_OP = () -> {
    };

    private final ScopedMetaCacheRegistry registry;
    private final String name;
    private final boolean effectiveEnabled;
    private final Cache<K, VersionedValue<K, V>> data;
    private final ConcurrentMap<K, KeyNode<K, V>> keyNodes = new ConcurrentHashMap<>();
    private final ConcurrentMap<LoadAddress<K>, CompletableFuture<V>> inFlightLoads = new ConcurrentHashMap<>();
    private final ConcurrentMap<K, VersionedValue<K, V>> refreshing = new ConcurrentHashMap<>();
    private final StripedPhaseGate bulkInvalidationGate = new StripedPhaseGate();
    private final Map<K, BigInteger> exactInvalidations = new HashMap<>();
    private final NavigableMap<BigInteger, Integer> activeBulkStarts = new TreeMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final LongAdder requestCount = new LongAdder();
    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder loadSuccessCount = new LongAdder();
    private final LongAdder loadFailureCount = new LongAdder();
    private final LongAdder totalLoadTimeNanos = new LongAdder();
    private final LongAdder evictionCount = new LongAdder();
    private final LongAdder invalidateCount = new LongAdder();
    private final AtomicReference<Long> lastLoadSuccessTimeMs = new AtomicReference<>(-1L);
    private final AtomicReference<Long> lastLoadFailureTimeMs = new AtomicReference<>(-1L);
    private final AtomicReference<String> lastError = new AtomicReference<>("");
    private final AtomicReference<String> lastWeightRejectReason = new AtomicReference<>("");
    private final LongAdder weightRejectCount = new LongAdder();
    private final AtomicLong lastWeightRejectWarnNanos = new AtomicLong(Long.MIN_VALUE);
    private final RemovalListener<K, V> beforeRemoval;
    private final BiConsumer<K, V> discardListener;
    private final Ticker ticker;
    private final long refreshAfterWriteNanos;
    private final Executor refreshExecutor;
    private final Runnable afterLoadElection;
    private final Runnable afterBulkStage;
    private final Runnable afterRefreshRegistration;
    private final MetaCacheSizeEstimator<K, V> sizeEstimator;
    private final MetaCacheBudgetManager.EntryBudget entryBudget;
    private final boolean weightBounded;
    private final ThreadLocal<RemovalDeferral<K, V>> removalDeferrals =
            ThreadLocal.withInitial(RemovalDeferral::new);
    private final ThreadLocal<Registration> budgetEvictionRegistration = new ThreadLocal<>();
    private BigInteger exactInvalidationSequence = BigInteger.ZERO;

    ScopedMetaCache(
            ScopedMetaCacheRegistry registry,
            String name,
            CacheSpec cacheSpec,
            Ticker ticker,
            RemovalListener<K, V> beforeRemoval,
            BiConsumer<K, V> discardListener,
            Duration refreshAfterWrite,
            Executor refreshExecutor,
            Runnable afterLoadElection,
            Runnable afterBulkStage,
            Runnable afterRefreshRegistration,
            MetaCacheSizeEstimator<K, V> sizeEstimator,
            MetaCacheBudgetManager.EntryBudget entryBudget) {
        this.registry = Objects.requireNonNull(registry, "registry can not be null");
        this.name = Objects.requireNonNull(name, "name can not be null");
        Objects.requireNonNull(cacheSpec, "cacheSpec can not be null");
        this.beforeRemoval = beforeRemoval;
        this.discardListener = discardListener;
        this.ticker = ticker == null ? Ticker.systemTicker() : ticker;
        this.refreshAfterWriteNanos = refreshAfterWrite == null ? 0L : refreshAfterWrite.toNanos();
        this.refreshExecutor = refreshExecutor;
        this.afterLoadElection =
                Objects.requireNonNull(afterLoadElection, "afterLoadElection can not be null");
        this.afterBulkStage = Objects.requireNonNull(afterBulkStage, "afterBulkStage can not be null");
        this.afterRefreshRegistration = Objects.requireNonNull(
                afterRefreshRegistration, "afterRefreshRegistration can not be null");
        this.sizeEstimator = sizeEstimator;
        this.entryBudget = entryBudget;
        if ((sizeEstimator == null) != (entryBudget == null)) {
            throw new IllegalArgumentException(
                    "Weighted metadata cache requires both estimator and budget: " + name);
        }
        this.weightBounded = entryBudget != null;
        if (weightBounded) {
            entryBudget.setReclaimer(this::reclaimForPeer);
        }
        this.effectiveEnabled = cacheSpec.isCacheEnabled();

        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(effectiveEnabled ? cacheSpec.getCapacity() : 0L)
                .executor(Runnable::run)
                .removalListener(this::onRemoval);
        OptionalLong expiry = effectiveEnabled
                ? CacheSpec.toExpireAfterAccess(cacheSpec.getTtlSecond())
                : OptionalLong.empty();
        if (expiry.isPresent()) {
            builder.expireAfterAccess(Duration.ofSeconds(expiry.getAsLong()));
        }
        if (ticker != null) {
            builder.ticker(this.ticker);
        }
        this.data = builder.build();
    }

    public String name() {
        return name;
    }

    public V get(K key, ScopePath path, Function<K, V> loader) {
        return getWithPublicationAction(key, path, loader,
                (loaded, commit) -> commit.accept(NO_OP));
    }

    public V getWithPublicationAction(K key, ScopePath path, Function<K, V> loader,
            BiConsumer<V, Consumer<Runnable>> publicationCoordinator) {
        Objects.requireNonNull(key, "key can not be null");
        Objects.requireNonNull(path, "path can not be null");
        Function<K, V> loadFunction = Objects.requireNonNull(loader, "loader can not be null");
        BiConsumer<V, Consumer<Runnable>> coordinator = Objects.requireNonNull(
                publicationCoordinator, "publicationCoordinator can not be null");
        checkOpen();
        if (!effectiveEnabled) {
            recordAccess(false);
            V loaded = loadAndRecord(key, loadFunction);
            if (loaded != null) {
                AtomicBoolean commitInvoked = new AtomicBoolean(false);
                try {
                    coordinator.accept(loaded, beforePublication -> {
                        if (!commitInvoked.compareAndSet(false, true)) {
                            throw new IllegalStateException("Metadata cache publication callback was invoked twice");
                        }
                        Objects.requireNonNull(beforePublication, "beforePublication can not be null").run();
                    });
                    if (!commitInvoked.get()) {
                        throw new IllegalStateException("Metadata cache publication callback was not invoked");
                    }
                } finally {
                    notifyDiscarded(key, loaded);
                }
            }
            return loaded;
        }

        VersionedValue<K, V> presentVersioned = currentVersionedValue(key, path);
        if (presentVersioned != null) {
            recordAccess(true);
            scheduleRefresh(key, path, loader, presentVersioned);
            return presentVersioned.value;
        }
        recordAccess(false);
        try (PublicationLease<K, V> lease = acquirePublicationLease(key, path, true)) {
            LoadAddress<K> loadAddress = new LoadAddress<>(key, path, lease);
            CompletableFuture<V> ownLoad = new CompletableFuture<>();
            CompletableFuture<V> existingLoad = inFlightLoads.putIfAbsent(loadAddress, ownLoad);
            if (existingLoad != null) {
                return awaitLoad(existingLoad);
            }
            try {
                afterLoadElection.run();
                AtomicReference<VersionedValue<K, V>> electedValue = new AtomicReference<>();
                boolean electedValuePresent = deferRemovals(() -> {
                    VersionedValue<K, V> present;
                    synchronized (lease.keyNode) {
                        present = currentVersionedValue(key, path);
                    }
                    electedValue.set(present);
                    return present != null;
                });
                if (electedValuePresent) {
                    VersionedValue<K, V> present = electedValue.get();
                    ownLoad.complete(present.value);
                    return present.value;
                }
                V loaded = loadAndRecord(key, loadFunction);
                if (loaded != null) {
                    AtomicBoolean commitInvoked = new AtomicBoolean(false);
                    AtomicBoolean retained = new AtomicBoolean(false);
                    try {
                        coordinator.accept(loaded, beforePublication -> {
                            if (!commitInvoked.compareAndSet(false, true)) {
                                throw new IllegalStateException(
                                        "Metadata cache publication callback was invoked twice");
                            }
                            retained.set(commitLoaded(lease, key, loaded, beforePublication));
                        });
                        if (!commitInvoked.get()) {
                            throw new IllegalStateException("Metadata cache publication callback was not invoked");
                        }
                    } finally {
                        if (!retained.get()) {
                            notifyDiscarded(key, loaded);
                        }
                    }
                }
                ownLoad.complete(loaded);
                return loaded;
            } catch (RuntimeException | Error throwable) {
                ownLoad.completeExceptionally(throwable);
                throw throwable;
            } finally {
                inFlightLoads.remove(loadAddress, ownLoad);
            }
        }
    }

    public V getIfPresent(K key, ScopePath path) {
        Objects.requireNonNull(key, "key can not be null");
        Objects.requireNonNull(path, "path can not be null");
        checkOpen();
        if (!effectiveEnabled) {
            recordAccess(false);
            return null;
        }
        VersionedValue<K, V> versioned = currentVersionedValue(key, path);
        recordAccess(versioned != null);
        return versioned == null ? null : versioned.value;
    }

    private VersionedValue<K, V> currentVersionedValue(K key, ScopePath path) {
        return deferRemovals(() -> {
            VersionedValue<K, V> versioned = data.getIfPresent(key);
            if (versioned == null || !versioned.scopeSnapshot.path().equals(path)) {
                return null;
            }
            if (!versioned.isCurrent(registry, keyNodes)) {
                data.asMap().remove(key, versioned);
                return null;
            }
            return versioned;
        });
    }

    public void put(K key, ScopePath path, V value) {
        Objects.requireNonNull(key, "key can not be null");
        Objects.requireNonNull(path, "path can not be null");
        Objects.requireNonNull(value, "value can not be null");
        checkOpen();
        if (!effectiveEnabled) {
            return;
        }
        try (PublicationLease<K, V> lease = acquirePublicationLease(key, path, false)) {
            AtomicReference<VersionedValue<K, V>> preparedRef = new AtomicReference<>();
            AtomicReference<ReplacementValue<K, V>> replacementRef = new AtomicReference<>();
            AtomicBoolean admissionRejected = new AtomicBoolean(false);
            boolean retained = false;
            try {
                retained = guardedCommit(lease, () -> {
                    VersionedValue<K, V> current = currentVersionedValue(key, path);
                    if (current != null) {
                        ReplacementValue<K, V> replacement =
                                prepareReplacementVersionedValue(lease, key, value, current);
                        replacementRef.set(replacement);
                        if (replacement == null) {
                            admissionRejected.set(true);
                            lease.keyNode.loadPublicationState.set(new Object());
                            removeCurrentVersion(key, current);
                            return false;
                        }
                        lease.keyNode.loadPublicationState.set(new Object());
                        return installReplacement(replacement);
                    }
                    VersionedValue<K, V> prepared = prepareVersionedValue(lease, key, value);
                    preparedRef.set(prepared);
                    if (prepared == null) {
                        admissionRejected.set(true);
                        return false;
                    }
                    lease.keyNode.loadPublicationState.set(new Object());
                    install(prepared, lease);
                    return true;
                });
            } finally {
                if (!retained && replacementRef.get() != null) {
                    replacementRef.get().rollback();
                } else if (!retained && preparedRef.get() != null) {
                    releaseReservation(preparedRef.get());
                }
                if (admissionRejected.get()) {
                    notifyDiscarded(key, value);
                }
            }
        }
    }

    public boolean compareAndSet(K key, ScopePath path, V expectedValue, V updatedValue) {
        return compareAndSet(key, path, expectedValue, updatedValue, NO_OP);
    }

    public boolean compareAndSet(
            K key, ScopePath path, V expectedValue, V updatedValue, Runnable commitAction) {
        Objects.requireNonNull(key, "key can not be null");
        Objects.requireNonNull(path, "path can not be null");
        Runnable action = Objects.requireNonNull(commitAction, "commitAction can not be null");
        checkOpen();
        if (!effectiveEnabled) {
            action.run();
            return true;
        }
        try (PublicationLease<K, V> lease = acquirePublicationLease(key, path, false)) {
            AtomicReference<VersionedValue<K, V>> preparedRef = new AtomicReference<>();
            AtomicReference<ReplacementValue<K, V>> replacementRef = new AtomicReference<>();
            AtomicBoolean admissionRejected = new AtomicBoolean(false);
            boolean committed = false;
            try {
                committed = guardedCommit(lease, () -> {
                    VersionedValue<K, V> current = currentVersionedValue(key, path);
                    V currentValue = current == null ? null : current.value;
                    if (currentValue != expectedValue) {
                        return false;
                    }
                    if (updatedValue != null && updatedValue != currentValue) {
                        if (current != null) {
                            ReplacementValue<K, V> replacement =
                                    prepareReplacementVersionedValue(lease, key, updatedValue, current);
                            replacementRef.set(replacement);
                            if (replacement == null) {
                                admissionRejected.set(true);
                                lease.keyNode.loadPublicationState.set(new Object());
                                action.run();
                                removeCurrentVersion(key, current);
                                return true;
                            }
                        } else {
                            VersionedValue<K, V> prepared = prepareVersionedValue(lease, key, updatedValue);
                            preparedRef.set(prepared);
                            if (prepared == null) {
                                admissionRejected.set(true);
                                lease.keyNode.loadPublicationState.set(new Object());
                                action.run();
                                return true;
                            }
                        }
                    }
                    lease.keyNode.loadPublicationState.set(new Object());
                    try {
                        action.run();
                    } catch (RuntimeException | Error e) {
                        // Restore the old reservation before removal can acquire this key's lock.
                        if (replacementRef.get() != null) {
                            replacementRef.get().rollback();
                        }
                        throw e;
                    }
                    if (updatedValue == currentValue) {
                        lease.keyNode.loadPublicationState.set(new Object());
                        return true;
                    }
                    if (updatedValue != null) {
                        if (replacementRef.get() != null) {
                            lease.keyNode.loadPublicationState.set(new Object());
                            return installReplacement(replacementRef.get());
                        }
                        lease.keyNode.loadPublicationState.set(new Object());
                        install(preparedRef.get(), lease);
                        return true;
                    }
                    if (updatedValue == null) {
                        if (current != null) {
                            removeCurrentVersion(key, current);
                        }
                    }
                    lease.keyNode.loadPublicationState.set(new Object());
                    return true;
                });
            } finally {
                if (!committed && replacementRef.get() != null) {
                    replacementRef.get().rollback();
                } else if (!committed && preparedRef.get() != null) {
                    releaseReservation(preparedRef.get());
                }
                if (admissionRejected.get()) {
                    notifyDiscarded(key, updatedValue);
                }
            }
            return committed;
        }
    }

    public void invalidateKey(K key) {
        invalidateKey(key, NO_OP, NO_OP);
    }

    void invalidateKey(K key, Runnable afterStateReplacement) {
        invalidateKey(key, NO_OP, afterStateReplacement);
    }

    void invalidateKey(
            K key, Runnable beforeInvalidationLock, Runnable afterStateReplacement) {
        Objects.requireNonNull(key, "key can not be null");
        Objects.requireNonNull(beforeInvalidationLock, "beforeInvalidationLock can not be null");
        Objects.requireNonNull(afterStateReplacement, "afterStateReplacement can not be null");
        checkOpen();
        beforeInvalidationLock.run();
        InvalidatedKey<K, V> invalidated = bulkInvalidationGate.write(() -> {
            if (closed.get()) {
                return null;
            }
            exactInvalidationSequence = exactInvalidationSequence.add(BigInteger.ONE);
            if (!activeBulkStarts.isEmpty()) {
                exactInvalidations.put(key, exactInvalidationSequence);
            }
            KeyNode<K, V> node = keyNodes.get(key);
            KeyState invalidatedState = null;
            if (node != null) {
                invalidatedState = replaceKeyState(node);
            }
            return new InvalidatedKey<>(node, invalidatedState);
        });
        if (invalidated == null || invalidated.node == null) {
            return;
        }
        afterStateReplacement.run();
        Registration registered = invalidated.registration;
        deferRemovals(() -> {
            VersionedValue<K, V> current = data.getIfPresent(key);
            if (registered != null && current != null && current.registration == registered
                    && current.keyState == invalidated.keyState) {
                data.asMap().remove(key, current);
            }
            return null;
        });
        if (registered != null && invalidated.node.registration.compareAndSet(registered, null)) {
            releaseRegistration(registered);
        }
        tryPruneKey(key, invalidated.node);
    }

    public BulkLoadHandle beginBulkLoad(ScopePath parentScope) {
        Objects.requireNonNull(parentScope, "parentScope can not be null");
        checkOpen();
        ScopeLease scopeLease = registry.acquire(parentScope);
        BigInteger exactSequence = bulkInvalidationGate.write(() -> {
            if (closed.get()) {
                scopeLease.close();
                throw new IllegalStateException("Scoped meta cache '" + name + "' is closed");
            }
            BigInteger sequence = exactInvalidationSequence;
            activeBulkStarts.merge(sequence, 1, Integer::sum);
            return sequence;
        });
        return new BulkLoadHandle(
                this,
                parentScope,
                scopeLease,
                scopeLease.publicationState(),
                exactSequence);
    }

    public boolean publish(
            BulkLoadHandle handle, K key, ScopePath actualScope, V value) {
        Objects.requireNonNull(handle, "handle can not be null");
        Objects.requireNonNull(key, "key can not be null");
        Objects.requireNonNull(actualScope, "actualScope can not be null");
        Objects.requireNonNull(value, "value can not be null");
        checkOpen();
        handle.checkOwner(this);
        if (!handle.parentScope.contains(actualScope)) {
            throw new IllegalArgumentException(
                    "Actual scope " + actualScope + " is outside bulk-load parent " + handle.parentScope);
        }
        if (!effectiveEnabled) {
            return false;
        }
        try (PublicationLease<K, V> lease = acquirePublicationLease(key, actualScope, false)) {
            VersionedValue<K, V> staged = prepareVersionedValue(lease, key, value);
            if (staged == null) {
                return false;
            }
            boolean retained = false;
            try {
                afterBulkStage.run();
                retained = handle.tryCommit(key, lease, staged);
                return retained;
            } finally {
                if (!retained) {
                    releaseReservation(staged);
                }
            }
        }
    }

    boolean isBulkLoadCurrent(BulkLoadHandle handle, K key) {
        Objects.requireNonNull(handle, "handle can not be null");
        Objects.requireNonNull(key, "key can not be null");
        checkOpen();
        handle.checkOwner(this);
        return bulkInvalidationGate.readBoolean(() -> isBulkKeyCurrent(handle, key)
                && handle.scopeLease.commitIfPublicationCurrent(handle.scopePublicationState, () -> true));
    }

    public CacheMetrics metrics() {
        return bulkInvalidationGate.read(() -> new CacheMetrics(
                    data.estimatedSize(),
                    keyNodes.size(),
                    inFlightLoads.size(),
                    activeBulkStarts.values().stream().mapToInt(Integer::intValue).sum(),
                    exactInvalidations.size(),
                    effectiveEnabled,
                    requestCount.sum(),
                    hitCount.sum(),
                    missCount.sum(),
                    loadSuccessCount.sum(),
                    loadFailureCount.sum(),
                    totalLoadTimeNanos.sum(),
                    evictionCount.sum(),
                    invalidateCount.sum(),
                    lastLoadSuccessTimeMs.get(),
                    lastLoadFailureTimeMs.get(),
                    lastError.get(),
                    weightBounded,
                    entryBudget == null ? -1L : entryBudget.getEffectiveMaxWeight(),
                    entryBudget == null ? 0L : entryBudget.getUsedWeight(),
                    weightRejectCount.sum(),
                    lastWeightRejectReason.get()));
    }

    int refreshingCountForTest() {
        return refreshing.size();
    }

    public void forEach(BiConsumer<K, V> consumer) {
        Objects.requireNonNull(consumer, "consumer can not be null");
        deferRemovals(() -> {
            data.asMap().forEach((key, versioned) -> {
                if (versioned.isCurrent(registry, keyNodes)) {
                    consumer.accept(key, versioned.value);
                }
            });
            return null;
        });
    }

    private V loadAndRecord(K key, Function<K, V> loader) {
        long startNanos = System.nanoTime();
        try {
            V loaded = loader.apply(key);
            loadSuccessCount.increment();
            lastLoadSuccessTimeMs.set(System.currentTimeMillis());
            return loaded;
        } catch (RuntimeException | Error throwable) {
            loadFailureCount.increment();
            lastLoadFailureTimeMs.set(System.currentTimeMillis());
            lastError.set(throwable.toString());
            throw throwable;
        } finally {
            totalLoadTimeNanos.add(System.nanoTime() - startNanos);
        }
    }

    private void recordAccess(boolean hit) {
        requestCount.increment();
        if (hit) {
            hitCount.increment();
        } else {
            missCount.increment();
        }
    }

    public void cleanUp() {
        deferRemovals(() -> {
            data.cleanUp();
            return null;
        });
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        registry.removeCache(this);
        refreshing.clear();
        closePhysicalState();
    }

    void closeFromRegistry() {
        if (closed.compareAndSet(false, true)) {
            refreshing.clear();
            closePhysicalState();
        }
    }

    void removeExpectedRaw(Object rawKey, Object expectedValue) {
        @SuppressWarnings("unchecked")
        K key = (K) rawKey;
        Registration expectedRegistration = (Registration) expectedValue;
        deferRemovals(() -> {
            VersionedValue<K, V> current = data.getIfPresent(key);
            if (current != null && current.registration == expectedRegistration) {
                data.asMap().remove(key, current);
            }
            return null;
        });
    }

    private PublicationLease<K, V> acquirePublicationLease(
            K key, ScopePath path, boolean fenceAgainstDirectPublication) {
        while (true) {
            ScopeLease scopeLease = registry.acquire(path);
            KeyNode<K, V> keyNode = keyNodes.computeIfAbsent(key, ignored -> new KeyNode<>());
            keyNode.activeLoads.incrementAndGet();
            KeyState keyState = keyNode.current.get();
            Object loadPublicationState =
                    fenceAgainstDirectPublication ? keyNode.loadPublicationState.get() : null;
            if (keyNodes.get(key) == keyNode && scopeLease.isCurrent()) {
                return new PublicationLease<>(
                        this, key, scopeLease, keyNode, keyState, loadPublicationState);
            }
            releaseKey(key, keyNode);
            scopeLease.close();
        }
    }

    private boolean commitLoaded(PublicationLease<K, V> lease, K key, V value, Runnable beforePublication) {
        Runnable action = Objects.requireNonNull(beforePublication, "beforePublication can not be null");
        VersionedValue<K, V> prepared = prepareVersionedValue(lease, key, value);
        boolean retained = false;
        try {
            retained = guardedCommit(lease, () -> {
                action.run();
                if (prepared == null) {
                    return false;
                }
                install(prepared, lease);
                return true;
            });
            return retained;
        } finally {
            if (!retained && prepared != null) {
                releaseReservation(prepared);
            }
        }
    }

    private boolean guardedCommit(PublicationLease<K, V> lease, BooleanSupplier commitAction) {
        return deferRemovals(() -> bulkInvalidationGate.readBoolean(
                () -> lease.scopeLease.commitIfPublicationCurrent(
                        lease.scopePublicationState, () -> {
                            synchronized (lease.keyNode) {
                                return lease.isCurrent() && commitAction.getAsBoolean();
                            }
                        })));
    }

    private VersionedValue<K, V> prepareVersionedValue(
            PublicationLease<K, V> lease, K key, V value) {
        MetaCacheBudgetManager.AdmissionReservation reservation = reserve(key, value);
        if (weightBounded && reservation == null) {
            return null;
        }
        CacheAddress address = new CacheAddress(this, key);
        ScopeSnapshot scopeSnapshot = lease.scopeLease.snapshot();
        Registration registration = new Registration(reservation, address, scopeSnapshot);
        return new VersionedValue<>(
                key, value, address, scopeSnapshot, lease.keyNode, lease.keyState,
                ticker.read(), registration);
    }

    private MetaCacheBudgetManager.AdmissionReservation reserve(K key, V value) {
        if (!weightBounded) {
            return null;
        }
        long bytes = estimateWeight(key, value);
        return bytes < 0L ? null : reserveEstimated(bytes);
    }

    private long estimateWeight(K key, V value) {
        MetaCacheSizeEstimate estimate = MetaCacheSizeEstimator.estimateSafely(
                "estimator_failure", () -> sizeEstimator.estimate(key, value));
        if (!estimate.isComplete()) {
            rejectWeight("incomplete_estimate:" + estimate.getIncompleteReason());
            return -1L;
        }
        if (estimate.getBytes() == 0L) {
            rejectWeight("invalid_zero_estimate");
            return -1L;
        }
        long bytes = JvmSizeUtils.saturatedAdd(
                estimate.getBytes(), FIXED_ENTRY_ACCOUNTING_OVERHEAD_BYTES);
        if (bytes > entryBudget.getEffectiveMaxWeight()) {
            rejectWeight("entry_too_large");
            return -1L;
        }
        return bytes;
    }

    private MetaCacheBudgetManager.AdmissionReservation reserveEstimated(long bytes) {
        Optional<MetaCacheBudgetManager.AdmissionReservation> reservation = entryBudget.tryReserve(bytes);
        if (!reservation.isPresent()) {
            entryBudget.requestPeerReclaim(bytes);
            rejectWeight("budget_exceeded");
            return null;
        }
        return reservation.get();
    }

    private ReplacementValue<K, V> prepareReplacementVersionedValue(
            PublicationLease<K, V> lease, K key, V value, VersionedValue<K, V> previous) {
        if (!weightBounded) {
            VersionedValue<K, V> versioned = prepareVersionedValue(lease, key, value);
            return versioned == null ? null : new ReplacementValue<>(versioned, previous, null);
        }
        long bytes = estimateWeight(key, value);
        if (bytes < 0L) {
            return null;
        }
        Optional<MetaCacheBudgetManager.ReservationReplacement> replacement =
                entryBudget.tryReplace(previous.registration.reservation, bytes);
        if (!replacement.isPresent()) {
            long additionalBytes = Math.max(0L,
                    bytes - previous.registration.reservation.getBytes());
            entryBudget.requestPeerReclaim(additionalBytes);
            rejectWeight("budget_exceeded");
            return null;
        }
        MetaCacheBudgetManager.ReservationReplacement accounting = replacement.get();
        if (!previous.registration.released.compareAndSet(false, true)) {
            accounting.rollback();
            return null;
        }
        CacheAddress address = new CacheAddress(this, key);
        ScopeSnapshot scopeSnapshot = lease.scopeLease.snapshot();
        Registration registration = new Registration(accounting.current(), address, scopeSnapshot);
        VersionedValue<K, V> versioned = new VersionedValue<>(
                key, value, address, scopeSnapshot, lease.keyNode, lease.keyState,
                ticker.read(), registration);
        return new ReplacementValue<>(versioned, previous, accounting);
    }

    private boolean installReplacement(ReplacementValue<K, V> replacement) {
        VersionedValue<K, V> previous = replacement.previous;
        VersionedValue<K, V> current = replacement.current;
        registry.register(current.address, current.registration, current.scopeSnapshot);
        if (!current.keyNode.registration.compareAndSet(
                previous.registration, current.registration)) {
            registry.register(previous.address, previous.registration, previous.scopeSnapshot);
            replacement.rollback();
            return false;
        }
        if (!data.asMap().replace(current.key, previous, current)) {
            current.keyNode.registration.compareAndSet(current.registration, previous.registration);
            registry.register(previous.address, previous.registration, previous.scopeSnapshot);
            replacement.rollback();
            return false;
        }
        replacement.commit();
        return true;
    }

    private long evictLocalColdest() {
        return deferRemovals(this::evictLocalColdestWithDeferredRemovals);
    }

    private long evictLocalColdestWithDeferredRemovals() {
        if (!data.policy().eviction().isPresent()) {
            return 0L;
        }
        Map<K, VersionedValue<K, V>> coldest = data.policy().eviction().get().coldest(1);
        for (Map.Entry<K, VersionedValue<K, V>> candidate : coldest.entrySet()) {
            VersionedValue<K, V> current = data.getIfPresent(candidate.getKey());
            if (current == candidate.getValue()) {
                long weight = current.registration.reservation.getBytes();
                budgetEvictionRegistration.set(current.registration);
                try {
                    if (data.asMap().remove(candidate.getKey(), current)) {
                        return weight;
                    }
                } finally {
                    budgetEvictionRegistration.remove();
                }
            }
        }
        return 0L;
    }

    private long reclaimForPeer(long targetBytes) {
        if (targetBytes <= 0L || closed.get()) {
            return 0L;
        }
        long reclaimed = 0L;
        long removed;
        while (reclaimed < targetBytes && (removed = evictLocalColdest()) > 0L) {
            reclaimed = JvmSizeUtils.saturatedAdd(reclaimed, removed);
        }
        return reclaimed;
    }

    private void rejectWeight(String reason) {
        weightRejectCount.increment();
        lastWeightRejectReason.set(reason);
        long now = System.nanoTime();
        long last = lastWeightRejectWarnNanos.get();
        if ((last == Long.MIN_VALUE || now - last >= WEIGHT_REJECT_WARN_INTERVAL_NANOS)
                && lastWeightRejectWarnNanos.compareAndSet(last, now)) {
            LOG.warn("Metadata cache entry '{}' rejected a value by weight: reason={}, used={}, max={}",
                    name, reason, entryBudget == null ? 0L : entryBudget.getUsedWeight(),
                    entryBudget == null ? -1L : entryBudget.getEffectiveMaxWeight());
        }
    }

    private static void releaseReservation(VersionedValue<?, ?> versioned) {
        releaseRegistration(versioned.registration);
    }

    private static void releaseRegistration(Registration registration) {
        if (registration != null && registration.released.compareAndSet(false, true)
                && registration.reservation != null) {
            registration.reservation.release();
        }
    }

    private void scheduleRefresh(K key, ScopePath path, Function<K, V> loader, VersionedValue<K, V> current) {
        if (refreshAfterWriteNanos == 0L || ticker.read() - current.writeTimeNanos < refreshAfterWriteNanos
                || refreshing.putIfAbsent(key, current) != null) {
            return;
        }
        try {
            afterRefreshRegistration.run();
            checkOpen();
        } catch (RuntimeException | Error throwable) {
            refreshing.remove(key, current);
            throw throwable;
        }
        if (deferRemovals(() -> data.getIfPresent(key)) != current || !current.isCurrent(registry, keyNodes)) {
            refreshing.remove(key, current);
            return;
        }
        try {
            refreshExecutor.execute(() -> {
                try {
                    if (closed.get()) {
                        return;
                    }
                    try (PublicationLease<K, V> lease = acquirePublicationLease(key, path, true)) {
                        if (deferRemovals(() -> data.getIfPresent(key)) != current
                                || !current.isCurrent(registry, keyNodes)) {
                            return;
                        }
                        V refreshed = loadAndRecord(key, loader);
                        if (refreshed != null) {
                            boolean retained = false;
                            try {
                                retained = replaceRefreshExpected(lease, key, current, refreshed);
                            } finally {
                                if (!retained && refreshed != current.value) {
                                    notifyDiscarded(key, refreshed);
                                }
                            }
                        }
                    }
                } catch (RuntimeException | Error throwable) {
                    LOG.warn("Scoped metadata cache refresh failed", throwable);
                } finally {
                    refreshing.remove(key, current);
                }
            });
        } catch (RejectedExecutionException exception) {
            refreshing.remove(key, current);
        }
    }

    private void install(
            VersionedValue<K, V> versioned, PublicationLease<K, V> lease) {
        registry.register(versioned.address, versioned.registration, versioned.scopeSnapshot);
        lease.keyNode.registration.set(versioned.registration);
        data.asMap().put(versioned.key, versioned);
    }

    private boolean replaceRefreshExpected(PublicationLease<K, V> lease, K key,
            VersionedValue<K, V> expected, V refreshed) {
        if (refreshed == expected.value) {
            return guardedCommit(lease, () -> {
                if (data.getIfPresent(key) != expected || !expected.isCurrent(registry, keyNodes)) {
                    return false;
                }
                expected.writeTimeNanos = ticker.read();
                return true;
            });
        }
        AtomicReference<ReplacementValue<K, V>> replacementRef = new AtomicReference<>();
        boolean retained = false;
        try {
            retained = guardedCommit(lease, () -> {
                if (data.getIfPresent(key) != expected || !expected.isCurrent(registry, keyNodes)) {
                    return false;
                }
                ReplacementValue<K, V> replacement =
                        prepareReplacementVersionedValue(lease, key, refreshed, expected);
                replacementRef.set(replacement);
                if (replacement == null) {
                    lease.keyNode.loadPublicationState.set(new Object());
                    removeCurrentVersion(key, expected);
                    return false;
                }
                return installReplacement(replacement);
            });
            return retained;
        } finally {
            if (!retained && replacementRef.get() != null) {
                replacementRef.get().rollback();
            }
        }
    }

    private boolean tryCommitBulk(
            BulkLoadHandle handle,
            Object rawKey,
            PublicationLease<?, ?> rawLease,
            VersionedValue<?, ?> rawStaged) {
        @SuppressWarnings("unchecked")
        K key = (K) rawKey;
        @SuppressWarnings("unchecked")
        PublicationLease<K, V> lease = (PublicationLease<K, V>) rawLease;
        @SuppressWarnings("unchecked")
        VersionedValue<K, V> staged = (VersionedValue<K, V>) rawStaged;
        return deferRemovals(() -> bulkInvalidationGate.readBoolean(() -> {
            if (!isBulkKeyCurrent(handle, key)) {
                return false;
            }
            return handle.scopeLease.commitIfPublicationCurrent(
                    handle.scopePublicationState, () -> {
                        synchronized (lease.keyNode) {
                            if (!lease.isCurrent()) {
                                return false;
                            }
                            lease.keyNode.loadPublicationState.set(new Object());
                            install(staged, lease);
                            return true;
                        }
                    });
        }));
    }

    private void removeCurrentVersion(K key, VersionedValue<K, V> current) {
        data.asMap().remove(key, current);
        if (current.keyNode.registration.compareAndSet(current.registration, null)) {
            releaseRegistration(current.registration);
        }
    }

    // Caffeine's direct executor can call removal listeners while holding its maintenance lock, even on a
    // read or policy snapshot. Drain only after that operation returns, outside both maintenance and (for
    // publication callers) KeyNode locks; otherwise a full Caffeine write buffer can invert their lock order.
    private <T> T deferRemovals(Supplier<T> action) {
        RemovalDeferral<K, V> removalDeferral = removalDeferrals.get();
        removalDeferral.depth++;
        try {
            return action.get();
        } finally {
            removalDeferral.depth--;
            if (removalDeferral.depth == 0 && !removalDeferral.draining) {
                drainDeferredRemovals(removalDeferral);
            }
        }
    }

    private boolean isBulkKeyCurrent(BulkLoadHandle handle, Object rawKey) {
        BigInteger invalidation = exactInvalidations.get(rawKey);
        return !closed.get()
                && !handle.closed.get()
                && (invalidation == null || invalidation.compareTo(handle.exactInvalidationSequence) <= 0);
    }

    private void closeBulkHandle(BulkLoadHandle handle) {
        ScopeLease leaseToClose = bulkInvalidationGate.write(() -> {
            if (!handle.closed.compareAndSet(false, true)) {
                return null;
            }
            Integer count = activeBulkStarts.get(handle.exactInvalidationSequence);
            if (count == null) {
                throw new IllegalStateException("Bulk-load handle start sequence is not registered");
            }
            if (count == 1) {
                activeBulkStarts.remove(handle.exactInvalidationSequence);
            } else {
                activeBulkStarts.put(handle.exactInvalidationSequence, count - 1);
            }
            pruneExactInvalidations();
            return handle.scopeLease;
        });
        if (leaseToClose != null) {
            leaseToClose.close();
        }
    }

    private void pruneExactInvalidations() {
        if (activeBulkStarts.isEmpty()) {
            exactInvalidations.clear();
            return;
        }
        BigInteger oldestStart = activeBulkStarts.firstKey();
        exactInvalidations.entrySet().removeIf(entry -> entry.getValue().compareTo(oldestStart) <= 0);
    }

    private V awaitLoad(CompletableFuture<V> load) {
        try {
            return load.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalStateException("Unexpected checked exception from metadata loader", cause);
        }
    }

    private void closePhysicalState() {
        bulkInvalidationGate.write(exactInvalidations::clear);
        deferRemovals(() -> {
            keyNodes.forEach((key, node) -> {
                replaceKeyState(node);
                Registration registered = node.registration.get();
                VersionedValue<K, V> versioned = data.getIfPresent(key);
                if (versioned != null && versioned.registration == registered) {
                    data.asMap().remove(key, versioned);
                }
                if (registered != null && node.registration.compareAndSet(registered, null)) {
                    releaseRegistration(registered);
                }
                tryPruneKey(key, node);
            });
            data.invalidateAll();
            data.cleanUp();
            return null;
        });
        if (entryBudget != null) {
            entryBudget.close();
        }
    }

    private void onRemoval(
            Object rawKey, Object rawValue, RemovalCause cause) {
        @SuppressWarnings("unchecked")
        VersionedValue<K, V> versioned = (VersionedValue<K, V>) rawValue;
        RemovalDeferral<K, V> removalDeferral = removalDeferrals.get();
        removalDeferral.removals.addLast(new DeferredRemoval<>(
                versioned, cause, budgetEvictionRegistration.get() == versioned.registration));
        if (removalDeferral.depth > 0 || removalDeferral.draining) {
            return;
        }
        drainDeferredRemovals(removalDeferral);
    }

    private void drainDeferredRemovals(RemovalDeferral<K, V> removalDeferral) {
        removalDeferral.draining = true;
        Throwable failure = null;
        try {
            DeferredRemoval<K, V> removal;
            while ((removal = removalDeferral.removals.pollFirst()) != null) {
                try {
                    completeRemoval(removal.versioned, removal.cause, removal.budgetEviction);
                } catch (RuntimeException | Error e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
        } finally {
            removalDeferral.draining = false;
            removalDeferral.removals.clear();
            removalDeferrals.remove();
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure != null) {
            throw (Error) failure;
        }
    }

    private void completeRemoval(
            VersionedValue<K, V> versioned, RemovalCause cause, boolean budgetEviction) {
        if (cause.wasEvicted() || budgetEviction) {
            evictionCount.increment();
        } else if (cause == RemovalCause.EXPLICIT) {
            invalidateCount.increment();
        }
        K key = versioned.key;
        if (beforeRemoval != null) {
            try {
                beforeRemoval.onRemoval(key, versioned.value, cause);
            } catch (Throwable t) {
                LOG.warn("Scoped metadata cache removal callback failed", t);
            }
        }
        synchronized (versioned.keyNode) {
            registry.unregister(versioned.address, versioned.registration, versioned.scopeSnapshot);
            versioned.keyNode.registration.compareAndSet(versioned.registration, null);
            releaseRegistration(versioned.registration);
            tryPruneKey(key, versioned.keyNode);
        }
    }

    private void notifyDiscarded(K key, V value) {
        if (discardListener == null) {
            return;
        }
        try {
            discardListener.accept(key, value);
        } catch (Throwable t) {
            LOG.warn("Scoped metadata cache discard callback failed", t);
        }
    }

    private static final class RemovalDeferral<K, V> {
        private final Deque<DeferredRemoval<K, V>> removals = new ArrayDeque<>();
        private int depth;
        private boolean draining;
    }

    private static final class DeferredRemoval<K, V> {
        private final VersionedValue<K, V> versioned;
        private final RemovalCause cause;
        private final boolean budgetEviction;

        private DeferredRemoval(
                VersionedValue<K, V> versioned, RemovalCause cause, boolean budgetEviction) {
            this.versioned = versioned;
            this.cause = cause;
            this.budgetEviction = budgetEviction;
        }
    }

    private static final class InvalidatedKey<K, V> {
        private final KeyNode<K, V> node;
        private final KeyState keyState;
        private final Registration registration;

        private InvalidatedKey(KeyNode<K, V> node, KeyState keyState) {
            this.node = node;
            this.keyState = keyState;
            // Captured under the publication fence: cleanup must never detach a newer generation.
            this.registration = node == null ? null : node.registration.get();
        }
    }

    private KeyState replaceKeyState(KeyNode<K, V> node) {
        while (true) {
            KeyState oldState = node.current.get();
            if (node.current.compareAndSet(oldState, new KeyState())) {
                return oldState;
            }
        }
    }

    private void releaseKey(K key, KeyNode<K, V> node) {
        int remaining = node.activeLoads.decrementAndGet();
        if (remaining < 0) {
            throw new IllegalStateException("Cache key active-load count became negative");
        }
        tryPruneKey(key, node);
    }

    private void tryPruneKey(K key, KeyNode<K, V> node) {
        if (node.activeLoads.get() == 0 && node.registration.get() == null) {
            keyNodes.remove(key, node);
        }
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Scoped meta cache '" + name + "' is closed");
        }
        registry.checkOpen();
    }

    public static final class BulkLoadHandle implements AutoCloseable {
        private final ScopedMetaCache<?, ?> owner;
        private final ScopePath parentScope;
        private final ScopeLease scopeLease;
        private final PublicationState scopePublicationState;
        private final BigInteger exactInvalidationSequence;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private BulkLoadHandle(
                ScopedMetaCache<?, ?> owner,
                ScopePath parentScope,
                ScopeLease scopeLease,
                PublicationState scopePublicationState,
                BigInteger exactInvalidationSequence) {
            this.owner = owner;
            this.parentScope = parentScope;
            this.scopeLease = scopeLease;
            this.scopePublicationState = scopePublicationState;
            this.exactInvalidationSequence = exactInvalidationSequence;
        }

        private void checkOwner(ScopedMetaCache<?, ?> expectedOwner) {
            if (owner != expectedOwner) {
                throw new IllegalArgumentException("Bulk-load handle belongs to another cache");
            }
            if (closed.get()) {
                throw new IllegalStateException("Bulk-load handle is closed");
            }
        }

        private boolean tryCommit(
                Object key, PublicationLease<?, ?> lease, VersionedValue<?, ?> staged) {
            return owner.tryCommitBulk(this, key, lease, staged);
        }

        @Override
        public void close() {
            owner.closeBulkHandle(this);
        }
    }

    public static final class CacheMetrics {
        private final long physicalEntryCount;
        private final int keyNodeCount;
        private final int inFlightLoadCount;
        private final int activeBulkHandleCount;
        private final int exactInvalidationTombstoneCount;
        private final boolean effectiveEnabled;
        private final long requestCount;
        private final long hitCount;
        private final long missCount;
        private final long loadSuccessCount;
        private final long loadFailureCount;
        private final long totalLoadTimeNanos;
        private final long evictionCount;
        private final long invalidateCount;
        private final long lastLoadSuccessTimeMs;
        private final long lastLoadFailureTimeMs;
        private final String lastError;
        private final boolean weightBounded;
        private final long maxWeight;
        private final long estimatedWeight;
        private final long weightRejectCount;
        private final String lastWeightRejectReason;

        private CacheMetrics(
                long physicalEntryCount,
                int keyNodeCount,
                int inFlightLoadCount,
                int activeBulkHandleCount,
                int exactInvalidationTombstoneCount,
                boolean effectiveEnabled,
                long requestCount,
                long hitCount,
                long missCount,
                long loadSuccessCount,
                long loadFailureCount,
                long totalLoadTimeNanos,
                long evictionCount,
                long invalidateCount,
                long lastLoadSuccessTimeMs,
                long lastLoadFailureTimeMs,
                String lastError,
                boolean weightBounded,
                long maxWeight,
                long estimatedWeight,
                long weightRejectCount,
                String lastWeightRejectReason) {
            this.physicalEntryCount = physicalEntryCount;
            this.keyNodeCount = keyNodeCount;
            this.inFlightLoadCount = inFlightLoadCount;
            this.activeBulkHandleCount = activeBulkHandleCount;
            this.exactInvalidationTombstoneCount = exactInvalidationTombstoneCount;
            this.effectiveEnabled = effectiveEnabled;
            this.requestCount = requestCount;
            this.hitCount = hitCount;
            this.missCount = missCount;
            this.loadSuccessCount = loadSuccessCount;
            this.loadFailureCount = loadFailureCount;
            this.totalLoadTimeNanos = totalLoadTimeNanos;
            this.evictionCount = evictionCount;
            this.invalidateCount = invalidateCount;
            this.lastLoadSuccessTimeMs = lastLoadSuccessTimeMs;
            this.lastLoadFailureTimeMs = lastLoadFailureTimeMs;
            this.lastError = lastError;
            this.weightBounded = weightBounded;
            this.maxWeight = maxWeight;
            this.estimatedWeight = estimatedWeight;
            this.weightRejectCount = weightRejectCount;
            this.lastWeightRejectReason = lastWeightRejectReason;
        }

        public long getPhysicalEntryCount() {
            return physicalEntryCount;
        }

        public int getKeyNodeCount() {
            return keyNodeCount;
        }

        public int getInFlightLoadCount() {
            return inFlightLoadCount;
        }

        public int getActiveBulkHandleCount() {
            return activeBulkHandleCount;
        }

        public int getExactInvalidationTombstoneCount() {
            return exactInvalidationTombstoneCount;
        }

        public boolean isEffectiveEnabled() {
            return effectiveEnabled;
        }

        public long getLoadSuccessCount() {
            return loadSuccessCount;
        }

        public long getRequestCount() {
            return requestCount;
        }

        public long getHitCount() {
            return hitCount;
        }

        public long getMissCount() {
            return missCount;
        }

        public long getLoadFailureCount() {
            return loadFailureCount;
        }

        public long getTotalLoadTimeNanos() {
            return totalLoadTimeNanos;
        }

        public long getEvictionCount() {
            return evictionCount;
        }

        public long getInvalidateCount() {
            return invalidateCount;
        }

        public long getLastLoadSuccessTimeMs() {
            return lastLoadSuccessTimeMs;
        }

        public long getLastLoadFailureTimeMs() {
            return lastLoadFailureTimeMs;
        }

        public String getLastError() {
            return lastError;
        }

        public boolean isWeightBounded() {
            return weightBounded;
        }

        public long getMaxWeight() {
            return maxWeight;
        }

        public long getEstimatedWeight() {
            return estimatedWeight;
        }

        public long getWeightRejectCount() {
            return weightRejectCount;
        }

        public String getLastWeightRejectReason() {
            return lastWeightRejectReason;
        }
    }

    private static final class PublicationLease<K, V> implements AutoCloseable {
        private final ScopedMetaCache<K, V> owner;
        private final K key;
        private final ScopeLease scopeLease;
        private final KeyNode<K, V> keyNode;
        private final KeyState keyState;
        private final Object loadPublicationState;
        private final PublicationState scopePublicationState;
        private final AtomicBoolean released = new AtomicBoolean(false);

        private PublicationLease(
                ScopedMetaCache<K, V> owner,
                K key,
                ScopeLease scopeLease,
                KeyNode<K, V> keyNode,
                KeyState keyState,
                Object loadPublicationState) {
            this.owner = owner;
            this.key = key;
            this.scopeLease = scopeLease;
            this.keyNode = keyNode;
            this.keyState = keyState;
            this.loadPublicationState = loadPublicationState;
            this.scopePublicationState = scopeLease.publicationState();
        }

        private boolean isCurrent() {
            return !owner.closed.get()
                    && scopeLease.isCurrent()
                    && owner.keyNodes.get(key) == keyNode
                    && keyNode.current.get() == keyState
                    && (loadPublicationState == null
                            || keyNode.loadPublicationState.get() == loadPublicationState);
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                owner.releaseKey(key, keyNode);
                scopeLease.close();
            }
        }
    }

    private static final class VersionedValue<K, V> {
        private final K key;
        private final V value;
        private final CacheAddress address;
        private final ScopeSnapshot scopeSnapshot;
        private final KeyNode<K, V> keyNode;
        private final KeyState keyState;
        private volatile long writeTimeNanos;
        private final Registration registration;

        private VersionedValue(
                K key,
                V value,
                CacheAddress address,
                ScopeSnapshot scopeSnapshot,
                KeyNode<K, V> keyNode,
                KeyState keyState,
                long writeTimeNanos,
                Registration registration) {
            this.key = key;
            this.value = value;
            this.address = address;
            this.scopeSnapshot = scopeSnapshot;
            this.keyNode = keyNode;
            this.keyState = keyState;
            this.writeTimeNanos = writeTimeNanos;
            this.registration = registration;
        }

        private boolean isCurrent(
                ScopedMetaCacheRegistry registry,
                Map<K, KeyNode<K, V>> currentKeyNodes) {
            return scopeSnapshot.isCurrent(registry)
                    && currentKeyNodes.get(key) == keyNode
                    && keyNode.current.get() == keyState
                    && keyNode.registration.get() == registration;
        }
    }

    private static final class ReplacementValue<K, V> {
        private final VersionedValue<K, V> current;
        private final VersionedValue<K, V> previous;
        private final MetaCacheBudgetManager.ReservationReplacement accounting;
        private boolean finished;

        private ReplacementValue(VersionedValue<K, V> current,
                VersionedValue<K, V> previous,
                MetaCacheBudgetManager.ReservationReplacement accounting) {
            this.current = current;
            this.previous = previous;
            this.accounting = accounting;
        }

        private void commit() {
            if (accounting != null) {
                accounting.commit();
            }
            finished = true;
        }

        private void rollback() {
            if (finished) {
                return;
            }
            if (accounting != null) {
                current.registration.released.set(true);
                previous.registration.released.set(false);
                accounting.rollback();
            }
            finished = true;
        }
    }

    /** Lightweight ownership token: deliberately does not retain the cached value. */
    private static final class Registration {
        private final MetaCacheBudgetManager.AdmissionReservation reservation;
        private final CacheAddress address;
        private final ScopeSnapshot scopeSnapshot;
        private final AtomicBoolean released = new AtomicBoolean(false);

        private Registration(MetaCacheBudgetManager.AdmissionReservation reservation,
                CacheAddress address, ScopeSnapshot scopeSnapshot) {
            this.reservation = reservation;
            this.address = address;
            this.scopeSnapshot = scopeSnapshot;
        }
    }

    private static final class KeyNode<K, V> {
        private final AtomicReference<KeyState> current = new AtomicReference<>(new KeyState());
        private final AtomicReference<Object> loadPublicationState = new AtomicReference<>(new Object());
        private final AtomicReference<Registration> registration = new AtomicReference<>();
        private final AtomicInteger activeLoads = new AtomicInteger();
    }

    private static final class KeyState {
    }

    private static final class LoadAddress<K> {
        private final K key;
        private final ScopePath path;
        private final ScopeSnapshot scopeSnapshot;
        private final KeyNode<?, ?> keyNode;
        private final KeyState keyState;
        private final Object loadPublicationState;
        private final int hashCode;

        private LoadAddress(K key, ScopePath path, PublicationLease<K, ?> lease) {
            this.key = key;
            this.path = path;
            this.scopeSnapshot = lease.scopeLease.snapshot();
            this.keyNode = lease.keyNode;
            this.keyState = lease.keyState;
            this.loadPublicationState = lease.loadPublicationState;
            this.hashCode = 31 * key.hashCode() + scopeSnapshot.pathHashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof LoadAddress)) {
                return false;
            }
            LoadAddress<?> other = (LoadAddress<?>) obj;
            return key.equals(other.key)
                    && path.equals(other.path)
                    && scopeSnapshot.sameGeneration(other.scopeSnapshot)
                    && keyNode == other.keyNode
                    && keyState == other.keyState
                    && loadPublicationState == other.loadPublicationState;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
