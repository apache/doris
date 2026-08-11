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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

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
    private final RemovalListener<K, V> beforeRemoval;
    private final Ticker ticker;
    private final long refreshAfterWriteNanos;
    private final Executor refreshExecutor;
    private final Runnable afterLoadElection;
    private final Runnable afterBulkStage;
    private final Runnable afterRefreshRegistration;
    private final ThreadLocal<RemovalDeferral<K, V>> removalDeferrals =
            ThreadLocal.withInitial(RemovalDeferral::new);
    private BigInteger exactInvalidationSequence = BigInteger.ZERO;

    ScopedMetaCache(
            ScopedMetaCacheRegistry registry,
            String name,
            CacheSpec cacheSpec,
            Ticker ticker,
            RemovalListener<K, V> beforeRemoval,
            Duration refreshAfterWrite,
            Executor refreshExecutor,
            Runnable afterLoadElection,
            Runnable afterBulkStage,
            Runnable afterRefreshRegistration) {
        this.registry = Objects.requireNonNull(registry, "registry can not be null");
        this.name = Objects.requireNonNull(name, "name can not be null");
        Objects.requireNonNull(cacheSpec, "cacheSpec can not be null");
        this.beforeRemoval = beforeRemoval;
        this.ticker = ticker == null ? Ticker.systemTicker() : ticker;
        this.refreshAfterWriteNanos = refreshAfterWrite == null ? 0L : refreshAfterWrite.toNanos();
        this.refreshExecutor = refreshExecutor;
        this.afterLoadElection =
                Objects.requireNonNull(afterLoadElection, "afterLoadElection can not be null");
        this.afterBulkStage = Objects.requireNonNull(afterBulkStage, "afterBulkStage can not be null");
        this.afterRefreshRegistration = Objects.requireNonNull(
                afterRefreshRegistration, "afterRefreshRegistration can not be null");
        this.effectiveEnabled = CacheSpec.isCacheEnabled(
                cacheSpec.isEnable(), cacheSpec.getTtlSecond(), cacheSpec.getCapacity());

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
                coordinator.accept(loaded, beforePublication -> {
                    if (!commitInvoked.compareAndSet(false, true)) {
                        throw new IllegalStateException("Metadata cache publication callback was invoked twice");
                    }
                    Objects.requireNonNull(beforePublication, "beforePublication can not be null").run();
                });
                if (!commitInvoked.get()) {
                    throw new IllegalStateException("Metadata cache publication callback was not invoked");
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
                synchronized (lease.keyNode) {
                    VersionedValue<K, V> present = currentVersionedValue(key, path);
                    if (present != null) {
                        ownLoad.complete(present.value);
                        return present.value;
                    }
                }
                V loaded = loadAndRecord(key, loadFunction);
                if (loaded != null) {
                    AtomicBoolean commitInvoked = new AtomicBoolean(false);
                    coordinator.accept(loaded, beforePublication -> {
                        if (!commitInvoked.compareAndSet(false, true)) {
                            throw new IllegalStateException("Metadata cache publication callback was invoked twice");
                        }
                        commitLoaded(lease, key, loaded, beforePublication);
                    });
                    if (!commitInvoked.get()) {
                        throw new IllegalStateException("Metadata cache publication callback was not invoked");
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
        VersionedValue<K, V> versioned = data.getIfPresent(key);
        if (versioned == null) {
            return null;
        }
        if (!versioned.scopeSnapshot.path().equals(path)) {
            return null;
        }
        if (!versioned.isCurrent(registry, keyNodes)) {
            data.asMap().remove(key, versioned);
            return null;
        }
        return versioned;
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
            synchronized (lease.keyNode) {
                lease.keyNode.loadPublicationState.set(new Object());
                publishCommitted(lease, key, value);
            }
        }
    }

    public boolean compareAndSet(K key, ScopePath path, V expectedValue, V updatedValue) {
        Objects.requireNonNull(key, "key can not be null");
        Objects.requireNonNull(path, "path can not be null");
        checkOpen();
        if (!effectiveEnabled) {
            return true;
        }
        try (PublicationLease<K, V> lease = acquirePublicationLease(key, path, false)) {
            return guardedCommit(lease, () -> {
                VersionedValue<K, V> current = currentVersionedValue(key, path);
                V currentValue = current == null ? null : current.value;
                if (currentValue != expectedValue) {
                    return false;
                }
                lease.keyNode.loadPublicationState.set(new Object());
                if (updatedValue == currentValue) {
                    return true;
                }
                if (updatedValue == null) {
                    if (current != null) {
                        data.asMap().remove(key, current);
                        lease.keyNode.registration.compareAndSet(current, null);
                    }
                } else {
                    publishCommitted(lease, key, updatedValue);
                }
                return true;
            });
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
        VersionedValue<K, V> registered = invalidated.node.registration.get();
        if (registered != null && registered.keyState == invalidated.keyState) {
            data.asMap().remove(key, registered);
            invalidated.node.registration.compareAndSet(registered, null);
        }
        tryPruneKey(key, invalidated.node);
    }

    public BulkLoadHandle beginBulkLoad(ScopePath parentScope) {
        Objects.requireNonNull(parentScope, "parentScope can not be null");
        checkOpen();
        if (!effectiveEnabled) {
            return BulkLoadHandle.disabled(this, parentScope);
        }
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
            synchronized (lease.keyNode) {
                VersionedValue<K, V> staged = newVersionedValue(lease, key, value);
                afterBulkStage.run();
                if (handle.tryCommit(key, lease, staged)) {
                    return true;
                }
                return false;
            }
        }
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
                    lastError.get()));
    }

    int refreshingCountForTest() {
        return refreshing.size();
    }

    public void forEach(BiConsumer<K, V> consumer) {
        Objects.requireNonNull(consumer, "consumer can not be null");
        data.asMap().forEach((key, versioned) -> {
            if (versioned.isCurrent(registry, keyNodes)) {
                consumer.accept(key, versioned.value);
            }
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
        data.cleanUp();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        registry.removeCache(this);
        closePhysicalState();
    }

    void closeFromRegistry() {
        if (closed.compareAndSet(false, true)) {
            closePhysicalState();
        }
    }

    void removeExpectedRaw(Object rawKey, Object expectedValue) {
        @SuppressWarnings("unchecked")
        K key = (K) rawKey;
        @SuppressWarnings("unchecked")
        VersionedValue<K, V> versionedValue = (VersionedValue<K, V>) expectedValue;
        data.asMap().remove(key, versionedValue);
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

    private VersionedValue<K, V> publish(PublicationLease<K, V> lease, K key, V value) {
        if (!lease.isCurrent()) {
            return null;
        }
        VersionedValue<K, V> versioned = newVersionedValue(lease, key, value);
        install(versioned, lease);
        if (!lease.isCurrent()) {
            data.asMap().remove(key, versioned);
            return null;
        }
        return versioned;
    }

    private VersionedValue<K, V> publishCommitted(PublicationLease<K, V> lease, K key, V value) {
        return publish(lease, key, value);
    }

    private boolean commitLoaded(PublicationLease<K, V> lease, K key, V value, Runnable beforePublication) {
        Runnable action = Objects.requireNonNull(beforePublication, "beforePublication can not be null");
        return guardedCommit(lease, () -> {
            action.run();
            return publishCommitted(lease, key, value) != null;
        });
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

    private VersionedValue<K, V> newVersionedValue(
            PublicationLease<K, V> lease, K key, V value) {
        CacheAddress address = new CacheAddress(this, key);
        return new VersionedValue<>(
                key, value, address, lease.scopeLease.snapshot(), lease.keyNode, lease.keyState, ticker.read());
    }

    private void scheduleRefresh(K key, ScopePath path, Function<K, V> loader, VersionedValue<K, V> current) {
        if (refreshAfterWriteNanos == 0L || ticker.read() - current.writeTimeNanos < refreshAfterWriteNanos
                || refreshing.putIfAbsent(key, current) != null) {
            return;
        }
        PublicationLease<K, V> lease;
        try {
            afterRefreshRegistration.run();
            lease = acquirePublicationLease(key, path, true);
        } catch (RuntimeException | Error throwable) {
            refreshing.remove(key, current);
            throw throwable;
        }
        if (data.getIfPresent(key) != current || !current.isCurrent(registry, keyNodes)) {
            lease.close();
            refreshing.remove(key, current);
            return;
        }
        try {
            refreshExecutor.execute(() -> {
                try (PublicationLease<K, V> ignored = lease) {
                    if (closed.get()) {
                        return;
                    }
                    V refreshed = loadAndRecord(key, loader);
                    if (refreshed != null) {
                        replaceRefreshExpected(lease, key, current, refreshed);
                    }
                } catch (RuntimeException | Error throwable) {
                    LOG.warn("Scoped metadata cache refresh failed", throwable);
                } finally {
                    refreshing.remove(key, current);
                }
            });
        } catch (RejectedExecutionException exception) {
            lease.close();
            refreshing.remove(key, current);
        }
    }

    private void install(
            VersionedValue<K, V> versioned, PublicationLease<K, V> lease) {
        registry.register(versioned.address, versioned, versioned.scopeSnapshot);
        lease.keyNode.registration.set(versioned);
        data.asMap().put(versioned.key, versioned);
    }

    private boolean replaceRefreshExpected(PublicationLease<K, V> lease, K key,
            VersionedValue<K, V> expected, V refreshed) {
        return guardedCommit(lease, () -> {
            if (data.getIfPresent(key) != expected || !expected.isCurrent(registry, keyNodes)) {
                return false;
            }
            VersionedValue<K, V> replacement = newVersionedValue(lease, key, refreshed);
            registry.register(replacement.address, replacement, replacement.scopeSnapshot);
            if (!lease.keyNode.registration.compareAndSet(expected, replacement)) {
                registry.unregister(replacement.address, replacement, replacement.scopeSnapshot);
                return false;
            }
            if (!data.asMap().replace(key, expected, replacement)) {
                lease.keyNode.registration.compareAndSet(replacement, null);
                registry.unregister(replacement.address, replacement, replacement.scopeSnapshot);
                return false;
            }
            return true;
        });
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
                        if (!lease.isCurrent()) {
                            return false;
                        }
                        lease.keyNode.loadPublicationState.set(new Object());
                        install(staged, lease);
                        return true;
                    });
        }));
    }

    private boolean deferRemovals(BooleanSupplier action) {
        RemovalDeferral<K, V> removalDeferral = removalDeferrals.get();
        removalDeferral.depth++;
        try {
            return action.getAsBoolean();
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
            if (handle.scopeLease != null) {
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
            }
            return null;
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
        keyNodes.forEach((key, node) -> {
            replaceKeyState(node);
            VersionedValue<K, V> versioned = node.registration.get();
            if (versioned != null) {
                data.asMap().remove(key, versioned);
                node.registration.compareAndSet(versioned, null);
            }
            tryPruneKey(key, node);
        });
        data.invalidateAll();
        data.cleanUp();
    }

    private void onRemoval(
            Object rawKey, Object rawValue, RemovalCause cause) {
        Objects.requireNonNull(rawKey, "removed cache key can not be null");
        Objects.requireNonNull(rawValue, "removed cache value can not be null");
        @SuppressWarnings("unchecked")
        VersionedValue<K, V> versioned = (VersionedValue<K, V>) rawValue;
        RemovalDeferral<K, V> removalDeferral = removalDeferrals.get();
        removalDeferral.removals.addLast(new DeferredRemoval<>(versioned, cause));
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
                    completeRemoval(removal.versioned, removal.cause);
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

    private void completeRemoval(VersionedValue<K, V> versioned, RemovalCause cause) {
        if (cause.wasEvicted()) {
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
        registry.unregister(versioned.address, versioned, versioned.scopeSnapshot);
        versioned.keyNode.registration.compareAndSet(versioned, null);
        tryPruneKey(key, versioned.keyNode);
    }

    private static final class RemovalDeferral<K, V> {
        private final Deque<DeferredRemoval<K, V>> removals = new ArrayDeque<>();
        private int depth;
        private boolean draining;
    }

    private static final class DeferredRemoval<K, V> {
        private final VersionedValue<K, V> versioned;
        private final RemovalCause cause;

        private DeferredRemoval(VersionedValue<K, V> versioned, RemovalCause cause) {
            this.versioned = versioned;
            this.cause = cause;
        }
    }

    private static final class InvalidatedKey<K, V> {
        private final KeyNode<K, V> node;
        private final KeyState keyState;

        private InvalidatedKey(KeyNode<K, V> node, KeyState keyState) {
            this.node = node;
            this.keyState = keyState;
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

        private static BulkLoadHandle disabled(
                ScopedMetaCache<?, ?> owner, ScopePath parentScope) {
            return new BulkLoadHandle(owner, parentScope, null, null, BigInteger.ZERO);
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
                String lastError) {
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
        private final long writeTimeNanos;

        private VersionedValue(
                K key,
                V value,
                CacheAddress address,
                ScopeSnapshot scopeSnapshot,
                KeyNode<K, V> keyNode,
                KeyState keyState,
                long writeTimeNanos) {
            this.key = key;
            this.value = value;
            this.address = address;
            this.scopeSnapshot = scopeSnapshot;
            this.keyNode = keyNode;
            this.keyState = keyState;
            this.writeTimeNanos = writeTimeNanos;
        }

        private boolean isCurrent(
                ScopedMetaCacheRegistry registry,
                Map<K, KeyNode<K, V>> currentKeyNodes) {
            return scopeSnapshot.isCurrent(registry)
                    && currentKeyNodes.get(key) == keyNode
                    && keyNode.current.get() == keyState
                    && keyNode.registration.get() == this;
        }
    }

    private static final class KeyNode<K, V> {
        private final AtomicReference<KeyState> current = new AtomicReference<>(new KeyState());
        private final AtomicReference<Object> loadPublicationState = new AtomicReference<>(new Object());
        private final AtomicReference<VersionedValue<K, V>> registration = new AtomicReference<>();
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
