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
import com.github.benmanes.caffeine.cache.Ticker;

import java.math.BigInteger;
import java.time.Duration;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
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
    private static final Runnable NO_OP = () -> {
    };

    private final ScopedMetaCacheRegistry registry;
    private final String name;
    private final boolean effectiveEnabled;
    private final Cache<K, VersionedValue<K, V>> data;
    private final ConcurrentMap<K, KeyNode<K, V>> keyNodes = new ConcurrentHashMap<>();
    private final ConcurrentMap<LoadAddress<K>, CompletableFuture<V>> inFlightLoads = new ConcurrentHashMap<>();
    private final Object bulkInvalidationLock = new Object();
    private final Map<K, BigInteger> exactInvalidations = new HashMap<>();
    private final NavigableMap<BigInteger, Integer> activeBulkStarts = new TreeMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final BiConsumer<K, V> beforeRemoval;
    private final Runnable afterLoadElection;
    private final Runnable afterBulkStage;
    private BigInteger exactInvalidationSequence = BigInteger.ZERO;

    ScopedMetaCache(
            ScopedMetaCacheRegistry registry,
            String name,
            CacheSpec cacheSpec,
            Ticker ticker,
            BiConsumer<K, V> beforeRemoval,
            Runnable afterLoadElection,
            Runnable afterBulkStage) {
        this.registry = Objects.requireNonNull(registry, "registry can not be null");
        this.name = Objects.requireNonNull(name, "name can not be null");
        Objects.requireNonNull(cacheSpec, "cacheSpec can not be null");
        this.beforeRemoval = beforeRemoval;
        this.afterLoadElection =
                Objects.requireNonNull(afterLoadElection, "afterLoadElection can not be null");
        this.afterBulkStage = Objects.requireNonNull(afterBulkStage, "afterBulkStage can not be null");
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
            builder.ticker(ticker);
        }
        this.data = builder.build();
    }

    public String name() {
        return name;
    }

    public V get(K key, ScopePath path, Function<K, V> loader) {
        Objects.requireNonNull(key, "key can not be null");
        Objects.requireNonNull(path, "path can not be null");
        Function<K, V> loadFunction = Objects.requireNonNull(loader, "loader can not be null");
        checkOpen();
        if (!effectiveEnabled) {
            return loadFunction.apply(key);
        }

        V present = getIfPresent(key, path);
        if (present != null) {
            return present;
        }
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
                    present = getIfPresent(key, path);
                    if (present != null) {
                        ownLoad.complete(present);
                        return present;
                    }
                }
                V loaded = loadFunction.apply(key);
                if (loaded != null) {
                    synchronized (lease.keyNode) {
                        publishCommitted(lease, key, loaded);
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
            return null;
        }
        VersionedValue<K, V> versioned = data.getIfPresent(key);
        if (versioned == null) {
            return null;
        }
        if (!versioned.scopeSnapshot.path().equals(path)) {
            return null;
        }
        if (!versioned.committed.get()) {
            return null;
        }
        if (!versioned.isCurrent(registry, keyNodes)) {
            data.asMap().remove(key, versioned);
            return null;
        }
        return versioned.value;
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
        KeyNode<K, V> node;
        KeyState invalidatedState = null;
        synchronized (bulkInvalidationLock) {
            if (closed.get()) {
                return;
            }
            exactInvalidationSequence = exactInvalidationSequence.add(BigInteger.ONE);
            if (!activeBulkStarts.isEmpty()) {
                exactInvalidations.put(key, exactInvalidationSequence);
            }
            node = keyNodes.get(key);
            if (node != null) {
                invalidatedState = replaceKeyState(node);
            }
        }
        if (node == null) {
            return;
        }
        afterStateReplacement.run();
        VersionedValue<K, V> registered = node.registration.get();
        if (registered != null && registered.keyState == invalidatedState) {
            data.asMap().remove(key, registered);
            node.registration.compareAndSet(registered, null);
        }
        tryPruneKey(key, node);
    }

    public BulkLoadHandle beginBulkLoad(ScopePath parentScope) {
        Objects.requireNonNull(parentScope, "parentScope can not be null");
        checkOpen();
        if (!effectiveEnabled) {
            return BulkLoadHandle.disabled(this, parentScope);
        }
        ScopeLease scopeLease = registry.acquire(parentScope);
        BigInteger exactSequence;
        synchronized (bulkInvalidationLock) {
            if (closed.get()) {
                scopeLease.close();
                throw new IllegalStateException("Scoped meta cache '" + name + "' is closed");
            }
            exactSequence = exactInvalidationSequence;
            activeBulkStarts.merge(exactSequence, 1, Integer::sum);
        }
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
                if (!handle.canStage(key)) {
                    return false;
                }
                lease.keyNode.loadPublicationState.set(new Object());
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
        synchronized (bulkInvalidationLock) {
            return new CacheMetrics(
                    data.estimatedSize(),
                    keyNodes.size(),
                    inFlightLoads.size(),
                    activeBulkStarts.values().stream().mapToInt(Integer::intValue).sum(),
                    exactInvalidations.size());
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
        versioned.committed.set(true);
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

    private VersionedValue<K, V> newVersionedValue(
            PublicationLease<K, V> lease, K key, V value) {
        CacheAddress address = new CacheAddress(this, key);
        return new VersionedValue<>(
                key, value, address, lease.scopeLease.snapshot(), lease.keyNode, lease.keyState);
    }

    private void install(
            VersionedValue<K, V> versioned, PublicationLease<K, V> lease) {
        registry.register(versioned.address, versioned, versioned.scopeSnapshot);
        lease.keyNode.registration.set(versioned);
        data.asMap().put(versioned.key, versioned);
    }

    private boolean canStageBulk(BulkLoadHandle handle, Object rawKey) {
        synchronized (bulkInvalidationLock) {
            return isBulkKeyCurrent(handle, rawKey);
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
        synchronized (bulkInvalidationLock) {
            if (!isBulkKeyCurrent(handle, key)) {
                return false;
            }
            return handle.scopeLease.commitIfPublicationCurrent(
                    handle.scopePublicationState, () -> {
                        if (!lease.isCurrent()) {
                            return false;
                        }
                        staged.committed.set(true);
                        install(staged, lease);
                        return true;
                    });
        }
    }

    private boolean isBulkKeyCurrent(BulkLoadHandle handle, Object rawKey) {
        BigInteger invalidation = exactInvalidations.get(rawKey);
        return !closed.get()
                && !handle.closed.get()
                && handle.scopeLease.isPublicationCurrent(handle.scopePublicationState)
                && (invalidation == null || invalidation.compareTo(handle.exactInvalidationSequence) <= 0);
    }

    private void closeBulkHandle(BulkLoadHandle handle) {
        ScopeLease leaseToClose = null;
        synchronized (bulkInvalidationLock) {
            if (!handle.closed.compareAndSet(false, true)) {
                return;
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
                leaseToClose = handle.scopeLease;
            }
        }
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
        synchronized (bulkInvalidationLock) {
            exactInvalidations.clear();
        }
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
        K key = (K) rawKey;
        @SuppressWarnings("unchecked")
        VersionedValue<K, V> versioned = (VersionedValue<K, V>) rawValue;
        if (beforeRemoval != null) {
            beforeRemoval.accept(key, versioned.value);
        }
        registry.unregister(versioned.address, versioned, versioned.scopeSnapshot);
        versioned.keyNode.registration.compareAndSet(versioned, null);
        tryPruneKey(key, versioned.keyNode);
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

        private boolean canStage(Object key) {
            return owner.canStageBulk(this, key);
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

        private CacheMetrics(
                long physicalEntryCount,
                int keyNodeCount,
                int inFlightLoadCount,
                int activeBulkHandleCount,
                int exactInvalidationTombstoneCount) {
            this.physicalEntryCount = physicalEntryCount;
            this.keyNodeCount = keyNodeCount;
            this.inFlightLoadCount = inFlightLoadCount;
            this.activeBulkHandleCount = activeBulkHandleCount;
            this.exactInvalidationTombstoneCount = exactInvalidationTombstoneCount;
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
    }

    private static final class PublicationLease<K, V> implements AutoCloseable {
        private final ScopedMetaCache<K, V> owner;
        private final K key;
        private final ScopeLease scopeLease;
        private final KeyNode<K, V> keyNode;
        private final KeyState keyState;
        private final Object loadPublicationState;
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
        private final AtomicBoolean committed = new AtomicBoolean(false);

        private VersionedValue(
                K key,
                V value,
                CacheAddress address,
                ScopeSnapshot scopeSnapshot,
                KeyNode<K, V> keyNode,
                KeyState keyState) {
            this.key = key;
            this.value = value;
            this.address = address;
            this.scopeSnapshot = scopeSnapshot;
            this.keyNode = keyNode;
            this.keyState = keyState;
        }

        private boolean isCurrent(
                ScopedMetaCacheRegistry registry,
                Map<K, KeyNode<K, V>> currentKeyNodes) {
            return scopeSnapshot.isCurrent(registry)
                    && currentKeyNodes.get(key) == keyNode
                    && keyNode.current.get() == keyState
                    && keyNode.registration.get() == this
                    && committed.get();
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

        private LoadAddress(K key, ScopePath path, PublicationLease<K, ?> lease) {
            this.key = key;
            this.path = path;
            this.scopeSnapshot = lease.scopeLease.snapshot();
            this.keyNode = lease.keyNode;
            this.keyState = lease.keyState;
            this.loadPublicationState = lease.loadPublicationState;
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
            int result = 31 * key.hashCode() + path.hashCode();
            result = 31 * result + scopeSnapshot.generationHashCode();
            result = 31 * result + System.identityHashCode(keyNode);
            result = 31 * result + System.identityHashCode(keyState);
            return 31 * result + System.identityHashCode(loadPublicationState);
        }
    }
}
