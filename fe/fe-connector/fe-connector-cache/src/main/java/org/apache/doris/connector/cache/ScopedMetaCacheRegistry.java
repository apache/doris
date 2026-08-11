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

import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

/**
 * Catalog-local hierarchy shared by independent physical metadata caches.
 *
 * <p>Logical invalidation replaces one state object. Values capture state identities and are rejected after a
 * replacement even when physical Caffeine cleanup is delayed. Each state also owns the exact registrations that
 * must be physically removed. Empty child nodes are pruned bottom-up so scope-directory memory remains bounded by
 * live values and in-flight loads rather than by every name ever observed.
 */
public final class ScopedMetaCacheRegistry implements AutoCloseable {
    private static final Runnable NO_OP = () -> {
    };
    private static final BiConsumer<ScopePath.Level, Object> NO_OP_SCOPE = (level, key) -> {
    };

    private final ScopeNode root = new ScopeNode(null, null, ScopePath.Level.CATALOG);
    private final Set<ScopedMetaCache<?, ?>> caches = ConcurrentHashMap.newKeySet();
    private final StripedPhaseGate publicationGate = new StripedPhaseGate();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final LongAdder[] retainedActiveLoads = newCounters(ScopePath.Level.values().length);
    private final BiConsumer<ScopePath.Level, Object> afterParentPin;
    private final BiConsumer<ScopePath.Level, Object> beforePruneMark;
    private final BiConsumer<ScopePath.Level, Object> afterPruneMark;

    public ScopedMetaCacheRegistry() {
        this(NO_OP_SCOPE, NO_OP_SCOPE, NO_OP_SCOPE);
    }

    ScopedMetaCacheRegistry(
            BiConsumer<ScopePath.Level, Object> afterParentPin,
            BiConsumer<ScopePath.Level, Object> beforePruneMark,
            BiConsumer<ScopePath.Level, Object> afterPruneMark) {
        this.afterParentPin = Objects.requireNonNull(afterParentPin, "afterParentPin can not be null");
        this.beforePruneMark = Objects.requireNonNull(beforePruneMark, "beforePruneMark can not be null");
        this.afterPruneMark = Objects.requireNonNull(afterPruneMark, "afterPruneMark can not be null");
    }

    public <K, V> ScopedMetaCache<K, V> createCache(String name, CacheSpec cacheSpec) {
        return createCache(name, cacheSpec, null, null, NO_OP, NO_OP);
    }

    <K, V> ScopedMetaCache<K, V> createCacheWithRemovalListener(
            String name, CacheSpec cacheSpec, RemovalListener<K, V> removalListener,
            Duration refreshAfterWrite, Executor refreshExecutor) {
        return createCacheWithRemovalListener(name, cacheSpec, null, removalListener,
                refreshAfterWrite, refreshExecutor, NO_OP, NO_OP, NO_OP);
    }

    <K, V> ScopedMetaCache<K, V> createCacheWithMetaRemovalListener(
            String name, CacheSpec cacheSpec, MetaCacheRemovalListener<K, V> removalListener,
            Duration refreshAfterWrite, Executor refreshExecutor) {
        RemovalListener<K, V> caffeineListener = removalListener == null ? null
                : (key, value, cause) -> removalListener.onRemoval(
                        key, value, MetaCacheRemovalReason.valueOf(cause.name()));
        return createCacheWithRemovalListener(
                name, cacheSpec, caffeineListener, refreshAfterWrite, refreshExecutor);
    }

    <K, V> ScopedMetaCache<K, V> createCache(
            String name,
            CacheSpec cacheSpec,
            Ticker ticker,
            BiConsumer<K, V> beforeRemoval) {
        return createCache(name, cacheSpec, ticker, beforeRemoval, NO_OP, NO_OP);
    }

    <K, V> ScopedMetaCache<K, V> createCache(
            String name,
            CacheSpec cacheSpec,
            Ticker ticker,
            BiConsumer<K, V> beforeRemoval,
            Runnable afterBulkStage) {
        return createCache(name, cacheSpec, ticker, beforeRemoval, NO_OP, afterBulkStage);
    }

    <K, V> ScopedMetaCache<K, V> createCache(
            String name,
            CacheSpec cacheSpec,
            Ticker ticker,
            BiConsumer<K, V> beforeRemoval,
            Runnable afterLoadElection,
            Runnable afterBulkStage) {
        RemovalListener<K, V> listener = beforeRemoval == null
                ? null
                : (key, value, cause) -> beforeRemoval.accept(key, value);
        return createCacheWithRemovalListener(
                name, cacheSpec, ticker, listener, null, null, afterLoadElection, afterBulkStage, NO_OP);
    }

    <K, V> ScopedMetaCache<K, V> createCacheWithRefresh(
            String name,
            CacheSpec cacheSpec,
            Duration refreshAfterWrite,
            Executor refreshExecutor,
            Runnable afterRefreshRegistration) {
        return createCacheWithRemovalListener(
                name, cacheSpec, null, null, refreshAfterWrite, refreshExecutor,
                NO_OP, NO_OP, afterRefreshRegistration);
    }

    private <K, V> ScopedMetaCache<K, V> createCacheWithRemovalListener(
            String name,
            CacheSpec cacheSpec,
            Ticker ticker,
            RemovalListener<K, V> removalListener,
            Duration refreshAfterWrite,
            Executor refreshExecutor,
            Runnable afterLoadElection,
            Runnable afterBulkStage,
            Runnable afterRefreshRegistration) {
        checkOpen();
        ScopedMetaCache<K, V> cache = new ScopedMetaCache<>(
                this,
                name,
                cacheSpec,
                ticker,
                removalListener,
                refreshAfterWrite,
                refreshExecutor,
                afterLoadElection,
                afterBulkStage,
                afterRefreshRegistration);
        caches.add(cache);
        if (closed.get()) {
            caches.remove(cache);
            cache.closeFromRegistry();
            throw new IllegalStateException("Scoped meta cache registry is closed");
        }
        return cache;
    }

    public void invalidate(ScopePath path) {
        invalidate(path, NO_OP);
    }

    void invalidate(List<ScopePath> paths) {
        invalidate(paths, NO_OP);
    }

    void invalidate(ScopePath path, Runnable afterStateReplacement) {
        Objects.requireNonNull(path, "path can not be null");
        Objects.requireNonNull(afterStateReplacement, "afterStateReplacement can not be null");
        checkOpen();
        InvalidatedScope invalidated = publicationGate.write(() -> {
            List<ScopeNode> existing = resolveExisting(path);
            bumpPublicationStates(existing);
            if (existing.size() != path.level().ordinal() + 1) {
                return null;
            }
            ScopeNode target = existing.get(existing.size() - 1);
            return new InvalidatedScope(existing, replaceState(target));
        });
        if (invalidated == null) {
            return;
        }
        afterStateReplacement.run();
        cleanDetachedState(invalidated.oldState);
        tryPrune(invalidated.existing);
    }

    void invalidate(List<ScopePath> paths, Runnable afterStateReplacement) {
        Objects.requireNonNull(paths, "paths can not be null");
        Objects.requireNonNull(afterStateReplacement, "afterStateReplacement can not be null");
        checkOpen();
        List<InvalidatedScope> invalidated = publicationGate.write(() -> {
            List<List<ScopeNode>> resolvedScopes = new ArrayList<>(paths.size());
            Set<ScopeNode> targets = Collections.newSetFromMap(new IdentityHashMap<>());
            Set<ScopeNode> publicationNodes = Collections.newSetFromMap(new IdentityHashMap<>());
            for (ScopePath path : paths) {
                Objects.requireNonNull(path, "path can not be null");
                List<ScopeNode> existing = resolveExisting(path);
                publicationNodes.addAll(existing);
                if (existing.size() == path.level().ordinal() + 1) {
                    ScopeNode target = existing.get(existing.size() - 1);
                    if (targets.add(target)) {
                        resolvedScopes.add(existing);
                    }
                }
            }
            bumpPublicationStates(new ArrayList<>(publicationNodes));
            List<InvalidatedScope> result = new ArrayList<>(resolvedScopes.size());
            resolvedScopes.forEach(existing -> result.add(new InvalidatedScope(
                    existing, replaceState(existing.get(existing.size() - 1)))));
            return result;
        });
        afterStateReplacement.run();
        invalidated.forEach(scope -> cleanDetachedState(scope.oldState));
        invalidated.forEach(scope -> tryPrune(scope.existing));
    }

    public ScopeMetrics metrics() {
        ScopeMetricsAccumulator accumulator = new ScopeMetricsAccumulator();
        collectMetrics(root, accumulator);
        for (ScopePath.Level level : ScopePath.Level.values()) {
            accumulator.retainedActiveLoads[level.ordinal()] = retainedActiveLoads[level.ordinal()].intValue();
        }
        return accumulator.snapshot();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        ScopeState oldState = publicationGate.write(() -> {
            bumpPublicationStates(Collections.singletonList(root));
            return replaceState(root);
        });
        cleanDetachedState(oldState);
        List<ScopedMetaCache<?, ?>> snapshot = new ArrayList<>(caches);
        caches.clear();
        snapshot.forEach(ScopedMetaCache::closeFromRegistry);
    }

    ScopeLease acquire(ScopePath path) {
        Objects.requireNonNull(path, "path can not be null");
        while (true) {
            checkOpen();
            ScopeSnapshot snapshot = resolveOrCreateSnapshot(path);
            if (snapshot == null || !retain(snapshot)) {
                continue;
            }
            if (snapshot.isCurrent(this)) {
                return new ScopeLease(this, snapshot);
            }
            release(snapshot);
        }
    }

    void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Scoped meta cache registry is closed");
        }
    }

    void removeCache(ScopedMetaCache<?, ?> cache) {
        caches.remove(cache);
    }

    void register(CacheAddress address, Object versionedValue, ScopeSnapshot snapshot) {
        snapshot.leafState().entries.put(address, versionedValue);
    }

    void unregister(CacheAddress address, Object versionedValue, ScopeSnapshot snapshot) {
        snapshot.leafState().entries.remove(address, versionedValue);
        tryPrune(snapshot);
    }

    private ScopeState replaceState(ScopeNode node) {
        while (true) {
            ScopeState oldState = node.current.get();
            ScopeState newState = new ScopeState(oldState.generation + 1L);
            if (node.current.compareAndSet(oldState, newState)) {
                return oldState;
            }
        }
    }

    private ScopeSnapshot resolveOrCreateSnapshot(ScopePath path) {
        ScopeNode catalogNode = root;
        if (path.level() == ScopePath.Level.CATALOG) {
            return ScopeSnapshot.capture(path, catalogNode, null, null, null);
        }
        ScopeState catalogState = catalogNode.current.get();
        ScopeNode dbNode = child(catalogNode, catalogState, path.database(), ScopePath.Level.DATABASE);
        if (dbNode == null) {
            return null;
        }
        if (path.level() == ScopePath.Level.DATABASE) {
            return ScopeSnapshot.capture(path, catalogNode, dbNode, null, null);
        }
        ScopeState dbState = dbNode.current.get();
        ScopeNode tableNode = child(dbNode, dbState, path.table(), ScopePath.Level.TABLE);
        if (tableNode == null) {
            return null;
        }
        if (path.level() == ScopePath.Level.TABLE) {
            return ScopeSnapshot.capture(path, catalogNode, dbNode, tableNode, null);
        }
        ScopeState tableState = tableNode.current.get();
        ScopeNode partitionNode = child(
                tableNode, tableState, path.partition(), ScopePath.Level.PARTITION);
        if (partitionNode == null) {
            return null;
        }
        return ScopeSnapshot.capture(path, catalogNode, dbNode, tableNode, partitionNode);
    }

    private List<ScopeNode> resolveExisting(ScopePath path) {
        List<ScopeNode> nodes = new ArrayList<>(path.level().ordinal() + 1);
        ScopeNode catalogNode = root;
        nodes.add(catalogNode);
        if (path.level() == ScopePath.Level.CATALOG) {
            return nodes;
        }
        ScopeNode dbNode = catalogNode.current.get().children.get(path.database());
        if (dbNode == null) {
            return nodes;
        }
        nodes.add(dbNode);
        if (path.level() == ScopePath.Level.DATABASE) {
            return nodes;
        }
        ScopeNode tableNode = dbNode.current.get().children.get(path.table());
        if (tableNode == null) {
            return nodes;
        }
        nodes.add(tableNode);
        if (path.level() == ScopePath.Level.TABLE) {
            return nodes;
        }
        ScopeNode partitionNode = tableNode.current.get().children.get(path.partition());
        if (partitionNode != null) {
            nodes.add(partitionNode);
        }
        return nodes;
    }

    private ScopeNode child(
            ScopeNode parent, ScopeState parentState, Object childKey, ScopePath.Level childLevel) {
        ScopeNode existing = parentState.children.get(childKey);
        if (existing != null) {
            return existing;
        }
        if (!parent.tryRetain()) {
            return null;
        }
        try {
            if (parent.current.get() != parentState) {
                return null;
            }
            afterParentPin.accept(childLevel, childKey);
            existing = parentState.children.get(childKey);
            if (existing != null) {
                return existing;
            }
            ScopeNode created = new ScopeNode(parent, childKey, childLevel);
            ScopeNode raced = parentState.children.putIfAbsent(childKey, created);
            ScopeNode child = raced == null ? created : raced;
            if (parent.current.get() != parentState) {
                parentState.children.remove(childKey, child);
                return null;
            }
            return child;
        } finally {
            parent.releaseRetain();
        }
    }

    private void bumpPublicationStates(List<ScopeNode> nodes) {
        nodes.forEach(node -> {
            while (true) {
                PublicationState oldState = node.descendantPublication.get();
                synchronized (oldState) {
                    if (node.descendantPublication.compareAndSet(oldState, new PublicationState())) {
                        break;
                    }
                }
            }
        });
    }

    private void cleanDetachedState(ScopeState state) {
        state.entries.forEach((address, value) -> {
            if (state.entries.remove(address, value)) {
                address.removeExpected(value);
            }
        });
        state.children.values().forEach(child -> cleanDetachedState(child.current.get()));
        state.children.clear();
    }

    private boolean retain(ScopeSnapshot snapshot) {
        if (!snapshot.leafNode().tryRetain()) {
            return false;
        }
        retainedActiveLoads[ScopePath.Level.CATALOG.ordinal()].increment();
        if (snapshot.databaseNode != null) {
            retainedActiveLoads[ScopePath.Level.DATABASE.ordinal()].increment();
        }
        if (snapshot.tableNode != null) {
            retainedActiveLoads[ScopePath.Level.TABLE.ordinal()].increment();
        }
        if (snapshot.partitionNode != null) {
            retainedActiveLoads[ScopePath.Level.PARTITION.ordinal()].increment();
        }
        return true;
    }

    private void release(ScopeSnapshot snapshot) {
        snapshot.leafNode().releaseRetain();
        retainedActiveLoads[ScopePath.Level.CATALOG.ordinal()].decrement();
        if (snapshot.databaseNode != null) {
            retainedActiveLoads[ScopePath.Level.DATABASE.ordinal()].decrement();
        }
        if (snapshot.tableNode != null) {
            retainedActiveLoads[ScopePath.Level.TABLE.ordinal()].decrement();
        }
        if (snapshot.partitionNode != null) {
            retainedActiveLoads[ScopePath.Level.PARTITION.ordinal()].decrement();
        }
        tryPrune(snapshot);
    }

    private void tryPrune(List<ScopeNode> nodes) {
        for (int i = nodes.size() - 1; i > 0; i--) {
            tryPrune(nodes.get(i));
        }
    }

    private void tryPrune(ScopeSnapshot snapshot) {
        if (snapshot.partitionNode != null) {
            tryPrune(snapshot.partitionNode);
        }
        if (snapshot.tableNode != null) {
            tryPrune(snapshot.tableNode);
        }
        if (snapshot.databaseNode != null) {
            tryPrune(snapshot.databaseNode);
        }
    }

    private boolean tryPrune(ScopeNode node) {
        if (node.parent == null) {
            return false;
        }
        while (true) {
            ScopeState state = node.current.get();
            if (!state.entries.isEmpty() || !state.children.isEmpty()) {
                return false;
            }
            beforePruneMark.accept(node.level, node.childKey);
            if (!node.tryBeginPrune()) {
                return false;
            }
            try {
                afterPruneMark.accept(node.level, node.childKey);
            } catch (RuntimeException | Error e) {
                node.cancelPrune();
                throw e;
            }
            if (node.current.get() != state || !state.entries.isEmpty() || !state.children.isEmpty()) {
                node.cancelPrune();
                continue;
            }
            ScopeState parentState = node.parent.current.get();
            return parentState.children.remove(node.childKey, node);
        }
    }

    private void collectMetrics(ScopeNode node, ScopeMetricsAccumulator accumulator) {
        ScopeState state = node.current.get();
        accumulator.registrations += state.entries.size();
        state.children.values().forEach(child -> {
            switch (child.level) {
                case DATABASE:
                    accumulator.databaseNodes++;
                    break;
                case TABLE:
                    accumulator.tableNodes++;
                    break;
                case PARTITION:
                    accumulator.partitionNodes++;
                    break;
                default:
                    throw new IllegalStateException("Unexpected child scope level: " + child.level);
            }
            collectMetrics(child, accumulator);
        });
    }

    private static LongAdder[] newCounters(int count) {
        LongAdder[] counters = new LongAdder[count];
        for (int index = 0; index < count; index++) {
            counters[index] = new LongAdder();
        }
        return counters;
    }

    void forceGenerationForTest(ScopePath path, long generation) {
        List<ScopeNode> existing = resolveExisting(path);
        if (existing.size() != path.level().ordinal() + 1) {
            throw new IllegalStateException("Scope does not exist: " + path);
        }
        ScopeNode node = existing.get(existing.size() - 1);
        ScopeState oldState = node.current.get();
        if (!oldState.entries.isEmpty()) {
            throw new IllegalStateException("Test generation can only be changed on an empty scope");
        }
        ScopeState replacement = new ScopeState(generation);
        replacement.children.putAll(oldState.children);
        if (!node.current.compareAndSet(oldState, replacement)) {
            throw new IllegalStateException("Concurrent scope mutation while forcing test generation");
        }
    }

    long generationForTest(ScopePath path) {
        List<ScopeNode> existing = resolveExisting(path);
        if (existing.size() != path.level().ordinal() + 1) {
            throw new IllegalStateException("Scope does not exist: " + path);
        }
        return existing.get(existing.size() - 1).current.get().generation;
    }

    static final class ScopeLease implements AutoCloseable {
        private final ScopedMetaCacheRegistry registry;
        private final ScopeSnapshot snapshot;
        private final AtomicBoolean released = new AtomicBoolean(false);

        private ScopeLease(ScopedMetaCacheRegistry registry, ScopeSnapshot snapshot) {
            this.registry = registry;
            this.snapshot = snapshot;
        }

        ScopeSnapshot snapshot() {
            return snapshot;
        }

        PublicationState publicationState() {
            return snapshot.leafNode().descendantPublication.get();
        }

        boolean isCurrent() {
            return snapshot.isCurrent(registry);
        }

        boolean isPublicationCurrent(PublicationState publicationState) {
            return isCurrent() && snapshot.leafNode().descendantPublication.get() == publicationState;
        }

        boolean commitIfPublicationCurrent(
                PublicationState publicationState, BooleanSupplier commitAction) {
            return registry.publicationGate.readBoolean(
                    () -> isPublicationCurrent(publicationState) && commitAction.getAsBoolean());
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                registry.release(snapshot);
            }
        }
    }

    private static final class InvalidatedScope {
        private final List<ScopeNode> existing;
        private final ScopeState oldState;

        private InvalidatedScope(List<ScopeNode> existing, ScopeState oldState) {
            this.existing = existing;
            this.oldState = oldState;
        }
    }

    static final class ScopeSnapshot {
        private final ScopePath path;
        private final ScopeNode catalogNode;
        private final ScopeState catalogState;
        private final ScopeNode databaseNode;
        private final ScopeState databaseState;
        private final ScopeNode tableNode;
        private final ScopeState tableState;
        private final ScopeNode partitionNode;
        private final ScopeState partitionState;
        private final int pathHashCode;

        private ScopeSnapshot(
                ScopePath path,
                ScopeNode catalogNode,
                ScopeState catalogState,
                ScopeNode databaseNode,
                ScopeState databaseState,
                ScopeNode tableNode,
                ScopeState tableState,
                ScopeNode partitionNode,
                ScopeState partitionState) {
            this.path = path;
            this.catalogNode = catalogNode;
            this.catalogState = catalogState;
            this.databaseNode = databaseNode;
            this.databaseState = databaseState;
            this.tableNode = tableNode;
            this.tableState = tableState;
            this.partitionNode = partitionNode;
            this.partitionState = partitionState;
            this.pathHashCode = path.hashCode();
        }

        static ScopeSnapshot capture(
                ScopePath path,
                ScopeNode catalogNode,
                ScopeNode databaseNode,
                ScopeNode tableNode,
                ScopeNode partitionNode) {
            ScopeState catalogState = catalogNode.current.get();
            ScopeState databaseState = databaseNode == null ? null : databaseNode.current.get();
            ScopeState tableState = tableNode == null ? null : tableNode.current.get();
            ScopeState partitionState = partitionNode == null ? null : partitionNode.current.get();
            ScopeState leafState = partitionState != null
                    ? partitionState
                    : tableState != null ? tableState : databaseState != null ? databaseState : catalogState;
            ScopeSnapshot currentSnapshot = leafState.scopeSnapshot;
            if (currentSnapshot != null && currentSnapshot.matches(
                    catalogNode,
                    catalogState,
                    databaseNode,
                    databaseState,
                    tableNode,
                    tableState,
                    partitionNode,
                    partitionState)) {
                return currentSnapshot;
            }
            synchronized (leafState) {
                currentSnapshot = leafState.scopeSnapshot;
                if (currentSnapshot == null || !currentSnapshot.matches(
                        catalogNode,
                        catalogState,
                        databaseNode,
                        databaseState,
                        tableNode,
                        tableState,
                        partitionNode,
                        partitionState)) {
                    currentSnapshot = new ScopeSnapshot(
                            path,
                            catalogNode,
                            catalogState,
                            databaseNode,
                            databaseState,
                            tableNode,
                            tableState,
                            partitionNode,
                            partitionState);
                    leafState.scopeSnapshot = currentSnapshot;
                }
                return currentSnapshot;
            }
        }

        boolean isCurrent(ScopedMetaCacheRegistry registry) {
            if (registry.closed.get() || registry.root != catalogNode
                    || catalogNode.current.get() != catalogState) {
                return false;
            }
            if (databaseNode == null) {
                return true;
            }
            if (catalogState.children.get(databaseNode.childKey) != databaseNode
                    || databaseNode.current.get() != databaseState) {
                return false;
            }
            if (tableNode == null) {
                return true;
            }
            if (databaseState.children.get(tableNode.childKey) != tableNode
                    || tableNode.current.get() != tableState) {
                return false;
            }
            if (partitionNode == null) {
                return true;
            }
            return tableState.children.get(partitionNode.childKey) == partitionNode
                    && partitionNode.current.get() == partitionState;
        }

        ScopePath path() {
            return path;
        }

        ScopeNode leafNode() {
            if (partitionNode != null) {
                return partitionNode;
            }
            if (tableNode != null) {
                return tableNode;
            }
            if (databaseNode != null) {
                return databaseNode;
            }
            return catalogNode;
        }

        ScopeState leafState() {
            if (partitionState != null) {
                return partitionState;
            }
            if (tableState != null) {
                return tableState;
            }
            if (databaseState != null) {
                return databaseState;
            }
            return catalogState;
        }

        boolean sameGeneration(ScopeSnapshot other) {
            return matches(
                    other.catalogNode,
                    other.catalogState,
                    other.databaseNode,
                    other.databaseState,
                    other.tableNode,
                    other.tableState,
                    other.partitionNode,
                    other.partitionState);
        }

        int pathHashCode() {
            return pathHashCode;
        }

        private boolean matches(
                ScopeNode expectedCatalogNode,
                ScopeState expectedCatalogState,
                ScopeNode expectedDatabaseNode,
                ScopeState expectedDatabaseState,
                ScopeNode expectedTableNode,
                ScopeState expectedTableState,
                ScopeNode expectedPartitionNode,
                ScopeState expectedPartitionState) {
            return catalogNode == expectedCatalogNode
                    && catalogState == expectedCatalogState
                    && databaseNode == expectedDatabaseNode
                    && databaseState == expectedDatabaseState
                    && tableNode == expectedTableNode
                    && tableState == expectedTableState
                    && partitionNode == expectedPartitionNode
                    && partitionState == expectedPartitionState;
        }
    }

    static final class PublicationState {
    }

    static final class CacheAddress {
        private final ScopedMetaCache<?, ?> owner;
        private final Object key;
        private final int hashCode;

        CacheAddress(ScopedMetaCache<?, ?> owner, Object key) {
            this.owner = owner;
            this.key = key;
            this.hashCode = 31 * System.identityHashCode(owner) + key.hashCode();
        }

        void removeExpected(Object expectedValue) {
            owner.removeExpectedRaw(key, expectedValue);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof CacheAddress)) {
                return false;
            }
            CacheAddress other = (CacheAddress) obj;
            return owner == other.owner && key.equals(other.key);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    private static final class ScopeNode {
        private final ScopeNode parent;
        private final Object childKey;
        private final ScopePath.Level level;
        private final AtomicReference<ScopeState> current = new AtomicReference<>(new ScopeState(0L));
        private final AtomicReference<PublicationState> descendantPublication =
                new AtomicReference<>(new PublicationState());
        // -1 reserves the node for pruning. A canceled prune restores zero; a detached node keeps the marker.
        private final AtomicInteger activeLoads = new AtomicInteger();

        private ScopeNode(
                ScopeNode parent, Object childKey, ScopePath.Level level) {
            this.parent = parent;
            this.childKey = childKey;
            this.level = level;
        }

        private boolean tryRetain() {
            while (true) {
                int active = activeLoads.get();
                if (active < 0) {
                    return false;
                }
                if (activeLoads.compareAndSet(active, active + 1)) {
                    return true;
                }
            }
        }

        private void releaseRetain() {
            int active = activeLoads.decrementAndGet();
            if (active < 0) {
                throw new IllegalStateException("Scope node retain count became negative");
            }
        }

        private boolean tryBeginPrune() {
            return activeLoads.compareAndSet(0, -1);
        }

        private void cancelPrune() {
            if (!activeLoads.compareAndSet(-1, 0)) {
                throw new IllegalStateException("Scope node prune marker changed unexpectedly");
            }
        }
    }

    private static final class ScopeState {
        private final long generation;
        private final ConcurrentMap<Object, ScopeNode> children = new ConcurrentHashMap<>();
        private final ConcurrentMap<CacheAddress, Object> entries = new ConcurrentHashMap<>();
        private volatile ScopeSnapshot scopeSnapshot;

        private ScopeState(long generation) {
            this.generation = generation;
        }
    }

    public static final class ScopeMetrics {
        private final int databaseNodeCount;
        private final int tableNodeCount;
        private final int partitionNodeCount;
        private final int registrationCount;
        private final int activeCatalogLoadCount;
        private final int activeDatabaseLoadCount;
        private final int activeTableLoadCount;
        private final int activePartitionLoadCount;

        private ScopeMetrics(
                int databaseNodeCount,
                int tableNodeCount,
                int partitionNodeCount,
                int registrationCount,
                int activeCatalogLoadCount,
                int activeDatabaseLoadCount,
                int activeTableLoadCount,
                int activePartitionLoadCount) {
            this.databaseNodeCount = databaseNodeCount;
            this.tableNodeCount = tableNodeCount;
            this.partitionNodeCount = partitionNodeCount;
            this.registrationCount = registrationCount;
            this.activeCatalogLoadCount = activeCatalogLoadCount;
            this.activeDatabaseLoadCount = activeDatabaseLoadCount;
            this.activeTableLoadCount = activeTableLoadCount;
            this.activePartitionLoadCount = activePartitionLoadCount;
        }

        public int getDatabaseNodeCount() {
            return databaseNodeCount;
        }

        public int getTableNodeCount() {
            return tableNodeCount;
        }

        public int getPartitionNodeCount() {
            return partitionNodeCount;
        }

        public int getRegistrationCount() {
            return registrationCount;
        }

        public int getActiveLoadCount() {
            return activeCatalogLoadCount
                    + activeDatabaseLoadCount
                    + activeTableLoadCount
                    + activePartitionLoadCount;
        }

        public int getActiveCatalogLoadCount() {
            return activeCatalogLoadCount;
        }

        public int getActiveDatabaseLoadCount() {
            return activeDatabaseLoadCount;
        }

        public int getActiveTableLoadCount() {
            return activeTableLoadCount;
        }

        public int getActivePartitionLoadCount() {
            return activePartitionLoadCount;
        }
    }

    private static final class ScopeMetricsAccumulator {
        private int databaseNodes;
        private int tableNodes;
        private int partitionNodes;
        private int registrations;
        private final int[] retainedActiveLoads = new int[ScopePath.Level.values().length];

        private ScopeMetrics snapshot() {
            return new ScopeMetrics(
                    databaseNodes,
                    tableNodes,
                    partitionNodes,
                    registrations,
                    retainedActiveLoads[ScopePath.Level.CATALOG.ordinal()],
                    retainedActiveLoads[ScopePath.Level.DATABASE.ordinal()],
                    retainedActiveLoads[ScopePath.Level.TABLE.ordinal()],
                    retainedActiveLoads[ScopePath.Level.PARTITION.ordinal()]);
        }
    }
}
