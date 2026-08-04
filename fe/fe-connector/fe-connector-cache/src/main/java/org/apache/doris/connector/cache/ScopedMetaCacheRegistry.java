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

import com.github.benmanes.caffeine.cache.Ticker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
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

    private final ScopeNode root = new ScopeNode(null, null, ScopePath.Level.CATALOG);
    private final Set<ScopedMetaCache<?, ?>> caches = ConcurrentHashMap.newKeySet();
    private final Object publicationLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicIntegerArray retainedActiveLoads =
            new AtomicIntegerArray(ScopePath.Level.values().length);

    public <K, V> ScopedMetaCache<K, V> createCache(String name, CacheSpec cacheSpec) {
        return createCache(name, cacheSpec, null, null, NO_OP, NO_OP);
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
        checkOpen();
        ScopedMetaCache<K, V> cache = new ScopedMetaCache<>(
                this,
                name,
                cacheSpec,
                ticker,
                beforeRemoval,
                afterLoadElection,
                afterBulkStage);
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

    void invalidate(ScopePath path, Runnable afterStateReplacement) {
        Objects.requireNonNull(path, "path can not be null");
        Objects.requireNonNull(afterStateReplacement, "afterStateReplacement can not be null");
        checkOpen();
        List<ScopeNode> existing;
        ScopeState oldState;
        synchronized (publicationLock) {
            existing = resolveExisting(path);
            bumpPublicationStates(existing);
            if (existing.size() != path.level().ordinal() + 1) {
                return;
            }
            ScopeNode target = existing.get(existing.size() - 1);
            oldState = replaceState(target);
        }
        afterStateReplacement.run();
        cleanDetachedState(oldState);
        tryPrune(existing);
    }

    public ScopeMetrics metrics() {
        ScopeMetricsAccumulator accumulator = new ScopeMetricsAccumulator();
        collectMetrics(root, accumulator);
        for (ScopePath.Level level : ScopePath.Level.values()) {
            accumulator.retainedActiveLoads[level.ordinal()] = retainedActiveLoads.get(level.ordinal());
        }
        return accumulator.snapshot();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        ScopeState oldState;
        synchronized (publicationLock) {
            bumpPublicationStates(Collections.singletonList(root));
            oldState = replaceState(root);
        }
        cleanDetachedState(oldState);
        List<ScopedMetaCache<?, ?>> snapshot = new ArrayList<>(caches);
        caches.clear();
        snapshot.forEach(ScopedMetaCache::closeFromRegistry);
    }

    ScopeLease acquire(ScopePath path) {
        Objects.requireNonNull(path, "path can not be null");
        while (true) {
            checkOpen();
            List<ScopeNode> nodes = resolveOrCreate(path);
            nodes.forEach(node -> {
                node.activeLoads.incrementAndGet();
                retainedActiveLoads.incrementAndGet(node.level.ordinal());
            });
            ScopeSnapshot snapshot = ScopeSnapshot.capture(path, nodes);
            if (snapshot.isCurrent(this)) {
                return new ScopeLease(this, snapshot);
            }
            release(nodes);
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
        tryPrune(snapshot.nodes);
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

    private List<ScopeNode> resolveOrCreate(ScopePath path) {
        List<ScopeNode> nodes = new ArrayList<>(path.level().ordinal() + 1);
        ScopeNode catalogNode = root;
        nodes.add(catalogNode);
        if (path.level() == ScopePath.Level.CATALOG) {
            return nodes;
        }
        ScopeState catalogState = catalogNode.current.get();
        ScopeNode dbNode = child(catalogNode, catalogState, path.database(), ScopePath.Level.DATABASE);
        nodes.add(dbNode);
        if (path.level() == ScopePath.Level.DATABASE) {
            return nodes;
        }
        ScopeState dbState = dbNode.current.get();
        ScopeNode tableNode = child(dbNode, dbState, path.table(), ScopePath.Level.TABLE);
        nodes.add(tableNode);
        if (path.level() == ScopePath.Level.TABLE) {
            return nodes;
        }
        ScopeState tableState = tableNode.current.get();
        nodes.add(child(tableNode, tableState, path.partition(), ScopePath.Level.PARTITION));
        return nodes;
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
        return parentState.children.computeIfAbsent(
                childKey, ignored -> new ScopeNode(parent, childKey, childLevel));
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
            address.removeExpected(value);
            state.entries.remove(address, value);
        });
        state.children.values().forEach(child -> cleanDetachedState(child.current.get()));
        state.children.clear();
    }

    private void release(List<ScopeNode> nodes) {
        nodes.forEach(node -> {
            int remaining = node.activeLoads.decrementAndGet();
            int retainedRemaining = retainedActiveLoads.decrementAndGet(node.level.ordinal());
            if (remaining < 0) {
                throw new IllegalStateException("Scope active-load count became negative");
            }
            if (retainedRemaining < 0) {
                throw new IllegalStateException("Retained scope active-load count became negative");
            }
        });
        tryPrune(nodes);
    }

    private void tryPrune(List<ScopeNode> nodes) {
        for (int i = nodes.size() - 1; i > 0; i--) {
            tryPrune(nodes.get(i));
        }
    }

    private boolean tryPrune(ScopeNode node) {
        if (node.parent == null) {
            return false;
        }
        ScopeState state = node.current.get();
        if (node.activeLoads.get() != 0 || !state.entries.isEmpty() || !state.children.isEmpty()) {
            return false;
        }
        ScopeState parentState = node.parent.current.get();
        return parentState.children.remove(node.childKey, node);
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
            synchronized (registry.publicationLock) {
                return isPublicationCurrent(publicationState) && commitAction.getAsBoolean();
            }
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                registry.release(snapshot.nodes);
            }
        }
    }

    static final class ScopeSnapshot {
        private final ScopePath path;
        private final List<ScopeNode> nodes;
        private final List<ScopeState> states;

        private ScopeSnapshot(ScopePath path, List<ScopeNode> nodes, List<ScopeState> states) {
            this.path = path;
            this.nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
            this.states = Collections.unmodifiableList(states);
        }

        static ScopeSnapshot capture(ScopePath path, List<ScopeNode> nodes) {
            List<ScopeState> states = new ArrayList<>(nodes.size());
            nodes.forEach(node -> states.add(node.current.get()));
            return new ScopeSnapshot(path, nodes, states);
        }

        boolean isCurrent(ScopedMetaCacheRegistry registry) {
            if (registry.closed.get() || registry.root != nodes.get(0)
                    || registry.root.current.get() != states.get(0)) {
                return false;
            }
            for (int i = 1; i < nodes.size(); i++) {
                ScopeNode parent = nodes.get(i - 1);
                ScopeNode node = nodes.get(i);
                ScopeState parentState = states.get(i - 1);
                if (parent.current.get() != parentState
                        || parentState.children.get(node.childKey) != node
                        || node.current.get() != states.get(i)) {
                    return false;
                }
            }
            return true;
        }

        ScopePath path() {
            return path;
        }

        ScopeNode leafNode() {
            return nodes.get(nodes.size() - 1);
        }

        ScopeState leafState() {
            return states.get(states.size() - 1);
        }

        boolean sameGeneration(ScopeSnapshot other) {
            if (nodes.size() != other.nodes.size()) {
                return false;
            }
            for (int i = 0; i < nodes.size(); i++) {
                if (nodes.get(i) != other.nodes.get(i) || states.get(i) != other.states.get(i)) {
                    return false;
                }
            }
            return true;
        }

        int generationHashCode() {
            int result = 1;
            for (int i = 0; i < nodes.size(); i++) {
                result = 31 * result + System.identityHashCode(nodes.get(i));
                result = 31 * result + System.identityHashCode(states.get(i));
            }
            return result;
        }
    }

    static final class PublicationState {
    }

    static final class CacheAddress {
        private final ScopedMetaCache<?, ?> owner;
        private final Object key;

        CacheAddress(ScopedMetaCache<?, ?> owner, Object key) {
            this.owner = owner;
            this.key = key;
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
            return 31 * System.identityHashCode(owner) + key.hashCode();
        }
    }

    private static final class ScopeNode {
        private final ScopeNode parent;
        private final Object childKey;
        private final ScopePath.Level level;
        private final AtomicReference<ScopeState> current = new AtomicReference<>(new ScopeState(0L));
        private final AtomicReference<PublicationState> descendantPublication =
                new AtomicReference<>(new PublicationState());
        private final AtomicInteger activeLoads = new AtomicInteger();

        private ScopeNode(
                ScopeNode parent, Object childKey, ScopePath.Level level) {
            this.parent = parent;
            this.childKey = childKey;
            this.level = level;
        }
    }

    private static final class ScopeState {
        private final long generation;
        private final ConcurrentMap<Object, ScopeNode> children = new ConcurrentHashMap<>();
        private final ConcurrentMap<CacheAddress, Object> entries = new ConcurrentHashMap<>();

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
