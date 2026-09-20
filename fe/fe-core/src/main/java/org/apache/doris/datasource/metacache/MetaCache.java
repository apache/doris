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
import org.apache.doris.common.Pair;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongPredicate;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MetaCache<T> {
    private static final Logger LOG = LogManager.getLogger(MetaCache.class);
    private static final int MAX_NAMES_LOAD_ATTEMPTS = 4;
    private static final int MAX_NAMES_REFRESH_FLIGHTS = 2;
    private static final int MAX_PHYSICAL_NAMES_LOADS = 2;
    private Cache<String, NamesCacheValue> namesCache;
    private final CacheLoader<String, List<Pair<String, String>>> namesCacheLoader;
    private final Consumer<List<Pair<String, String>>> namesCacheUpdateAction;
    private final BiConsumer<String, String> nameUpdateAction;
    private final Consumer<String> nameInvalidationAction;
    private final LongSupplier namesLoadEpochSupplier;
    private final LongPredicate namesLoadEpochValidator;
    private final ExecutorService namesRefreshExecutor;
    private final long namesRefreshAfterWriteNanos;
    // Order explicit mutations with validation and publication of a loaded names snapshot.
    private final Object namesMutationLock = new Object();
    private final AtomicLong namesGeneration = new AtomicLong();
    private NamesLoad activeNamesLoad;
    private final List<NamesLoad> physicalNamesLoads = Lists.newArrayList();
    private final Map<Long, NamesRefresh> activeNamesRefreshes = Maps.newHashMap();
    private Long pendingNamesRefreshGeneration;
    private long minimumLoadGeneration;
    //Pair<String, String> : <Remote name, Local name>
    private Map<Long, String> idToName = Maps.newConcurrentMap();
    private final CacheLoader<String, Optional<T>> metaObjCacheLoader;
    private final CacheFactory metaObjCacheFactory;
    private final RemovalListener<String, Optional<T>> metaObjRemovalListener;
    private LoadingCache<String, Optional<T>> metaObjCache;
    // Bulk invalidation retires the current cache generation before running removal callbacks.
    // Loads may run without this lock, but may publish only into the generation they started in.
    private final ReentrantReadWriteLock metaObjLifecycleLock = new ReentrantReadWriteLock();
    private final AtomicLong metaObjGeneration = new AtomicLong();
    // Object loads and event mutations for the same name share one lock. Locks are reference-counted
    // so a blocked connector call never serializes unrelated names or leaves an unbounded lock map.
    private final ConcurrentMap<String, MetaObjKeyLock> metaObjKeyLocks = Maps.newConcurrentMap();

    private String name;

    public MetaCache(String name,
            ExecutorService executor,
            OptionalLong expireAfterAccessSec,
            OptionalLong refreshAfterWriteSec,
            long maxSize,
            CacheLoader<String, List<Pair<String, String>>> namesCacheLoader,
            CacheLoader<String, Optional<T>> metaObjCacheLoader,
            RemovalListener<String, Optional<T>> removalListener) {
        this(name, executor, expireAfterAccessSec, refreshAfterWriteSec, maxSize,
                namesCacheLoader, ignored -> { }, (remoteName, localName) -> { }, ignored -> { },
                metaObjCacheLoader, removalListener, () -> 0L, ignored -> true);
    }

    public MetaCache(String name,
            ExecutorService executor,
            OptionalLong expireAfterAccessSec,
            OptionalLong refreshAfterWriteSec,
            long maxSize,
            CacheLoader<String, List<Pair<String, String>>> namesCacheLoader,
            Consumer<List<Pair<String, String>>> namesCacheUpdateAction,
            CacheLoader<String, Optional<T>> metaObjCacheLoader,
            RemovalListener<String, Optional<T>> removalListener) {
        this(name, executor, expireAfterAccessSec, refreshAfterWriteSec, maxSize,
                namesCacheLoader, namesCacheUpdateAction, (remoteName, localName) -> { }, ignored -> { },
                metaObjCacheLoader, removalListener, () -> 0L, ignored -> true);
    }

    public MetaCache(String name,
            ExecutorService executor,
            OptionalLong expireAfterAccessSec,
            OptionalLong refreshAfterWriteSec,
            long maxSize,
            CacheLoader<String, List<Pair<String, String>>> namesCacheLoader,
            Consumer<List<Pair<String, String>>> namesCacheUpdateAction,
            BiConsumer<String, String> nameUpdateAction,
            Consumer<String> nameInvalidationAction,
            CacheLoader<String, Optional<T>> metaObjCacheLoader,
            RemovalListener<String, Optional<T>> removalListener) {
        this(name, executor, expireAfterAccessSec, refreshAfterWriteSec, maxSize,
                namesCacheLoader, namesCacheUpdateAction, nameUpdateAction, nameInvalidationAction,
                metaObjCacheLoader, removalListener, () -> 0L, ignored -> true);
    }

    public MetaCache(String name,
            ExecutorService executor,
            OptionalLong expireAfterAccessSec,
            OptionalLong refreshAfterWriteSec,
            long maxSize,
            CacheLoader<String, List<Pair<String, String>>> namesCacheLoader,
            Consumer<List<Pair<String, String>>> namesCacheUpdateAction,
            BiConsumer<String, String> nameUpdateAction,
            Consumer<String> nameInvalidationAction,
            CacheLoader<String, Optional<T>> metaObjCacheLoader,
            RemovalListener<String, Optional<T>> removalListener,
            LongSupplier namesLoadEpochSupplier,
            LongPredicate namesLoadEpochValidator) {
        this.name = name;
        this.namesCacheLoader = namesCacheLoader;
        this.namesCacheUpdateAction = namesCacheUpdateAction;
        this.nameUpdateAction = nameUpdateAction;
        this.nameInvalidationAction = nameInvalidationAction;
        this.namesLoadEpochSupplier = namesLoadEpochSupplier;
        this.namesLoadEpochValidator = namesLoadEpochValidator;
        this.namesRefreshExecutor = executor;
        this.namesRefreshAfterWriteNanos = refreshAfterWriteSec.isPresent()
                ? TimeUnit.SECONDS.toNanos(refreshAfterWriteSec.getAsLong()) : Long.MAX_VALUE;
        this.metaObjCacheLoader = metaObjCacheLoader;
        this.metaObjRemovalListener = removalListener;

        // ATTN:
        // The refreshAfterWriteSec is only used for metaObjCache, not for namesCache.
        // Because namesCache need to be refreshed at interval so that user can get the latest meta list.
        // But metaObjCache does not need to be refreshed at interval, because the object is actually not
        // from remote datasource, it is just a local generated object to represent the meta info.
        // So it only need to be expired after specified duration.
        CacheFactory namesCacheFactory = new CacheFactory(
                expireAfterAccessSec,
                OptionalLong.empty(),
                1, // names cache has one and only one entry
                true,
                null);
        metaObjCacheFactory = new CacheFactory(
                expireAfterAccessSec,
                OptionalLong.empty(),
                maxSize,
                true,
                null);
        namesCache = namesCacheFactory.buildCache();
        // Use sync removal listener to prevent deadlock (removal listener calls invalidateAll)
        // NOTE: This cache should NOT use refreshAfterWrite, as it would become synchronous
        metaObjCache = buildMetaObjCache();
    }

    public List<String> listNames() {
        return getNames(false).stream().map(Pair::value).collect(Collectors.toList());
    }

    public List<String> refreshNames() {
        throwIfInterrupted();
        // Retire any active load so the forced refresh is not blocked behind a stuck
        // background refresh. Keep the retired physical owner accounted until its loader
        // exits, otherwise repeated forced refreshes can bypass MAX_PHYSICAL_NAMES_LOADS.
        // Only advance the generation when the active load is still running (not done); a
        // completed load has already been cleared by finishNamesLoad and cannot publish stale results.
        synchronized (namesMutationLock) {
            if (activeNamesLoad != null && !activeNamesLoad.result.isDone()) {
                NamesCacheValue current = namesCache.getIfPresent("");
                long currentGeneration = namesGeneration.get();
                long nextGeneration = advanceNamesGeneration();
                if (current != null && current.complete && current.generation == currentGeneration) {
                    namesCache.put("", current.withGeneration(nextGeneration));
                }
            }
            activeNamesLoad = null;
        }
        throwIfInterrupted();
        return getNames(true).stream().map(Pair::value).collect(Collectors.toList());
    }

    private void throwIfInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new CompletionException(new InterruptedException());
        }
    }

    private List<Pair<String, String>> getNames(boolean forceRefresh) {
        for (int attempt = 0; attempt < MAX_NAMES_LOAD_ATTEMPTS; attempt++) {
            if (forceRefresh) {
                throwIfInterrupted();
            }
            NamesCacheValue value = forceRefresh ? null : namesCache.getIfPresent("");
            List<Pair<String, String>> currentNames = null;
            synchronized (namesMutationLock) {
                if (value != null && value.complete && value.generation == namesGeneration.get()) {
                    currentNames = value.snapshot();
                }
            }
            if (currentNames != null) {
                scheduleNamesRefresh(value);
                return currentNames;
            }
            value = loadNames(forceRefresh, true, null);
            synchronized (namesMutationLock) {
                if (value != null && value.complete && value.generation == namesGeneration.get()) {
                    return value.snapshot();
                }
            }
        }
        throw new IllegalStateException("Failed to load names for " + name
                + " because metadata kept changing");
    }

    private NamesCacheValue loadNames(boolean forceRefresh, boolean awaitActiveLoad, Long expectedGeneration) {
        NamesLoad namesLoad = null;
        boolean loadOwner = false;
        long requestedGeneration;
        synchronized (namesMutationLock) {
            if (expectedGeneration != null && expectedGeneration != namesGeneration.get()) {
                return null;
            }
            NamesCacheValue cached = namesCache.getIfPresent("");
            if (!forceRefresh && cached != null && cached.complete
                    && cached.generation == namesGeneration.get()) {
                return cached;
            }
            long loadGeneration = namesGeneration.get();
            if (activeNamesLoad != null && activeNamesLoad.generation == loadGeneration) {
                if (!awaitActiveLoad) {
                    return null;
                }
                namesLoad = activeNamesLoad;
            }
            requestedGeneration = loadGeneration;
        }

        if (namesLoad != null) {
            return awaitNamesLoad(namesLoad);
        }

        // Lifecycle admission may acquire the catalog monitor. Keep it outside the names
        // mutation lock because catalog reset advances the names generation under that monitor.
        long loadEpoch = namesLoadEpochSupplier.getAsLong();
        synchronized (namesMutationLock) {
            if (requestedGeneration != namesGeneration.get()
                    || expectedGeneration != null && expectedGeneration != namesGeneration.get()) {
                return null;
            }
            NamesCacheValue cached = namesCache.getIfPresent("");
            if (!forceRefresh && cached != null && cached.complete
                    && cached.generation == requestedGeneration) {
                return cached;
            }
            if (activeNamesLoad != null && activeNamesLoad.generation == requestedGeneration) {
                if (!awaitActiveLoad) {
                    return null;
                }
                namesLoad = activeNamesLoad;
            } else {
                if (physicalNamesLoads.size() >= MAX_PHYSICAL_NAMES_LOADS) {
                    return null;
                }
                Map<String, Pair<String, String>> incompleteNames = cached != null && !cached.complete
                        ? Maps.newLinkedHashMap(cached.names) : Maps.newLinkedHashMap();
                namesLoad = new NamesLoad(requestedGeneration, loadEpoch, incompleteNames);
                activeNamesLoad = namesLoad;
                physicalNamesLoads.add(namesLoad);
                loadOwner = true;
            }
        }

        if (!loadOwner) {
            return awaitNamesLoad(namesLoad);
        }

        try {
            List<Pair<String, String>> loadedNames = Objects.requireNonNull(namesCacheLoader.load(""));
            NamesCacheValue value = null;
            synchronized (namesMutationLock) {
                if (namesLoad.generation == namesGeneration.get()
                        && namesLoad.generation >= minimumLoadGeneration
                        && namesLoadEpochValidator.test(namesLoad.loadEpoch)) {
                    Map<String, Pair<String, String>> names = toNamesMap(loadedNames);
                    names.putAll(namesLoad.incompleteNames);
                    value = new NamesCacheValue(namesGeneration.get(), names, true);
                    namesCache.put("", value);
                    publishNames(value);
                }
            }
            finishNamesLoad(namesLoad);
            namesLoad.result.complete(value);
            return value;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            CompletionException failure = new CompletionException(e);
            finishNamesLoad(namesLoad);
            namesLoad.result.completeExceptionally(failure);
            throw failure;
        } catch (RuntimeException e) {
            finishNamesLoad(namesLoad);
            if (!namesLoad.result.completeExceptionally(e)) {
                return null;
            }
            throw e;
        } catch (Error e) {
            finishNamesLoad(namesLoad);
            namesLoad.result.completeExceptionally(e);
            throw e;
        } catch (Exception e) {
            CompletionException failure = new CompletionException(e);
            finishNamesLoad(namesLoad);
            if (!namesLoad.result.completeExceptionally(failure)) {
                return null;
            }
            throw failure;
        } finally {
            finishNamesLoad(namesLoad);
        }
    }

    private void finishNamesLoad(NamesLoad namesLoad) {
        synchronized (namesMutationLock) {
            if (activeNamesLoad == namesLoad) {
                activeNamesLoad = null;
            }
            physicalNamesLoads.remove(namesLoad);
        }
    }

    private NamesCacheValue awaitNamesLoad(NamesLoad namesLoad) {
        try {
            return namesLoad.result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new CompletionException(cause);
        }
    }

    private Map<String, Pair<String, String>> toNamesMap(List<Pair<String, String>> names) {
        Map<String, Pair<String, String>> namesMap = Maps.newLinkedHashMap();
        for (Pair<String, String> pair : names) {
            namesMap.putIfAbsent(pair.value(), pair);
        }
        return namesMap;
    }

    private void scheduleNamesRefresh(NamesCacheValue value) {
        if (System.nanoTime() - value.writeNanos < namesRefreshAfterWriteNanos) {
            return;
        }
        scheduleNamesRefresh(value.generation);
    }

    private void scheduleNamesRefresh(long generation) {
        NamesRefresh refresh;
        synchronized (namesMutationLock) {
            if (generation != namesGeneration.get()
                    || activeNamesRefreshes.containsKey(generation)) {
                return;
            }
            if (activeNamesRefreshes.size() >= MAX_NAMES_REFRESH_FLIGHTS) {
                pendingNamesRefreshGeneration = generation;
                return;
            }
            refresh = new NamesRefresh(generation);
            activeNamesRefreshes.put(generation, refresh);
            if (pendingNamesRefreshGeneration != null && pendingNamesRefreshGeneration == generation) {
                pendingNamesRefreshGeneration = null;
            }
        }
        try {
            namesRefreshExecutor.execute(() -> {
                try {
                    loadNames(true, false, refresh.generation);
                } catch (Exception e) {
                    LOG.warn("Failed to refresh names cache for {}", name, e);
                } finally {
                    clearNamesRefresh(refresh);
                }
            });
        } catch (RuntimeException e) {
            clearNamesRefresh(refresh);
            LOG.warn("Failed to schedule names cache refresh for {}", name, e);
        }
    }

    private void clearNamesRefresh(NamesRefresh refresh) {
        Long pendingGeneration = null;
        synchronized (namesMutationLock) {
            if (activeNamesRefreshes.get(refresh.generation) == refresh) {
                activeNamesRefreshes.remove(refresh.generation);
                if (pendingNamesRefreshGeneration != null) {
                    pendingGeneration = pendingNamesRefreshGeneration;
                    pendingNamesRefreshGeneration = null;
                }
            }
        }
        if (pendingGeneration != null) {
            scheduleNamesRefresh(pendingGeneration);
        }
    }

    public String getRemoteName(String localName) {
        NamesCacheValue value = namesCache.getIfPresent("");
        synchronized (namesMutationLock) {
            if (value != null && value.generation == namesGeneration.get()) {
                Pair<String, String> pair = value.names.get(localName);
                if (pair != null) {
                    return pair.key();
                }
                if (value.complete) {
                    return null;
                }
            }
        }
        return getNames(false).stream()
                .filter(pair -> pair.value().equals(localName))
                .map(Pair::key)
                .findFirst()
                .orElse(null);
    }

    private void publishNames(NamesCacheValue value) {
        namesCacheUpdateAction.accept(value.snapshot());
    }

    public Optional<T> getMetaObj(String name, long id) {
        Optional<T> val = withMetaObjLifecycleReadLock(() -> metaObjCache.getIfPresent(name));
        if (val != null && val.isPresent()) {
            return val;
        }
        return withMetaObjKeyLock(name, () -> {
            while (true) {
                long generation;
                metaObjLifecycleLock.readLock().lock();
                try {
                    Optional<T> current = metaObjCache.getIfPresent(name);
                    if (current != null && current.isPresent()) {
                        return current;
                    }
                    generation = metaObjGeneration.get();
                } finally {
                    metaObjLifecycleLock.readLock().unlock();
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("trigger getMetaObj in metacache {}, obj name: {}, id: {}",
                            this.name, name, id, new Exception());
                }
                Optional<T> loaded = loadMetaObj(name);
                metaObjLifecycleLock.readLock().lock();
                try {
                    if (generation != metaObjGeneration.get()) {
                        continue;
                    }
                    metaObjCache.put(name, loaded);
                    idToName.put(id, name);
                    return loaded;
                } finally {
                    metaObjLifecycleLock.readLock().unlock();
                }
            }
        });
    }

    private Optional<T> loadMetaObj(String key) {
        try {
            return metaObjCacheLoader.load(key);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new CompletionException(e);
        }
    }

    public Optional<T> tryGetMetaObj(String name) {
        Optional<T> val = withMetaObjLifecycleReadLock(() -> metaObjCache.getIfPresent(name));
        if (val == null || !val.isPresent()) {
            return Optional.empty();
        }
        return val;
    }

    public Optional<T> getMetaObjById(long id) {
        String name = withMetaObjLifecycleReadLock(() -> idToName.get(id));
        return name == null ? Optional.empty() : getMetaObj(name, id);
    }

    public void updateCache(String remoteName, String localName, T obj, long id) {
        updateCache(remoteName, localName, obj, id, namesLoadEpochSupplier.getAsLong());
    }

    public boolean updateCache(String remoteName, String localName, T obj, long id, long expectedEpoch) {
        return withMetaObjKeyLock(localName, () -> {
            return withMetaObjLifecycleReadLock(() -> {
                synchronized (namesMutationLock) {
                    if (!namesLoadEpochValidator.test(expectedEpoch)) {
                        return false;
                    }
                    long generation = advanceNamesGeneration();
                    NamesCacheValue currentNames = namesCache.getIfPresent("");
                    Map<String, Pair<String, String>> names = currentNames == null
                            ? Maps.newLinkedHashMap() : currentNames.names;
                    names.put(localName, Pair.of(remoteName, localName));
                    namesCache.put("", new NamesCacheValue(
                            generation, names, currentNames != null && currentNames.complete));
                    nameUpdateAction.accept(remoteName, localName);
                    metaObjCache.put(localName, Optional.of(obj));
                    idToName.put(id, localName);
                }
                return true;
            });
        });
    }

    // The action runs under the same-name mutation lock and must not mutate another name in this MetaCache.
    // A bulk invalidation may retire the cache while the action runs, so verify the generation afterwards.
    public boolean executeIfMetaObjCurrent(String localName, T expectedObj, BooleanSupplier action) {
        return withMetaObjKeyLock(localName, () -> {
            long generation;
            metaObjLifecycleLock.readLock().lock();
            try {
                Optional<T> cachedObj = metaObjCache.getIfPresent(localName);
                if (cachedObj == null || !cachedObj.isPresent() || cachedObj.get() != expectedObj) {
                    return false;
                }
                generation = metaObjGeneration.get();
            } finally {
                metaObjLifecycleLock.readLock().unlock();
            }
            boolean result = action.getAsBoolean();
            return result && withMetaObjLifecycleReadLock(() -> {
                Optional<T> cachedObj = metaObjCache.getIfPresent(localName);
                return generation == metaObjGeneration.get()
                        && cachedObj != null && cachedObj.isPresent() && cachedObj.get() == expectedObj;
            });
        });
    }

    public void invalidate(String localName, long id) {
        withMetaObjKeyLock(localName, () -> {
            withMetaObjLifecycleReadLock(() -> {
                synchronized (namesMutationLock) {
                    long generation = advanceNamesGeneration();
                    NamesCacheValue currentNames = namesCache.getIfPresent("");
                    if (currentNames != null) {
                        currentNames.names.remove(localName);
                        namesCache.put("", new NamesCacheValue(
                                generation, currentNames.names, currentNames.complete));
                    }
                    nameInvalidationAction.accept(localName);
                    idToName.remove(id);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("invalidate obj in metacache {}, obj name: {}, id: {}",
                            name, localName, id, new Exception());
                }
                metaObjCache.invalidate(localName);
                return null;
            });
            return null;
        });
    }

    public void invalidateNames() {
        synchronized (namesMutationLock) {
            minimumLoadGeneration = advanceNamesGeneration();
            namesCache.invalidateAll();
            namesCacheUpdateAction.accept(Lists.newArrayList());
        }
    }

    public void invalidateObjects() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalidate objects in metacache {}", name, new Exception());
        }
        LoadingCache<String, Optional<T>> retiredCache;
        metaObjLifecycleLock.writeLock().lock();
        try {
            metaObjGeneration.incrementAndGet();
            idToName.clear();
            retiredCache = metaObjCache;
            metaObjCache = buildMetaObjCache();
        } finally {
            metaObjLifecycleLock.writeLock().unlock();
        }
        retiredCache.invalidateAll();
    }

    public void invalidateAll() {
        invalidateNames();
        invalidateObjects();
    }

    @VisibleForTesting
    public LoadingCache<String, Optional<T>> getMetaObjCache() {
        return withMetaObjLifecycleReadLock(() -> metaObjCache);
    }

    @VisibleForTesting
    public void refreshNamesForTest() {
        scheduleNamesRefresh(namesGeneration.get());
    }

    @VisibleForTesting
    public void addObjForTest(long id, String name, T db) {
        withMetaObjLifecycleReadLock(() -> {
            metaObjCache.put(name, Optional.of(db));
            idToName.put(id, name);
            return null;
        });
    }

    private LoadingCache<String, Optional<T>> buildMetaObjCache() {
        return metaObjCacheFactory.buildCacheWithSyncRemovalListener(
                metaObjCacheLoader, metaObjRemovalListener);
    }

    private <R> R withMetaObjLifecycleReadLock(Supplier<R> action) {
        metaObjLifecycleLock.readLock().lock();
        try {
            return action.get();
        } finally {
            metaObjLifecycleLock.readLock().unlock();
        }
    }

    private <R> R withMetaObjKeyLock(String key, Supplier<R> action) {
        MetaObjKeyLock keyLock = acquireMetaObjKeyLock(key);
        try {
            synchronized (keyLock) {
                return action.get();
            }
        } finally {
            releaseMetaObjKeyLock(key);
        }
    }

    private MetaObjKeyLock acquireMetaObjKeyLock(String key) {
        return metaObjKeyLocks.compute(key, (ignored, current) -> {
            MetaObjKeyLock result = current == null ? new MetaObjKeyLock() : current;
            result.users++;
            return result;
        });
    }

    private void releaseMetaObjKeyLock(String key) {
        metaObjKeyLocks.compute(key, (ignored, current) -> {
            current.users--;
            return current.users == 0 ? null : current;
        });
    }

    /**
     * Reset the names cache.
     * Should only be used after creating new database/table
     */
    public void resetNames() {
        synchronized (namesMutationLock) {
            minimumLoadGeneration = advanceNamesGeneration();
            namesCache.invalidateAll();
        }
    }

    private long advanceNamesGeneration() {
        long generation = namesGeneration.incrementAndGet();
        if (activeNamesLoad != null && activeNamesLoad.generation < generation) {
            activeNamesLoad.result.complete(null);
            activeNamesLoad = null;
        }
        return generation;
    }

    private static class NamesCacheValue {
        private final long generation;
        private final Map<String, Pair<String, String>> names;
        private final boolean complete;
        private final long writeNanos;

        private NamesCacheValue(long generation, Map<String, Pair<String, String>> names, boolean complete) {
            this(generation, names, complete, System.nanoTime());
        }

        private NamesCacheValue(long generation, Map<String, Pair<String, String>> names,
                boolean complete, long writeNanos) {
            this.generation = generation;
            this.names = names;
            this.complete = complete;
            this.writeNanos = writeNanos;
        }

        private NamesCacheValue withGeneration(long generation) {
            return new NamesCacheValue(generation, Maps.newLinkedHashMap(names), complete, writeNanos);
        }

        private List<Pair<String, String>> snapshot() {
            return Lists.newArrayList(names.values());
        }
    }

    private static class MetaObjKeyLock {
        private int users;
    }

    private static class NamesLoad {
        private final long generation;
        private final long loadEpoch;
        private final Map<String, Pair<String, String>> incompleteNames;
        private final CompletableFuture<NamesCacheValue> result = new CompletableFuture<>();

        private NamesLoad(long generation, long loadEpoch, Map<String, Pair<String, String>> incompleteNames) {
            this.generation = generation;
            this.loadEpoch = loadEpoch;
            this.incompleteNames = incompleteNames;
        }
    }

    private static class NamesRefresh {
        private final long generation;

        private NamesRefresh(long generation) {
            this.generation = generation;
        }
    }

}
