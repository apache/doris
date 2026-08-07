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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Unified cache entry abstraction.
 * It stores one logical cache dataset and provides optional lazy loading,
 * key/predicate/full invalidation, and lightweight runtime stats.
 */
public class MetaCacheEntry<K, V> {
    private static final int SINGLE_KEY_STRIPES = 1;

    private static final class StripeState<K> {
        // Protect slow miss loads without holding the short-lived publication monitor.
        private final Object loadLock = new Object();
        // Read outside the publication monitor only for the intentionally best-effort async refresh admission.
        private volatile long generation;
        // Retain exact-key state only while loads with auxiliary publication actions are active.
        @Nullable
        private Map<K, ActiveActionState> activeActionLoads;
    }

    private static final class ActiveActionState {
        // All accesses are protected by the owning StripeState monitor.
        private long generation;
        private int referenceCount;
    }

    private static final class ActionPublicationToken<K> {
        private final K key;
        private final ActiveActionState state;
        private final long generation;

        private ActionPublicationToken(K key, ActiveActionState state, long generation) {
            this.key = key;
            this.state = state;
            this.generation = generation;
        }
    }

    private final String name;
    @Nullable
    private final Function<K, V> loader;
    private final CacheSpec cacheSpec;
    private final boolean effectiveEnabled;
    private final boolean autoRefresh;
    private final int stripeCount;
    private final LoadingCache<K, V> loadingData;
    // Use the plain cache view for manual miss load so slow I/O does not happen in Caffeine's sync load path.
    private final Cache<K, V> data;
    // Lazily allocate coordination state for active stripes; StripeState itself is the publication monitor.
    private final AtomicReferenceArray<StripeState<K>> stripeStates;
    private final AtomicLong invalidateCount = new AtomicLong(0);
    // Track load statistics outside Caffeine because manual miss loads bypass the built-in load counters.
    private final AtomicLong loadSuccessCount = new AtomicLong(0);
    private final AtomicLong loadFailureCount = new AtomicLong(0);
    private final AtomicLong totalLoadTimeNanos = new AtomicLong(0);
    private final AtomicLong lastLoadSuccessTimeMs = new AtomicLong(-1L);
    private final AtomicLong lastLoadFailureTimeMs = new AtomicLong(-1L);
    private final AtomicReference<String> lastError = new AtomicReference<>("");

    public MetaCacheEntry(String name, Function<K, V> loader, CacheSpec cacheSpec, ExecutorService refreshExecutor) {
        this(name, loader, cacheSpec, refreshExecutor, true, false, defaultObjectStripeCount(), null, false);
    }

    public MetaCacheEntry(String name, Function<K, V> loader, CacheSpec cacheSpec, ExecutorService refreshExecutor,
            boolean autoRefresh) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, false, defaultObjectStripeCount(), null, false);
    }

    public MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, contextualOnly,
                defaultObjectStripeCount(), null, false);
    }

    public MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly,
            @Nullable MetaCacheSizeEstimator<K, V> sizeEstimator) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, contextualOnly,
                defaultObjectStripeCount(), sizeEstimator, null, false);
    }

    public MetaCacheEntry(String name, Function<K, V> loader, CacheSpec cacheSpec, ExecutorService refreshExecutor,
            boolean autoRefresh, int stripeCount) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, false, stripeCount, null, false);
    }

    public MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly, int stripeCount) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, contextualOnly, stripeCount, null, false);
    }

    public MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly, int stripeCount,
            @Nullable MetaCacheSizeEstimator<K, V> sizeEstimator) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, contextualOnly,
                stripeCount, sizeEstimator, null, false);
    }

    public static <K, V> MetaCacheEntry<K, V> withSyncRemovalListener(String name, Function<K, V> loader,
            CacheSpec cacheSpec, ExecutorService refreshExecutor, RemovalListener<K, V> removalListener) {
        return withSyncRemovalListener(name, loader, cacheSpec, refreshExecutor,
                defaultObjectStripeCount(), removalListener);
    }

    public static <K, V> MetaCacheEntry<K, V> withSyncRemovalListener(String name, Function<K, V> loader,
            CacheSpec cacheSpec, ExecutorService refreshExecutor, int stripeCount,
            RemovalListener<K, V> removalListener) {
        return new MetaCacheEntry<>(
                name,
                loader,
                cacheSpec,
                refreshExecutor,
                false,
                false,
                stripeCount,
                Objects.requireNonNull(removalListener, "removalListener can not be null"),
                true);
    }

    public static <K, V> MetaCacheEntry<K, V> withSyncRemovalListener(String name, Function<K, V> loader,
            CacheSpec cacheSpec, ExecutorService refreshExecutor, MetaCacheSizeEstimator<K, V> sizeEstimator,
            RemovalListener<K, V> removalListener) {
        return withSyncRemovalListener(name, loader, cacheSpec, refreshExecutor,
                defaultObjectStripeCount(), sizeEstimator, removalListener);
    }

    public static <K, V> MetaCacheEntry<K, V> withSyncRemovalListener(String name, Function<K, V> loader,
            CacheSpec cacheSpec, ExecutorService refreshExecutor, int stripeCount,
            MetaCacheSizeEstimator<K, V> sizeEstimator, RemovalListener<K, V> removalListener) {
        return new MetaCacheEntry<>(
                name,
                loader,
                cacheSpec,
                refreshExecutor,
                false,
                false,
                stripeCount,
                Objects.requireNonNull(sizeEstimator, "sizeEstimator can not be null"),
                Objects.requireNonNull(removalListener, "removalListener can not be null"),
                true);
    }

    private MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly,
            int stripeCount, @Nullable RemovalListener<K, V> removalListener, boolean syncRemovalListener) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, contextualOnly,
                stripeCount, null, removalListener, syncRemovalListener);
    }

    private MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly,
            int stripeCount, @Nullable MetaCacheSizeEstimator<K, V> sizeEstimator,
            @Nullable RemovalListener<K, V> removalListener, boolean syncRemovalListener) {
        this.name = Objects.requireNonNull(name, "name can not be null");
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
        if (syncRemovalListener && autoRefresh) {
            throw new IllegalArgumentException("sync removal listener cache can not enable refreshAfterWrite");
        }
        if (removalListener != null && !syncRemovalListener) {
            throw new IllegalArgumentException("asynchronous removal listener is not supported");
        }
        this.loader = loader;
        this.cacheSpec = Objects.requireNonNull(cacheSpec, "cacheSpec can not be null");
        this.autoRefresh = autoRefresh;
        if (cacheSpec.isWeightBounded() && sizeEstimator == null) {
            throw new IllegalArgumentException("max-weight requires an entry size estimator: " + name);
        }
        if (stripeCount < 1) {
            throw new IllegalArgumentException("stripeCount must be positive");
        }
        this.stripeCount = stripeCount;
        this.stripeStates = new AtomicReferenceArray<>(stripeCount);
        if (stripeCount == SINGLE_KEY_STRIPES) {
            // Names entries always use their sole stripe, so keep their established allocation behavior.
            stripeStates.set(0, new StripeState<>());
        }
        Objects.requireNonNull(refreshExecutor, "refreshExecutor can not be null");
        this.effectiveEnabled = this.cacheSpec.isCacheEnabled();
        OptionalLong expireAfterAccessSec =
                effectiveEnabled ? CacheSpec.toExpireAfterAccess(this.cacheSpec.getTtlSecond()) : OptionalLong.empty();
        OptionalLong refreshAfterWriteSec =
                effectiveEnabled && autoRefresh
                        ? OptionalLong.of(Config.external_cache_refresh_time_minutes * 60)
                        : OptionalLong.empty();
        long maxSize = effectiveEnabled && !cacheSpec.isWeightBounded() ? this.cacheSpec.getCapacity() : 0L;
        CacheFactory cacheFactory = new CacheFactory(
                expireAfterAccessSec,
                refreshAfterWriteSec,
                maxSize,
                true,
                null);
        // Build through a dedicated loader so refresh results admitted under an older generation are rejected.
        CacheLoader<K, V> cacheLoader = newCacheLoader();
        if (cacheSpec.isWeightBounded()) {
            long maxWeight = effectiveEnabled ? cacheSpec.getMaxWeight().getAsLong() : 0L;
            if (syncRemovalListener) {
                this.loadingData = cacheFactory.buildCacheWithWeightAndSyncRemovalListener(
                        cacheLoader,
                        maxWeight,
                        (key, value) -> toCaffeineWeight(sizeEstimator.estimateBytes(key, value)),
                        removalListener);
            } else {
                this.loadingData = cacheFactory.buildCacheWithWeight(
                        cacheLoader,
                        refreshExecutor,
                        maxWeight,
                        (key, value) -> toCaffeineWeight(sizeEstimator.estimateBytes(key, value)));
            }
        } else if (syncRemovalListener) {
            this.loadingData = cacheFactory.buildCacheWithSyncRemovalListener(cacheLoader, removalListener);
        } else {
            this.loadingData = cacheFactory.buildCache(cacheLoader, refreshExecutor);
        }
        this.data = loadingData;
    }

    public String name() {
        return name;
    }

    private int toCaffeineWeight(long estimatedBytes) {
        if (estimatedBytes < 0L) {
            throw new IllegalStateException("entry size estimator returned a negative weight: " + name);
        }
        return estimatedBytes >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) estimatedBytes;
    }

    public V get(K key) {
        return getWithManualLoad(key, this::applyDefaultLoader, null, null);
    }

    public V get(K key, Function<K, V> missLoader) {
        Function<K, V> loadFunction = Objects.requireNonNull(missLoader, "missLoader can not be null");
        return getWithManualLoad(key, loadFunction, null, null);
    }

    /**
     * Get the current value and run a short local action under the same per-key publication protocol.
     *
     * <p>For an enabled entry, a hot-value action only runs while that value remains current. A miss-load action uses
     * an exact-key fence, so an unrelated mutation in the same stripe may reject object publication without
     * suppressing the action. For a disabled entry, the value is not cached, but the exact-key action can still run.
     */
    public V getAndRunIfCurrent(K key, BiConsumer<K, V> currentValueAction) {
        return getAndRunIfCurrent(key, (ignored, value) -> true, currentValueAction);
    }

    /**
     * Get the current value and conditionally run a short local action under the per-key publication protocol.
     *
     * <p>A hot value returns without entering the publication lock when {@code actionRequired} is false. When the
     * action is required, both the value identity and the condition are re-checked under the lock before running it.
     * Both callbacks must be short, deterministic, non-blocking local operations. They must not perform remote I/O,
     * call back into this entry, mutate its object cache or active action state, or acquire locks that reverse the
     * caller's lock order. The action is intended only for local auxiliary state such as an ID-to-name index.
     */
    public V getAndRunIfCurrent(K key, BiPredicate<K, V> actionRequired,
            BiConsumer<K, V> currentValueAction) {
        BiPredicate<K, V> required = Objects.requireNonNull(
                actionRequired, "actionRequired can not be null");
        BiConsumer<K, V> action = Objects.requireNonNull(
                currentValueAction, "currentValueAction can not be null");
        return getWithManualLoad(key, this::applyDefaultLoader, required, action);
    }

    public V getIfPresent(K key) {
        if (!effectiveEnabled) {
            return null;
        }
        return data.getIfPresent(key);
    }

    @Nullable
    public V findIfPresent(Predicate<K> keyPredicate) {
        if (!effectiveEnabled) {
            return null;
        }
        // Replay-only fallback needs a cache-only scan over current hot keys without triggering load-through.
        for (java.util.Map.Entry<K, V> entry : data.asMap().entrySet()) {
            if (keyPredicate.test(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    public void put(K key, V value) {
        // Public mutations advance the generation so loads admitted under an older generation cannot overwrite them.
        Objects.requireNonNull(key, "key can not be null");
        Objects.requireNonNull(value, "value can not be null");
        if (!effectiveEnabled) {
            return;
        }
        StripeState<K> state = stripeState(key);
        synchronized (state) {
            bumpGenerationLocked(state);
            bumpActiveActionGenerationLocked(state, key);
            beforePublicMutationWriteForTest(key);
            data.put(key, value);
        }
    }

    public V compute(K key, BiFunction<K, V, V> remappingFunction) {
        // Public compute must also advance the stripe generation before mutating the cache state.
        Objects.requireNonNull(key, "key can not be null");
        Objects.requireNonNull(remappingFunction, "remappingFunction can not be null");
        if (!effectiveEnabled) {
            return null;
        }
        StripeState<K> state = stripeState(key);
        synchronized (state) {
            bumpGenerationLocked(state);
            bumpActiveActionGenerationLocked(state, key);
            beforePublicMutationWriteForTest(key);
            return data.asMap().compute(key, remappingFunction);
        }
    }

    /**
     * Compute the cached value and update related local state in one per-key publication window.
     *
     * <p>The action always runs, including when the cache is disabled or the remapping function keeps a cold key
     * absent. This allows callers to maintain lightweight auxiliary indexes without warming the object entry.
     */
    public V computeAndRun(K key, BiFunction<K, V, V> remappingFunction, Runnable afterMutation) {
        Objects.requireNonNull(key, "key can not be null");
        Objects.requireNonNull(remappingFunction, "remappingFunction can not be null");
        Runnable action = Objects.requireNonNull(afterMutation, "afterMutation can not be null");
        StripeState<K> state = stripeState(key);
        synchronized (state) {
            bumpGenerationLocked(state);
            bumpActiveActionGenerationLocked(state, key);
            beforePublicMutationWriteForTest(key);
            V value = effectiveEnabled ? data.asMap().compute(key, remappingFunction) : null;
            action.run();
            return value;
        }
    }

    public void invalidateKey(K key) {
        invalidateKeyAndRun(key, () -> {
        });
    }

    /**
     * Invalidate one cached key and update related local state in the same publication window.
     *
     * <p>The action runs even if the object entry is already absent or disabled because auxiliary indexes may
     * intentionally outlive object-cache eviction.
     */
    public void invalidateKeyAndRun(K key, Runnable afterInvalidation) {
        Objects.requireNonNull(key, "key can not be null");
        Runnable action = Objects.requireNonNull(afterInvalidation, "afterInvalidation can not be null");
        // A cold explicit invalidation intentionally initializes and retains its bounded stripe state so future
        // publication for the same stripe observes this generation change.
        StripeState<K> state = stripeState(key);
        synchronized (state) {
            bumpGenerationLocked(state);
            bumpActiveActionGenerationLocked(state, key);
            if (data.asMap().remove(key) != null) {
                invalidateCount.incrementAndGet();
            }
            action.run();
        }
    }

    public void invalidateIf(Predicate<K> predicate) {
        Objects.requireNonNull(predicate, "predicate can not be null");
        // The predicate must be a short, deterministic, non-throwing local-only check. It must not block, perform
        // remote I/O, or call back into this entry because it runs while holding each active stripe monitor.
        bumpForInvalidateIf(predicate);
        for (K key : data.asMap().keySet()) {
            if (predicate.test(key)) {
                invalidateKey(key);
            }
        }
    }

    public void invalidateAll() {
        // Cover in-flight manual loads whose keys are still outside the cache map.
        bumpForInvalidateAll();
        for (K key : data.asMap().keySet()) {
            invalidateKey(key);
        }
    }

    public void forEach(BiConsumer<K, V> consumer) {
        data.asMap().forEach(consumer);
    }

    public MetaCacheEntryStats stats() {
        // Keep statistics reads lightweight and side-effect free. Caffeine policy values may be briefly stale while
        // asynchronous maintenance is pending; querying stats must not trigger expiration or removal listeners.
        CacheStats cacheStats = loadingData.stats();
        Policy.Eviction<K, V> evictionPolicy = loadingData.policy().eviction()
                .orElseThrow(() -> new IllegalStateException("cache has no eviction policy: " + name));
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
                cacheSpec.isWeightBounded(),
                cacheSpec.getMaxWeight().orElse(-1L),
                data.estimatedSize(),
                evictionPolicy.weightedSize().orElse(-1L),
                cacheStats.requestCount(),
                cacheStats.hitCount(),
                cacheStats.missCount(),
                cacheStats.hitRate(),
                successCount,
                failureCount,
                totalLoadTime,
                totalLoadCount == 0 ? 0D : (double) totalLoadTime / totalLoadCount,
                cacheStats.evictionCount(),
                cacheStats.evictionWeight(),
                invalidateCount.get(),
                lastLoadSuccessTimeMs.get(),
                lastLoadFailureTimeMs.get(),
                lastError.get());
    }

    // Execute slow miss loads outside Caffeine's sync load path and suppress stale write-back after invalidation.
    private V getWithManualLoad(K key, Function<K, V> loadFunction,
            @Nullable BiPredicate<K, V> currentValueActionRequired,
            @Nullable BiConsumer<K, V> currentValueAction) {
        if (!effectiveEnabled) {
            if (currentValueAction == null) {
                // Preserve the ordinary disabled-entry path without adding publication coordination.
                return loadAndTrack(key, loadFunction);
            }
            return loadAndRunCurrentValueActionForDisabledEntry(
                    key, loadFunction, currentValueActionRequired, currentValueAction);
        }

        V value = data.getIfPresent(key);
        if (value != null) {
            runCurrentValueActionIfPresent(
                    key, value, currentValueActionRequired, currentValueAction);
            return value;
        }

        // Keep the slow miss load under the per-key load lock so concurrent misses for the same key
        // are still deduplicated. The StripeState monitor only protects the short publication window.
        StripeState<K> state = stripeState(key);
        synchronized (state.loadLock) {
            value = data.asMap().get(key);
            if (value != null) {
                runCurrentValueActionIfPresent(
                        state, key, value, currentValueActionRequired, currentValueAction);
                return value;
            }

            long objectGeneration = 0L;
            ActionPublicationToken<K> actionToken = null;
            V observedValue;
            synchronized (state) {
                observedValue = data.asMap().get(key);
                if (observedValue == null) {
                    objectGeneration = generationOf(state);
                    if (currentValueAction != null) {
                        actionToken = beginActionLoadLocked(state, key);
                    }
                }
            }

            if (observedValue != null) {
                runCurrentValueActionIfPresent(
                        state, key, observedValue, currentValueActionRequired, currentValueAction);
                return observedValue;
            }

            try {
                V loaded = loadAndTrack(key, loadFunction);
                if (loaded == null) {
                    return null;
                }
                if (actionToken != null) {
                    beforeCurrentValueActionForTest(key, loaded);
                }
                publishLoadedValueAndAction(
                        state,
                        key,
                        loaded,
                        objectGeneration,
                        actionToken,
                        currentValueActionRequired,
                        currentValueAction);
                return loaded;
            } finally {
                if (actionToken != null) {
                    releaseActionLoad(state, actionToken);
                }
            }
        }
    }

    private V loadAndRunCurrentValueActionForDisabledEntry(
            K key,
            Function<K, V> loadFunction,
            BiPredicate<K, V> currentValueActionRequired,
            BiConsumer<K, V> currentValueAction) {
        // Disabled entries never publish object values, but their local auxiliary-index action still needs an
        // exact-key fence. Unrelated mutations in the same stripe must not suppress that action.
        StripeState<K> state = stripeState(key);
        ActionPublicationToken<K> actionToken;
        synchronized (state) {
            actionToken = beginActionLoadLocked(state, key);
        }
        try {
            V loaded = loadAndTrack(key, loadFunction);
            if (loaded == null) {
                return null;
            }
            beforeCurrentValueActionForTest(key, loaded);
            synchronized (state) {
                runCurrentValueActionIfTokenCurrentLocked(
                        state,
                        actionToken,
                        key,
                        loaded,
                        currentValueActionRequired,
                        currentValueAction);
            }
            return loaded;
        } finally {
            releaseActionLoad(state, actionToken);
        }
    }

    private void publishLoadedValueAndAction(
            StripeState<K> state,
            K key,
            V loaded,
            long objectGeneration,
            @Nullable ActionPublicationToken<K> actionToken,
            @Nullable BiPredicate<K, V> currentValueActionRequired,
            @Nullable BiConsumer<K, V> currentValueAction) {
        synchronized (state) {
            if (objectGeneration == generationOf(state)) {
                // Leave a narrow hook for tests to exercise a reentrant invalidation before publication.
                beforeManualCachePutForTest(key, loaded);
                putLoadedValueWithoutGenerationBump(key, loaded);
                if (objectGeneration != generationOf(state)) {
                    removeLoadedValueWithoutGenerationBump(key, loaded);
                }
            }
            if (actionToken != null) {
                runCurrentValueActionIfTokenCurrentLocked(
                        state,
                        actionToken,
                        key,
                        loaded,
                        currentValueActionRequired,
                        currentValueAction);
            }
        }
    }

    private ActionPublicationToken<K> beginActionLoadLocked(StripeState<K> state, K key) {
        if (state.activeActionLoads == null) {
            state.activeActionLoads = Maps.newHashMap();
        }
        ActiveActionState activeState =
                state.activeActionLoads.computeIfAbsent(key, ignored -> new ActiveActionState());
        activeState.referenceCount++;
        return new ActionPublicationToken<>(key, activeState, activeState.generation);
    }

    private boolean isActionTokenCurrentLocked(
            StripeState<K> state, ActionPublicationToken<K> actionToken) {
        ActiveActionState currentState = state.activeActionLoads == null
                ? null
                : state.activeActionLoads.get(actionToken.key);
        return currentState == actionToken.state
                && currentState.generation == actionToken.generation;
    }

    private void bumpActiveActionGenerationLocked(StripeState<K> state, K key) {
        if (state.activeActionLoads == null) {
            return;
        }
        ActiveActionState activeState = state.activeActionLoads.get(key);
        if (activeState != null) {
            activeState.generation++;
        }
    }

    private void releaseActionLoad(
            StripeState<K> state, ActionPublicationToken<K> actionToken) {
        synchronized (state) {
            Preconditions.checkState(state.activeActionLoads != null);
            ActiveActionState currentState = state.activeActionLoads.get(actionToken.key);
            Preconditions.checkState(currentState == actionToken.state);
            Preconditions.checkState(actionToken.state.referenceCount > 0);
            actionToken.state.referenceCount--;
            if (actionToken.state.referenceCount == 0) {
                state.activeActionLoads.remove(actionToken.key);
                if (state.activeActionLoads.isEmpty()) {
                    state.activeActionLoads = null;
                }
            }
        }
    }

    private void runCurrentValueActionIfTokenCurrentLocked(
            StripeState<K> state,
            ActionPublicationToken<K> actionToken,
            K key,
            V loaded,
            BiPredicate<K, V> currentValueActionRequired,
            BiConsumer<K, V> currentValueAction) {
        if (!isActionTokenCurrentLocked(state, actionToken)) {
            return;
        }
        if (!currentValueActionRequired.test(key, loaded)) {
            return;
        }
        // Re-check after the caller predicate in case an accidental reentrant callback changed the token.
        if (isActionTokenCurrentLocked(state, actionToken)) {
            currentValueAction.accept(key, loaded);
        }
    }

    private void runCurrentValueActionIfPresent(K key, V value,
            @Nullable BiPredicate<K, V> currentValueActionRequired,
            @Nullable BiConsumer<K, V> currentValueAction) {
        if (currentValueAction == null) {
            return;
        }
        if (!currentValueActionRequired.test(key, value)) {
            return;
        }
        runRequiredCurrentValueActionIfPresent(
                stripeState(key), key, value, currentValueActionRequired, currentValueAction);
    }

    private void runCurrentValueActionIfPresent(StripeState<K> state, K key, V value,
            @Nullable BiPredicate<K, V> currentValueActionRequired,
            @Nullable BiConsumer<K, V> currentValueAction) {
        if (currentValueAction == null) {
            return;
        }
        if (!currentValueActionRequired.test(key, value)) {
            return;
        }
        runRequiredCurrentValueActionIfPresent(
                state, key, value, currentValueActionRequired, currentValueAction);
    }

    private void runRequiredCurrentValueActionIfPresent(StripeState<K> state, K key, V value,
            BiPredicate<K, V> currentValueActionRequired, BiConsumer<K, V> currentValueAction) {
        beforeCurrentValueActionForTest(key, value);
        synchronized (state) {
            if (data.asMap().get(key) == value
                    && currentValueActionRequired.test(key, value)) {
                currentValueAction.accept(key, value);
            }
        }
    }

    // Keep internal load write-back separate from public mutation so it does not advance generation.
    private void putLoadedValueWithoutGenerationBump(K key, V loaded) {
        data.put(key, loaded);
    }

    // Remove only the value loaded by the current request and keep newer replacements intact.
    private void removeLoadedValueWithoutGenerationBump(K key, V loaded) {
        data.asMap().computeIfPresent(key, (ignored, currentValue) -> currentValue == loaded ? null : currentValue);
    }

    private CacheLoader<K, V> newCacheLoader() {
        return new CacheLoader<K, V>() {
            @Override
            public V load(K key) {
                return loadFromDefaultLoader(key);
            }

            @Override
            public CompletableFuture<V> asyncReload(K key, V oldValue, Executor executor) {
                // This fences refreshes admitted before a later generation bump. Admission intentionally remains
                // outside the publication monitor: a refresh admitted after the bump but before key removal may
                // repopulate the key, which is accepted under the external metadata cache's eventual-consistency
                // semantics.
                StripeState<K> state = stripeState(key);
                long generation = generationOf(state);
                CompletableFuture<V> result = new CompletableFuture<>();
                CompletableFuture.supplyAsync(() -> loadFromDefaultLoader(key), executor)
                        .whenComplete((loaded, error) -> {
                            if (error != null) {
                                result.completeExceptionally(error);
                                return;
                            }
                            synchronized (state) {
                                if (generation == generationOf(state)) {
                                    result.complete(loaded);
                                } else {
                                    result.cancel(false);
                                }
                            }
                        });
                return result;
            }
        };
    }

    private int stripe(K key) {
        int hash = key == null ? 0 : key.hashCode();
        return (hash & Integer.MAX_VALUE) % stripeCount;
    }

    private StripeState<K> stripeState(K key) {
        int index = stripe(key);
        StripeState<K> state = stripeStates.get(index);
        if (state != null) {
            return state;
        }
        StripeState<K> created = new StripeState<>();
        if (stripeStates.compareAndSet(index, null, created)) {
            return created;
        }
        // A failed CAS must use the retained state installed by the winner; states are never removed or replaced.
        return stripeStates.get(index);
    }

    private long generationOf(StripeState<K> state) {
        return state.generation;
    }

    private void bumpGenerationLocked(StripeState<K> state) {
        state.generation++;
    }

    private void bumpForInvalidateIf(Predicate<K> predicate) {
        for (int i = 0; i < stripeCount; i++) {
            StripeState<K> state = stripeStates.get(i);
            if (state == null) {
                continue;
            }
            synchronized (state) {
                bumpGenerationLocked(state);
                if (state.activeActionLoads == null) {
                    continue;
                }
                for (Map.Entry<K, ActiveActionState> entry : state.activeActionLoads.entrySet()) {
                    if (predicate.test(entry.getKey())) {
                        entry.getValue().generation++;
                    }
                }
            }
        }
    }

    private void bumpForInvalidateAll() {
        // Only initialized stripe states need to be bumped. Any operation that can later publish has already created
        // and retained its StripeState. A state installed after its slot is scanned is a post-invalidation admission.
        // Stripe states are never removed or replaced.
        for (int i = 0; i < stripeCount; i++) {
            StripeState<K> state = stripeStates.get(i);
            if (state == null) {
                continue;
            }
            synchronized (state) {
                bumpGenerationLocked(state);
                if (state.activeActionLoads != null) {
                    for (ActiveActionState activeState : state.activeActionLoads.values()) {
                        activeState.generation++;
                    }
                }
            }
        }
    }

    public static int defaultObjectStripeCount() {
        return Config.external_meta_cache_object_entry_lock_stripes;
    }

    public static int singleKeyStripeCount() {
        return SINGLE_KEY_STRIPES;
    }

    int stripeCountForTest() {
        return stripeCount;
    }

    int initializedStripeCountForTest() {
        int count = 0;
        for (int i = 0; i < stripeCount; i++) {
            if (stripeStates.get(i) != null) {
                count++;
            }
        }
        return count;
    }

    int activeActionReferenceCountForTest() {
        int count = 0;
        for (int i = 0; i < stripeCount; i++) {
            StripeState<K> state = stripeStates.get(i);
            if (state == null) {
                continue;
            }
            synchronized (state) {
                if (state.activeActionLoads != null) {
                    for (ActiveActionState activeState : state.activeActionLoads.values()) {
                        count += activeState.referenceCount;
                    }
                }
            }
        }
        return count;
    }

    // Let tests pause between the first generation check and data.put without affecting production behavior.
    void beforeManualCachePutForTest(K key, V loaded) {
    }

    void beforePublicMutationWriteForTest(K key) {
    }

    // Let tests pause after a hot value is observed but before its related local state is published.
    protected void beforeCurrentValueActionForTest(K key, V value) {
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
}
