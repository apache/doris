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

import org.apache.doris.common.Config;
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.MetaCacheRemovalReason;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.cache.ScopedMetaCache.CacheMetrics;

import com.github.benmanes.caffeine.cache.RemovalListener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * FE naming-cache adapter over the shared connector-cache runtime.
 *
 * <p>The common runtime owns value storage, load deduplication, generations, eviction and leak-free indexes. This
 * adapter only retains the short per-key publication window needed to update FE's auxiliary ID/name indexes together
 * with a database or table object.
 */
public class MetaCacheEntry<K, V> {
    private static final int SINGLE_KEY_STRIPES = 1;

    private static final class StripeState<K> {
        @Nullable
        private Map<K, ActionState> activeActions;
    }

    private static final class ActionState {
        private long generation;
        private int references;
    }

    private static final class ActionToken<K> {
        private final K key;
        private final ActionState state;
        private final long generation;

        private ActionToken(K key, ActionState state) {
            this.key = key;
            this.state = state;
            this.generation = state.generation;
        }
    }

    private final String name;
    @Nullable
    private final Function<K, V> loader;
    private final CacheSpec cacheSpec;
    private final boolean effectiveEnabled;
    private final boolean autoRefresh;
    private final int stripeCount;
    private final AtomicReferenceArray<StripeState<K>> stripeStates;
    private final CatalogMetaCache owner = new CatalogMetaCache();
    private final MetaCache<K, V> data;

    public MetaCacheEntry(String name, Function<K, V> loader, CacheSpec cacheSpec, ExecutorService refreshExecutor) {
        this(name, loader, cacheSpec, refreshExecutor, true, false, defaultObjectStripeCount(), null);
    }

    public MetaCacheEntry(String name, Function<K, V> loader, CacheSpec cacheSpec, ExecutorService refreshExecutor,
            boolean autoRefresh) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, false, defaultObjectStripeCount(), null);
    }

    public MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, contextualOnly,
                defaultObjectStripeCount(), null);
    }

    public MetaCacheEntry(String name, Function<K, V> loader, CacheSpec cacheSpec, ExecutorService refreshExecutor,
            boolean autoRefresh, int stripeCount) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, false, stripeCount, null);
    }

    public MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly, int stripeCount) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, contextualOnly, stripeCount, null);
    }

    public static <K, V> MetaCacheEntry<K, V> withSyncRemovalListener(String name, Function<K, V> loader,
            CacheSpec cacheSpec, ExecutorService refreshExecutor, RemovalListener<K, V> removalListener) {
        return withSyncRemovalListener(name, loader, cacheSpec, refreshExecutor,
                defaultObjectStripeCount(), removalListener);
    }

    public static <K, V> MetaCacheEntry<K, V> withSyncRemovalListener(String name, Function<K, V> loader,
            CacheSpec cacheSpec, ExecutorService refreshExecutor, int stripeCount,
            RemovalListener<K, V> removalListener) {
        return new MetaCacheEntry<>(name, loader, cacheSpec, refreshExecutor, false, false, stripeCount,
                Objects.requireNonNull(removalListener, "removalListener can not be null"));
    }

    private MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly, int stripeCount,
            @Nullable RemovalListener<K, V> removalListener) {
        this.name = Objects.requireNonNull(name, "name can not be null");
        this.loader = loader;
        this.cacheSpec = Objects.requireNonNull(cacheSpec, "cacheSpec can not be null");
        this.autoRefresh = autoRefresh;
        Objects.requireNonNull(refreshExecutor, "refreshExecutor can not be null");
        if (contextualOnly && loader != null) {
            throw new IllegalArgumentException("contextual-only entry loader must be null");
        }
        if (contextualOnly && autoRefresh) {
            throw new IllegalArgumentException("contextual-only entry can not enable auto refresh");
        }
        if (!contextualOnly) {
            Objects.requireNonNull(loader, "loader can not be null");
        }
        if (removalListener != null && autoRefresh) {
            throw new IllegalArgumentException("sync removal listener cache can not enable refreshAfterWrite");
        }
        if (stripeCount < 1) {
            throw new IllegalArgumentException("stripeCount must be positive");
        }
        this.stripeCount = stripeCount;
        stripeStates = new AtomicReferenceArray<>(stripeCount);
        if (stripeCount == SINGLE_KEY_STRIPES) {
            stripeStates.set(0, new StripeState<>());
        }
        effectiveEnabled = CacheSpec.isCacheEnabled(
                cacheSpec.isEnable(), cacheSpec.getTtlSecond(), cacheSpec.getCapacity());
        MetaCacheDefinition.Builder<K, V> builder = MetaCacheDefinition.builder(
                name, cacheSpec, ignored -> ScopePath.catalog());
        if (loader != null) {
            builder.loader(key -> loadAndPause(key, loader));
        }
        if (removalListener != null) {
            builder.removalListener((key, value, reason) ->
                    removalListener.onRemoval(key, value, toCaffeineRemovalCause(reason)));
        }
        if (autoRefresh && Config.external_cache_refresh_time_minutes > 0) {
            builder.refreshAfterWrite(
                    Duration.ofMinutes(Config.external_cache_refresh_time_minutes), refreshExecutor);
        }
        data = owner.create(builder.build());
    }

    public String name() {
        return name;
    }

    public V get(K key) {
        if (loader == null) {
            throw new UnsupportedOperationException(String.format(
                    "Entry '%s' requires a contextual miss loader.", name));
        }
        return data.get(key);
    }

    public V get(K key, Function<K, V> missLoader) {
        Function<K, V> nonNullLoader = Objects.requireNonNull(missLoader, "missLoader can not be null");
        return data.get(key, loadKey -> loadAndPause(loadKey, nonNullLoader));
    }

    public V getAndRunIfCurrent(K key, BiConsumer<K, V> currentValueAction) {
        return getAndRunIfCurrent(key, (ignored, value) -> true, currentValueAction);
    }

    public V getAndRunIfCurrent(K key, BiPredicate<K, V> actionRequired,
            BiConsumer<K, V> currentValueAction) {
        BiPredicate<K, V> required = Objects.requireNonNull(actionRequired, "actionRequired can not be null");
        BiConsumer<K, V> action = Objects.requireNonNull(currentValueAction, "currentValueAction can not be null");
        V cached = data.getIfPresent(key);
        if (cached != null && !required.test(key, cached)) {
            return cached;
        }
        StripeState<K> stripe = stripeState(key);
        ActionToken<K> token;
        synchronized (stripe) {
            token = beginAction(stripe, key);
        }
        try {
            V value = cached == null ? get(key) : cached;
            if (value == null) {
                return null;
            }
            if (!required.test(key, value)) {
                return value;
            }
            beforeCurrentValueActionForTest(key, value);
            synchronized (stripe) {
                boolean current = isCurrent(stripe, token)
                        && (!effectiveEnabled || data.getIfPresent(key) == value);
                if (current && required.test(key, value) && isCurrent(stripe, token)) {
                    try {
                        action.accept(key, value);
                    } catch (RuntimeException | Error throwable) {
                        data.invalidateKey(key);
                        throw throwable;
                    }
                }
            }
            return value;
        } finally {
            synchronized (stripe) {
                endAction(stripe, token);
            }
        }
    }

    public V getIfPresent(K key) {
        return data.getIfPresent(key);
    }

    @Nullable
    public V findIfPresent(Predicate<K> keyPredicate) {
        Objects.requireNonNull(keyPredicate, "keyPredicate can not be null");
        List<V> result = new ArrayList<>(1);
        data.forEach((key, value) -> {
            if (result.isEmpty() && keyPredicate.test(key)) {
                result.add(value);
            }
        });
        return result.isEmpty() ? null : result.get(0);
    }

    public void put(K key, V value) {
        Objects.requireNonNull(value, "value can not be null");
        StripeState<K> stripe = stripeState(key);
        synchronized (stripe) {
            bumpAction(stripe, key);
            beforePublicMutationWriteForTest(key);
            data.put(key, value);
        }
    }

    public V compute(K key, BiFunction<K, V, V> remappingFunction) {
        return computeAndRun(key, remappingFunction, () -> {
        });
    }

    public V computeAndRun(K key, BiFunction<K, V, V> remappingFunction, Runnable afterMutation) {
        BiFunction<K, V, V> remapper = Objects.requireNonNull(remappingFunction, "remappingFunction can not be null");
        Runnable action = Objects.requireNonNull(afterMutation, "afterMutation can not be null");
        StripeState<K> stripe = stripeState(key);
        synchronized (stripe) {
            bumpAction(stripe, key);
            beforePublicMutationWriteForTest(key);
            V updated = effectiveEnabled ? remapper.apply(key, data.getIfPresent(key)) : null;
            data.invalidateKey(key);
            if (updated != null) {
                data.put(key, updated);
            }
            action.run();
            return updated;
        }
    }

    public V computeAfterValidation(K key, BiFunction<K, V, V> remappingFunction, Runnable validationAction) {
        BiFunction<K, V, V> remapper = Objects.requireNonNull(remappingFunction, "remappingFunction can not be null");
        Runnable validation = Objects.requireNonNull(validationAction, "validationAction can not be null");
        StripeState<K> stripe = stripeState(key);
        synchronized (stripe) {
            V updated = effectiveEnabled ? remapper.apply(key, data.getIfPresent(key)) : null;
            validation.run();
            bumpAction(stripe, key);
            beforePublicMutationWriteForTest(key);
            data.invalidateKey(key);
            if (updated != null) {
                data.put(key, updated);
            }
            return updated;
        }
    }

    public void invalidateKey(K key) {
        invalidateKeyAndRun(key, () -> {
        });
    }

    public void invalidateKeyAndRun(K key, Runnable afterInvalidation) {
        Runnable action = Objects.requireNonNull(afterInvalidation, "afterInvalidation can not be null");
        StripeState<K> stripe = stripeState(key);
        synchronized (stripe) {
            bumpAction(stripe, key);
            data.invalidateKey(key);
            action.run();
        }
    }

    public void invalidateAll() {
        for (int i = 0; i < stripeCount; i++) {
            StripeState<K> stripe = stripeStates.get(i);
            if (stripe != null) {
                synchronized (stripe) {
                    if (stripe.activeActions != null) {
                        stripe.activeActions.values().forEach(state -> state.generation++);
                    }
                }
            }
        }
        owner.invalidateCatalog();
    }

    public void forEach(BiConsumer<K, V> consumer) {
        data.forEach(consumer);
    }

    public MetaCacheEntryStats stats() {
        CacheMetrics metrics = data.metrics();
        long requests = metrics.getRequestCount();
        long loads = metrics.getLoadSuccessCount() + metrics.getLoadFailureCount();
        return new MetaCacheEntryStats(
                cacheSpec.isEnable(), effectiveEnabled, autoRefresh, cacheSpec.getTtlSecond(), cacheSpec.getCapacity(),
                metrics.getPhysicalEntryCount(), requests, metrics.getHitCount(), metrics.getMissCount(),
                requests == 0L ? 0D : (double) metrics.getHitCount() / requests,
                metrics.getLoadSuccessCount(), metrics.getLoadFailureCount(), metrics.getTotalLoadTimeNanos(),
                loads == 0L ? 0D : (double) metrics.getTotalLoadTimeNanos() / loads,
                metrics.getEvictionCount(), metrics.getInvalidateCount(), metrics.getLastLoadSuccessTimeMs(),
                metrics.getLastLoadFailureTimeMs(), metrics.getLastError());
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
        int initialized = 0;
        for (int i = 0; i < stripeCount; i++) {
            if (stripeStates.get(i) != null) {
                initialized++;
            }
        }
        return initialized;
    }

    int activeActionReferenceCountForTest() {
        int references = 0;
        for (int i = 0; i < stripeCount; i++) {
            StripeState<K> stripe = stripeStates.get(i);
            if (stripe != null) {
                synchronized (stripe) {
                    if (stripe.activeActions != null) {
                        for (ActionState state : stripe.activeActions.values()) {
                            references += state.references;
                        }
                    }
                }
            }
        }
        return references;
    }

    void beforeManualCachePutForTest(K key, V loaded) {
    }

    void beforePublicMutationWriteForTest(K key) {
    }

    protected void beforeCurrentValueActionForTest(K key, V value) {
    }

    private V loadAndPause(K key, Function<K, V> loadFunction) {
        V loaded = loadFunction.apply(key);
        if (loaded != null) {
            beforeManualCachePutForTest(key, loaded);
        }
        return loaded;
    }

    private static com.github.benmanes.caffeine.cache.RemovalCause toCaffeineRemovalCause(
            MetaCacheRemovalReason reason) {
        return com.github.benmanes.caffeine.cache.RemovalCause.valueOf(reason.name());
    }

    private StripeState<K> stripeState(K key) {
        int hash = key == null ? 0 : key.hashCode();
        int index = (hash & Integer.MAX_VALUE) % stripeCount;
        StripeState<K> current = stripeStates.get(index);
        if (current != null) {
            return current;
        }
        StripeState<K> created = new StripeState<>();
        if (stripeStates.compareAndSet(index, null, created)) {
            return created;
        }
        return stripeStates.get(index);
    }

    private ActionToken<K> beginAction(StripeState<K> stripe, K key) {
        if (stripe.activeActions == null) {
            stripe.activeActions = new HashMap<>();
        }
        ActionState state = stripe.activeActions.computeIfAbsent(key, ignored -> new ActionState());
        state.references++;
        return new ActionToken<>(key, state);
    }

    private boolean isCurrent(StripeState<K> stripe, ActionToken<K> token) {
        return stripe.activeActions != null
                && stripe.activeActions.get(token.key) == token.state
                && token.state.generation == token.generation;
    }

    private void bumpAction(StripeState<K> stripe, K key) {
        if (stripe.activeActions != null) {
            ActionState state = stripe.activeActions.get(key);
            if (state != null) {
                state.generation++;
            }
        }
    }

    private void endAction(StripeState<K> stripe, ActionToken<K> token) {
        ActionState state = stripe.activeActions.get(token.key);
        if (state != token.state || state.references <= 0) {
            throw new IllegalStateException("Invalid naming-cache action state");
        }
        state.references--;
        if (state.references == 0) {
            stripe.activeActions.remove(token.key);
            if (stripe.activeActions.isEmpty()) {
                stripe.activeActions = null;
            }
        }
    }
}
