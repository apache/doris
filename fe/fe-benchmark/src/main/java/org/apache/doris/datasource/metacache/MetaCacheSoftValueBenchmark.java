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

import org.apache.doris.datasource.metacache.ExternalMetaCacheBudgetManager.EntryBudget;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.util.concurrent.MoreExecutors;

import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/** Measures reservation cleanup after Caffeine reports soft values as COLLECTED. */
public final class MetaCacheSoftValueBenchmark {
    private static final int SAMPLE_COUNT = 3;
    private static final int VALUE_COUNT = 10_000;
    private static final long MAX_WEIGHT_BYTES = 16L * 1024L * 1024L;

    private MetaCacheSoftValueBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        long totalNanos = 0L;
        for (int sample = 0; sample < SAMPLE_COUNT; sample++) {
            CollectedState state = CollectedState.create(VALUE_COUNT);
            try {
                state.enqueueAll();
                long start = System.nanoTime();
                state.cleanUp();
                totalNanos += System.nanoTime() - start;
            } finally {
                state.close();
            }
        }
        double averageNanos = (double) totalNanos / SAMPLE_COUNT;
        System.out.printf(Locale.ROOT,
                "%-72s %12.3f us/batch  (%.3f ns/value, %d samples)%n",
                "metacache.collectedCleanup[values=" + VALUE_COUNT + "]",
                averageNanos / TimeUnit.MICROSECONDS.toNanos(1L),
                averageNanos / VALUE_COUNT,
                SAMPLE_COUNT);
    }

    private static final class CollectedState implements AutoCloseable {
        private final ExecutorService executor;
        private final ExternalMetaCacheBudgetManager budgetManager;
        private final MetaCacheEntry<String, byte[]> entry;
        private final LoadingCache<?, ?> loadingCache;
        private final List<Reference<?>> references;

        private CollectedState(ExecutorService executor,
                ExternalMetaCacheBudgetManager budgetManager,
                MetaCacheEntry<String, byte[]> entry,
                LoadingCache<?, ?> loadingCache,
                List<Reference<?>> references) {
            this.executor = executor;
            this.budgetManager = budgetManager;
            this.entry = entry;
            this.loadingCache = loadingCache;
            this.references = references;
        }

        private static CollectedState create(int valueCount) throws Exception {
            ExecutorService executor = MoreExecutors.newDirectExecutorService();
            ExternalMetaCacheBudgetManager budgetManager =
                    new ExternalMetaCacheBudgetManager(OptionalLong.of(MAX_WEIGHT_BYTES));
            EntryBudget budget = budgetManager.createEntryBudget(
                    1L, "benchmark", "soft_cleanup", OptionalLong.empty(), OptionalLong.empty());
            MetaCacheEntry<String, byte[]> entry = new MetaCacheEntry<>(
                    "soft_cleanup",
                    key -> new byte[1],
                    CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, valueCount, MAX_WEIGHT_BYTES),
                    executor,
                    false,
                    false,
                    (key, value) -> MetaCacheSizeEstimate.complete(value.length),
                    budget);
            for (int index = 0; index < valueCount; index++) {
                entry.put("key-" + index, new byte[1]);
            }

            LoadingCache<?, ?> loadingCache = (LoadingCache<?, ?>) readField(entry, "loadingData");
            Object boundedLocalCache = readField(loadingCache, "cache");
            Map<?, ?> nodes = (Map<?, ?>) readField(boundedLocalCache, "data");
            List<Reference<?>> references = new ArrayList<>(nodes.size());
            for (Object node : nodes.values()) {
                Method valueReferenceMethod = findMethod(node.getClass(), "getValueReference");
                references.add((Reference<?>) valueReferenceMethod.invoke(node));
            }
            if (references.size() != valueCount) {
                entry.close();
                executor.shutdownNow();
                throw new IllegalStateException(
                        "benchmark admission retained " + references.size() + " of " + valueCount + " values");
            }
            return new CollectedState(executor, budgetManager, entry, loadingCache, references);
        }

        private void enqueueAll() {
            for (Reference<?> reference : references) {
                reference.clear();
                reference.enqueue();
            }
        }

        private void cleanUp() {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
            while (budgetManager.getGlobalUsedWeight() != 0L
                    && System.nanoTime() < deadline) {
                loadingCache.cleanUp();
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100L));
            }
            if (budgetManager.getGlobalUsedWeight() != 0L) {
                throw new IllegalStateException(
                        "COLLECTED cleanup retained " + budgetManager.getGlobalUsedWeight() + " bytes");
            }
        }

        @Override
        public void close() {
            entry.close();
            executor.shutdownNow();
        }
    }

    private static Object readField(Object target, String name) throws Exception {
        for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
            try {
                Field field = type.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(target);
            } catch (NoSuchFieldException ignored) {
                // Continue through Caffeine's generated cache hierarchy.
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static Method findMethod(Class<?> type, String name) throws Exception {
        for (Class<?> current = type; current != null; current = current.getSuperclass()) {
            try {
                Method method = current.getDeclaredMethod(name);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ignored) {
                // Continue through Caffeine's generated node hierarchy.
            }
        }
        throw new NoSuchMethodException(name);
    }
}
