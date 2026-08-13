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

package org.apache.doris.datasource.hive;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.benchmark.BenchmarkHarness;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.hive.HiveExternalMetaCache.HivePartitionValues;
import org.apache.doris.datasource.hive.HiveExternalMetaCache.PartitionValueCacheKey;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.ExternalMetaCacheBudgetManager;
import org.apache.doris.datasource.metacache.ExternalMetaCacheBudgetManager.EntryBudget;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** Measures count publication, weighted preparation, and prepared-value admission separately. */
public class HivePartitionValuesSizeBenchmark {
    private static final int TAIL_PAYLOAD_BYTES = 1024 * 1024;
    private static final long MAX_WEIGHT_BYTES = 4L * 1024L * 1024L * 1024L;

    public int countPublicationBaseline(UnsealedState state) {
        state.partitionValues.rebuildSortedPartitionRangesForPublication();
        return state.partitionValues.getSortedPartitionRanges()
                .map(ranges -> ranges.sortedPartitions.size() + ranges.defaultPartitions.size())
                .orElse(0);
    }

    public int sealPublicationWithoutEstimate(UnsealedState state) {
        state.partitionValues.sealForPublication();
        return state.partitionValues.getIdToPartitionItem().size();
    }

    public long weightedPublication(UnsealedState state) {
        state.partitionValues.rebuildSortedPartitionRangesForPublication();
        state.partitionValues.prepareForCachePublication(state.key);
        return requireComplete(state.partitionValues);
    }

    public long eventCopySealAndEstimate(PreparedState state) {
        HivePartitionValues copy = state.partitionValues.mutableCopy();
        copy.rebuildSortedPartitionRangesForPublication();
        copy.prepareForCachePublication(state.key);
        return requireComplete(copy);
    }

    public long preparedSizeProvider(PreparedState state) {
        return state.partitionValues.getSizeEstimate().getBytes();
    }

    public long estimateFormula(PreparedState state) {
        state.partitionValues.prepareSizeEstimate(state.key);
        return requireComplete(state.partitionValues);
    }

    public void replacementAdmission(PreparedState state) {
        MetaCacheEntry.ReplaceResult result = state.cacheEntry.tryReplace(
                state.key, state.currentPartitionValues, state.nextPartitionValues);
        if (result != MetaCacheEntry.ReplaceResult.REPLACED) {
            throw new IllegalStateException("replacement failed: " + result);
        }
        state.currentPartitionValues = state.nextPartitionValues;
        state.nextPartitionValues = state.nextPartitionValues == state.partitionValues
                ? state.replacementPartitionValues : state.partitionValues;
    }

    public HivePartitionValues countStrongCacheHit(PreparedState state) {
        return state.countCacheEntry.getIfPresent(state.key);
    }

    public HivePartitionValues weightedSoftCacheHit(PreparedState state) {
        return state.cacheEntry.getIfPresent(state.key);
    }

    public static void main(String[] args) throws Exception {
        HivePartitionValuesSizeBenchmark benchmark = new HivePartitionValuesSizeBenchmark();
        for (int partitionCount : new int[] {1000, 10000, 100000}) {
            for (String distribution : new String[] {"uniform", "tail_skew"}) {
                String suffix = "[partitions=" + partitionCount + ",distribution=" + distribution + "]";
                BenchmarkHarness.measure("hive.countPublicationBaseline" + suffix, TimeUnit.MILLISECONDS, () -> {
                    UnsealedState state = unsealedState(partitionCount, distribution);
                    return benchmark.countPublicationBaseline(state);
                });
                BenchmarkHarness.measure("hive.sealPublicationWithoutEstimate" + suffix,
                        TimeUnit.MILLISECONDS, () -> {
                            UnsealedState state = unsealedState(partitionCount, distribution);
                            return benchmark.sealPublicationWithoutEstimate(state);
                        });
                BenchmarkHarness.measure("hive.weightedPublication" + suffix, TimeUnit.MILLISECONDS, () -> {
                    UnsealedState state = unsealedState(partitionCount, distribution);
                    return benchmark.weightedPublication(state);
                });

                PreparedState prepared = new PreparedState();
                prepared.partitionCount = partitionCount;
                prepared.distribution = distribution;
                prepared.setup();
                try {
                    BenchmarkHarness.measure("hive.eventCopySealAndEstimate" + suffix,
                            TimeUnit.MILLISECONDS, () -> benchmark.eventCopySealAndEstimate(prepared));
                    BenchmarkHarness.measure("hive.preparedSizeProvider" + suffix,
                            TimeUnit.NANOSECONDS, () -> benchmark.preparedSizeProvider(prepared));
                    BenchmarkHarness.measure("hive.estimateFormula" + suffix,
                            TimeUnit.NANOSECONDS, () -> benchmark.estimateFormula(prepared));
                    BenchmarkHarness.measure("hive.countStrongCacheHit" + suffix,
                            TimeUnit.NANOSECONDS, () -> benchmark.countStrongCacheHit(prepared));
                    BenchmarkHarness.measure("hive.weightedSoftCacheHit" + suffix,
                            TimeUnit.NANOSECONDS, () -> benchmark.weightedSoftCacheHit(prepared));
                    BenchmarkHarness.measure("hive.replacementAdmission" + suffix,
                            TimeUnit.NANOSECONDS, () -> {
                                benchmark.replacementAdmission(prepared);
                                return null;
                            });
                } finally {
                    prepared.tearDown();
                }
            }
        }
    }

    private static UnsealedState unsealedState(int partitionCount, String distribution) throws Exception {
        UnsealedState state = new UnsealedState();
        state.partitionCount = partitionCount;
        state.distribution = distribution;
        state.setupInvocation();
        return state;
    }

    /** Fresh graph per invocation so all publication work stays inside the measured method. */
    public static class UnsealedState {
        public int partitionCount;

        public String distribution;

        private PartitionValueCacheKey key;
        private HivePartitionValues partitionValues;

        public void setupInvocation() throws Exception {
            List<Type> types = benchmarkTypes();
            key = benchmarkKey(types);
            partitionValues = createPartitionValues(partitionCount, distribution, types);
        }
    }

    public static class PreparedState {
        public int partitionCount;

        public String distribution;

        private PartitionValueCacheKey key;
        private HivePartitionValues partitionValues;
        private HivePartitionValues replacementPartitionValues;
        private HivePartitionValues currentPartitionValues;
        private HivePartitionValues nextPartitionValues;
        private MetaCacheEntry<PartitionValueCacheKey, HivePartitionValues> cacheEntry;
        private MetaCacheEntry<PartitionValueCacheKey, HivePartitionValues> countCacheEntry;
        private ExecutorService cacheExecutor;

        public void setup() throws Exception {
            List<Type> types = benchmarkTypes();
            key = benchmarkKey(types);
            partitionValues = createPartitionValues(partitionCount, distribution, types);
            partitionValues.prepareForCachePublication(key);
            requireComplete(partitionValues);

            // A distinct value root makes Caffeine perform a real replacement while sharing
            // immutable payload objects to keep the fixture's resident heap bounded.
            replacementPartitionValues = new HivePartitionValues(
                    partitionValues.getIdToPartitionItem(),
                    partitionValues.getPartitionNameToIdMap(),
                    partitionValues.getPartitionValuesMap());
            replacementPartitionValues.prepareForCachePublication(key);
            requireComplete(replacementPartitionValues);

            cacheExecutor = MoreExecutors.newDirectExecutorService();
            ExternalMetaCacheBudgetManager budgetManager =
                    new ExternalMetaCacheBudgetManager(OptionalLong.of(MAX_WEIGHT_BYTES));
            EntryBudget entryBudget = budgetManager.createEntryBudget(
                    1L, "hive", "partition_values_benchmark", OptionalLong.empty(), OptionalLong.empty());
            cacheEntry = new MetaCacheEntry<>(
                    "partition_values_benchmark",
                    ignored -> partitionValues,
                    CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 1L, MAX_WEIGHT_BYTES),
                    cacheExecutor,
                    false,
                    false,
                    (entryKey, value) -> value.prepareForCachePublication(entryKey),
                    entryBudget);
            cacheEntry.put(key, partitionValues);
            countCacheEntry = new MetaCacheEntry<>(
                    "partition_values_count_benchmark",
                    ignored -> partitionValues,
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 1L),
                    cacheExecutor,
                    false,
                    false);
            countCacheEntry.put(key, partitionValues);
            currentPartitionValues = partitionValues;
            nextPartitionValues = replacementPartitionValues;
        }

        public void tearDown() {
            if (cacheEntry != null) {
                cacheEntry.close();
            }
            if (countCacheEntry != null) {
                countCacheEntry.close();
            }
            if (cacheExecutor != null) {
                cacheExecutor.shutdownNow();
            }
        }
    }

    private static List<Type> benchmarkTypes() {
        return Collections.singletonList(Type.STRING);
    }

    private static PartitionValueCacheKey benchmarkKey(List<Type> types) {
        return new PartitionValueCacheKey(
                NameMapping.createForTest(1L, "benchmark_db", "benchmark_table"), types);
    }

    private static long requireComplete(HivePartitionValues value) {
        MetaCacheSizeEstimate estimate = value.getSizeEstimate();
        if (!estimate.isComplete()) {
            throw new IllegalStateException("benchmark graph is not fully measurable: "
                    + estimate.getIncompleteReason());
        }
        return estimate.getBytes();
    }

    private static HivePartitionValues createPartitionValues(
            int count, String distribution, List<Type> types) throws Exception {
        HashBiMap<String, Long> nameToId = HashBiMap.create(count);
        Map<Long, PartitionItem> idToItem = Maps.newHashMapWithExpectedSize(count);
        Map<Long, List<String>> idToValues = Maps.newHashMapWithExpectedSize(count);
        String tailPayload = "tail_skew".equals(distribution) ? repeat('x', TAIL_PAYLOAD_BYTES) : null;
        long partitionNameCharacterCount = 0L;

        for (int i = 0; i < count; i++) {
            long id = i;
            String value = i == count - 1 && tailPayload != null ? tailPayload : "value-" + i;
            String name = "p=" + value;
            partitionNameCharacterCount += name.length();
            List<PartitionValue> rawValues = Collections.singletonList(new PartitionValue(value));
            PartitionKey partitionKey = PartitionKey.createListPartitionKeyWithTypes(rawValues, types, true);
            List<PartitionKey> keys = new ArrayList<>(1);
            keys.add(partitionKey);

            nameToId.put(name, id);
            idToItem.put(id, new ListPartitionItem(keys));
            idToValues.put(id, new ArrayList<>(Collections.singletonList(value)));
        }
        return new HivePartitionValues(
                idToItem, nameToId, idToValues, partitionNameCharacterCount, types.size());
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
