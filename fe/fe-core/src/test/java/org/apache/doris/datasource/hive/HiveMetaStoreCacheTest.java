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
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.metacache.EstimatorCalibrationAssertions;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;

import com.google.common.collect.HashBiMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

public class HiveMetaStoreCacheTest {

    @Test
    public void testPartitionValueWeightScalesLinearlyToOneHundredThousandPartitions() {
        HiveExternalMetaCache.PartitionValueCacheKey key = new HiveExternalMetaCache.PartitionValueCacheKey(
                NameMapping.createForTest("db", "tbl"), Collections.singletonList(Type.STRING));
        long base = partitionValueWeight(key, 0);
        long oneThousand = partitionValueWeight(key, 1_000);
        long tenThousand = partitionValueWeight(key, 10_000);
        long oneHundredThousand = partitionValueWeight(key, 100_000);

        long oneThousandPayload = oneThousand - base;
        Assertions.assertTrue(oneThousandPayload > 0L);
        Assertions.assertEquals(oneThousandPayload * 10L, tenThousand - base);
        Assertions.assertEquals(oneThousandPayload * 100L, oneHundredThousand - base);
    }

    @Test
    public void testInvalidateTableCache() {
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "refresh", 1, false);
        ThreadPoolExecutor listExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "file", 1, false);

        HiveExternalMetaCache hiveMetaStoreCache = new HiveExternalMetaCache(executor, listExecutor);
        hiveMetaStoreCache.initCatalog(0, new HashMap<>());

        MetaCacheEntry<HiveExternalMetaCache.FileCacheKey, HiveExternalMetaCache.FileCacheValue> fileCache =
                hiveMetaStoreCache.entry(0, HiveExternalMetaCache.ENTRY_FILE,
                        HiveExternalMetaCache.FileCacheKey.class,
                        HiveExternalMetaCache.FileCacheValue.class);
        MetaCacheEntry<HiveExternalMetaCache.PartitionCacheKey, HivePartition> partitionCache =
                hiveMetaStoreCache.entry(0, HiveExternalMetaCache.ENTRY_PARTITION,
                        HiveExternalMetaCache.PartitionCacheKey.class,
                        HivePartition.class);
        MetaCacheEntry<HiveExternalMetaCache.PartitionValueCacheKey, HiveExternalMetaCache.HivePartitionValues>
                partitionValuesCache = hiveMetaStoreCache.entry(0, HiveExternalMetaCache.ENTRY_PARTITION_VALUES,
                HiveExternalMetaCache.PartitionValueCacheKey.class,
                HiveExternalMetaCache.HivePartitionValues.class);

        String dbName = "db";
        String tbName = "tb";
        String tbName2 = "tb2";

        putCache(fileCache, partitionCache, partitionValuesCache, dbName, tbName);
        Assertions.assertEquals(2, entrySize(fileCache));
        Assertions.assertEquals(1, entrySize(partitionCache));
        Assertions.assertEquals(1, entrySize(partitionValuesCache));

        putCache(fileCache, partitionCache, partitionValuesCache, dbName, tbName2);
        Assertions.assertEquals(4, entrySize(fileCache));
        Assertions.assertEquals(2, entrySize(partitionCache));
        Assertions.assertEquals(2, entrySize(partitionValuesCache));

        hiveMetaStoreCache.invalidateTableCache(NameMapping.createForTest(dbName, tbName2));
        Assertions.assertEquals(2, entrySize(fileCache));
        Assertions.assertEquals(1, entrySize(partitionCache));
        Assertions.assertEquals(1, entrySize(partitionValuesCache));

        hiveMetaStoreCache.invalidateTableCache(NameMapping.createForTest(dbName, tbName));
        Assertions.assertEquals(0, entrySize(fileCache));
        Assertions.assertEquals(0, entrySize(partitionCache));
        Assertions.assertEquals(0, entrySize(partitionValuesCache));
    }

    @Test
    public void testInvalidatePartitionCacheClearsStaleFileCacheOnPartitionMiss() {
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "refresh", 1, false);
        ThreadPoolExecutor listExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "file", 1, false);
        try {
            HiveExternalMetaCache cache = new HiveExternalMetaCache(executor, listExecutor);
            cache.initCatalog(0, new HashMap<>());

            MetaCacheEntry<HiveExternalMetaCache.FileCacheKey, HiveExternalMetaCache.FileCacheValue> fileCache =
                    cache.entry(0, HiveExternalMetaCache.ENTRY_FILE,
                            HiveExternalMetaCache.FileCacheKey.class,
                            HiveExternalMetaCache.FileCacheValue.class);

            String dbName = "db";
            String tbName = "tb";
            NameMapping nameMapping = NameMapping.createForTest(dbName, tbName);
            long catalogId = nameMapping.getCtlId();
            long tableId = Util.genIdByName(dbName, tbName);
            long otherTableId = Util.genIdByName(dbName, "tb2");

            String targetPartName = "dt=2024-01-01";
            List<String> targetValues = Collections.singletonList("2024-01-01");
            String otherPartName = "dt=2024-01-02";
            List<String> otherValues = Collections.singletonList("2024-01-02");

            // Neither the `partition` cache nor the `partition_values` cache is populated for this table,
            // simulating entries that were evicted or never loaded. invalidatePartitionCache must still
            // clear the stale file listing: it derives the partition values from the partition name and
            // cannot rebuild the exact FileCacheKey (which needs the partition path / input format).
            HiveExternalMetaCache.FileCacheKey targetFileKey = new HiveExternalMetaCache.FileCacheKey(
                    catalogId, tableId, "/wh/db/tb/" + targetPartName, "orc", targetValues);
            // Same table, a different partition -> must be kept.
            HiveExternalMetaCache.FileCacheKey otherPartFileKey = new HiveExternalMetaCache.FileCacheKey(
                    catalogId, tableId, "/wh/db/tb/" + otherPartName, "orc", otherValues);
            // A different table that merely shares the same partition value names at a different location
            // -> must be kept (the fallback is intentionally scoped by table id, not by values alone).
            HiveExternalMetaCache.FileCacheKey otherTableFileKey = new HiveExternalMetaCache.FileCacheKey(
                    catalogId, otherTableId, "/wh/db/tb2/" + targetPartName, "orc", targetValues);
            fileCache.put(targetFileKey, new HiveExternalMetaCache.FileCacheValue());
            fileCache.put(otherPartFileKey, new HiveExternalMetaCache.FileCacheValue());
            fileCache.put(otherTableFileKey, new HiveExternalMetaCache.FileCacheValue());
            Assertions.assertEquals(3, entrySize(fileCache));

            // Partition-level refresh for the target partition. Even though its `partition` cache entry
            // is missing, the stale file listing for that partition must still be invalidated.
            cache.invalidatePartitionCache(nameMapping, targetPartName);

            Assertions.assertNull(fileCache.getIfPresent(targetFileKey),
                    "stale file cache for the refreshed partition must be cleared even on partition cache miss");
            Assertions.assertNotNull(fileCache.getIfPresent(otherPartFileKey),
                    "file cache for other partitions of the same table must NOT be affected");
            Assertions.assertNotNull(fileCache.getIfPresent(otherTableFileKey),
                    "file cache for other tables sharing the same partition values must NOT be affected");
            Assertions.assertEquals(2, entrySize(fileCache));
        } finally {
            executor.shutdownNow();
            listExecutor.shutdownNow();
        }
    }

    @Test
    public void testPartitionValuesEstimateIsPreparedAgainAfterCopy() {
        HiveExternalMetaCache.PartitionValueCacheKey key = new HiveExternalMetaCache.PartitionValueCacheKey(
                NameMapping.createForTest("db", "tbl"), Collections.emptyList());
        PartitionKey partitionKey = new PartitionKey();
        ListPartitionItem partitionItem = new ListPartitionItem(Collections.singletonList(partitionKey));
        partitionItem.setDefaultPartition(true);
        HashMap<Long, PartitionItem> items = new HashMap<>();
        items.put(1L, partitionItem);
        HashBiMap<String, Long> names = HashBiMap.create();
        names.put("p", 1L);
        HashMap<Long, List<String>> partitionValues = new HashMap<>();
        partitionValues.put(1L, Collections.emptyList());
        HiveExternalMetaCache.HivePartitionValues values = new HiveExternalMetaCache.HivePartitionValues(
                items, names, partitionValues);

        values.sealForPublication();
        values.prepareSizeEstimate(key);
        Assertions.assertTrue(values.getSizeEstimate().isComplete());
        Assertions.assertTrue(values.getSizeEstimate().getBytes() > 0L);

        HiveExternalMetaCache.HivePartitionValues copy = values.mutableCopy();
        Assertions.assertFalse(copy.getSizeEstimate().isComplete());
        copy.sealForPublication();
        copy.prepareSizeEstimate(new HiveExternalMetaCache.PartitionValueCacheKey(
                key.getNameMapping(), null));
        Assertions.assertTrue(copy.getSizeEstimate().isComplete());
        Assertions.assertTrue(copy.getSizeEstimate().getBytes() > 0L);
        ListPartitionItem publishedItem = (ListPartitionItem) values.getIdToPartitionItem().get(1L);
        Assertions.assertSame(partitionItem, publishedItem,
                "cache publication must not rewrite common catalog partition objects");
        Assertions.assertSame(partitionKey, publishedItem.getItems().get(0));
    }

    @Test
    public void testPartitionValuesEstimateSupportsRealLiteralGraph() throws Exception {
        List<Type> types = java.util.Arrays.asList(Type.STRING, Type.INT, Type.DATEV2, Type.DECIMALV2);
        HiveExternalMetaCache.PartitionValueCacheKey key = new HiveExternalMetaCache.PartitionValueCacheKey(
                NameMapping.createForTest("db", "tbl"), types);
        PartitionKey partitionKey = PartitionKey.createListPartitionKeyWithTypes(
                java.util.Arrays.asList(
                        new PartitionValue("tail-value"),
                        new PartitionValue("42"),
                        new PartitionValue("2026-08-12"),
                        new PartitionValue("123456789.0123")),
                types, true);
        ListPartitionItem partitionItem = new ListPartitionItem(Collections.singletonList(partitionKey));
        HashMap<Long, PartitionItem> items = new HashMap<>();
        items.put(1L, partitionItem);
        HashBiMap<String, Long> names = HashBiMap.create();
        names.put("s=tail-value/i=42/d=2026-08-12/n=123456789.0123", 1L);
        HashMap<Long, List<String>> partitionValues = new HashMap<>();
        partitionValues.put(1L, java.util.Arrays.asList(
                "tail-value", "42", "2026-08-12", "123456789.0123"));
        HiveExternalMetaCache.HivePartitionValues values = new HiveExternalMetaCache.HivePartitionValues(
                items, names, partitionValues);

        values.sealForPublication();
        values.prepareSizeEstimate(key);

        Assertions.assertTrue(values.getSizeEstimate().isComplete(),
                values.getSizeEstimate().getIncompleteReason());
        long estimatedBytes = values.getSizeEstimate().getBytes();
        PartitionKey publishedKey = ((ListPartitionItem) values.getIdToPartitionItem().get(1L)).getItems().get(0);
        StringLiteral publishedString = (StringLiteral) publishedKey.getKeys().get(0);
        // Exercise normal read-only lazy paths after publication. Their bounded memoized state is
        // covered by estimator headroom without changing or cloning common expression classes.
        publishedString.getExprName();
        values.getSortedPartitionRanges().orElseThrow(AssertionError::new).sortedPartitions
                .forEach(partition -> partition.range.toString());
        values.prepareSizeEstimate(key);
        Assertions.assertEquals(estimatedBytes, values.getSizeEstimate().getBytes());
        Assertions.assertSame(partitionKey, publishedKey,
                "cache publication must not rewrite common catalog partition objects");
    }

    @Test
    public void testPartitionValuesFormulaAgainstJolOwnedGraph() throws Exception {
        List<Type> types = Collections.singletonList(Type.STRING);
        HiveExternalMetaCache.PartitionValueCacheKey key = new HiveExternalMetaCache.PartitionValueCacheKey(
                NameMapping.createForTest("db", "tbl"), types);
        HiveExternalMetaCache.HivePartitionValues empty = realPartitionValues(types, 0, 16);
        HiveExternalMetaCache.HivePartitionValues populated = realPartitionValues(types, 32, 16);
        HiveExternalMetaCache.HivePartitionValues shortTail = realPartitionValues(types, 1, 16);
        HiveExternalMetaCache.HivePartitionValues longTail = realPartitionValues(types, 1, 4096);

        long emptyEstimate = HiveCacheSizeEstimator.estimatePartitionValuesEntry(key, empty).getBytes();
        long populatedEstimate = HiveCacheSizeEstimator.estimatePartitionValuesEntry(key, populated).getBytes();
        long shortTailEstimate = HiveCacheSizeEstimator.estimatePartitionValuesEntry(key, shortTail).getBytes();
        long longTailEstimate = HiveCacheSizeEstimator.estimatePartitionValuesEntry(key, longTail).getBytes();

        EstimatorCalibrationAssertions.assertConservativeDelta(
                "hive partition values", emptyEstimate, populatedEstimate, empty, populated);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "hive long-tail partition", shortTailEstimate, longTailEstimate, shortTail, longTail);
    }

    private void putCache(
            MetaCacheEntry<HiveExternalMetaCache.FileCacheKey, HiveExternalMetaCache.FileCacheValue> fileCache,
            MetaCacheEntry<HiveExternalMetaCache.PartitionCacheKey, HivePartition> partitionCache,
            MetaCacheEntry<HiveExternalMetaCache.PartitionValueCacheKey, HiveExternalMetaCache.HivePartitionValues>
                    partitionValuesCache,
            String dbName, String tbName) {
        NameMapping nameMapping = NameMapping.createForTest(dbName, tbName);
        long catalogId = nameMapping.getCtlId();
        long fileId = Util.genIdByName(dbName, tbName);
        HiveExternalMetaCache.FileCacheKey fileCacheKey1 = new HiveExternalMetaCache.FileCacheKey(
                catalogId, fileId, tbName, "", new ArrayList<>());
        HiveExternalMetaCache.FileCacheKey fileCacheKey2 = HiveExternalMetaCache.FileCacheKey
                .createDummyCacheKey(catalogId, fileId, tbName, "");
        fileCache.put(fileCacheKey1, new HiveExternalMetaCache.FileCacheValue());
        fileCache.put(fileCacheKey2, new HiveExternalMetaCache.FileCacheValue());

        HiveExternalMetaCache.PartitionCacheKey partitionCacheKey = new HiveExternalMetaCache.PartitionCacheKey(
                nameMapping,
                new ArrayList<>()
        );
        partitionCache.put(partitionCacheKey,
                new HivePartition(nameMapping, false, "", "", new ArrayList<>(), new HashMap<>()));

        HiveExternalMetaCache.PartitionValueCacheKey partitionValueCacheKey
                = new HiveExternalMetaCache.PartitionValueCacheKey(nameMapping, new ArrayList<>());
        partitionValuesCache.put(partitionValueCacheKey, new HiveExternalMetaCache.HivePartitionValues());

    }

    private long entrySize(MetaCacheEntry<?, ?> entry) {
        AtomicLong count = new AtomicLong();
        entry.forEach((k, v) -> count.incrementAndGet());
        return count.get();
    }

    private long partitionValueWeight(
            HiveExternalMetaCache.PartitionValueCacheKey key, int partitionCount) {
        Map<Long, PartitionItem> items = sizeOnlyMap(partitionCount);
        HiveExternalMetaCache.HivePartitionValues values =
                new HiveExternalMetaCache.HivePartitionValues(
                        items, null, null, partitionCount * 16L, 1);
        MetaCacheSizeEstimate estimate = HiveCacheSizeEstimator.estimatePartitionValuesEntry(key, values);
        Assertions.assertTrue(estimate.isComplete(), estimate.getIncompleteReason());
        return estimate.getBytes();
    }

    private HiveExternalMetaCache.HivePartitionValues realPartitionValues(
            List<Type> types, int partitionCount, int valueLength) throws Exception {
        Map<Long, PartitionItem> items = new HashMap<>();
        HashBiMap<String, Long> names = HashBiMap.create();
        Map<Long, List<String>> values = new HashMap<>();
        for (int index = 0; index < partitionCount; index++) {
            String value = "p" + index + String.join("", Collections.nCopies(valueLength, "x"));
            long id = index + 1L;
            PartitionKey partitionKey = PartitionKey.createListPartitionKeyWithTypes(
                    Collections.singletonList(new PartitionValue(value)), types, true);
            items.put(id, new ListPartitionItem(Collections.singletonList(partitionKey)));
            names.put("p=" + value, id);
            values.put(id, Collections.singletonList(value));
        }
        HiveExternalMetaCache.HivePartitionValues result =
                new HiveExternalMetaCache.HivePartitionValues(items, names, values);
        result.sealForPublication();
        return result;
    }

    @SuppressWarnings("unchecked")
    private <K, V> Map<K, V> sizeOnlyMap(int size) {
        Map<K, V> map = Mockito.mock(Map.class);
        Mockito.when(map.size()).thenReturn(size);
        return map;
    }
}
