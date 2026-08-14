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

import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.metacache.MetaCacheEntry;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class HiveMetaStoreCacheTest {

    @Test
    public void testGenerationAdvancesAfterFileInvalidation() throws Exception {
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
            HiveExternalMetaCache.FileCacheKey fileCacheKey =
                    Mockito.mock(HiveExternalMetaCache.FileCacheKey.class);
            CountDownLatch invalidationEntered = new CountDownLatch(1);
            CountDownLatch allowInvalidation = new CountDownLatch(1);
            Mockito.when(fileCacheKey.isSameTable(Mockito.anyLong())).thenAnswer(invocation -> {
                invalidationEntered.countDown();
                Assertions.assertTrue(allowInvalidation.await(10, TimeUnit.SECONDS));
                Assertions.assertEquals(0L, cache.getFileCacheInvalidationGeneration(0L));
                return true;
            });
            fileCache.put(fileCacheKey, new HiveExternalMetaCache.FileCacheValue());

            CompletableFuture<Void> invalidation = CompletableFuture.runAsync(
                    () -> cache.invalidateTable(0L, "db", "table"));
            Assertions.assertTrue(invalidationEntered.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(0L, cache.getFileCacheInvalidationGeneration(0L));
            allowInvalidation.countDown();
            invalidation.get(10, TimeUnit.SECONDS);

            Assertions.assertNull(fileCache.getIfPresent(fileCacheKey));
            Assertions.assertEquals(1L, cache.getFileCacheInvalidationGeneration(0L));
            Mockito.verify(fileCacheKey).isSameTable(Util.genIdByName("db", "table"));
        } finally {
            executor.shutdownNow();
            listExecutor.shutdownNow();
        }
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
}
