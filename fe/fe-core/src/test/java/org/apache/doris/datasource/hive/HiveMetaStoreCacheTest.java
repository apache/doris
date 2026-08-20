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

import com.github.benmanes.caffeine.cache.LoadingCache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

public class HiveMetaStoreCacheTest {

    @Test
    public void testInvalidateTableCache() {
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "refresh", 1, false);
        ThreadPoolExecutor listExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "file", 1, false);

        HiveMetaStoreCache hiveMetaStoreCache = new HiveMetaStoreCache(
                new HMSExternalCatalog(1L, "catalog", null, new HashMap<>(), null), executor, listExecutor);

        LoadingCache<HiveMetaStoreCache.FileCacheKey, HiveMetaStoreCache.FileCacheValue> fileCache = hiveMetaStoreCache.getFileCacheRef().get();
        LoadingCache<HiveMetaStoreCache.PartitionCacheKey, HivePartition> partitionCache = hiveMetaStoreCache.getPartitionCache();
        LoadingCache<HiveMetaStoreCache.PartitionValueCacheKey, HiveMetaStoreCache.HivePartitionValues> partitionValuesCache = hiveMetaStoreCache.getPartitionValuesCache();

        String dbName = "db";
        String tbName = "tb";
        String tbName2 = "tb2";

        putCache(fileCache, partitionCache, partitionValuesCache, dbName, tbName);
        Assertions.assertEquals(2, fileCache.asMap().size());
        Assertions.assertEquals(1, partitionCache.asMap().size());
        Assertions.assertEquals(1, partitionValuesCache.asMap().size());

        putCache(fileCache, partitionCache, partitionValuesCache, dbName, tbName2);
        Assertions.assertEquals(4, fileCache.asMap().size());
        Assertions.assertEquals(2, partitionCache.asMap().size());
        Assertions.assertEquals(2, partitionValuesCache.asMap().size());

        hiveMetaStoreCache.invalidateTableCache(NameMapping.createForTest(dbName, tbName2));
        Assertions.assertEquals(2, fileCache.asMap().size());
        Assertions.assertEquals(1, partitionCache.asMap().size());
        Assertions.assertEquals(1, partitionValuesCache.asMap().size());

        hiveMetaStoreCache.invalidateTableCache(NameMapping.createForTest(dbName, tbName));
        Assertions.assertEquals(0, fileCache.asMap().size());
        Assertions.assertEquals(0, partitionCache.asMap().size());
        Assertions.assertEquals(0, partitionValuesCache.asMap().size());
    }

    @Test
    public void testInvalidatePartitionCacheClearsStaleFileCacheOnPartitionMiss() {
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "refresh", 1, false);
        ThreadPoolExecutor listExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "file", 1, false);
        try {
            HiveMetaStoreCache cache = new HiveMetaStoreCache(
                    new HMSExternalCatalog(1L, "catalog", null, new HashMap<>(), null), executor, listExecutor);
            LoadingCache<HiveMetaStoreCache.FileCacheKey, HiveMetaStoreCache.FileCacheValue> fileCache =
                    cache.getFileCacheRef().get();

            String dbName = "db";
            String tbName = "tb";
            NameMapping nameMapping = NameMapping.createForTest(dbName, tbName);
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
            HiveMetaStoreCache.FileCacheKey targetFileKey = new HiveMetaStoreCache.FileCacheKey(
                    tableId, "/wh/db/tb/" + targetPartName, "orc", targetValues);
            // Same table, a different partition -> must be kept.
            HiveMetaStoreCache.FileCacheKey otherPartFileKey = new HiveMetaStoreCache.FileCacheKey(
                    tableId, "/wh/db/tb/" + otherPartName, "orc", otherValues);
            // A different table that merely shares the same partition value names at a different location
            // -> must be kept (the fallback is intentionally scoped by table id, not by values alone).
            HiveMetaStoreCache.FileCacheKey otherTableFileKey = new HiveMetaStoreCache.FileCacheKey(
                    otherTableId, "/wh/db/tb2/" + targetPartName, "orc", targetValues);
            fileCache.put(targetFileKey, new HiveMetaStoreCache.FileCacheValue());
            fileCache.put(otherPartFileKey, new HiveMetaStoreCache.FileCacheValue());
            fileCache.put(otherTableFileKey, new HiveMetaStoreCache.FileCacheValue());
            Assertions.assertEquals(3, fileCache.asMap().size());

            // Partition-level refresh for the target partition. Even though its `partition` cache entry
            // is missing, the stale file listing for that partition must still be invalidated.
            cache.invalidatePartitionCache(nameMapping, targetPartName);

            Assertions.assertNull(fileCache.getIfPresent(targetFileKey),
                    "stale file cache for the refreshed partition must be cleared even on partition cache miss");
            Assertions.assertNotNull(fileCache.getIfPresent(otherPartFileKey),
                    "file cache for other partitions of the same table must NOT be affected");
            Assertions.assertNotNull(fileCache.getIfPresent(otherTableFileKey),
                    "file cache for other tables sharing the same partition values must NOT be affected");
            Assertions.assertEquals(2, fileCache.asMap().size());
        } finally {
            executor.shutdownNow();
            listExecutor.shutdownNow();
        }
    }

    private void putCache(
            LoadingCache<HiveMetaStoreCache.FileCacheKey, HiveMetaStoreCache.FileCacheValue> fileCache,
            LoadingCache<HiveMetaStoreCache.PartitionCacheKey, HivePartition> partitionCache,
            LoadingCache<HiveMetaStoreCache.PartitionValueCacheKey, HiveMetaStoreCache.HivePartitionValues> partitionValuesCache,
            String dbName, String tbName) {
        NameMapping nameMapping = NameMapping.createForTest(dbName, tbName);
        long fileId = Util.genIdByName(dbName, tbName);
        HiveMetaStoreCache.FileCacheKey fileCacheKey1 = new HiveMetaStoreCache.FileCacheKey(fileId, tbName, "", new ArrayList<>());
        HiveMetaStoreCache.FileCacheKey fileCacheKey2 = HiveMetaStoreCache.FileCacheKey.createDummyCacheKey(fileId, tbName, "");
        fileCache.put(fileCacheKey1, new HiveMetaStoreCache.FileCacheValue());
        fileCache.put(fileCacheKey2, new HiveMetaStoreCache.FileCacheValue());

        HiveMetaStoreCache.PartitionCacheKey partitionCacheKey = new HiveMetaStoreCache.PartitionCacheKey(
                nameMapping,
                new ArrayList<>()
        );
        partitionCache.put(partitionCacheKey,
                new HivePartition(nameMapping, false, "", "", new ArrayList<>(), new HashMap<>()));

        HiveMetaStoreCache.PartitionValueCacheKey partitionValueCacheKey
                = new HiveMetaStoreCache.PartitionValueCacheKey(nameMapping, new ArrayList<>());
        partitionValuesCache.put(partitionValueCacheKey, new HiveMetaStoreCache.HivePartitionValues());

    }
}
