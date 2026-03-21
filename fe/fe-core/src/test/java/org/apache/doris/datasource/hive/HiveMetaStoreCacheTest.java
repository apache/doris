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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

public class HiveMetaStoreCacheTest {

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
