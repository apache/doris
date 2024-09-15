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

import com.github.benmanes.caffeine.cache.LoadingCache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ThreadPoolExecutor;

public class HiveMetaStoreCacheTest {

    @Test
    public void testInvalidateTableCache() {
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "refresh", 10, false);
        ThreadPoolExecutor listExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "file", 10, false);

        HiveMetaStoreCache hiveMetaStoreCache = new HiveMetaStoreCache(
                new HMSExternalCatalog(1L, "catalog", null, new HashMap<>(), null), executor, listExecutor);

        LoadingCache<HiveMetaStoreCache.FileCacheKey, HiveMetaStoreCache.FileCacheValue> fileCache = hiveMetaStoreCache.getFileCacheRef().get();
        LoadingCache<HiveMetaStoreCache.PartitionCacheKey, HivePartition> partitionCache = hiveMetaStoreCache.getPartitionCache();
        LoadingCache<HiveMetaStoreCache.PartitionValueCacheKey, HiveMetaStoreCache.HivePartitionValues> partitionValuesCache = hiveMetaStoreCache.getPartitionValuesCache();

        String tbName = "tb";
        String dbName = "db";

        HiveMetaStoreCache.FileCacheKey fileCacheKey1 = new HiveMetaStoreCache.FileCacheKey(dbName, tbName, "", "", new ArrayList<>(), null);
        HiveMetaStoreCache.FileCacheKey fileCacheKey2 = HiveMetaStoreCache.FileCacheKey.createDummyCacheKey(dbName, tbName, "", "", null);
        fileCache.put(fileCacheKey1, new HiveMetaStoreCache.FileCacheValue());
        fileCache.put(fileCacheKey2, new HiveMetaStoreCache.FileCacheValue());

        HiveMetaStoreCache.PartitionCacheKey partitionCacheKey = new HiveMetaStoreCache.PartitionCacheKey(
                dbName,
                tbName,
                new ArrayList<>()
        );
        partitionCache.put(partitionCacheKey, new HivePartition(dbName, tbName, false, "", "", new ArrayList<>(), new HashMap<>()));

        HiveMetaStoreCache.PartitionValueCacheKey partitionValueCacheKey = new HiveMetaStoreCache.PartitionValueCacheKey(dbName, tbName, new ArrayList<>());
        partitionValuesCache.put(partitionValueCacheKey, new HiveMetaStoreCache.HivePartitionValues());

        Assertions.assertEquals(2, fileCache.asMap().size());
        Assertions.assertEquals(1, partitionCache.asMap().size());
        Assertions.assertEquals(1, partitionValuesCache.asMap().size());

        hiveMetaStoreCache.invalidateTableCache(dbName, tbName);

        Assertions.assertEquals(0, fileCache.asMap().size());
        Assertions.assertEquals(0, partitionCache.asMap().size());
        Assertions.assertEquals(0, partitionValuesCache.asMap().size());
    }
}
