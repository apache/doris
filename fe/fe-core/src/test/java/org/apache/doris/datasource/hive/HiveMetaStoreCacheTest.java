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

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryStats;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            MetaCacheEntry<HiveExternalMetaCache.PartitionValueCacheKey, HiveExternalMetaCache.HivePartitionValues>
                    partitionValuesCache = cache.entry(0, HiveExternalMetaCache.ENTRY_PARTITION_VALUES,
                            HiveExternalMetaCache.PartitionValueCacheKey.class,
                            HiveExternalMetaCache.HivePartitionValues.class);

            String dbName = "db";
            String tbName = "tb";
            NameMapping nameMapping = NameMapping.createForTest(dbName, tbName);
            long catalogId = nameMapping.getCtlId();
            long tableId = Util.genIdByName(dbName, tbName);

            String targetPartName = "dt=2024-01-01";
            List<String> targetValues = Collections.singletonList("2024-01-01");
            String otherPartName = "dt=2024-01-02";
            List<String> otherValues = Collections.singletonList("2024-01-02");

            // The partition_values cache knows both partitions (name -> values), but the per-partition
            // `partition` cache is intentionally NOT populated for the target partition, simulating an
            // entry that was evicted or never loaded. In that state invalidatePartitionCache cannot build
            // the exact FileCacheKey (which needs the partition path / input format).
            Map<String, List<String>> nameToPartitionValues = new HashMap<>();
            nameToPartitionValues.put(targetPartName, targetValues);
            nameToPartitionValues.put(otherPartName, otherValues);
            partitionValuesCache.put(new HiveExternalMetaCache.PartitionValueCacheKey(nameMapping, null),
                    new HiveExternalMetaCache.HivePartitionValues(new HashMap<>(), nameToPartitionValues));

            // Stale file listings exist for BOTH partitions.
            HiveExternalMetaCache.FileCacheKey targetFileKey = new HiveExternalMetaCache.FileCacheKey(
                    catalogId, tableId, "/wh/db/tb/" + targetPartName, "orc", targetValues);
            HiveExternalMetaCache.FileCacheKey otherFileKey = new HiveExternalMetaCache.FileCacheKey(
                    catalogId, tableId, "/wh/db/tb/" + otherPartName, "orc", otherValues);
            fileCache.put(targetFileKey, new HiveExternalMetaCache.FileCacheValue());
            fileCache.put(otherFileKey, new HiveExternalMetaCache.FileCacheValue());
            Assertions.assertEquals(2, entrySize(fileCache));

            // Partition-level refresh for the target partition. Even though its `partition` cache entry
            // is missing, the stale file listing for that partition must still be invalidated.
            cache.invalidatePartitionCache(nameMapping, targetPartName);

            Assertions.assertNull(fileCache.getIfPresent(targetFileKey),
                    "stale file cache for the refreshed partition must be cleared even on partition cache miss");
            Assertions.assertNotNull(fileCache.getIfPresent(otherFileKey),
                    "file cache for other partitions must NOT be affected");
            Assertions.assertEquals(1, entrySize(fileCache));
        } finally {
            executor.shutdownNow();
            listExecutor.shutdownNow();
        }
    }

    @Test
    public void testDefaultSpecsFollowConfig() {
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "refresh", 1, false);
        ThreadPoolExecutor listExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "file", 1, false);
        long originalExpireAfterAccess = Config.external_cache_expire_time_seconds_after_access;
        long originalPartitionCapacity = Config.max_hive_partition_cache_num;
        long originalPartitionTableCapacity = Config.max_hive_partition_table_cache_num;
        long originalFileCapacity = Config.max_external_file_cache_num;
        try {
            Config.external_cache_expire_time_seconds_after_access = 321L;
            Config.max_hive_partition_cache_num = 100L;
            Config.max_hive_partition_table_cache_num = 20L;
            Config.max_external_file_cache_num = 30L;

            HiveExternalMetaCache hiveMetaStoreCache = new HiveExternalMetaCache(executor, listExecutor);
            hiveMetaStoreCache.initCatalog(0, Collections.emptyMap());

            Map<String, MetaCacheEntryStats> stats = hiveMetaStoreCache.stats(0);
            MetaCacheEntryStats partitionValuesStats = stats.get(HiveExternalMetaCache.ENTRY_PARTITION_VALUES);
            MetaCacheEntryStats partitionStats = stats.get(HiveExternalMetaCache.ENTRY_PARTITION);
            MetaCacheEntryStats fileStats = stats.get(HiveExternalMetaCache.ENTRY_FILE);
            Assertions.assertEquals(321L, partitionValuesStats.getTtlSecond());
            Assertions.assertEquals(20L, partitionValuesStats.getCapacity());
            Assertions.assertEquals(321L, partitionStats.getTtlSecond());
            Assertions.assertEquals(100L, partitionStats.getCapacity());
            Assertions.assertEquals(321L, fileStats.getTtlSecond());
            Assertions.assertEquals(30L, fileStats.getCapacity());
        } finally {
            Config.external_cache_expire_time_seconds_after_access = originalExpireAfterAccess;
            Config.max_hive_partition_cache_num = originalPartitionCapacity;
            Config.max_hive_partition_table_cache_num = originalPartitionTableCapacity;
            Config.max_external_file_cache_num = originalFileCapacity;
            executor.shutdownNow();
            listExecutor.shutdownNow();
        }
    }

    @Test
    public void testHivePartitionValuesCopyKeepsIndependentNameMaps() {
        Map<String, PartitionItem> nameToPartitionItem = new HashMap<>();
        Map<String, List<String>> nameToPartitionValues = new HashMap<>();
        nameToPartitionValues.put("dt=2026-06-26", Collections.singletonList("2026-06-26"));
        HiveExternalMetaCache.HivePartitionValues partitionValues =
                new HiveExternalMetaCache.HivePartitionValues(nameToPartitionItem, nameToPartitionValues);

        HiveExternalMetaCache.HivePartitionValues copy = partitionValues.copy();
        copy.getNameToPartitionValues().put("dt=2026-06-27", Collections.singletonList("2026-06-27"));
        Assertions.assertFalse(partitionValues.getNameToPartitionValues().containsKey("dt=2026-06-27"));
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

    // -------------------------------------------------------------------------
    // FileCacheKey identity: inputFormat must be part of equals() / hashCode()
    // so that two tables at the same partition location but with different
    // InputFormats (e.g. TextInputFormat vs LzoTextInputFormat) never share
    // a cached FileCacheValue.
    // -------------------------------------------------------------------------

    @Test
    public void testFileCacheKeyIdentity_SameInputFormat_Equal() {
        long catalogId = 1L;
        long id = 100L;
        String location = "hdfs://namenode/warehouse/db/tbl/dt=2024-01-01";
        String inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
        ArrayList<String> partitionValues = new ArrayList<>();

        HiveExternalMetaCache.FileCacheKey key1 = new HiveExternalMetaCache.FileCacheKey(
                catalogId, id, location, inputFormat, partitionValues);
        HiveExternalMetaCache.FileCacheKey key2 = new HiveExternalMetaCache.FileCacheKey(
                catalogId, id, location, inputFormat, partitionValues);

        Assertions.assertEquals(key1, key2,
                "Keys with same catalogId, location, inputFormat and partitionValues must be equal");
        Assertions.assertEquals(key1.hashCode(), key2.hashCode(),
                "Equal keys must have equal hashCodes");
    }

    @Test
    public void testFileCacheKeyIdentity_DifferentInputFormat_NotEqual() {
        long catalogId = 1L;
        long id = 100L;
        String location = "hdfs://namenode/warehouse/db/tbl/dt=2024-01-01";
        ArrayList<String> partitionValues = new ArrayList<>();

        // TextInputFormat table and LzoTextInputFormat table share the same partition path.
        // Without inputFormat in the cache identity they would collide and one table could
        // inherit the other's file list (e.g. TextInputFormat table inherits filtered .lzo
        // listing, or LZO table inherits an unfiltered, splittable listing).
        HiveExternalMetaCache.FileCacheKey textKey = new HiveExternalMetaCache.FileCacheKey(
                catalogId, id, location,
                "org.apache.hadoop.mapred.TextInputFormat", partitionValues);
        HiveExternalMetaCache.FileCacheKey lzoKey = new HiveExternalMetaCache.FileCacheKey(
                catalogId, id, location,
                "com.hadoop.mapreduce.LzoTextInputFormat", partitionValues);

        Assertions.assertNotEquals(textKey, lzoKey,
                "Keys with different inputFormats must NOT be equal even when location is identical");
        Assertions.assertNotEquals(textKey.hashCode(), lzoKey.hashCode(),
                "Keys with different inputFormats should have different hashCodes");
    }

    @Test
    public void testFileCacheKeyIdentity_AllLzoVariants_Distinct() {
        long catalogId = 1L;
        long id = 100L;
        String location = "hdfs://namenode/warehouse/db/tbl/dt=2024-01-01";
        ArrayList<String> partitionValues = new ArrayList<>();

        HiveExternalMetaCache.FileCacheKey lzoKey = new HiveExternalMetaCache.FileCacheKey(
                catalogId, id, location, "com.hadoop.compression.lzo.LzoTextInputFormat", partitionValues);
        HiveExternalMetaCache.FileCacheKey lzoMrKey = new HiveExternalMetaCache.FileCacheKey(
                catalogId, id, location, "com.hadoop.mapreduce.LzoTextInputFormat", partitionValues);
        HiveExternalMetaCache.FileCacheKey deprecatedLzoKey = new HiveExternalMetaCache.FileCacheKey(
                catalogId, id, location, "com.hadoop.mapred.DeprecatedLzoTextInputFormat", partitionValues);

        // All three LZO variants are distinct input formats and must produce distinct cache keys.
        Assertions.assertNotEquals(lzoKey, lzoMrKey);
        Assertions.assertNotEquals(lzoKey, deprecatedLzoKey);
        Assertions.assertNotEquals(lzoMrKey, deprecatedLzoKey);
    }

    @Test
    public void testFileCacheKeyIdentity_DummyKey_IgnoresInputFormat() {
        // Dummy keys are keyed by (catalogId, id) only; inputFormat must not affect them.
        long catalogId = 1L;
        long id = 100L;
        String location = "hdfs://namenode/warehouse/db/tbl";

        HiveExternalMetaCache.FileCacheKey dummy1 = HiveExternalMetaCache.FileCacheKey
                .createDummyCacheKey(catalogId, id, location, "org.apache.hadoop.mapred.TextInputFormat");
        HiveExternalMetaCache.FileCacheKey dummy2 = HiveExternalMetaCache.FileCacheKey
                .createDummyCacheKey(catalogId, id, location, "com.hadoop.mapreduce.LzoTextInputFormat");

        Assertions.assertEquals(dummy1, dummy2,
                "Dummy keys with same catalogId and id must be equal regardless of inputFormat");
        Assertions.assertEquals(dummy1.hashCode(), dummy2.hashCode());
    }
}
