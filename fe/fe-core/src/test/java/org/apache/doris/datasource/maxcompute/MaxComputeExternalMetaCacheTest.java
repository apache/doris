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

package org.apache.doris.datasource.maxcompute;

import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryStats;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MaxComputeExternalMetaCacheTest {

    @Test
    public void testPartitionValuesLoadFromSchemaEntryInsideEngineCache() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            MaxComputeExternalMetaCache cache = new MaxComputeExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());

            NameMapping table = new NameMapping(catalogId, "db1", "tbl1", "remote_db1", "remote_tbl1");
            MetaCacheEntry<SchemaCacheKey, SchemaCacheValue> schemaEntry = cache.entry(
                    catalogId, MaxComputeExternalMetaCache.ENTRY_SCHEMA, SchemaCacheKey.class, SchemaCacheValue.class);
            schemaEntry.put(new SchemaCacheKey(table), new MaxComputeSchemaCacheValue(
                    Collections.emptyList(),
                    null,
                    null,
                    Collections.singletonList("pt"),
                    Collections.singletonList("pt=20250101"),
                    Collections.emptyList(),
                    Collections.singletonList(Type.INT),
                    Collections.emptyMap()));

            TablePartitionValues partitionValues = cache.getPartitionValues(table);

            Assert.assertEquals(1, partitionValues.getPartitionNameToIdMap().size());
            Assert.assertTrue(partitionValues.getPartitionNameToIdMap().containsKey("pt=20250101"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testInvalidateTablePrecise() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            MaxComputeExternalMetaCache cache = new MaxComputeExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());

            NameMapping t1 = new NameMapping(catalogId, "db1", "tbl1", "remote_db1", "remote_tbl1");
            NameMapping t2 = new NameMapping(catalogId, "db1", "tbl2", "remote_db1", "remote_tbl2");

            MetaCacheEntry<NameMapping, TablePartitionValues> partitionEntry = cache.entry(
                    catalogId,
                    MaxComputeExternalMetaCache.ENTRY_PARTITION_VALUES,
                    NameMapping.class,
                    TablePartitionValues.class);
            partitionEntry.put(t1, new TablePartitionValues());
            partitionEntry.put(t2, new TablePartitionValues());

            cache.invalidateTable(catalogId, "db1", "tbl1");

            Assert.assertNull(partitionEntry.getIfPresent(t1));
            Assert.assertNotNull(partitionEntry.getIfPresent(t2));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testStatsIncludePartitionValuesEntry() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            MaxComputeExternalMetaCache cache = new MaxComputeExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            Assert.assertTrue(stats.containsKey(MaxComputeExternalMetaCache.ENTRY_PARTITION_VALUES));
            Assert.assertTrue(stats.containsKey(MaxComputeExternalMetaCache.ENTRY_SCHEMA));
        } finally {
            executor.shutdownNow();
        }
    }
}
