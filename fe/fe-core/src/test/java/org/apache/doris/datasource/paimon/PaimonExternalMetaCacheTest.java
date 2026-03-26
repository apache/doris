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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.metacache.MetaCacheEntryStats;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PaimonExternalMetaCacheTest {

    @Test
    public void testInvalidateTablePrecise() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());
            NameMapping t1 = new NameMapping(catalogId, "db1", "tbl1", "rdb1", "rtbl1");
            NameMapping t2 = new NameMapping(catalogId, "db1", "tbl2", "rdb1", "rtbl2");

            org.apache.doris.datasource.metacache.MetaCacheEntry<NameMapping, PaimonTableCacheValue> tableEntry =
                    cache.entry(catalogId, PaimonExternalMetaCache.ENTRY_TABLE,
                            NameMapping.class, PaimonTableCacheValue.class);
            tableEntry.put(t1, new PaimonTableCacheValue(null,
                    () -> new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, 1L, null))));
            tableEntry.put(t2, new PaimonTableCacheValue(null,
                    () -> new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY, new PaimonSnapshot(2L, 2L, null))));

            cache.invalidateTable(catalogId, "db1", "tbl1");

            Assert.assertNull(tableEntry.getIfPresent(t1));
            Assert.assertNotNull(tableEntry.getIfPresent(t2));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testInvalidateDbAndStats() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());
            NameMapping db1Table = new NameMapping(catalogId, "db1", "tbl1", "rdb1", "rtbl1");
            NameMapping db2Table = new NameMapping(catalogId, "db2", "tbl1", "rdb2", "rtbl1");

            org.apache.doris.datasource.metacache.MetaCacheEntry<NameMapping, PaimonTableCacheValue> tableEntry =
                    cache.entry(catalogId, PaimonExternalMetaCache.ENTRY_TABLE,
                            NameMapping.class, PaimonTableCacheValue.class);
            tableEntry.put(db1Table, new PaimonTableCacheValue(null,
                    () -> new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, 1L, null))));
            tableEntry.put(db2Table, new PaimonTableCacheValue(null,
                    () -> new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY, new PaimonSnapshot(2L, 2L, null))));

            org.apache.doris.datasource.metacache.MetaCacheEntry<PaimonSchemaCacheKey, SchemaCacheValue> schemaEntry =
                    cache.entry(catalogId, PaimonExternalMetaCache.ENTRY_SCHEMA,
                            PaimonSchemaCacheKey.class, SchemaCacheValue.class);
            PaimonSchemaCacheKey db1Schema = new PaimonSchemaCacheKey(db1Table, 1L);
            PaimonSchemaCacheKey db2Schema = new PaimonSchemaCacheKey(db2Table, 2L);
            schemaEntry.put(db1Schema, new SchemaCacheValue(Collections.emptyList()));
            schemaEntry.put(db2Schema, new SchemaCacheValue(Collections.emptyList()));

            cache.invalidateDb(catalogId, "db1");

            Assert.assertNull(tableEntry.getIfPresent(db1Table));
            Assert.assertNotNull(tableEntry.getIfPresent(db2Table));
            Assert.assertNull(schemaEntry.getIfPresent(db1Schema));
            Assert.assertNotNull(schemaEntry.getIfPresent(db2Schema));

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            Assert.assertTrue(stats.containsKey(PaimonExternalMetaCache.ENTRY_TABLE));
            Assert.assertTrue(stats.containsKey(PaimonExternalMetaCache.ENTRY_SCHEMA));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSchemaStatsWhenSchemaCacheDisabled() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
            long catalogId = 1L;
            Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
            properties.put(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, "0");
            cache.initCatalog(catalogId, properties);

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            MetaCacheEntryStats schemaStats = stats.get(PaimonExternalMetaCache.ENTRY_SCHEMA);
            Assert.assertNotNull(schemaStats);
            Assert.assertEquals(0L, schemaStats.getTtlSecond());
            Assert.assertTrue(schemaStats.isConfigEnabled());
            Assert.assertFalse(schemaStats.isEffectiveEnabled());
        } finally {
            executor.shutdownNow();
        }
    }
}
