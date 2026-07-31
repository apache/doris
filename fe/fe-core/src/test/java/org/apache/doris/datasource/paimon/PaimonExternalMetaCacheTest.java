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
import org.apache.doris.datasource.metacache.paimon.PaimonLatestSnapshotProjectionLoader;
import org.apache.doris.datasource.metacache.paimon.PaimonPartitionInfoLoader;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PaimonExternalMetaCacheTest {

    @Test
    public void testLatestSnapshotUsesLatestSchemaForPinnedRead() {
        PaimonLatestSnapshotProjectionLoader loader = new PaimonLatestSnapshotProjectionLoader(
                new PaimonPartitionInfoLoader(),
                (nameMapping, schemaId) -> new PaimonSchemaCacheValue(
                        Collections.emptyList(), Collections.emptyList(), null));
        NameMapping nameMapping = new NameMapping(1L, "db", "table", "remote_db", "remote_table");
        FileStoreTable baseTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable pinnedTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable latestSchemaTable = Mockito.mock(FileStoreTable.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        TableSchema latestSchema = Mockito.mock(TableSchema.class);
        Mockito.when(snapshot.id()).thenReturn(12L);
        Mockito.when(baseTable.copyWithLatestSchema()).thenReturn(latestSchemaTable);
        Mockito.when(latestSchemaTable.latestSnapshot()).thenReturn(Optional.of(snapshot));
        Mockito.when(latestSchemaTable.copyWithoutTimeTravel(Collections.singletonMap(
                CoreOptions.SCAN_SNAPSHOT_ID.key(), "12"))).thenReturn(pinnedTable);
        Mockito.when(latestSchemaTable.schemaManager()).thenReturn(schemaManager);
        Mockito.when(schemaManager.latest()).thenReturn(Optional.of(latestSchema));
        Mockito.when(latestSchema.id()).thenReturn(4L);

        PaimonSnapshotCacheValue value = loader.load(nameMapping, baseTable);

        Assert.assertEquals(12L, value.getSnapshot().getSnapshotId());
        Assert.assertEquals(4L, value.getSnapshot().getSchemaId());
        Assert.assertSame(pinnedTable, value.getSnapshot().getTable());
        Mockito.verify(latestSchemaTable).copyWithoutTimeTravel(Collections.singletonMap(
                CoreOptions.SCAN_SNAPSHOT_ID.key(), "12"));
    }

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
