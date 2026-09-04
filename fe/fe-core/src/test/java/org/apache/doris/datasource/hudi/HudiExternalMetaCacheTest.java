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

package org.apache.doris.datasource.hudi;

import org.apache.doris.common.Config;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryStats;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HudiExternalMetaCacheTest {

    private static final ExecutionAuthenticator AUTHENTICATOR = new ExecutionAuthenticator() { };

    @Test
    public void testEntryAccessAfterExplicitInit() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            HudiExternalMetaCache cache = new HudiExternalMetaCache(executor);
            cache.initCatalog(1L, Collections.emptyMap());
            MetaCacheEntry<HudiPartitionCacheKey, TablePartitionValues> partitionEntry = cache.entry(
                    1L, HudiExternalMetaCache.ENTRY_PARTITION, HudiPartitionCacheKey.class,
                    TablePartitionValues.class);
            Assert.assertNotNull(partitionEntry);
            cache.checkCatalogInitialized(1L);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testInvalidateTablePreciseAcrossEntries() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            HudiExternalMetaCache cache = new HudiExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());

            NameMapping t1 = nameMapping(catalogId, "db1", "tbl1");
            NameMapping t2 = nameMapping(catalogId, "db1", "tbl2");

            HudiPartitionCacheKey partitionKey1 = partitionKey(t1, 1L, true);
            HudiPartitionCacheKey partitionKey2 = partitionKey(t2, 2L, false);
            MetaCacheEntry<HudiPartitionCacheKey, TablePartitionValues> partitionEntry = cache.entry(catalogId,
                    HudiExternalMetaCache.ENTRY_PARTITION, HudiPartitionCacheKey.class, TablePartitionValues.class);
            partitionEntry.put(partitionKey1, new TablePartitionValues());
            partitionEntry.put(partitionKey2, new TablePartitionValues());

            HudiMetaClientCacheKey metaKey1 = metaClientKey(t1);
            HudiMetaClientCacheKey metaKey2 = metaClientKey(t2);
            MetaCacheEntry<HudiMetaClientCacheKey, HoodieTableMetaClient> metaClientEntry = cache.entry(catalogId,
                    HudiExternalMetaCache.ENTRY_META_CLIENT, HudiMetaClientCacheKey.class, HoodieTableMetaClient.class);
            metaClientEntry.put(metaKey1, new HoodieTableMetaClient());
            metaClientEntry.put(metaKey2, new HoodieTableMetaClient());

            HudiSchemaCacheKey schemaKey1 = new HudiSchemaCacheKey(t1, 1L);
            HudiSchemaCacheKey schemaKey2 = new HudiSchemaCacheKey(t2, 2L);
            MetaCacheEntry<HudiSchemaCacheKey, SchemaCacheValue> schemaEntry = cache.entry(catalogId,
                    HudiExternalMetaCache.ENTRY_SCHEMA, HudiSchemaCacheKey.class, SchemaCacheValue.class);
            schemaEntry.put(schemaKey1, new SchemaCacheValue(Collections.emptyList()));
            schemaEntry.put(schemaKey2, new SchemaCacheValue(Collections.emptyList()));

            cache.invalidateTable(catalogId, "db1", "tbl1");

            Assert.assertNull(partitionEntry.getIfPresent(partitionKey1));
            Assert.assertNotNull(partitionEntry.getIfPresent(partitionKey2));
            Assert.assertNull(metaClientEntry.getIfPresent(metaKey1));
            Assert.assertNotNull(metaClientEntry.getIfPresent(metaKey2));
            Assert.assertNull(schemaEntry.getIfPresent(schemaKey1));
            Assert.assertNotNull(schemaEntry.getIfPresent(schemaKey2));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testInvalidatePartitionsFallsBackToTableInvalidation() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            HudiExternalMetaCache cache = new HudiExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());

            HudiPartitionCacheKey partitionKey1 = partitionKey(nameMapping(catalogId, "db1", "tbl1"), 1L, true);
            HudiPartitionCacheKey partitionKey2 = partitionKey(nameMapping(catalogId, "db1", "tbl2"), 2L, false);
            MetaCacheEntry<HudiPartitionCacheKey, TablePartitionValues> partitionEntry = cache.entry(catalogId,
                    HudiExternalMetaCache.ENTRY_PARTITION, HudiPartitionCacheKey.class, TablePartitionValues.class);
            partitionEntry.put(partitionKey1, new TablePartitionValues());
            partitionEntry.put(partitionKey2, new TablePartitionValues());

            cache.invalidatePartitions(catalogId, "db1", "tbl1", Collections.singletonList("dt=20250101"));

            Assert.assertNull(partitionEntry.getIfPresent(partitionKey1));
            Assert.assertNotNull(partitionEntry.getIfPresent(partitionKey2));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSchemaStatsWhenSchemaCacheDisabled() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            HudiExternalMetaCache cache = new HudiExternalMetaCache(executor);
            long catalogId = 1L;
            Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
            properties.put(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, "0");
            cache.initCatalog(catalogId, properties);

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            MetaCacheEntryStats schemaStats = stats.get(HudiExternalMetaCache.ENTRY_SCHEMA);
            Assert.assertNotNull(schemaStats);
            Assert.assertEquals(0L, schemaStats.getTtlSecond());
            Assert.assertTrue(schemaStats.isConfigEnabled());
            Assert.assertFalse(schemaStats.isEffectiveEnabled());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testDefaultSpecsFollowConfig() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        long originalExpireAfterAccess = Config.external_cache_expire_time_seconds_after_access;
        long originalTableCapacity = Config.max_external_table_cache_num;
        long originalSchemaCapacity = Config.max_external_schema_cache_num;
        try {
            Config.external_cache_expire_time_seconds_after_access = 321L;
            Config.max_external_table_cache_num = 7L;
            Config.max_external_schema_cache_num = 11L;

            HudiExternalMetaCache cache = new HudiExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            MetaCacheEntryStats partitionStats = stats.get(HudiExternalMetaCache.ENTRY_PARTITION);
            MetaCacheEntryStats schemaStats = stats.get(HudiExternalMetaCache.ENTRY_SCHEMA);
            Assert.assertNotNull(partitionStats);
            Assert.assertNotNull(schemaStats);
            Assert.assertEquals(321L, partitionStats.getTtlSecond());
            Assert.assertEquals(7L, partitionStats.getCapacity());
            Assert.assertEquals(321L, schemaStats.getTtlSecond());
            Assert.assertEquals(11L, schemaStats.getCapacity());
        } finally {
            Config.external_cache_expire_time_seconds_after_access = originalExpireAfterAccess;
            Config.max_external_table_cache_num = originalTableCapacity;
            Config.max_external_schema_cache_num = originalSchemaCapacity;
            executor.shutdownNow();
        }
    }

    @Test
    public void testFsViewGenerationDoesNotCrossCatalogReset() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            HudiExternalMetaCache cache = new HudiExternalMetaCache(executor);
            long catalogId = 1L;
            NameMapping nameMapping = nameMapping(catalogId, "db1", "tbl1");
            HudiFsViewCacheKey key = HudiFsViewCacheKey.of(nameMapping);

            cache.initCatalog(catalogId, Collections.emptyMap());
            HoodieTableFileSystemView oldView = Mockito.mock(HoodieTableFileSystemView.class);
            cache.entry(catalogId, HudiExternalMetaCache.ENTRY_FS_VIEW,
                    HudiFsViewCacheKey.class, HudiFsViewCacheValue.class)
                    .put(key, new HudiFsViewCacheValue(oldView));
            HudiExternalMetaCache.FsViewGeneration oldGeneration =
                    cache.captureFsViewGeneration(catalogId);

            cache.invalidateCatalog(catalogId);
            cache.initCatalog(catalogId, Collections.emptyMap());
            HoodieTableFileSystemView newView = Mockito.mock(HoodieTableFileSystemView.class);
            cache.entry(catalogId, HudiExternalMetaCache.ENTRY_FS_VIEW,
                    HudiFsViewCacheKey.class, HudiFsViewCacheValue.class)
                    .put(key, new HudiFsViewCacheValue(newView));

            try {
                oldGeneration.getFsView(nameMapping, AUTHENTICATOR);
                Assert.fail("stale generation must not acquire the replacement cache entry");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage().contains("runtime changed"));
            }
            Mockito.verify(oldView).close();
            Mockito.verify(oldView, Mockito.never()).sync();
            Mockito.verify(newView, Mockito.never()).sync();

            HudiExternalMetaCache.FsViewGeneration newGeneration =
                    cache.captureFsViewGeneration(catalogId);
            AtomicBoolean authenticated = new AtomicBoolean();
            ExecutionAuthenticator authenticator = new ExecutionAuthenticator() {
                @Override
                public <T> T execute(Callable<T> task) throws Exception {
                    authenticated.set(true);
                    try {
                        return task.call();
                    } finally {
                        authenticated.set(false);
                    }
                }
            };
            Mockito.doAnswer(invocation -> {
                Assert.assertTrue(authenticated.get());
                return null;
            }).when(newView).sync();
            try (HudiFsViewCacheValue.Lease lease = newGeneration.getFsView(nameMapping, authenticator)) {
                Assert.assertSame(newView, lease.get());
            }
            Assert.assertFalse(authenticated.get());
            Mockito.verify(newView).sync();
            cache.invalidateCatalog(catalogId);
            Mockito.verify(newView).close();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testColdLoadDoesNotBlockCatalogRetirement() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch allowLoad = new CountDownLatch(1);
        HoodieTableFileSystemView view = Mockito.mock(HoodieTableFileSystemView.class);
        try {
            HudiExternalMetaCache cache = new HudiExternalMetaCache(refreshExecutor) {
                @Override
                protected HudiFsViewCacheValue createFsView(HudiFsViewCacheKey key) {
                    loadStarted.countDown();
                    try {
                        Assert.assertTrue(allowLoad.await(3L, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    return new HudiFsViewCacheValue(view);
                }
            };
            long catalogId = 1L;
            NameMapping nameMapping = nameMapping(catalogId, "db1", "tbl1");
            cache.initCatalog(catalogId, Collections.emptyMap());
            HudiExternalMetaCache.FsViewGeneration generation =
                    cache.captureFsViewGeneration(catalogId);

            Future<HudiFsViewCacheValue.Lease> acquisition = workers.submit(
                    () -> generation.getFsView(nameMapping, AUTHENTICATOR));
            Assert.assertTrue(loadStarted.await(3L, TimeUnit.SECONDS));

            Future<?> retirement = workers.submit(() -> cache.invalidateCatalog(catalogId));
            retirement.get(3L, TimeUnit.SECONDS);
            allowLoad.countDown();

            try {
                acquisition.get(3L, TimeUnit.SECONDS);
                Assert.fail("load from the retired generation must not be handed to the scan");
            } catch (ExecutionException expected) {
                Assert.assertTrue(expected.getCause() instanceof IllegalStateException);
            }
            Mockito.verify(view).close();
            Mockito.verify(view, Mockito.never()).sync();
        } finally {
            allowLoad.countDown();
            workers.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    private NameMapping nameMapping(long catalogId, String dbName, String tableName) {
        return new NameMapping(catalogId, dbName, tableName, "remote_" + dbName, "remote_" + tableName);
    }

    private HudiPartitionCacheKey partitionKey(NameMapping nameMapping, long timestamp, boolean useHiveSyncPartition) {
        return HudiPartitionCacheKey.of(nameMapping, timestamp, useHiveSyncPartition);
    }

    private HudiMetaClientCacheKey metaClientKey(NameMapping nameMapping) {
        return HudiMetaClientCacheKey.of(nameMapping);
    }
}
