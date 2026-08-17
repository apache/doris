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

package org.apache.doris.datasource.metacache;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AbstractExternalMetaCacheTest {

    @Test
    public void testEntryRequiresExplicitInit() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            TestExternalMetaCache cache = new TestExternalMetaCache(refreshExecutor);
            Assert.assertThrows(IllegalStateException.class, () -> cache.entry(
                    1L, "schema", SchemaCacheKey.class, SchemaCacheValue.class));

            cache.initCatalog(1L, Maps.newHashMap());
            Assert.assertNotNull(cache.entry(1L, "schema", SchemaCacheKey.class, SchemaCacheValue.class));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testCheckCatalogInitializedRequiresExplicitInit() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            TestExternalMetaCache cache = new TestExternalMetaCache(refreshExecutor);
            Assert.assertThrows(IllegalStateException.class, () -> cache.checkCatalogInitialized(1L));
            cache.initCatalog(1L, Maps.newHashMap());
            cache.checkCatalogInitialized(1L);
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testSchemaEntryValidatesDuplicateColumnsOnLoad() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            TestExternalMetaCache cache = new TestExternalMetaCache(refreshExecutor);
            cache.initCatalog(1L, Maps.newHashMap());

            MetaCacheEntry<SchemaCacheKey, SchemaCacheValue> schemaEntry = cache.entry(
                    1L, "schema", SchemaCacheKey.class, SchemaCacheValue.class);

            IllegalArgumentException exception = Assert.assertThrows(
                    IllegalArgumentException.class,
                    () -> schemaEntry.get(new SchemaCacheKey(NameMapping.createForTest(1L, "db1", "tbl1"))));
            Assert.assertTrue(exception.getMessage().contains("Duplicate column name found"));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testEntryFailsFastAfterCatalogRemoved() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            TestExternalMetaCache cache = new TestExternalMetaCache(refreshExecutor);
            cache.initCatalog(1L, Maps.newHashMap());
            cache.invalidateCatalog(1L);

            IllegalStateException exception = Assert.assertThrows(IllegalStateException.class,
                    () -> cache.entry(1L, "schema", SchemaCacheKey.class, SchemaCacheValue.class));
            Assert.assertTrue(exception.getMessage().contains("not initialized"));
            Assert.assertFalse(cache.isCatalogInitialized(1L));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testCapturedCatalogGroupReturnsClosedEntryDuringConcurrentRemoval() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService workers = Executors.newSingleThreadExecutor();
        CountDownLatch groupCaptured = new CountDownLatch(1);
        CountDownLatch releaseEntryLookup = new CountDownLatch(1);
        LookupRaceExternalMetaCache cache =
                new LookupRaceExternalMetaCache(refreshExecutor, groupCaptured, releaseEntryLookup);
        try {
            cache.initCatalog(1L, Maps.newHashMap());
            Future<MetaCacheEntry<String, Integer>> lookup = workers.submit(
                    () -> cache.entry(1L, "value", String.class, Integer.class));
            Assert.assertTrue(groupCaptured.await(3L, TimeUnit.SECONDS));

            cache.invalidateCatalog(1L);
            releaseEntryLookup.countDown();

            MetaCacheEntry<String, Integer> capturedClosedEntry = lookup.get(3L, TimeUnit.SECONDS);
            Assert.assertEquals(Integer.valueOf(1), capturedClosedEntry.get("k"));
            Assert.assertNull(capturedClosedEntry.peekIfPresent("k"));
        } finally {
            releaseEntryLookup.countDown();
            cache.close();
            workers.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testEntryLevelInvalidationUsesRegisteredMatcher() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            TestExternalMetaCache cache = new TestExternalMetaCache(refreshExecutor);
            cache.initCatalog(1L, Maps.newHashMap());

            MetaCacheEntry<SchemaCacheKey, SchemaCacheValue> schemaEntry = cache.entry(
                    1L, "schema", SchemaCacheKey.class, SchemaCacheValue.class);
            SchemaCacheKey matched = new SchemaCacheKey(NameMapping.createForTest(1L, "db1", "tbl1"));
            SchemaCacheKey unmatched = new SchemaCacheKey(NameMapping.createForTest(1L, "db2", "tbl2"));
            schemaEntry.put(matched, new SchemaCacheValue(Lists.newArrayList(new Column("id", PrimitiveType.INT))));
            schemaEntry.put(unmatched, new SchemaCacheValue(Lists.newArrayList(new Column("id", PrimitiveType.INT))));

            cache.invalidateTable(1L, "db1", "tbl1");

            Assert.assertNull(schemaEntry.getIfPresent(matched));
            Assert.assertNotNull(schemaEntry.getIfPresent(unmatched));
            Assert.assertTrue(cache.isCatalogInitialized(1L));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testGlobalWeightAutomaticallyActivatesEntriesWithEstimator() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        WeightedExternalMetaCache cache = new WeightedExternalMetaCache(
                refreshExecutor, new ExternalMetaCacheBudgetManager(OptionalLong.of(600L)));
        try {
            cache.initCatalog(1L, Maps.newHashMap());
            MetaCacheEntry<String, Integer> entry = cache.entry(1L, "value", String.class, Integer.class);

            Assert.assertTrue(entry.isWeightBounded());
            Assert.assertEquals(600L, entry.stats().getMaxWeight());
            entry.put("first", 60);
            entry.put("second", 60);
            Assert.assertNull(entry.getIfPresent("first"));
            Assert.assertEquals(Integer.valueOf(60), entry.getIfPresent("second"));
            Assert.assertEquals(60L + MetaCacheEntry.FIXED_ENTRY_ACCOUNTING_OVERHEAD_BYTES,
                    entry.stats().getGlobalEstimatedWeight());
        } finally {
            cache.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRuntimeInitIgnoresEntryWeightWithoutEstimator() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            TestExternalMetaCache cache = new TestExternalMetaCache(refreshExecutor);
            Map<String, String> properties = Maps.newHashMap();
            properties.put("meta.cache.test_engine.schema.max-weight", "1KB");

            cache.initCatalog(1L, properties);

            Assert.assertTrue(cache.isCatalogInitialized(1L));
            Assert.assertFalse(cache.entry(1L, "schema", SchemaCacheKey.class, SchemaCacheValue.class)
                    .isWeightBounded());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRuntimeInitIgnoresEntryWeightAboveCatalogWeight() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        WeightedExternalMetaCache cache = new WeightedExternalMetaCache(
                refreshExecutor, new ExternalMetaCacheBudgetManager(OptionalLong.of(4L * 1024L)));
        try {
            Map<String, String> properties = Maps.newHashMap();
            properties.put(ExternalMetaCacheBudgetManager.CATALOG_MAX_WEIGHT_PROPERTY, "1KB");
            properties.put("meta.cache.weighted_test.value.max-weight", "2KB");

            cache.initCatalog(1L, properties);

            Assert.assertTrue(cache.isCatalogInitialized(1L));
            Assert.assertEquals(1024L, cache.entry(1L, "value", String.class, Integer.class)
                    .stats().getMaxWeight());
        } finally {
            cache.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRuntimeInitClampsCatalogAcceptedOnLargerFe() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        WeightedExternalMetaCache cache = new WeightedExternalMetaCache(
                refreshExecutor, new ExternalMetaCacheBudgetManager(OptionalLong.of(1024L)));
        try {
            Map<String, String> properties = Maps.newHashMap();
            properties.put(ExternalMetaCacheBudgetManager.CATALOG_MAX_WEIGHT_PROPERTY, "4KB");
            properties.put("meta.cache.weighted_test.value.max-weight", "2KB");

            Assert.assertThrows(IllegalArgumentException.class,
                    () -> cache.validateCatalogProperties(properties));

            cache.initCatalog(1L, properties);

            MetaCacheEntryStats stats = cache.stats(1L).get("value");
            Assert.assertEquals(1024L, stats.getMaxWeight());
            Assert.assertEquals(1024L, stats.getCatalogMaxWeight());
        } finally {
            cache.close();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testConcurrentCatalogRemoveAndInitDoesNotDuplicateBudgetScope() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        WeightedExternalMetaCache cache = new WeightedExternalMetaCache(
                refreshExecutor, new ExternalMetaCacheBudgetManager(OptionalLong.of(100L)));
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<?> first = workers.submit(() -> repeatedlyRebuildCatalog(cache, start));
            Future<?> second = workers.submit(() -> repeatedlyRebuildCatalog(cache, start));
            start.countDown();
            first.get(10L, TimeUnit.SECONDS);
            second.get(10L, TimeUnit.SECONDS);
            cache.initCatalog(1L, Maps.newHashMap());
            Assert.assertTrue(cache.isCatalogInitialized(1L));
        } finally {
            cache.close();
            workers.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    private static void repeatedlyRebuildCatalog(WeightedExternalMetaCache cache, CountDownLatch start) {
        try {
            Assert.assertTrue(start.await(3L, TimeUnit.SECONDS));
            for (int i = 0; i < 100; i++) {
                cache.initCatalog(1L, Maps.newHashMap());
                cache.invalidateCatalog(1L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static final class TestExternalMetaCache extends AbstractExternalMetaCache {
        private TestExternalMetaCache(ExecutorService refreshExecutor) {
            super("test_engine", refreshExecutor);
            registerEntry(MetaCacheEntryDef.of(
                    "schema",
                    SchemaCacheKey.class,
                    SchemaCacheValue.class,
                    key -> new SchemaCacheValue(Lists.newArrayList(
                            new Column("id", PrimitiveType.INT),
                            new Column("ID", PrimitiveType.INT))),
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    MetaCacheEntryInvalidation.forNameMapping(SchemaCacheKey::getNameMapping)));
        }
    }

    private static final class WeightedExternalMetaCache extends AbstractExternalMetaCache {
        private WeightedExternalMetaCache(
                ExecutorService refreshExecutor, ExternalMetaCacheBudgetManager budgetManager) {
            super("weighted_test", refreshExecutor, budgetManager);
            registerEntry(MetaCacheEntryDef.of(
                    "value",
                    String.class,
                    Integer.class,
                    key -> 1,
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L))
                    .withSizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(value.longValue())));
        }
    }

    private static final class LookupRaceExternalMetaCache extends AbstractExternalMetaCache {
        private final CountDownLatch groupCaptured;
        private final CountDownLatch releaseEntryLookup;

        private LookupRaceExternalMetaCache(ExecutorService refreshExecutor,
                CountDownLatch groupCaptured, CountDownLatch releaseEntryLookup) {
            super("lookup_race", refreshExecutor);
            this.groupCaptured = groupCaptured;
            this.releaseEntryLookup = releaseEntryLookup;
            registerEntry(MetaCacheEntryDef.of(
                    "value", String.class, Integer.class, key -> 1,
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L)));
        }

        @Override
        void beforeCatalogEntryLookupForTest(long catalogId, String entryName) {
            groupCaptured.countDown();
            try {
                Assert.assertTrue(releaseEntryLookup.await(3L, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
