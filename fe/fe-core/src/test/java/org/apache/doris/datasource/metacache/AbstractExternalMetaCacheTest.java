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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    public void testEngineEntriesDoNotInitializeMultiKeyStripeStatesEagerly() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            TestExternalMetaCache cache = new TestExternalMetaCache(refreshExecutor);
            cache.initCatalog(1L, Maps.newHashMap());
            MetaCacheEntry<SchemaCacheKey, SchemaCacheValue> enabledEntry = cache.entry(
                    1L, "schema", SchemaCacheKey.class, SchemaCacheValue.class);
            Assert.assertTrue(enabledEntry.stats().isEffectiveEnabled());
            Assert.assertEquals(0, enabledEntry.initializedStripeCountForTest());

            Map<String, String> disabledProperties = Maps.newHashMap();
            disabledProperties.put("meta.cache.test_engine.schema.ttl-second", "0");
            cache.initCatalog(2L, disabledProperties);
            MetaCacheEntry<SchemaCacheKey, SchemaCacheValue> disabledEntry = cache.entry(
                    2L, "schema", SchemaCacheKey.class, SchemaCacheValue.class);
            Assert.assertFalse(disabledEntry.stats().isEffectiveEnabled());
            Assert.assertEquals(0, disabledEntry.initializedStripeCountForTest());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testMaximumWeightRequiresRegisteredEntryEstimator() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            TestExternalMetaCache cache = new TestExternalMetaCache(refreshExecutor);
            Map<String, String> properties = Maps.newHashMap();
            properties.put("meta.cache.test_engine.schema.max-weight", "5");

            IllegalArgumentException exception = Assert.assertThrows(
                    IllegalArgumentException.class, () -> cache.initCatalog(1L, properties));
            Assert.assertTrue(exception.getMessage().contains("size estimator"));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRegisteredEntryEstimatorEnablesMaximumWeight() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            WeightedExternalMetaCache cache = new WeightedExternalMetaCache(refreshExecutor);
            Map<String, String> properties = Maps.newHashMap();
            properties.put("meta.cache.weighted_engine.value.max-weight", "5");
            cache.initCatalog(1L, properties);

            MetaCacheEntry<String, Integer> entry = cache.entry(1L, "value", String.class, Integer.class);
            entry.put("first", 4);
            entry.put("second", 4);
            // Wait for Caffeine's queued maintenance without making the statistics read trigger it.
            refreshExecutor.submit(() -> null).get(3L, TimeUnit.SECONDS);

            MetaCacheEntryStats stats = entry.stats();
            Assert.assertTrue(stats.isWeightBounded());
            Assert.assertEquals(5L, stats.getMaxWeight());
            Assert.assertTrue(stats.getEstimatedWeight() <= 5L);
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
        private WeightedExternalMetaCache(ExecutorService refreshExecutor) {
            super("weighted_engine", refreshExecutor);
            registerEntry(MetaCacheEntryDef.of(
                    "value",
                    String.class,
                    Integer.class,
                    String::length,
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 100L))
                    .withSizeEstimator((key, value) -> value));
        }
    }
}
