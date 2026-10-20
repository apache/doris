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
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ExternalCatalogMetaCacheTest {

    @Test
    public void contextualEntriesPreserveTheirSizeEstimatorAcrossCatalogRuntimes() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        AtomicInteger estimates = new AtomicInteger();
        ExternalCatalogMetaCache cache = new ExternalCatalogMetaCache("weighted_test", refreshExecutor) {
            {
                registerEntry(MetaCacheEntryDef.contextualOnly("large", String.class, LargeValue.class,
                        CacheSpec.ofWeight(true, CacheSpec.CACHE_NO_TTL, 10, 2_000_000))
                        .withSizeEstimator((key, value) -> {
                            estimates.incrementAndGet();
                            return MetaCacheSizeEstimate.complete(800_000);
                        }));
            }
        };
        try {
            LargeValue value = new LargeValue();
            Assertions.assertEquals(20_000, value.values.size());
            for (long catalogId : new long[] {1L, 2L}) {
                cache.initCatalog(catalogId, Map.of("meta.cache.weighted_test.large.max-weight", "2000000"));
                MetaCache<String, LargeValue> entry = cache.entry(catalogId, "large", String.class, LargeValue.class);
                Assertions.assertSame(value, entry.get("key", key -> value));
                Assertions.assertSame(value, entry.get("key", key -> {
                    throw new AssertionError("contextual entry must use its own complete size estimate");
                }));
                cache.invalidateCatalog(catalogId);
            }
            Assertions.assertEquals(2, estimates.get());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    private static final class LargeValue {
        private final List<String> values = IntStream.range(0, 20_000)
                .mapToObj(Integer::toString).collect(Collectors.toList());
    }

    @Test
    public void testEntryRequiresExplicitInit() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            TestExternalMetaCache cache = new TestExternalMetaCache(refreshExecutor);
            Assertions.assertThrows(IllegalStateException.class, () -> cache.entry(
                    1L, "schema", SchemaCacheKey.class, SchemaCacheValue.class));

            cache.initCatalog(1L, Maps.newHashMap());
            Assertions.assertNotNull(cache.entry(1L, "schema", SchemaCacheKey.class, SchemaCacheValue.class));
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
            MetaCache<SchemaCacheKey, SchemaCacheValue> enabledEntry = cache.entry(
                    1L, "schema", SchemaCacheKey.class, SchemaCacheValue.class);
            Assertions.assertTrue(enabledEntry.isEnabled());

            Map<String, String> disabledProperties = Maps.newHashMap();
            disabledProperties.put("meta.cache.test_engine.schema.ttl-second", "0");
            cache.initCatalog(2L, disabledProperties);
            MetaCache<SchemaCacheKey, SchemaCacheValue> disabledEntry = cache.entry(
                    2L, "schema", SchemaCacheKey.class, SchemaCacheValue.class);
            Assertions.assertFalse(disabledEntry.isEnabled());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testCheckCatalogInitializedRequiresExplicitInit() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            TestExternalMetaCache cache = new TestExternalMetaCache(refreshExecutor);
            Assertions.assertThrows(IllegalStateException.class, () -> cache.checkCatalogInitialized(1L));
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

            MetaCache<SchemaCacheKey, SchemaCacheValue> schemaEntry = cache.entry(
                    1L, "schema", SchemaCacheKey.class, SchemaCacheValue.class);

            IllegalArgumentException exception = Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> schemaEntry.get(new SchemaCacheKey(NameMapping.createForTest(1L, "db1", "tbl1"))));
            Assertions.assertTrue(exception.getMessage().contains("Duplicate column name found"));
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

            IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                    () -> cache.entry(1L, "schema", SchemaCacheKey.class, SchemaCacheValue.class));
            Assertions.assertTrue(exception.getMessage().contains("not initialized"));
            Assertions.assertFalse(cache.isCatalogInitialized(1L));
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

            MetaCache<SchemaCacheKey, SchemaCacheValue> schemaEntry = cache.entry(
                    1L, "schema", SchemaCacheKey.class, SchemaCacheValue.class);
            SchemaCacheKey matched = new SchemaCacheKey(NameMapping.createForTest(1L, "db1", "tbl1"));
            SchemaCacheKey unmatched = new SchemaCacheKey(NameMapping.createForTest(1L, "db2", "tbl2"));
            schemaEntry.put(matched, new SchemaCacheValue(Lists.newArrayList(new Column("id", PrimitiveType.INT))));
            schemaEntry.put(unmatched, new SchemaCacheValue(Lists.newArrayList(new Column("id", PrimitiveType.INT))));

            cache.invalidateTable(1L, "db1", "tbl1");

            Assertions.assertNull(schemaEntry.getIfPresent(matched));
            Assertions.assertNotNull(schemaEntry.getIfPresent(unmatched));
            Assertions.assertTrue(cache.isCatalogInitialized(1L));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    private static final class TestExternalMetaCache extends ExternalCatalogMetaCache {
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
}
