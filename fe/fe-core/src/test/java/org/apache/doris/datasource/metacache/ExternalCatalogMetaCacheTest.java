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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheSizeEstimate;
import org.apache.doris.connector.cache.MetaCacheSizeEstimator;
import org.apache.doris.connector.cache.ReflectiveObjectSizeEstimator;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.plugin.PluginDrivenSchemaCacheValue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
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

    @Test
    public void testWideSchemaIsCachedUnderAWeightLimit() {
        // A walk of the whole value ran out of the 10,000-visit budget at about 3,300 INT or 830 struct columns, so a
        // wider table's schema was loaded again on every statement once a weight limit was set. MUTATION: estimating
        // the schema as one graph -> two loads and a rejected admission -> red.
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            SchemaCacheValue wide = new SchemaCacheValue(wideColumns(4_000, 1_000));
            AtomicInteger loads = new AtomicInteger();
            WideSchemaMetaCache cache = new WideSchemaMetaCache(refreshExecutor, () -> {
                loads.incrementAndGet();
                return wide;
            });
            Map<String, String> properties = Maps.newHashMap();
            properties.put("meta.cache.max-weight", "64MB");
            cache.initCatalog(2L, properties);
            MetaCache<SchemaCacheKey, SchemaCacheValue> schemaEntry = cache.entry(
                    2L, "schema", SchemaCacheKey.class, SchemaCacheValue.class);
            SchemaCacheKey key = new SchemaCacheKey(NameMapping.createForTest(2L, "db1", "wide"));
            for (int i = 0; i < 2; i++) {
                Assertions.assertSame(wide, schemaEntry.get(key));
            }

            Assertions.assertEquals(1, loads.get());
            Assertions.assertEquals(0L, schemaEntry.metrics().getWeightRejectCount());
            cache.invalidateCatalog(2L);
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testSchemaEstimateIsCompleteLinearAndCountsReusedDisplayColumnsOnce() {
        MetaCacheSizeEstimator<SchemaCacheKey, SchemaCacheValue> estimator =
                MetaCacheEntryDef.defaultSizeEstimator(SchemaCacheValue.class);
        SchemaCacheKey key = new SchemaCacheKey(NameMapping.createForTest(3L, "db1", "wide"));
        MetaCacheSizeEstimate half = estimator.estimate(key, new SchemaCacheValue(wideColumns(2_000, 500)));
        List<Column> columns = wideColumns(4_000, 1_000);
        SchemaCacheValue value = new SchemaCacheValue(columns);
        MetaCacheSizeEstimate full = estimator.estimate(key, value);

        Assertions.assertTrue(full.isComplete(), full.getIncompleteReason());
        // Never below an unbounded walk of the value; above it only by the objects that columns share.
        long walked = ReflectiveObjectSizeEstimator.estimateComplete(key, Integer.MAX_VALUE, 20)
                + ReflectiveObjectSizeEstimator.estimateComplete(value, Integer.MAX_VALUE, 20);
        Assertions.assertTrue(full.getBytes() >= walked, full.getBytes() + " < " + walked);
        Assertions.assertTrue(full.getBytes() <= walked * 2, full.getBytes() + " > 2 * " + walked);
        double growth = (double) full.getBytes() / half.getBytes();
        Assertions.assertTrue(growth > 1.8 && growth < 2.2, "doubling the columns grew the estimate " + growth);

        // A plugin table's display columns reuse its column objects; they must not be weighed twice. MUTATION:
        // estimating every element of every column list -> about twice the weight -> red.
        MetaCacheSizeEstimator<SchemaCacheKey, PluginDrivenSchemaCacheValue> pluginEstimator =
                MetaCacheEntryDef.defaultSizeEstimator(PluginDrivenSchemaCacheValue.class);
        long sameList = pluginEstimator.estimate(key, new PluginDrivenSchemaCacheValue(columns,
                Collections.emptyList(), Collections.emptyList(), null, null, null, columns)).getBytes();
        long copiedList = pluginEstimator.estimate(key, new PluginDrivenSchemaCacheValue(columns,
                Collections.emptyList(), Collections.emptyList(), null, null, null, new ArrayList<>(columns)))
                .getBytes();
        Assertions.assertTrue(copiedList < sameList * 1.1, sameList + " -> " + copiedList);
    }

    private static List<Column> wideColumns(int intColumns, int structColumns) {
        List<Column> columns = new ArrayList<>(intColumns + structColumns);
        for (int i = 0; i < intColumns; i++) {
            Column column = new Column("int_" + i, PrimitiveType.INT);
            column.setComment("comment " + i);
            columns.add(column);
        }
        for (int i = 0; i < structColumns; i++) {
            columns.add(new Column("struct_" + i, new StructType(
                    new StructField("a", ScalarType.createType(PrimitiveType.INT)),
                    new StructField("b", ScalarType.createStringType()))));
        }
        return columns;
    }

    private static final class WideSchemaMetaCache extends ExternalCatalogMetaCache {
        private WideSchemaMetaCache(ExecutorService refreshExecutor, Supplier<SchemaCacheValue> loader) {
            super("test_engine", refreshExecutor);
            registerEntry(MetaCacheEntryDef.of(
                    "schema",
                    SchemaCacheKey.class,
                    SchemaCacheValue.class,
                    key -> loader.get(),
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    MetaCacheEntryInvalidation.forNameMapping(SchemaCacheKey::getNameMapping)));
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
