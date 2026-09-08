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

package org.apache.doris.connector.cache;

import org.apache.doris.connector.cache.ScopedMetaCache.BulkLoadHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ScopedMetaCacheHierarchyTest {
    private static final CacheSpec ENABLED =
            CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 1_000L);
    private static final ScopePath CATALOG = ScopePath.catalog();
    private static final ScopePath DB = ScopePath.database("db");
    private static final ScopePath TABLE = ScopePath.table("db", "tbl");
    private static final ScopePath PARTITION = ScopePath.partition("db", "tbl", "p=1");

    @Test
    public void everyInvalidationLevelMatchesOnlyItsContainedEntries() {
        List<ScopePath> entryScopes = Arrays.asList(CATALOG, DB, TABLE, PARTITION);
        List<ScopePath> invalidationScopes = Arrays.asList(CATALOG, DB, TABLE, PARTITION);

        for (ScopePath entryScope : entryScopes) {
            for (ScopePath invalidationScope : invalidationScopes) {
                try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
                    ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
                    cache.put("key", entryScope, "value");
                    registry.invalidate(invalidationScope);
                    if (invalidationScope.contains(entryScope)) {
                        Assertions.assertNull(
                                cache.getIfPresent("key", entryScope),
                                invalidationScope + " must invalidate " + entryScope);
                    } else {
                        Assertions.assertEquals(
                                "value",
                                cache.getIfPresent("key", entryScope),
                                invalidationScope + " must not invalidate " + entryScope);
                    }
                }
            }
        }
    }

    @Test
    public void siblingDatabaseTableAndPartitionRemainIsolated() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            ScopePath db1Table1Part1 = ScopePath.partition("db1", "tbl1", "p=1");
            ScopePath db1Table1Part2 = ScopePath.partition("db1", "tbl1", "p=2");
            ScopePath db1Table2Part1 = ScopePath.partition("db1", "tbl2", "p=1");
            ScopePath db2Table1Part1 = ScopePath.partition("db2", "tbl1", "p=1");
            cache.put("p11", db1Table1Part1, "p11");
            cache.put("p12", db1Table1Part2, "p12");
            cache.put("p21", db1Table2Part1, "p21");
            cache.put("db2", db2Table1Part1, "db2");

            registry.invalidate(db1Table1Part1);
            Assertions.assertNull(cache.getIfPresent("p11", db1Table1Part1));
            Assertions.assertEquals("p12", cache.getIfPresent("p12", db1Table1Part2));
            Assertions.assertEquals("p21", cache.getIfPresent("p21", db1Table2Part1));
            Assertions.assertEquals("db2", cache.getIfPresent("db2", db2Table1Part1));

            registry.invalidate(ScopePath.table("db1", "tbl1"));
            Assertions.assertNull(cache.getIfPresent("p12", db1Table1Part2));
            Assertions.assertEquals("p21", cache.getIfPresent("p21", db1Table2Part1));
            Assertions.assertEquals("db2", cache.getIfPresent("db2", db2Table1Part1));

            registry.invalidate(ScopePath.database("db1"));
            Assertions.assertNull(cache.getIfPresent("p21", db1Table2Part1));
            Assertions.assertEquals("db2", cache.getIfPresent("db2", db2Table1Part1));
        }
    }

    @Test
    public void sharedRegistryInvalidatesAllPhysicalCachesAtTheSameScope() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> tableCache = registry.createCache("tables", ENABLED);
            ScopedMetaCache<Integer, String> fileCache = registry.createCache("files", ENABLED);
            ScopePath sibling = ScopePath.table("db", "sibling");
            tableCache.put("tbl", TABLE, "table");
            tableCache.put("sibling", sibling, "sibling");
            fileCache.put(1, PARTITION, "file");

            registry.invalidate(TABLE);

            Assertions.assertNull(tableCache.getIfPresent("tbl", TABLE));
            Assertions.assertNull(fileCache.getIfPresent(1, PARTITION));
            Assertions.assertEquals("sibling", tableCache.getIfPresent("sibling", sibling));
        }
    }

    @Test
    public void batchInvalidationReplacesEveryScopeBeforeReadersResume() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            ScopePath collection = ScopePath.partitionCollection("db", "tbl");
            ScopePath first = ScopePath.partition("db", "tbl", "p=1");
            ScopePath second = ScopePath.partition("db", "tbl", "p=2");
            cache.put("collection", collection, "collection-v1");
            cache.put("first", first, "first-v1");
            cache.put("second", second, "second-v1");

            registry.invalidate(Arrays.asList(collection, first), () -> {
                Assertions.assertNull(cache.getIfPresent("collection", collection));
                Assertions.assertNull(cache.getIfPresent("first", first));
                Assertions.assertEquals("second-v1", cache.getIfPresent("second", second));
            });

            Assertions.assertEquals(1, registry.metrics().getRegistrationCount());
            Assertions.assertEquals(1, registry.metrics().getPartitionNodeCount());
        }
    }

    @Test
    public void exactKeyInvalidationAffectsOnlyOneKeyInOnePhysicalCache() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> first = registry.createCache("first", ENABLED);
            ScopedMetaCache<String, String> second = registry.createCache("second", ENABLED);
            first.put("a", TABLE, "first-a");
            first.put("b", TABLE, "first-b");
            second.put("a", TABLE, "second-a");

            first.invalidateKey("a");

            Assertions.assertNull(first.getIfPresent("a", TABLE));
            Assertions.assertEquals("first-b", first.getIfPresent("b", TABLE));
            Assertions.assertEquals("second-a", second.getIfPresent("a", TABLE));
        }
    }

    @Test
    public void samePhysicalKeyCanMoveToANewScopeWithoutOldScopeOwningIt() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            ScopePath oldScope = ScopePath.table("old_db", "tbl");
            ScopePath newScope = ScopePath.table("new_db", "tbl");
            cache.put("key", oldScope, "old");
            cache.put("key", newScope, "new");

            Assertions.assertNull(cache.getIfPresent("key", oldScope));
            Assertions.assertEquals("new", cache.getIfPresent("key", newScope));
            registry.invalidate(oldScope);
            Assertions.assertEquals("new", cache.getIfPresent("key", newScope));
            Assertions.assertEquals(1, registry.metrics().getRegistrationCount());
        }
    }

    @Test
    public void disabledCacheNeverPublishesOrAllocatesIndexes() {
        List<CacheSpec> disabledSpecs = Arrays.asList(
                CacheSpec.of(false, CacheSpec.CACHE_NO_TTL, 100L),
                CacheSpec.of(true, CacheSpec.CACHE_TTL_DISABLE_CACHE, 100L),
                CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 0L));
        for (CacheSpec disabled : disabledSpecs) {
            try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
                ScopedMetaCache<String, Integer> cache = registry.createCache("disabled", disabled);
                AtomicInteger loads = new AtomicInteger();

                Assertions.assertEquals(1, cache.get("key", PARTITION, key -> loads.incrementAndGet()));
                Assertions.assertEquals(2, cache.get("key", PARTITION, key -> loads.incrementAndGet()));
                cache.put("key", PARTITION, 3);
                try (BulkLoadHandle handle = cache.beginBulkLoad(TABLE)) {
                    Assertions.assertFalse(cache.publish(handle, "bulk", PARTITION, 4));
                }

                Assertions.assertNull(cache.getIfPresent("key", PARTITION));
                assertEmpty(registry, cache);
            }
        }
    }

    @Test
    public void nullAndFailedLoadsDoNotLeaveState() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            Assertions.assertNull(cache.get("null", PARTITION, ignored -> null));
            Assertions.assertThrows(
                    IllegalStateException.class,
                    () -> cache.get("failure", PARTITION, ignored -> {
                        throw new IllegalStateException("expected");
                    }));
            assertEmpty(registry, cache);
        }
    }

    @Test
    public void generationWrapUsesStateIdentityInsteadOfOrdering() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            try (BulkLoadHandle ignored = cache.beginBulkLoad(TABLE)) {
                registry.forceGenerationForTest(TABLE, Long.MAX_VALUE);
                Assertions.assertEquals(Long.MAX_VALUE, registry.generationForTest(TABLE));
                registry.invalidate(TABLE);
                Assertions.assertEquals(Long.MIN_VALUE, registry.generationForTest(TABLE));
            }
            cache.put("key", TABLE, "new");
            Assertions.assertEquals("new", cache.getIfPresent("key", TABLE));
        }
    }

    @Test
    public void bulkLoadRequiresMatchingOwnerOpenHandleAndContainedScope() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> first = registry.createCache("first", ENABLED);
            ScopedMetaCache<String, String> second = registry.createCache("second", ENABLED);
            BulkLoadHandle handle = first.beginBulkLoad(TABLE);

            Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> second.publish(handle, "key", TABLE, "value"));
            Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> first.publish(
                            handle, "key", ScopePath.table("other", "tbl"), "value"));
            handle.close();
            Assertions.assertThrows(
                    IllegalStateException.class,
                    () -> first.publish(handle, "key", TABLE, "value"));
        }
    }

    @Test
    public void closedCacheAndRegistryRejectOperations() {
        ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry();
        ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
        cache.close();
        Assertions.assertThrows(
                IllegalStateException.class, () -> cache.getIfPresent("key", TABLE));
        Assertions.assertThrows(
                IllegalStateException.class, () -> cache.put("key", TABLE, "value"));

        registry.close();
        Assertions.assertThrows(
                IllegalStateException.class, () -> registry.createCache("late", ENABLED));
        Assertions.assertThrows(IllegalStateException.class, () -> registry.invalidate(TABLE));
        registry.close();
        cache.close();
    }

    @Test
    public void deterministicStateMachineMatchesReferenceModelAcrossMixedOperations() {
        Random random = new Random(0x5C0FE5L);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<Integer, Integer> first = registry.createCache("first", ENABLED);
            ScopedMetaCache<Integer, Integer> second = registry.createCache("second", ENABLED);
            Map<Integer, ReferenceEntry> firstReference = new HashMap<>();
            Map<Integer, ReferenceEntry> secondReference = new HashMap<>();

            for (int operation = 0; operation < 20_000; operation++) {
                ScopedMetaCache<Integer, Integer> cache = random.nextBoolean() ? first : second;
                Map<Integer, ReferenceEntry> reference =
                        cache == first ? firstReference : secondReference;
                int key = random.nextInt(32);
                int action = random.nextInt(5);
                if (action <= 1) {
                    ScopePath scope = randomScope(random);
                    cache.put(key, scope, operation);
                    reference.put(key, new ReferenceEntry(scope, operation));
                } else if (action == 2) {
                    ScopePath invalidation = randomScope(random);
                    registry.invalidate(invalidation);
                    firstReference.entrySet().removeIf(
                            entry -> invalidation.contains(entry.getValue().scope));
                    secondReference.entrySet().removeIf(
                            entry -> invalidation.contains(entry.getValue().scope));
                } else if (action == 3) {
                    cache.invalidateKey(key);
                    reference.remove(key);
                } else {
                    ReferenceEntry expected = reference.get(key);
                    if (expected == null) {
                        Assertions.assertNull(cache.getIfPresent(key, randomScope(random)));
                    } else {
                        Assertions.assertEquals(
                                expected.value, cache.getIfPresent(key, expected.scope));
                    }
                }

                if ((operation & 255) == 0) {
                    assertMatchesReference(first, firstReference);
                    assertMatchesReference(second, secondReference);
                    Assertions.assertEquals(
                            firstReference.size() + secondReference.size(),
                            registry.metrics().getRegistrationCount());
                }
            }

            assertMatchesReference(first, firstReference);
            assertMatchesReference(second, secondReference);
            registry.invalidate(CATALOG);
            first.cleanUp();
            second.cleanUp();
            Assertions.assertEquals(0L, first.metrics().getPhysicalEntryCount());
            Assertions.assertEquals(0L, second.metrics().getPhysicalEntryCount());
            Assertions.assertEquals(0, registry.metrics().getRegistrationCount());
            Assertions.assertEquals(0, registry.metrics().getDatabaseNodeCount());
            Assertions.assertEquals(0, registry.metrics().getTableNodeCount());
            Assertions.assertEquals(0, registry.metrics().getPartitionNodeCount());
        }
    }

    private static ScopePath randomScope(Random random) {
        int level = random.nextInt(4);
        String database = "db" + random.nextInt(3);
        String table = "tbl" + random.nextInt(3);
        if (level == 0) {
            return ScopePath.catalog();
        }
        if (level == 1) {
            return ScopePath.database(database);
        }
        if (level == 2) {
            return ScopePath.table(database, table);
        }
        return ScopePath.partition(database, table, "p=" + random.nextInt(3));
    }

    private static void assertMatchesReference(
            ScopedMetaCache<Integer, Integer> cache,
            Map<Integer, ReferenceEntry> reference) {
        cache.cleanUp();
        reference.forEach((key, entry) ->
                Assertions.assertEquals(entry.value, cache.getIfPresent(key, entry.scope)));
        Assertions.assertEquals(reference.size(), cache.metrics().getPhysicalEntryCount());
        Assertions.assertEquals(reference.size(), cache.metrics().getKeyNodeCount());
    }

    private static void assertEmpty(
            ScopedMetaCacheRegistry registry, ScopedMetaCache<?, ?> cache) {
        ScopedMetaCacheRegistry.ScopeMetrics scopeMetrics = registry.metrics();
        Assertions.assertEquals(0, scopeMetrics.getDatabaseNodeCount());
        Assertions.assertEquals(0, scopeMetrics.getTableNodeCount());
        Assertions.assertEquals(0, scopeMetrics.getPartitionNodeCount());
        Assertions.assertEquals(0, scopeMetrics.getRegistrationCount());
        Assertions.assertEquals(0, scopeMetrics.getActiveLoadCount());
        Assertions.assertEquals(0L, cache.metrics().getPhysicalEntryCount());
        Assertions.assertEquals(0, cache.metrics().getKeyNodeCount());
    }

    private static final class ReferenceEntry {
        private final ScopePath scope;
        private final int value;

        private ReferenceEntry(ScopePath scope, int value) {
            this.scope = scope;
            this.value = value;
        }
    }
}
