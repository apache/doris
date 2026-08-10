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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

class CatalogMetaCacheTest {
    private static final CacheSpec SPEC = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 100);

    @Test
    void tableInvalidationAutomaticallyCoversEveryPhysicalCache() {
        try (CatalogMetaCache catalog = new CatalogMetaCache()) {
            MetaCache<TableKey, String> tableCache = catalog.create(tableDefinition("table"));
            MetaCache<TableKey, String> snapshotCache = catalog.create(tableDefinition("snapshot"));
            MetaCache<TableKey, String> futureCache = catalog.create(tableDefinition("future-derived-cache"));
            TableKey target = new TableKey("db", "table");
            TableKey sibling = new TableKey("db", "sibling");

            tableCache.put(target, "table-v1");
            snapshotCache.put(target, "snapshot-v1");
            futureCache.put(target, "future-v1");
            tableCache.put(sibling, "sibling-v1");

            catalog.invalidateTable("db", "table");

            Assertions.assertNull(tableCache.getIfPresent(target));
            Assertions.assertNull(snapshotCache.getIfPresent(target));
            Assertions.assertNull(futureCache.getIfPresent(target));
            Assertions.assertEquals("sibling-v1", tableCache.getIfPresent(sibling));
        }
    }

    @Test
    void databaseInvalidationCoversDescendantsButNotSiblingDatabase() {
        try (CatalogMetaCache catalog = new CatalogMetaCache()) {
            MetaCache<TableKey, String> cache = catalog.create(tableDefinition("table"));
            TableKey first = new TableKey("db1", "table");
            TableKey second = new TableKey("db2", "table");
            cache.put(first, "first");
            cache.put(second, "second");

            catalog.invalidateDatabase("db1");

            Assertions.assertNull(cache.getIfPresent(first));
            Assertions.assertEquals("second", cache.getIfPresent(second));
        }
    }

    @Test
    void defaultAndContextualLoadersUseDefinitionScope() {
        AtomicInteger loads = new AtomicInteger();
        MetaCacheDefinition<TableKey, String> definition = MetaCacheDefinition
                .<TableKey, String>builder("table", SPEC, TableKey::scope)
                .loader(key -> "default-" + loads.incrementAndGet())
                .build();
        try (CatalogMetaCache catalog = new CatalogMetaCache()) {
            MetaCache<TableKey, String> cache = catalog.create(definition);
            TableKey first = new TableKey("db", "first");
            TableKey second = new TableKey("db", "second");

            Assertions.assertEquals("default-1", cache.get(first));
            Assertions.assertEquals("default-1", cache.get(first));
            Assertions.assertEquals("contextual", cache.get(second, key -> "contextual"));
            Assertions.assertEquals(1, loads.get());
        }
    }

    @Test
    void rejectsMissingScopeDuplicateNamesAndUseAfterClose() {
        Assertions.assertThrows(NullPointerException.class,
                () -> MetaCacheDefinition.builder("missing-scope", SPEC, null).build());

        CatalogMetaCache catalog = new CatalogMetaCache();
        catalog.create(tableDefinition("duplicate"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> catalog.create(tableDefinition("duplicate")));
        catalog.close();
        Assertions.assertThrows(IllegalStateException.class,
                () -> catalog.create(tableDefinition("after-close")));
    }

    @Test
    void refreshReturnsCurrentValueAndPublishesReloadedValue() {
        AtomicInteger loads = new AtomicInteger();
        MetaCacheDefinition<TableKey, String> definition = MetaCacheDefinition
                .<TableKey, String>builder("refresh", SPEC, TableKey::scope)
                .loader(key -> "v" + loads.incrementAndGet())
                .refreshAfterWrite(Duration.ofNanos(1), Runnable::run)
                .build();
        try (CatalogMetaCache catalog = new CatalogMetaCache()) {
            MetaCache<TableKey, String> cache = catalog.create(definition);
            TableKey key = new TableKey("db", "table");

            Assertions.assertEquals("v1", cache.get(key));
            Assertions.assertEquals("v1", cache.get(key));
            Assertions.assertEquals("v2", cache.getIfPresent(key));
        }
    }

    @Test
    void invalidationRejectsRefreshAdmittedBeforeGenerationChange() {
        AtomicInteger loads = new AtomicInteger();
        QueuedExecutor executor = new QueuedExecutor();
        MetaCacheDefinition<TableKey, String> definition = MetaCacheDefinition
                .<TableKey, String>builder("refresh", SPEC, TableKey::scope)
                .loader(key -> "v" + loads.incrementAndGet())
                .refreshAfterWrite(Duration.ofNanos(1), executor)
                .build();
        try (CatalogMetaCache catalog = new CatalogMetaCache()) {
            MetaCache<TableKey, String> cache = catalog.create(definition);
            TableKey key = new TableKey("db", "table");

            Assertions.assertEquals("v1", cache.get(key));
            Assertions.assertEquals("v1", cache.get(key));
            Assertions.assertEquals(1, executor.size());
            catalog.invalidateTable("db", "table");
            executor.runNext();

            Assertions.assertNull(cache.getIfPresent(key));
            Assertions.assertEquals(2, loads.get());
        }
    }

    @Test
    void closeSkipsRefreshLoaderAlreadyQueued() {
        AtomicInteger loads = new AtomicInteger();
        QueuedExecutor executor = new QueuedExecutor();
        MetaCacheDefinition<TableKey, String> definition = MetaCacheDefinition
                .<TableKey, String>builder("refresh", SPEC, TableKey::scope)
                .loader(key -> "v" + loads.incrementAndGet())
                .refreshAfterWrite(Duration.ofNanos(1), executor)
                .build();
        CatalogMetaCache catalog = new CatalogMetaCache();
        MetaCache<TableKey, String> cache = catalog.create(definition);
        TableKey key = new TableKey("db", "table");

        Assertions.assertEquals("v1", cache.get(key));
        Assertions.assertEquals("v1", cache.get(key));
        Assertions.assertEquals(1, executor.size());
        catalog.close();
        executor.runNext();

        Assertions.assertEquals(1, loads.get());
        Assertions.assertEquals(0, catalog.metrics().getRegistrationCount());
    }

    private static MetaCacheDefinition<TableKey, String> tableDefinition(String name) {
        return MetaCacheDefinition.<TableKey, String>builder(name, SPEC, TableKey::scope).build();
    }

    private static final class TableKey {
        private final String database;
        private final String table;

        private TableKey(String database, String table) {
            this.database = database;
            this.table = table;
        }

        private ScopePath scope() {
            return ScopePath.table(database, table);
        }
    }

    private static final class QueuedExecutor implements Executor {
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        private int size() {
            return tasks.size();
        }

        private void runNext() {
            tasks.remove().run();
        }
    }
}
