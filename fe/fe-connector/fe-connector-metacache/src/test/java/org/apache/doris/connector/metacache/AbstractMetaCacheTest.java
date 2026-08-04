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

package org.apache.doris.connector.metacache;

import org.apache.doris.connector.metacache.spi.CacheSpec;
import org.apache.doris.connector.metacache.spi.MetaCacheEntryDef;
import org.apache.doris.connector.metacache.spi.MetaCacheEntryInvalidation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AbstractMetaCacheTest {
    @Test
    public void catalogLifecycleAndScopedInvalidationStayInRuntime() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            TestMetaCache cache = new TestMetaCache("test", refreshExecutor);
            Assertions.assertThrows(IllegalStateException.class, () -> cache.tableEntry(1L));

            cache.initCatalog(1L, Collections.emptyMap());
            MetaCacheEntry<TableKey, String> entry = cache.tableEntry(1L);
            TableKey matched = new TableKey("db1", "table1");
            TableKey unmatched = new TableKey("db2", "table2");
            entry.put(matched, "matched");
            entry.put(unmatched, "unmatched");

            cache.invalidateTable(1L, "db1", "table1");

            Assertions.assertNull(entry.getIfPresent(matched));
            Assertions.assertEquals("unmatched", entry.getIfPresent(unmatched));
            Assertions.assertTrue(cache.isCatalogInitialized(1L));
            Assertions.assertTrue(cache.stats(1L).containsKey("table"));

            cache.invalidateCatalogEntries(1L);
            Assertions.assertTrue(cache.isCatalogInitialized(1L));
            Assertions.assertNull(entry.getIfPresent(unmatched));

            cache.invalidateCatalog(1L);
            Assertions.assertFalse(cache.isCatalogInitialized(1L));
            Assertions.assertThrows(IllegalStateException.class, () -> cache.tableEntry(1L));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void entryDefinitionsAreFrozenAfterCatalogInitialization() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            TestMetaCache cache = new TestMetaCache("test", refreshExecutor);
            cache.initCatalog(1L, Collections.emptyMap());

            Assertions.assertThrows(IllegalStateException.class,
                    () -> cache.registerAdditionalEntry());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    private static final class TestMetaCache extends AbstractMetaCache {
        private final EntryHandle<TableKey, String> tableEntry;

        private TestMetaCache(String engine, ExecutorService refreshExecutor) {
            super(engine, refreshExecutor, 60L, 16);
            tableEntry = registerEntry(MetaCacheEntryDef.of(
                    "table",
                    TableKey.class,
                    String.class,
                    key -> key.dbName + "." + key.tableName,
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L),
                    MetaCacheEntryInvalidation.forTableIdentity(
                            key -> key.dbName,
                            key -> key.tableName)));
        }

        private MetaCacheEntry<TableKey, String> tableEntry(long catalogId) {
            return tableEntry.get(catalogId);
        }

        private void registerAdditionalEntry() {
            registerEntryDef(MetaCacheEntryDef.of(
                    "additional",
                    String.class,
                    String.class,
                    value -> value,
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10L)));
        }
    }

    private static final class TableKey {
        private final String dbName;
        private final String tableName;

        private TableKey(String dbName, String tableName) {
            this.dbName = dbName;
            this.tableName = tableName;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof TableKey)) {
                return false;
            }
            TableKey that = (TableKey) other;
            return dbName.equals(that.dbName) && tableName.equals(that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName);
        }
    }
}
