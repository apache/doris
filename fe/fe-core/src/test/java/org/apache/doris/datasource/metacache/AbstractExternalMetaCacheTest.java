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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
}
