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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.iceberg.cache.ManifestCacheValue;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryStats;

import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IcebergExternalMetaCacheTest {

    @Test
    public void testInvalidateTableKeepsManifestCache() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, manifestCacheEnabledProperties());
            NameMapping t1 = new NameMapping(catalogId, "db1", "tbl1", "rdb1", "rtbl1");
            NameMapping t2 = new NameMapping(catalogId, "db1", "tbl2", "rdb1", "rtbl2");

            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tableEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_TABLE, NameMapping.class, IcebergTableCacheValue.class);
            tableEntry.put(t1, new IcebergTableCacheValue(newInterfaceProxy(Table.class),
                    () -> new IcebergSnapshotCacheValue(IcebergPartitionInfo.empty(), new IcebergSnapshot(1L, 1L))));
            tableEntry.put(t2, new IcebergTableCacheValue(newInterfaceProxy(Table.class),
                    () -> new IcebergSnapshotCacheValue(IcebergPartitionInfo.empty(), new IcebergSnapshot(2L, 2L))));

            MetaCacheEntry<NameMapping, org.apache.iceberg.view.View> viewEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_VIEW, NameMapping.class, org.apache.iceberg.view.View.class);
            viewEntry.put(t1, newInterfaceProxy(org.apache.iceberg.view.View.class));
            viewEntry.put(t2, newInterfaceProxy(org.apache.iceberg.view.View.class));

            String sharedManifestPath = "/tmp/shared.avro";
            IcebergManifestEntryKey m1 = mockManifestKey(sharedManifestPath);
            IcebergManifestEntryKey m2 = mockManifestKey(sharedManifestPath);
            MetaCacheEntry<IcebergManifestEntryKey, ManifestCacheValue> manifestEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_MANIFEST, IcebergManifestEntryKey.class, ManifestCacheValue.class);
            Assert.assertEquals(m1, m2);
            manifestEntry.put(m1, ManifestCacheValue.forDataFiles(com.google.common.collect.Lists.newArrayList()));
            manifestEntry.put(m2, ManifestCacheValue.forDataFiles(com.google.common.collect.Lists.newArrayList()));

            Assert.assertNotNull(manifestEntry.getIfPresent(m1));
            Assert.assertNotNull(manifestEntry.getIfPresent(m2));
            cache.invalidateTable(catalogId, "db1", "tbl1");

            Assert.assertNull(tableEntry.getIfPresent(t1));
            Assert.assertNotNull(tableEntry.getIfPresent(t2));
            Assert.assertNull(viewEntry.getIfPresent(t1));
            Assert.assertNotNull(viewEntry.getIfPresent(t2));
            Assert.assertNotNull(manifestEntry.getIfPresent(m1));
            Assert.assertNotNull(manifestEntry.getIfPresent(m2));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testInvalidateDbAndStats() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, manifestCacheEnabledProperties());
            NameMapping db1Table = new NameMapping(catalogId, "db1", "tbl1", "rdb1", "rtbl1");
            NameMapping db2Table = new NameMapping(catalogId, "db2", "tbl1", "rdb2", "rtbl1");

            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tableEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_TABLE, NameMapping.class, IcebergTableCacheValue.class);
            tableEntry.put(db1Table, new IcebergTableCacheValue(newInterfaceProxy(Table.class),
                    () -> new IcebergSnapshotCacheValue(IcebergPartitionInfo.empty(), new IcebergSnapshot(1L, 1L))));
            tableEntry.put(db2Table, new IcebergTableCacheValue(newInterfaceProxy(Table.class),
                    () -> new IcebergSnapshotCacheValue(IcebergPartitionInfo.empty(), new IcebergSnapshot(2L, 2L))));

            MetaCacheEntry<IcebergSchemaCacheKey, SchemaCacheValue> schemaEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_SCHEMA, IcebergSchemaCacheKey.class, SchemaCacheValue.class);
            IcebergSchemaCacheKey db1Schema = new IcebergSchemaCacheKey(db1Table, 1L);
            IcebergSchemaCacheKey db2Schema = new IcebergSchemaCacheKey(db2Table, 2L);
            schemaEntry.put(db1Schema, new SchemaCacheValue(Collections.emptyList()));
            schemaEntry.put(db2Schema, new SchemaCacheValue(Collections.emptyList()));
            MetaCacheEntry<IcebergManifestEntryKey, ManifestCacheValue> manifestEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_MANIFEST, IcebergManifestEntryKey.class, ManifestCacheValue.class);
            IcebergManifestEntryKey manifestKey = mockManifestKey("/tmp/db-invalidate.avro");
            manifestEntry.put(manifestKey,
                    ManifestCacheValue.forDataFiles(com.google.common.collect.Lists.newArrayList()));

            cache.invalidateDb(catalogId, "db1");

            Assert.assertNull(tableEntry.getIfPresent(db1Table));
            Assert.assertNotNull(tableEntry.getIfPresent(db2Table));
            Assert.assertNull(schemaEntry.getIfPresent(db1Schema));
            Assert.assertNotNull(schemaEntry.getIfPresent(db2Schema));
            Assert.assertNotNull(manifestEntry.getIfPresent(manifestKey));

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            Assert.assertTrue(stats.containsKey(IcebergExternalMetaCache.ENTRY_TABLE));
            Assert.assertTrue(stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST).isConfigEnabled());
            Assert.assertTrue(stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST).isEffectiveEnabled());
            Assert.assertFalse(stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST).isAutoRefresh());
            Assert.assertEquals(-1L, stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST).getTtlSecond());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSchemaStatsWhenSchemaCacheDisabled() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
            properties.put(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, "0");
            cache.initCatalog(catalogId, properties);

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            MetaCacheEntryStats schemaStats = stats.get(IcebergExternalMetaCache.ENTRY_SCHEMA);
            Assert.assertNotNull(schemaStats);
            Assert.assertEquals(0L, schemaStats.getTtlSecond());
            Assert.assertTrue(schemaStats.isConfigEnabled());
            Assert.assertFalse(schemaStats.isEffectiveEnabled());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testManifestStatsDisabledByDefault() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            MetaCacheEntryStats manifestStats = stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST);
            Assert.assertNotNull(manifestStats);
            Assert.assertFalse(manifestStats.isConfigEnabled());
            Assert.assertFalse(manifestStats.isEffectiveEnabled());
            Assert.assertFalse(manifestStats.isAutoRefresh());
            Assert.assertEquals(-1L, manifestStats.getTtlSecond());
            Assert.assertEquals(100000L, manifestStats.getCapacity());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testManifestEntryRequiresContextualLoader() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, manifestCacheEnabledProperties());
            MetaCacheEntry<IcebergManifestEntryKey, ManifestCacheValue> manifestEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_MANIFEST, IcebergManifestEntryKey.class, ManifestCacheValue.class);
            IcebergManifestEntryKey manifestKey = mockManifestKey("/tmp/contextual-only.avro");

            UnsupportedOperationException exception = Assert.assertThrows(UnsupportedOperationException.class,
                    () -> manifestEntry.get(manifestKey));
            Assert.assertTrue(exception.getMessage().contains("contextual miss loader"));

            ManifestCacheValue value = manifestEntry.get(manifestKey,
                    ignored -> ManifestCacheValue.forDataFiles(com.google.common.collect.Lists.newArrayList()));
            Assert.assertNotNull(value);
            Assert.assertSame(value, manifestEntry.getIfPresent(manifestKey));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testManifestEnableUsesDefaultCapacity() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
            properties.put("meta.cache.iceberg.manifest.enable", "true");
            cache.initCatalog(catalogId, properties);

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            MetaCacheEntryStats manifestStats = stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST);
            Assert.assertNotNull(manifestStats);
            Assert.assertTrue(manifestStats.isConfigEnabled());
            Assert.assertTrue(manifestStats.isEffectiveEnabled());
            Assert.assertEquals(-1L, manifestStats.getTtlSecond());
            Assert.assertEquals(100000L, manifestStats.getCapacity());
        } finally {
            executor.shutdownNow();
        }
    }

    private Map<String, String> manifestCacheEnabledProperties() {
        Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
        properties.put("meta.cache.iceberg.manifest.enable", "true");
        return properties;
    }

    private IcebergManifestEntryKey mockManifestKey(String path) {
        return IcebergManifestEntryKey.of(new TestingManifestFile(path, ManifestContent.DATA));
    }

    private <T> T newInterfaceProxy(Class<T> type) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) {
                switch (method.getName()) {
                    case "equals":
                        return proxy == args[0];
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "toString":
                        return type.getSimpleName() + "Proxy";
                    default:
                        return null;
                }
            }
            return defaultValue(method.getReturnType());
        }));
    }

    private Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        if (type == char.class) {
            return '\0';
        }
        throw new IllegalArgumentException("unsupported primitive type: " + type);
    }

    private static final class TestingManifestFile implements ManifestFile {
        private final String path;
        private final ManifestContent content;

        private TestingManifestFile(String path, ManifestContent content) {
            this.path = path;
            this.content = content;
        }

        @Override
        public String path() {
            return path;
        }

        @Override
        public ManifestContent content() {
            return content;
        }

        @Override
        public long length() {
            return 0;
        }

        @Override
        public int partitionSpecId() {
            return 0;
        }

        @Override
        public long sequenceNumber() {
            return 0;
        }

        @Override
        public long minSequenceNumber() {
            return 0;
        }

        @Override
        public Long snapshotId() {
            return null;
        }

        @Override
        public Integer addedFilesCount() {
            return null;
        }

        @Override
        public Long addedRowsCount() {
            return null;
        }

        @Override
        public Integer existingFilesCount() {
            return null;
        }

        @Override
        public Long existingRowsCount() {
            return null;
        }

        @Override
        public Integer deletedFilesCount() {
            return null;
        }

        @Override
        public Long deletedRowsCount() {
            return null;
        }

        @Override
        public List<PartitionFieldSummary> partitions() {
            return null;
        }

        @Override
        public ByteBuffer keyMetadata() {
            return null;
        }

        @Override
        public ManifestFile copy() {
            return new TestingManifestFile(path, content);
        }
    }
}
