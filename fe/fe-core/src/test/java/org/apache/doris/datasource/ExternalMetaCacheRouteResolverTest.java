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

package org.apache.doris.datasource;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergHMSExternalCatalog;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.metacache.ExternalMetaCache;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryStats;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ExternalMetaCacheRouteResolverTest {

    private MockedStatic<Env> envMockedStatic;

    @After
    public void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
            envMockedStatic = null;
        }
    }

    @Test
    public void testEngineAliasCompatibility() {
        ExternalMetaCacheMgr metaCacheMgr = new ExternalMetaCacheMgr(true);
        Assert.assertEquals("hive", metaCacheMgr.engine("hms").engine());
        Assert.assertEquals("doris", metaCacheMgr.engine("External_Doris").engine());
        Assert.assertEquals("maxcompute", metaCacheMgr.engine("max_compute").engine());
    }

    @Test
    public void testRouteByCatalogType() {
        ExternalMetaCacheMgr metaCacheMgr = new ExternalMetaCacheMgr(true);

        List<String> hmsEngines = metaCacheMgr.resolveCatalogEngineNamesForTest(
                new HMSExternalCatalog(1L, "hms", null, Collections.emptyMap(), ""), 1L);
        Assert.assertTrue(hmsEngines.contains("hive"));
        Assert.assertTrue(hmsEngines.contains("hudi"));
        Assert.assertTrue(hmsEngines.contains("iceberg"));
        Assert.assertFalse(hmsEngines.contains("paimon"));
        Assert.assertFalse(hmsEngines.contains("doris"));
        Assert.assertFalse(hmsEngines.contains("maxcompute"));
        Assert.assertFalse(hmsEngines.contains("default"));

        List<String> icebergEngines = metaCacheMgr.resolveCatalogEngineNamesForTest(
                new IcebergHMSExternalCatalog(2L, "iceberg", null, Collections.emptyMap(), ""), 2L);
        Assert.assertEquals(java.util.Collections.singletonList("iceberg"), icebergEngines);

        List<String> paimonEngines = metaCacheMgr.resolveCatalogEngineNamesForTest(
                new PaimonExternalCatalog(3L, "paimon", null, Collections.emptyMap(), ""), 3L);
        Assert.assertEquals(java.util.Collections.singletonList("paimon"), paimonEngines);

        List<String> maxComputeEngines = metaCacheMgr.resolveCatalogEngineNamesForTest(
                new MaxComputeExternalCatalog(4L, "maxcompute", null, Collections.emptyMap(), ""), 4L);
        Assert.assertEquals(java.util.Collections.singletonList("maxcompute"), maxComputeEngines);

        List<String> dorisEngines = metaCacheMgr.resolveCatalogEngineNamesForTest(
                new RemoteDorisExternalCatalog(5L, "doris", null, Collections.emptyMap(), ""), 5L);
        Assert.assertEquals(java.util.Collections.singletonList("doris"), dorisEngines);
    }

    @Test
    public void testMissingCatalogOnlyRoutesInitializedEngines() {
        ExternalMetaCacheMgr metaCacheMgr = new ExternalMetaCacheMgr(true);
        long catalogId = 7L;

        metaCacheMgr.prepareCatalogByEngine(catalogId, "hive", java.util.Collections.emptyMap());

        List<String> engines = metaCacheMgr.resolveCatalogEngineNamesForTest(null, catalogId);
        Assert.assertTrue(engines.contains("hive"));
        Assert.assertFalse(engines.contains("iceberg"));
        Assert.assertFalse(engines.contains("paimon"));
    }

    @Test
    public void testPrepareCatalogByEngineSkipsMissingCatalog() throws Exception {
        RecordingExternalMetaCache hive = new RecordingExternalMetaCache(
                "hive", Collections.singletonList("hms"), catalog -> catalog instanceof HMSExternalCatalog);
        ExternalMetaCacheMgr metaCacheMgr = newManagerWithCaches(hive);
        long catalogId = 10L;

        mockCurrentCatalog(catalogId, null);

        metaCacheMgr.prepareCatalog(catalogId);
        metaCacheMgr.prepareCatalogByEngine(catalogId, "hive");

        Assert.assertEquals(0, hive.initCatalogCalls);
    }

    @Test
    public void testGetSchemaCacheValueReturnsEmptyWhenCatalogMissing() throws Exception {
        MissingCatalogSchemaExternalMetaCache schemaCache = new MissingCatalogSchemaExternalMetaCache("default");
        ExternalMetaCacheMgr metaCacheMgr = newManagerWithCaches(schemaCache);
        long catalogId = 11L;

        mockCurrentCatalog(catalogId, null);

        TestingExternalTable table = new TestingExternalTable(catalogId, "default");
        Assert.assertFalse(metaCacheMgr.getSchemaCacheValue(
                table, new SchemaCacheKey(table.getOrBuildNameMapping())).isPresent());
        Assert.assertEquals(1, schemaCache.entryCalls);
    }

    @Test
    public void testLifecycleRoutingOnlyTouchesSupportedEngine() throws Exception {
        RecordingExternalMetaCache hive = new RecordingExternalMetaCache(
                "hive", Collections.singletonList("hms"), catalog -> catalog instanceof HMSExternalCatalog);
        RecordingExternalMetaCache hudi = new RecordingExternalMetaCache(
                "hudi", Collections.emptyList(), catalog -> catalog instanceof HMSExternalCatalog);
        RecordingExternalMetaCache iceberg = new RecordingExternalMetaCache(
                "iceberg", Collections.emptyList(), catalog -> catalog instanceof HMSExternalCatalog);
        RecordingExternalMetaCache paimon = new RecordingExternalMetaCache(
                "paimon", Collections.emptyList(), catalog -> catalog instanceof PaimonExternalCatalog);
        ExternalMetaCacheMgr metaCacheMgr = newManagerWithCaches(hive, hudi, iceberg, paimon);
        long catalogId = 8L;

        HMSExternalCatalog catalog = new HMSExternalCatalog(
                catalogId, "hms", null, Collections.singletonMap("k", "v"), "");
        mockCurrentCatalog(catalogId, catalog);

        metaCacheMgr.prepareCatalog(catalogId);
        metaCacheMgr.invalidateCatalog(catalogId);
        metaCacheMgr.invalidateDb(catalogId, "db1");
        metaCacheMgr.invalidateTable(catalogId, "db1", "tbl1");
        metaCacheMgr.invalidatePartitions(catalogId, "db1", "tbl1", Collections.singletonList("p=1"));
        metaCacheMgr.removeCatalog(catalogId);

        Assert.assertEquals(1, hive.initCatalogCalls);
        Assert.assertEquals(1, hive.invalidateCatalogEntriesCalls);
        Assert.assertEquals(1, hive.invalidateDbCalls);
        Assert.assertEquals(1, hive.invalidateTableCalls);
        Assert.assertEquals(1, hive.invalidatePartitionsCalls);
        Assert.assertEquals(1, hive.invalidateCatalogCalls);

        Assert.assertEquals(1, hudi.initCatalogCalls);
        Assert.assertEquals(1, hudi.invalidateCatalogEntriesCalls);
        Assert.assertEquals(1, hudi.invalidateDbCalls);
        Assert.assertEquals(1, hudi.invalidateTableCalls);
        Assert.assertEquals(1, hudi.invalidatePartitionsCalls);
        Assert.assertEquals(1, hudi.invalidateCatalogCalls);

        Assert.assertEquals(1, iceberg.initCatalogCalls);
        Assert.assertEquals(1, iceberg.invalidateCatalogEntriesCalls);
        Assert.assertEquals(1, iceberg.invalidateDbCalls);
        Assert.assertEquals(1, iceberg.invalidateTableCalls);
        Assert.assertEquals(1, iceberg.invalidatePartitionsCalls);
        Assert.assertEquals(1, iceberg.invalidateCatalogCalls);

        Assert.assertEquals(0, paimon.initCatalogCalls);
        Assert.assertEquals(0, paimon.invalidateCatalogEntriesCalls);
        Assert.assertEquals(0, paimon.invalidateDbCalls);
        Assert.assertEquals(0, paimon.invalidateTableCalls);
        Assert.assertEquals(0, paimon.invalidatePartitionsCalls);
        Assert.assertEquals(0, paimon.invalidateCatalogCalls);
    }

    @Test
    public void testMissingCatalogLifecycleOnlyTouchesInitializedEngine() throws Exception {
        RecordingExternalMetaCache hive = new RecordingExternalMetaCache(
                "hive", Collections.singletonList("hms"), catalog -> catalog instanceof HMSExternalCatalog);
        RecordingExternalMetaCache paimon = new RecordingExternalMetaCache(
                "paimon", Collections.emptyList(), catalog -> catalog instanceof PaimonExternalCatalog);
        ExternalMetaCacheMgr metaCacheMgr = newManagerWithCaches(hive, paimon);
        long catalogId = 9L;

        hive.initializedCatalogIds.add(catalogId);
        mockCurrentCatalog(catalogId, null);

        metaCacheMgr.invalidateCatalog(catalogId);
        metaCacheMgr.invalidateDb(catalogId, "db1");
        metaCacheMgr.invalidateTable(catalogId, "db1", "tbl1");
        metaCacheMgr.invalidatePartitions(catalogId, "db1", "tbl1", Collections.singletonList("p=1"));
        metaCacheMgr.removeCatalog(catalogId);

        Assert.assertEquals(1, hive.invalidateCatalogEntriesCalls);
        Assert.assertEquals(1, hive.invalidateDbCalls);
        Assert.assertEquals(1, hive.invalidateTableCalls);
        Assert.assertEquals(1, hive.invalidatePartitionsCalls);
        Assert.assertEquals(1, hive.invalidateCatalogCalls);

        Assert.assertEquals(0, paimon.invalidateCatalogEntriesCalls);
        Assert.assertEquals(0, paimon.invalidateDbCalls);
        Assert.assertEquals(0, paimon.invalidateTableCalls);
        Assert.assertEquals(0, paimon.invalidatePartitionsCalls);
        Assert.assertEquals(0, paimon.invalidateCatalogCalls);
    }

    @SuppressWarnings("unchecked")
    private ExternalMetaCacheMgr newManagerWithCaches(RecordingExternalMetaCache... caches) throws Exception {
        ExternalMetaCacheMgr metaCacheMgr = new ExternalMetaCacheMgr(true);
        metaCacheMgr.replaceEngineCachesForTest(java.util.Arrays.asList(caches));
        return metaCacheMgr;
    }

    private void mockCurrentCatalog(long catalogId,
            CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog) {
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
        CatalogMgr catalogMgr = new TestingCatalogMgr(catalogId, catalog);
        Env env = new TestingEnv(catalogMgr);
        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
    }

    private static final class TestingCatalogMgr extends CatalogMgr {
        private final Map<Long, CatalogIf<? extends DatabaseIf<? extends TableIf>>> catalogs = new HashMap<>();

        private TestingCatalogMgr(long catalogId, CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog) {
            catalogs.put(catalogId, catalog);
        }

        @Override
        public CatalogIf<? extends DatabaseIf<? extends TableIf>> getCatalog(long id) {
            return catalogs.get(id);
        }
    }

    private static final class TestingEnv extends Env {
        private final CatalogMgr catalogMgr;

        private TestingEnv(CatalogMgr catalogMgr) {
            super(true);
            this.catalogMgr = catalogMgr;
        }

        @Override
        public CatalogMgr getCatalogMgr() {
            return catalogMgr;
        }
    }

    private static final class TestingExternalTable extends ExternalTable {
        private final String metaCacheEngine;

        private TestingExternalTable(long catalogId, String metaCacheEngine) {
            this.metaCacheEngine = metaCacheEngine;
            this.catalog = new HMSExternalCatalog(catalogId, "hms", null, Collections.emptyMap(), "");
            this.dbName = "db1";
            this.name = "tbl1";
            this.remoteName = "remote_tbl1";
            this.nameMapping = new NameMapping(catalogId, "db1", "tbl1", "remote_db1", "remote_tbl1");
        }

        @Override
        public String getMetaCacheEngine() {
            return metaCacheEngine;
        }
    }

    private static class RecordingExternalMetaCache implements ExternalMetaCache {
        private final String engine;
        private final List<String> aliases;
        private final Set<Long> initializedCatalogIds = ConcurrentHashMap.newKeySet();

        private int initCatalogCalls;
        private int invalidateCatalogCalls;
        private int invalidateCatalogEntriesCalls;
        private int invalidateDbCalls;
        private int invalidateTableCalls;
        private int invalidatePartitionsCalls;

        private RecordingExternalMetaCache(String engine, List<String> aliases,
                java.util.function.Predicate<CatalogIf<?>> ignoredPredicate) {
            this.engine = engine;
            this.aliases = aliases;
        }

        @Override
        public String engine() {
            return engine;
        }

        @Override
        public List<String> aliases() {
            return aliases;
        }

        @Override
        public void initCatalog(long catalogId, Map<String, String> catalogProperties) {
            initializedCatalogIds.add(catalogId);
            initCatalogCalls++;
        }

        @Override
        public <K, V> MetaCacheEntry<K, V> entry(
                long catalogId, String entryName, Class<K> keyType, Class<V> valueType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCatalogInitialized(long catalogId) {
            if (!isCatalogInitialized(catalogId)) {
                throw new IllegalStateException("catalog " + catalogId + " is not initialized");
            }
        }

        @Override
        public boolean isCatalogInitialized(long catalogId) {
            return initializedCatalogIds.contains(catalogId);
        }

        @Override
        public void invalidateCatalog(long catalogId) {
            initializedCatalogIds.remove(catalogId);
            invalidateCatalogCalls++;
        }

        @Override
        public void invalidateCatalogEntries(long catalogId) {
            invalidateCatalogEntriesCalls++;
        }

        @Override
        public void invalidateDb(long catalogId, String dbName) {
            invalidateDbCalls++;
        }

        @Override
        public void invalidateTable(long catalogId, String dbName, String tableName) {
            invalidateTableCalls++;
        }

        @Override
        public void invalidatePartitions(long catalogId, String dbName, String tableName, List<String> partitions) {
            invalidatePartitionsCalls++;
        }

        @Override
        public Map<String, MetaCacheEntryStats> stats(long catalogId) {
            return Collections.emptyMap();
        }

        @Override
        public void close() {
        }
    }

    private static final class MissingCatalogSchemaExternalMetaCache extends RecordingExternalMetaCache {
        private int entryCalls;

        private MissingCatalogSchemaExternalMetaCache(String engine) {
            super(engine, Collections.emptyList(), catalog -> true);
        }

        @Override
        public <K, V> MetaCacheEntry<K, V> entry(long catalogId, String entryName, Class<K> keyType, Class<V> valueType) {
            entryCalls++;
            throw new IllegalStateException("catalog " + catalogId + " is not initialized");
        }
    }
}
