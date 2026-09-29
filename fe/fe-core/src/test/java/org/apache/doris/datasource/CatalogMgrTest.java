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
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;
import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;
import org.apache.doris.datasource.metacache.MetaCache;
import org.apache.doris.datasource.operations.ExternalMetadataOps;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.property.metastore.AbstractPaimonProperties;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.persist.DropDbInfo;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.statistics.query.QueryStats;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.ImmutableMap;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CatalogMgrTest {

    @Test
    void testDatabaseCapacityEvictionDoesNotFenceAllRowCounts() {
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(49L);
        @SuppressWarnings("unchecked")
        ExternalDatabase<? extends ExternalTable> db = Mockito.mock(ExternalDatabase.class);

        catalog.handleDatabaseMetaCacheRemoval(Optional.of(db), RemovalCause.SIZE);
        Mockito.verify(db).resetMetaToUninitialized(true, false);

        catalog.handleDatabaseMetaCacheRemoval(Optional.of(db), RemovalCause.EXPLICIT);
        Mockito.verify(db).resetMetaToUninitialized(true, true);
    }

    @Test
    void testReplayDatabaseIdentitySurvivesObjectEviction() throws Exception {
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(83L,
                ImmutableMap.of(ExternalCatalog.LOWER_CASE_DATABASE_NAMES, "2"));
        @SuppressWarnings("unchecked")
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = Mockito.mock(MetaCache.class);
        catalog.installMetaCache(metaCache);
        Field mappingField = ExternalCatalog.class.getDeclaredField("lowerCaseToDatabaseName");
        mappingField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, String> mapping = (Map<String, String>) mappingField.get(catalog);
        mapping.put("mixeddb", "MixedDb");

        @SuppressWarnings("unchecked")
        ExternalDatabase<? extends ExternalTable> warmDb = Mockito.mock(ExternalDatabase.class);
        Mockito.when(metaCache.tryGetMetaObj("MixedDb")).thenReturn(Optional.of(warmDb));
        Assertions.assertSame(warmDb, catalog.getDbForReplay("mixeddb").orElseThrow(AssertionError::new));
        Assertions.assertEquals(Pair.of("MixedDb", Util.genIdByName("testing_catalog", "MixedDb")),
                catalog.getDbIdentityForReplay("mixeddb", 0L).orElseThrow(AssertionError::new));
        Assertions.assertEquals(Pair.of("MixedDb", Util.genIdByName("testing_catalog", "MixedDb")),
                catalog.getDbIdentityForReplay("MixedDb", 0L).orElseThrow(AssertionError::new));
        Mockito.verify(metaCache, Mockito.never()).getMetaObj(Mockito.anyString(), Mockito.anyLong());
        Mockito.when(metaCache.getNameByIdIfPresent(84L)).thenReturn(Optional.of("MixedDb"));
        Assertions.assertEquals(Pair.of("MixedDb", 84L),
                catalog.getDbIdentityForReplay(null, 84L).orElseThrow(AssertionError::new));
    }

    @Test
    void testColdKnownDropInvalidatesOnlyItsDatabase() {
        long catalogId = 85L;
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(catalogId);
        @SuppressWarnings("unchecked")
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = Mockito.mock(MetaCache.class);
        catalog.installMetaCache(metaCache);
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalog.invalidateColdDatabaseForReplay("cold_db");
        }

        Mockito.verify(cacheMgr).invalidateDb(catalogId,
                Util.genIdByName("testing_catalog", "cold_db"), "cold_db");
        Mockito.verify(cacheMgr, Mockito.never()).invalidateCatalog(catalogId);
    }

    private static void addCatalog(CatalogMgr catalogMgr, ExternalCatalog catalog) throws Exception {
        Field idToCatalogField = CatalogMgr.class.getDeclaredField("idToCatalog");
        idToCatalogField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<Long, CatalogIf<? extends DatabaseIf<? extends TableIf>>> idToCatalog =
                (ConcurrentMap<Long, CatalogIf<? extends DatabaseIf<? extends TableIf>>>)
                        idToCatalogField.get(catalogMgr);
        idToCatalog.put(catalog.getId(), catalog);
    }

    private static void addNamedCatalog(CatalogMgr catalogMgr, ExternalCatalog catalog) throws Exception {
        addCatalog(catalogMgr, catalog);
        Field nameToCatalogField = CatalogMgr.class.getDeclaredField("nameToCatalog");
        nameToCatalogField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<String, CatalogIf<? extends DatabaseIf<? extends TableIf>>> nameToCatalog =
                (ConcurrentMap<String, CatalogIf<? extends DatabaseIf<? extends TableIf>>>)
                        nameToCatalogField.get(catalogMgr);
        nameToCatalog.put(catalog.getName(), catalog);
    }

    @Test
    void testAlterCatalogRollsBackUncheckedValidationFailure() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        long catalogId = 42L;
        Mockito.when(catalog.getId()).thenReturn(catalogId);

        addCatalog(catalogMgr, catalog);

        Map<String, String> oldProperties = ImmutableMap.of("read.batch-size", "1024");
        Map<String, String> newProperties = ImmutableMap.of("read.batch-size", "0");
        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalogId);
        log.setNewProps(newProperties);
        Mockito.doThrow(new IllegalArgumentException("invalid reader option"))
                .when(catalog).checkProperties();

        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> catalogMgr.replayAlterCatalogProps(log, oldProperties, false));

        Assertions.assertTrue(exception.getMessage().contains("invalid reader option"));
        Mockito.verify(catalog).tryModifyCatalogProps(newProperties);
        Mockito.verify(catalog).rollBackCatalogProps(oldProperties);
        Mockito.verify(catalog, Mockito.never()).modifyCatalogProps(newProperties);
    }

    @Test
    void testMalformedJdbcMappingAlterDoesNotPublishOrJournal() throws Exception {
        Map<String, String> properties = new HashMap<>(ImmutableMap.of(
                "type", "jdbc",
                JdbcResource.DRIVER_URL, "driver.jar",
                JdbcResource.JDBC_URL, "jdbc:oracle:thin:@127.0.0.1:1521:XE",
                JdbcResource.DRIVER_CLASS, "oracle.jdbc.driver.OracleDriver"));
        JdbcExternalCatalog catalog = new JdbcExternalCatalog(98L, "jdbc", null, properties, "");
        CatalogMgr catalogMgr = new CatalogMgr();
        addCatalog(catalogMgr, catalog);
        Map<String, String> oldProperties = new HashMap<>(catalog.getProperties());
        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalog.getId());
        log.setNewProps(ImmutableMap.of(ExternalCatalog.META_NAMES_MAPPING, "not-json"));

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(cacheMgr.withCatalogLifecycleLock(Mockito.eq(catalog.getId()), Mockito.any()))
                .thenAnswer(invocation -> {
                    java.util.function.Supplier<?> action = invocation.getArgument(1);
                    return action.get();
                });
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertThrows(DdlException.class,
                    () -> catalogMgr.replayAlterCatalogProps(log, oldProperties, false));
        }

        Assertions.assertEquals(oldProperties, catalog.getProperties());
        Mockito.verify(editLog, Mockito.never()).logCatalogLog(Mockito.anyShort(), Mockito.any());
    }

    @Test
    void testDetachedValidationNeverPublishesCandidateToConcurrentInitialization() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        Map<String, String> oldProperties = ImmutableMap.of("read.batch-size", "1024");
        Map<String, String> newProperties = ImmutableMap.of(
                "read.batch-size", "4096",
                CatalogMgr.METADATA_REFRESH_INTERVAL_SEC, "invalid");
        LatchingValidationCatalog catalog = new LatchingValidationCatalog(43L, oldProperties);
        addCatalog(catalogMgr, catalog);
        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalog.getId());
        log.setNewProps(newProperties);
        // Pay the one-time Env bootstrap cost here: the first Env.getCurrentEnv() call can take
        // many seconds on a loaded CI host, and it must not be counted against the latched
        // validation window below.
        Assertions.assertNotNull(Env.getCurrentEnv().getExtMetaCacheMgr());
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            Future<DdlException> alterResult = executor.submit(() -> {
                try {
                    catalogMgr.replayAlterCatalogProps(log, oldProperties, false);
                    return null;
                } catch (DdlException e) {
                    return e;
                }
            });
            Assertions.assertTrue(catalog.validationStarted.await(60, TimeUnit.SECONDS));

            Assertions.assertThrows(RuntimeException.class, catalog::makeSureInitialized);
            DdlException validationFailure = alterResult.get(60, TimeUnit.SECONDS);

            Assertions.assertNotNull(validationFailure);
            Assertions.assertEquals(oldProperties, catalog.propertiesSeenByInitialization);
            Assertions.assertEquals(oldProperties, catalog.getProperties());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testCommittedAlterRetiresTheOperationalContextButFailedAlterDoesNot() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        long catalogId = 45L;
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.validatePropertiesBeforeUpdate(Mockito.any(), Mockito.any()))
                .thenReturn(true);
        addCatalog(catalogMgr, catalog);
        Map<String, String> oldProperties = ImmutableMap.of("s3.access_key", "old");
        Map<String, String> newProperties = ImmutableMap.of("s3.access_key", "new");
        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalogId);
        log.setNewProps(newProperties);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        Mockito.when(cacheMgr.withCatalogLifecycleLock(Mockito.eq(catalogId), Mockito.any()))
                .thenAnswer(invocation -> {
                    java.util.function.Supplier<?> action = invocation.getArgument(1);
                    return action.get();
                });
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalogMgr.replayAlterCatalogProps(log, oldProperties, false);
        }
        // The commit reset the catalog execution context: cached generations bound to the old
        // context must be retired so the next statement loads a plannable one.
        Mockito.verify(catalog).modifyCatalogProps(newProperties);
        Mockito.verify(cacheMgr).onCatalogOperationalContextChanged(catalogId);

        // A failed validation never commits, so nothing may be retired.
        Mockito.reset(cacheMgr);
        Mockito.when(cacheMgr.withCatalogLifecycleLock(Mockito.eq(catalogId), Mockito.any()))
                .thenAnswer(invocation -> {
                    java.util.function.Supplier<?> action = invocation.getArgument(1);
                    return action.get();
                });
        Mockito.when(catalog.validatePropertiesBeforeUpdate(Mockito.any(), Mockito.any()))
                .thenThrow(new IllegalArgumentException("invalid"));
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertThrows(DdlException.class,
                    () -> catalogMgr.replayAlterCatalogProps(log, oldProperties, false));
        }
        Mockito.verify(cacheMgr, Mockito.never()).onCatalogOperationalContextChanged(catalogId);
    }

    @Test
    void testReplayKeepsPersistedLegacyPaimonOptionLoadableButInactive() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        Map<String, String> persistedProperties = new HashMap<>();
        persistedProperties.put("type", "paimon");
        persistedProperties.put("paimon.catalog.type", "filesystem");
        persistedProperties.put("warehouse", "s3://example-bucket/warehouse");
        persistedProperties.put("paimon.table-option.write.batch-size", "2048");
        ReplayCompatiblePaimonCatalog catalog = new ReplayCompatiblePaimonCatalog(44L, persistedProperties);
        addCatalog(catalogMgr, catalog);
        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalog.getId());
        log.setNewProps(ImmutableMap.of(ExternalCatalog.USE_META_CACHE, "false"));

        catalogMgr.replayAlterCatalogProps(log, persistedProperties, true);

        AbstractPaimonProperties restoredProperties = (AbstractPaimonProperties)
                catalog.getCatalogProperty().getMetastoreProperties();
        Assertions.assertEquals("2048",
                catalog.getProperties().get("paimon.table-option.write.batch-size"));
        Assertions.assertTrue(restoredProperties.getTableOptionsMap().isEmpty());
    }

    @Test
    void testReplayPublishesPropertiesOnlyThroughTheFencedCommit() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        long catalogId = 46L;
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        addCatalog(catalogMgr, catalog);
        Map<String, String> oldProperties = ImmutableMap.of("s3.access_key", "old");
        Map<String, String> newProperties = ImmutableMap.of("s3.access_key", "new");
        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalogId);
        log.setNewProps(newProperties);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(cacheMgr.withCatalogLifecycleLock(Mockito.eq(catalogId), Mockito.any()))
                .thenAnswer(invocation -> {
                    java.util.function.Supplier<?> action = invocation.getArgument(1);
                    return action.get();
                });
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalogMgr.replayAlterCatalogProps(log, oldProperties, true);
        }

        Mockito.verify(catalog, Mockito.never()).tryModifyCatalogProps(Mockito.any());
        InOrder order = Mockito.inOrder(cacheMgr, catalog);
        order.verify(cacheMgr).invalidateRowCountCache(catalogId);
        order.verify(catalog).modifyCatalogProps(newProperties);
        order.verify(cacheMgr).onCatalogOperationalContextChanged(catalogId);
    }

    @Test
    void testUnsupportedAddPartitionEventStillInvalidatesRowCount() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        long catalogId = 46L;
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getName()).thenReturn("hms");
        Mockito.doReturn(db).when(catalog).getDbNullable("db1");
        Mockito.doReturn(table).when(db).getTableNullable("tbl1");
        Mockito.when(table.getPartitionColumnTypes(Mockito.any()))
                .thenThrow(new NotSupportedException("unsupported table"));
        addNamedCatalog(catalogMgr, catalog);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalogMgr.addExternalPartitions(
                    "hms", "db1", "tbl1", Collections.singletonList("p=1"), 1L, false);
        }

        Mockito.verify(cacheMgr).invalidateRowCountCache(catalogId, "db1", "tbl1");
        Mockito.verify(cacheMgr, Mockito.never()).invalidateTableByNameOrWider(Mockito.anyLong(),
                Mockito.anyString(), Mockito.anyString());
        Mockito.verify(cacheMgr, Mockito.never()).hive(catalogId);
    }

    @Test
    void testDropPartitionEventInvalidatesRowCountBeforeCacheFailure() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        long catalogId = 49L;
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getName()).thenReturn("hms");
        Mockito.doReturn(db).when(catalog).getDbNullable("db1");
        Mockito.doReturn(table).when(db).getTableNullable("tbl1");
        addNamedCatalog(catalogMgr, catalog);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        HiveExternalMetaCache hiveCache = Mockito.mock(HiveExternalMetaCache.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(cacheMgr.hive(catalogId)).thenReturn(hiveCache);
        Mockito.doThrow(new IllegalStateException("partition cache failure"))
                .when(hiveCache).dropPartitionsCache(
                        Mockito.eq(table), Mockito.anyList(), Mockito.eq(true));
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertThrows(IllegalStateException.class,
                    () -> catalogMgr.dropExternalPartitions(
                            "hms", "db1", "tbl1", Collections.singletonList("p=1"), 1L, false));
        }

        // The opening fence and the failure-safe closing fence both run.
        Mockito.verify(cacheMgr, Mockito.times(2)).invalidateRowCountCache(catalogId, "db1", "tbl1");
    }

    @Test
    void testSuccessfulPartitionEventClosesRowCountAdmissionWindow() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        long catalogId = 62L;
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getName()).thenReturn("hms");
        Mockito.doReturn(db).when(catalog).getDbNullable("db1");
        Mockito.doReturn(table).when(db).getTableNullable("tbl1");
        Mockito.when(table.getPartitionColumnTypes(Mockito.any())).thenReturn(Collections.emptyList());
        addNamedCatalog(catalogMgr, catalog);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        HiveExternalMetaCache hiveCache = Mockito.mock(HiveExternalMetaCache.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(cacheMgr.hive(catalogId)).thenReturn(hiveCache);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalogMgr.addExternalPartitions(
                    "hms", "db1", "tbl1", Collections.singletonList("p=1"), 1L, false);
        }

        // One fence before the selective update, one after it closes the admission window.
        Mockito.verify(cacheMgr, Mockito.times(2))
                .invalidateRowCountCache(catalogId, "db1", "tbl1");
    }

    @Test
    void testColdPartitionEventsFenceRowCountBeforeIgnoredTableMiss() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        long catalogId = 50L;
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getName()).thenReturn("hms");
        Mockito.doReturn(db).when(catalog).getDbNullable("db1");
        Mockito.doReturn(null).when(db).getTableNullable("tbl1");
        addNamedCatalog(catalogMgr, catalog);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalogMgr.addExternalPartitions(
                    "hms", "db1", "tbl1", Collections.singletonList("p=1"), 1L, true);
            catalogMgr.dropExternalPartitions(
                    "hms", "db1", "tbl1", Collections.singletonList("p=1"), 1L, true);
        }

        Mockito.verify(cacheMgr, Mockito.times(2))
                .invalidateRowCountCache(catalogId, "db1", "tbl1");
        Mockito.verify(cacheMgr, Mockito.times(2))
                .invalidateTableByNameOrWider(catalogId, "db1", "tbl1");
        Mockito.verify(cacheMgr, Mockito.never()).hive(catalogId);
    }

    @Test
    void testCatalogRefreshUsesSingleRowCountInvalidationOwner() {
        long catalogId = 50L;
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(catalogId);
        @SuppressWarnings("unchecked")
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = Mockito.mock(MetaCache.class);
        catalog.installMetaCache(metaCache);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalog.onRefreshCache(true);
        }

        InOrder order = Mockito.inOrder(cacheMgr, metaCache);
        order.verify(cacheMgr).invalidateRowCountCache(catalogId);
        order.verify(metaCache).invalidateAll();
        order.verify(cacheMgr).invalidateCatalog(catalogId);
        Mockito.verify(cacheMgr, Mockito.never()).getRowCountCache();
    }

    @Test
    void testMetadataOnlyRefreshStillInvalidatesRowCount() {
        long catalogId = 51L;
        long dbId = 52L;
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(catalogId);
        ExternalDatabase<ExternalTable> db = new ExternalDatabase<ExternalTable>(
                catalog, dbId, "db1", "db1", InitDatabaseLog.Type.TEST) {
            @Override
            protected ExternalTable buildTableInternal(String remoteTableName, String localTableName, long tblId,
                    ExternalCatalog externalCatalog, ExternalDatabase externalDatabase) {
                return null;
            }
        };
        ExecutorService removalExecutor = Executors.newSingleThreadExecutor();
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = new MetaCache<>(
                "databaseCache",
                removalExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> Collections.emptyList(),
                key -> Optional.empty(),
                (key, value, cause) -> catalog.handleDatabaseMetaCacheRemoval(value, cause));
        metaCache.updateCache("db1", "db1", db, dbId);
        catalog.installMetaCache(metaCache);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        try {
            try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
                mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
                catalog.onRefreshCache(false);
            }
        } finally {
            removalExecutor.shutdownNow();
        }

        Mockito.verify(cacheMgr).invalidateDb(db);
        Mockito.verify(cacheMgr, Mockito.times(2)).invalidateRowCountCache(catalogId);
        Mockito.verify(cacheMgr, Mockito.never()).invalidateCatalog(Mockito.anyLong());
    }

    @Test
    void testUnregisterDatabaseRemovesLocalEntryWhenEngineInvalidationFails() {
        long catalogId = 47L;
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(catalogId);
        @SuppressWarnings("unchecked")
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = Mockito.mock(MetaCache.class);
        catalog.installMetaCache(metaCache);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.doThrow(new IllegalStateException("engine invalidation failed"))
                .when(cacheMgr).invalidateDb(catalogId,
                        Util.genIdByName("testing_catalog", "CanonicalDb"), "CanonicalDb");
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertThrows(IllegalStateException.class,
                    () -> catalog.unregisterDatabase("CanonicalDb"));
        }

        // The local metadata entry is removed before the engine invalidation runs, so a failing
        // engine flush cannot leave the dropped database resident.
        Mockito.verify(metaCache).invalidate("CanonicalDb",
                Util.genIdByName("testing_catalog", "CanonicalDb"));
        Mockito.verify(cacheMgr).invalidateRowCountCache(catalogId,
                Util.genIdByName("testing_catalog", "CanonicalDb"));
    }

    @Test
    void testResidentDatabaseRemovalInvalidatesLocalAndRoutedCache() {
        long catalogId = 53L;
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(catalogId);
        @SuppressWarnings("unchecked")
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = Mockito.mock(MetaCache.class);
        catalog.installMetaCache(metaCache);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalog.unregisterDatabase("CanonicalDb");
        }

        Mockito.verify(metaCache).invalidate("CanonicalDb",
                Util.genIdByName("testing_catalog", "CanonicalDb"));
        Mockito.verify(cacheMgr).invalidateRowCountCache(catalogId,
                Util.genIdByName("testing_catalog", "CanonicalDb"));
        Mockito.verify(cacheMgr).invalidateDb(catalogId,
                Util.genIdByName("testing_catalog", "CanonicalDb"), "CanonicalDb");
    }

    @Test
    void testColdDatabaseDropFencesBeforeRemovingNameSlot() throws Exception {
        long catalogId = 86L;
        long dbId = Util.genIdByName("testing_catalog", "cold_db");
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(catalogId);
        @SuppressWarnings("unchecked")
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = Mockito.mock(MetaCache.class);
        catalog.installMetaCache(metaCache);
        CountDownLatch nameRemovalStarted = new CountDownLatch(1);
        CountDownLatch releaseNameRemoval = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            nameRemovalStarted.countDown();
            Assertions.assertTrue(releaseNameRemoval.await(10, TimeUnit.SECONDS));
            return null;
        }).when(metaCache).invalidate("cold_db", dbId);
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        new MockUp<Env>() {
            @Mock
            Env getCurrentEnv() {
                return env;
            }
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> drop = executor.submit(() -> {
                catalog.unregisterDatabase("cold_db");
            });
            Assertions.assertTrue(nameRemovalStarted.await(10, TimeUnit.SECONDS));
            Mockito.verify(cacheMgr).invalidateRowCountCache(catalogId, dbId);
            Mockito.verify(cacheMgr, Mockito.never()).invalidateDb(catalogId, dbId, "cold_db");
            releaseNameRemoval.countDown();
            drop.get(10, TimeUnit.SECONDS);
            Mockito.verify(cacheMgr).invalidateDb(catalogId, dbId, "cold_db");
        } finally {
            releaseNameRemoval.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testUnresolvedModeTwoDatabaseRemovalRetiresCatalogScope() throws Exception {
        long catalogId = 60L;
        Map<String, String> properties = ImmutableMap.of(ExternalCatalog.LOWER_CASE_DATABASE_NAMES, "2");
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(catalogId, properties);
        @SuppressWarnings("unchecked")
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = Mockito.mock(MetaCache.class);
        catalog.installMetaCache(metaCache);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            // The mode-2 mapping is gone, so the canonical local key cannot be resolved.
            catalog.unregisterDatabase("dropped_db");
        }

        // Unknown scope: retire every cached database object and flush engine caches/row counts
        // catalog-wide instead of evicting a lowercased local key that may not exist.
        Mockito.verify(metaCache).invalidateObjects();
        Mockito.verify(cacheMgr).invalidateCatalog(catalogId);
        Mockito.verify(cacheMgr, Mockito.times(2)).invalidateRowCountCache(catalogId);
        Mockito.verify(cacheMgr, Mockito.never()).invalidateDb(Mockito.anyLong(), Mockito.anyString());
    }

    @Test
    void testReboundModeTwoDatabaseDropDoesNotRetireReplacement() throws Exception {
        long catalogId = 89L;
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(catalogId,
                ImmutableMap.of(ExternalCatalog.LOWER_CASE_DATABASE_NAMES, "2"));
        @SuppressWarnings("unchecked")
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = Mockito.mock(MetaCache.class);
        catalog.installMetaCache(metaCache);
        Field mappingField = ExternalCatalog.class.getDeclaredField("lowerCaseToDatabaseName");
        mappingField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, String> mapping = (Map<String, String>) mappingField.get(catalog);
        mapping.put("foo", "FOO");
        long oldId = Util.genIdByName("testing_catalog", "Foo");
        Mockito.when(metaCache.getNameByIdIfPresent(oldId)).thenReturn(Optional.of("Foo"));
        @SuppressWarnings("unchecked")
        ExternalDatabase<? extends ExternalTable> oldDb = Mockito.mock(ExternalDatabase.class);
        @SuppressWarnings("unchecked")
        ExternalDatabase<? extends ExternalTable> replacementDb = Mockito.mock(ExternalDatabase.class);
        Mockito.when(metaCache.tryGetMetaObj("Foo")).thenReturn(Optional.of(oldDb));
        Mockito.when(metaCache.tryGetMetaObj("FOO")).thenReturn(Optional.of(replacementDb));
        Assertions.assertSame(replacementDb, catalog.getDbForReplay("Foo").orElseThrow(AssertionError::new));
        Assertions.assertSame(oldDb, catalog.getDbForDropReplay("Foo").orElseThrow(AssertionError::new));
        Assertions.assertSame(replacementDb, catalog.getDbForDropReplay("FOO").orElseThrow(AssertionError::new));

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalog.unregisterDatabase("Foo");
        }

        Mockito.verify(metaCache).invalidate("Foo", oldId);
        Mockito.verify(metaCache, Mockito.never()).invalidate("FOO",
                Util.genIdByName("testing_catalog", "FOO"));
        Mockito.verify(cacheMgr).invalidateDb(catalogId, oldId, "Foo");
    }

    @Test
    void testColdReboundModeTwoDropUsesHistoricalIdentity() throws Exception {
        long catalogId = 90L;
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(catalogId,
                ImmutableMap.of(ExternalCatalog.LOWER_CASE_DATABASE_NAMES, "2"));
        @SuppressWarnings("unchecked")
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = Mockito.mock(MetaCache.class);
        catalog.installMetaCache(metaCache);
        Field mappingField = ExternalCatalog.class.getDeclaredField("lowerCaseToDatabaseName");
        mappingField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, String> mapping = (Map<String, String>) mappingField.get(catalog);
        mapping.put("foo", "FOO");
        long oldId = Util.genIdByName("testing_catalog", "Foo");
        Mockito.when(metaCache.getNameByIdIfPresent(oldId)).thenReturn(Optional.of("Foo"));
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalog.invalidateColdDatabaseForReplay("Foo");
        }

        Mockito.verify(cacheMgr).invalidateDb(catalogId, oldId, "Foo");
        Mockito.verify(cacheMgr, Mockito.never()).invalidateDb(catalogId,
                Util.genIdByName("testing_catalog", "FOO"), "FOO");
    }

    @Test
    void testModeTwoDropReplayUsesResolvedTargetAndLegacyLogRetiresBothCandidates() throws Exception {
        long catalogId = 91L;
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(catalogId,
                ImmutableMap.of(ExternalCatalog.LOWER_CASE_DATABASE_NAMES, "2"));
        @SuppressWarnings("unchecked")
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = Mockito.mock(MetaCache.class);
        catalog.installMetaCache(metaCache);
        ExternalMetadataOps metadataOps = Mockito.mock(ExternalMetadataOps.class);
        catalog.metadataOps = metadataOps;
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            // Both histories carry caller spelling Foo. New logs preserve the resolved target.
            catalog.replayDropTable("Foo", "t", "FOO");
            catalog.replayDropDb("Foo", "FOO");
            catalog.replayDropTable("Foo", "t", "Foo");
            catalog.replayDropDb("Foo", "Foo");
            // An old name-only log cannot distinguish those histories.
            catalog.replayDropTable("Foo", "t", null);
            catalog.replayDropDb("Foo", null);
        }

        Mockito.verify(metadataOps).afterDropTable("FOO", "t");
        Mockito.verify(metadataOps).afterDropDb("FOO");
        Mockito.verify(metadataOps).afterDropTable("Foo", "t");
        Mockito.verify(metadataOps).afterDropDb("Foo");
        Mockito.verify(metaCache, Mockito.times(2)).invalidateObjects();
        Mockito.verify(metaCache).invalidateNames();
        Mockito.verify(cacheMgr, Mockito.times(2)).invalidateCatalog(catalogId);
    }

    @Test
    void testLegacyModeTwoDatabaseDropRefreshesPopulatedNames() {
        long catalogId = 94L;
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(catalogId,
                ImmutableMap.of(ExternalCatalog.LOWER_CASE_DATABASE_NAMES, "2"));
        AtomicReference<List<Pair<String, String>>> remoteNames = new AtomicReference<>(
                Collections.singletonList(Pair.of("Foo", "Foo")));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = new MetaCache<>(
                "databaseCache", executor, OptionalLong.empty(), OptionalLong.empty(), 10,
                ignored -> remoteNames.get(), ignored -> Optional.empty(), (key, value, cause) -> { });
        catalog.installMetaCache(metaCache);
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals(Collections.singletonList("Foo"), catalog.getDbNames());
            remoteNames.set(Collections.emptyList());

            catalog.replayDropDb("Foo", null);

            Assertions.assertTrue(catalog.getDbNames().isEmpty());
            Mockito.verify(cacheMgr).invalidateCatalog(catalogId);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testNoOpDatabaseDropKeepsUnrelatedCachedDatabase() {
        long catalogId = 95L;
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(catalogId,
                ImmutableMap.of(ExternalCatalog.LOWER_CASE_DATABASE_NAMES, "2"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = new MetaCache<>(
                "databaseCache", executor, OptionalLong.empty(), OptionalLong.empty(), 10,
                ignored -> Collections.emptyList(), ignored -> Optional.empty(), (key, value, cause) -> { });
        long oldId = Util.genIdByName("testing_catalog", "Foo");
        long unrelatedId = Util.genIdByName("testing_catalog", "sales");
        ExternalDatabase<?> oldDb = Mockito.mock(ExternalDatabase.class);
        ExternalDatabase<?> unrelatedDb = Mockito.mock(ExternalDatabase.class);
        metaCache.addObjForTest(oldId, "Foo", oldDb);
        metaCache.addObjForTest(unrelatedId, "sales", unrelatedDb);
        catalog.installMetaCache(metaCache);
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalog.retireCachedDatabaseForNoOp("absent");
            Assertions.assertSame(unrelatedDb, metaCache.tryGetMetaObj("sales").orElseThrow(AssertionError::new));
            Mockito.verifyNoInteractions(cacheMgr);

            // A lost mode-2 mapping still permits narrow retirement of a retained historical ID.
            catalog.retireCachedDatabaseForNoOp("foo");
            Assertions.assertFalse(metaCache.tryGetMetaObj("Foo").isPresent());
            Assertions.assertSame(unrelatedDb, metaCache.tryGetMetaObj("sales").orElseThrow(AssertionError::new));
            Mockito.verify(cacheMgr).invalidateDb(catalogId, oldId, "Foo");
            Mockito.verify(cacheMgr, Mockito.never()).invalidateCatalog(catalogId);
            Mockito.verify(cacheMgr, Mockito.never()).invalidateRowCountCache(catalogId, unrelatedId);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testFilteredPartitionEventsLeaveOtherDatabaseCachesUntouched() throws Exception {
        long catalogId = 96L;
        HMSExternalCatalog catalog = new HMSExternalCatalog(catalogId, "hms", null,
                ImmutableMap.of(ExternalCatalog.LOWER_CASE_DATABASE_NAMES, "2",
                        ExternalCatalog.EXCLUDE_DATABASE_LIST, "archived",
                        ExternalCatalog.INCLUDE_TABLE_LIST, "sales.hot"), "");
        CatalogMgr catalogMgr = new CatalogMgr();
        addNamedCatalog(catalogMgr, catalog);
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertFalse(catalog.isPartitionEventTargetExcluded("sales", "hot"));
            for (String[] target : new String[][] {{"archived", "t"}, {"sales", "cold"}}) {
                catalogMgr.addExternalPartitions("hms", target[0], target[1],
                        Collections.singletonList("p=1"), 1L, true);
                catalogMgr.dropExternalPartitions("hms", target[0], target[1],
                        Collections.singletonList("p=1"), 1L, true);
                new RefreshManager().refreshPartitions("hms", target[0], target[1],
                        Collections.singletonList("p=1"), 1L, true);
                new RefreshManager().refreshExternalTableFromEvent("hms", target[0], target[1], 1L);
            }
            catalogMgr.unregisterExternalDatabase("archived", "hms");
            Mockito.verifyNoInteractions(cacheMgr);
        }
    }

    @Test
    void testHmsEventFilterMatchesListedCaseAndRefreshesSnapshot() throws Exception {
        HMSExternalCatalog catalog = new HMSExternalCatalog(97L, "hms", null,
                new HashMap<>(ImmutableMap.of(ExternalCatalog.EXCLUDE_DATABASE_LIST, "SALES",
                        ExternalCatalog.INCLUDE_TABLE_LIST, "SALES.cold")), "");
        // Listing compares the remote spelling exactly: neither filter hides sales.hot.
        Assertions.assertFalse(catalog.isDatabaseEventTargetExcluded("sales"));
        Assertions.assertFalse(catalog.isPartitionEventTargetExcluded("sales", "hot"));
        Assertions.assertTrue(catalog.isDatabaseEventTargetExcluded("SALES"));
        catalog.getCatalogProperty().addProperty(ExternalCatalog.INCLUDE_TABLE_LIST, "sales.cold");
        Assertions.assertTrue(catalog.isPartitionEventTargetExcluded("sales", "hot"));
    }

    @Test
    void testLiveDropLogsResolvedDatabaseInsteadOfCallerAlias() throws Exception {
        ExternalDatabase<?> resolvedDb = Mockito.mock(ExternalDatabase.class);
        Mockito.when(resolvedDb.getFullName()).thenReturn("FOO");
        ExternalTable resolvedTable = Mockito.mock(ExternalTable.class);
        Mockito.when(resolvedTable.getName()).thenReturn("t");
        Mockito.doReturn(resolvedTable).when(resolvedDb).getTableNullable("t");
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(92L,
                ImmutableMap.of(ExternalCatalog.LOWER_CASE_DATABASE_NAMES, "2")) {
            @Override
            public ExternalDatabase<? extends ExternalTable> getDbNullable(String dbName) {
                return resolvedDb;
            }
        };
        @SuppressWarnings("unchecked")
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = Mockito.mock(MetaCache.class);
        catalog.installMetaCache(metaCache);
        Field mappingField = ExternalCatalog.class.getDeclaredField("lowerCaseToDatabaseName");
        mappingField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, String> mapping = (Map<String, String>) mappingField.get(catalog);
        mapping.put("foo", "FOO");
        ExternalMetadataOps metadataOps = Mockito.mock(ExternalMetadataOps.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(metadataOps.dropDbImplWithResolvedName("Foo", false, false))
                .thenReturn(Optional.of("FOO"));
        catalog.metadataOps = metadataOps;
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalog.dropDb("Foo", false, false);
            catalog.dropTable("Foo", "t", false, false, false, false, false);
        }

        Mockito.verify(metadataOps).afterDropDb("FOO");
        ArgumentCaptor<DropDbInfo> log = ArgumentCaptor.forClass(DropDbInfo.class);
        Mockito.verify(editLog).logDropDb(log.capture());
        Assertions.assertEquals("FOO", log.getValue().getDbName());
        Assertions.assertEquals("FOO", log.getValue().getResolvedDbName());
        ArgumentCaptor<DropInfo> tableLog = ArgumentCaptor.forClass(DropInfo.class);
        Mockito.verify(editLog).logDropTable(tableLog.capture());
        Assertions.assertEquals("FOO", tableLog.getValue().getDb());
        Assertions.assertEquals("FOO", tableLog.getValue().getResolvedDb());
    }

    @Test
    void testUnresolvedDatabaseDropRetiresGenerationAndFencesCatalog() {
        long catalogId = 72L;
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(catalogId);
        @SuppressWarnings("unchecked")
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = Mockito.mock(MetaCache.class);
        catalog.installMetaCache(metaCache);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalog.retireUnresolvedDatabaseGeneration();
        }

        // Retire the hidden database generation and fence the catalog scope (engine + row counts),
        // even though the per-database removal callbacks suppress their own fences.
        Mockito.verify(metaCache).invalidateObjects();
        Mockito.verify(cacheMgr, Mockito.times(2)).invalidateRowCountCache(catalogId);
        Mockito.verify(cacheMgr).invalidateCatalog(catalogId);
    }

    @Test
    void testUnresolvedDatabaseFencesBeforeObjectGenerationSwap() throws Exception {
        long catalogId = 87L;
        TestingUnregisterCatalog catalog = new TestingUnregisterCatalog(catalogId);
        @SuppressWarnings("unchecked")
        MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache = Mockito.mock(MetaCache.class);
        catalog.installMetaCache(metaCache);
        CountDownLatch swapStarted = new CountDownLatch(1);
        CountDownLatch releaseSwap = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            swapStarted.countDown();
            Assertions.assertTrue(releaseSwap.await(10, TimeUnit.SECONDS));
            return null;
        }).when(metaCache).invalidateObjects();
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        new MockUp<Env>() {
            @Mock
            Env getCurrentEnv() {
                return env;
            }
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> retirement = executor.submit(() -> {
                catalog.retireAllDatabaseObjectsWithoutEngineInvalidation();
            });
            Assertions.assertTrue(swapStarted.await(10, TimeUnit.SECONDS));
            Mockito.verify(cacheMgr).invalidateRowCountCache(catalogId);
            releaseSwap.countDown();
            retirement.get(10, TimeUnit.SECONDS);
            Mockito.verify(cacheMgr, Mockito.times(2)).invalidateRowCountCache(catalogId);
        } finally {
            releaseSwap.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testUnresolvedTableNameRetiresHiddenTableGeneration() throws Exception {
        long catalogId = 73L;
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getLowerCaseTableNames()).thenReturn(2);
        ExternalDatabase<ExternalTable> db = new ExternalDatabase<ExternalTable>(
                catalog, 74L, "db1", "db1", InitDatabaseLog.Type.TEST) {
            @Override
            protected ExternalTable buildTableInternal(String remoteTableName, String localTableName, long tblId,
                    ExternalCatalog externalCatalog, ExternalDatabase externalDatabase) {
                return null;
            }
        };
        @SuppressWarnings("unchecked")
        MetaCache<ExternalTable> tableCache = Mockito.mock(MetaCache.class);
        Field metaCacheField = ExternalDatabase.class.getDeclaredField("metaCache");
        metaCacheField.setAccessible(true);
        metaCacheField.set(db, tableCache);

        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertFalse(db.unregisterTableForReplay("Tbl"));
        }

        // The mode-2 mapping is gone: retire the hidden table generation and widen the fence.
        Mockito.verify(tableCache).invalidateObjects();
        Mockito.verify(cacheMgr, Mockito.times(2)).invalidateRowCountCache(catalogId, 74L);
        Mockito.verify(cacheMgr).invalidateTableByNameOrWider(catalogId, "db1", "Tbl");
    }

    @Test
    void testCommittedAlterStillRetiresAndLogsWhenResetCleanupThrows() throws Exception {
        long catalogId = 58L;
        CatalogMgr catalogMgr = new CatalogMgr();
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getName()).thenReturn("catalog_name");
        Map<String, String> oldProperties = ImmutableMap.of("s3.access_key", "old", "warehouse", "s3://bucket");
        Map<String, String> committedProperties = ImmutableMap.of("s3.access_key", "new", "warehouse", "s3://bucket");
        Mockito.when(catalog.getProperties()).thenReturn(oldProperties, committedProperties);
        Mockito.doThrow(new RuntimeException("close failed"))
                .when(catalog).modifyCatalogProps(Mockito.any());
        addNamedCatalog(catalogMgr, catalog);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        RefreshManager refreshManager = Mockito.mock(RefreshManager.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);
        Mockito.when(cacheMgr.withCatalogLifecycleLock(Mockito.eq(catalogId), Mockito.any()))
                .thenAnswer(invocation -> {
                    java.util.function.Supplier<?> action = invocation.getArgument(1);
                    return action.get();
                });

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertThrows(RuntimeException.class,
                    () -> catalogMgr.alterCatalogProps("catalog_name", ImmutableMap.of("s3.access_key", "new")));
        }

        // The reset published the new properties before its throwable cleanup: retirement and
        // durability must still happen.
        Mockito.verify(cacheMgr).onCatalogOperationalContextChanged(catalogId);
        Mockito.verify(editLog).logCatalogLog(Mockito.anyShort(), Mockito.any());
    }

    @Test
    void testReplayAlterCatalogPropsSurvivesResetCleanupFailure() throws Exception {
        long catalogId = 61L;
        CatalogMgr catalogMgr = new CatalogMgr();
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getName()).thenReturn("catalog_name");
        Mockito.doThrow(new RuntimeException("close failed during replay"))
                .when(catalog).modifyCatalogProps(Mockito.any());
        addCatalog(catalogMgr, catalog);

        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalogId);
        log.setNewProps(ImmutableMap.of("s3.access_key", "new"));

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(cacheMgr.withCatalogLifecycleLock(Mockito.eq(catalogId), Mockito.any()))
                .thenAnswer(invocation -> {
                    java.util.function.Supplier<?> action = invocation.getArgument(1);
                    return action.get();
                });

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            // A follower must not terminate when local connector cleanup fails while applying an
            // already-durable ALTER record.
            Assertions.assertDoesNotThrow(() -> catalogMgr.replayAlterCatalogProps(log, null, true));
        }

        Mockito.verify(cacheMgr).onCatalogOperationalContextChanged(catalogId);
        // Replay never journals.
        Mockito.verify(editLog, Mockito.never()).logCatalogLog(Mockito.anyShort(), Mockito.any());
    }

    @Test
    void testDropCatalogFencesRowCountBeforeCleanup() throws Exception {
        long catalogId = 57L;
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getName()).thenReturn("dropme");
        CatalogMgr catalogMgr = new CatalogMgr();
        addNamedCatalog(catalogMgr, catalog);

        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        RefreshManager refreshManager = Mockito.mock(RefreshManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);
        Mockito.when(env.getQueryStats()).thenReturn(Mockito.mock(QueryStats.class));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalogMgr.dropCatalog("dropme", false);
        }

        InOrder order = Mockito.inOrder(cacheMgr, catalog);
        order.verify(cacheMgr).invalidateRowCountCache(catalogId);
        order.verify(catalog).onClose();
        Assertions.assertNull(catalogMgr.getCatalog("dropme"));
    }

    private static class LatchingValidationCatalog extends ExternalCatalog {
        private final CountDownLatch validationStarted = new CountDownLatch(1);
        private final CountDownLatch initializationReadProperties = new CountDownLatch(1);
        private volatile Map<String, String> propertiesSeenByInitialization;

        LatchingValidationCatalog(long id, Map<String, String> properties) {
            super(id, "latching_catalog", InitCatalogLog.Type.TEST, "");
            catalogProperty = new CatalogProperty(null, properties);
        }

        @Override
        public boolean validatePropertiesBeforeUpdate(
                Map<String, String> currentProperties, Map<String, String> updatedProperties) {
            validationStarted.countDown();
            try {
                Assertions.assertTrue(initializationReadProperties.await(60, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            throw new IllegalArgumentException("invalid candidate properties");
        }

        @Override
        protected void initLocalObjectsImpl() {
            propertiesSeenByInitialization = getProperties();
            initializationReadProperties.countDown();
            throw new IllegalStateException("stop after observing properties");
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return false;
        }
    }

    private static class ReplayCompatiblePaimonCatalog extends PaimonExternalCatalog {
        ReplayCompatiblePaimonCatalog(long id, Map<String, String> properties) {
            super(id, "persisted_paimon_catalog", null, properties, "");
        }

        @Override
        public void notifyPropertiesUpdated(Map<String, String> updatedProps) {
            // This test isolates edit-log property restoration from environment-owned cache services.
        }
    }

    private static class TestingUnregisterCatalog extends ExternalCatalog {
        TestingUnregisterCatalog(long id) {
            this(id, Collections.emptyMap());
        }

        TestingUnregisterCatalog(long id, Map<String, String> properties) {
            super(id, "testing_catalog", InitCatalogLog.Type.TEST, "");
            catalogProperty = new CatalogProperty(null, properties);
        }

        void installMetaCache(MetaCache<ExternalDatabase<? extends ExternalTable>> cache) {
            metaCache = cache;
            initialized = true;
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return false;
        }

        @Override
        protected void initLocalObjectsImpl() {
        }
    }
}
