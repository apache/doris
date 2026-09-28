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
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.property.metastore.AbstractPaimonProperties;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CatalogMgrTest {

    private static void addCatalog(CatalogMgr catalogMgr, ExternalCatalog catalog) throws Exception {
        Field idToCatalogField = CatalogMgr.class.getDeclaredField("idToCatalog");
        idToCatalogField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<Long, CatalogIf<? extends DatabaseIf<? extends TableIf>>> idToCatalog =
                (ConcurrentMap<Long, CatalogIf<? extends DatabaseIf<? extends TableIf>>>)
                        idToCatalogField.get(catalogMgr);
        idToCatalog.put(catalog.getId(), catalog);
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
        Mockito.verify(catalog).modifyCatalogProps(newProperties);
        Mockito.verify(cacheMgr).onCatalogOperationalContextChanged(catalogId);
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
}
