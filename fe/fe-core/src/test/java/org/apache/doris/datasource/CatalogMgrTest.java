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
import org.apache.doris.catalog.constraint.ConstraintManager;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.log.CatalogLog;
import org.apache.doris.datasource.log.InitCatalogLog;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CatalogMgrTest {

    private static void addCatalog(CatalogMgr catalogMgr,
            CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog) throws Exception {
        Field idToCatalogField = CatalogMgr.class.getDeclaredField("idToCatalog");
        idToCatalogField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<Long, CatalogIf<? extends DatabaseIf<? extends TableIf>>> idToCatalog =
                (ConcurrentMap<Long, CatalogIf<? extends DatabaseIf<? extends TableIf>>>)
                        idToCatalogField.get(catalogMgr);
        idToCatalog.put(catalog.getId(), catalog);
    }

    private static void addCatalogByName(
            CatalogMgr catalogMgr, String name,
            CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog)
            throws Exception {
        Field nameToCatalogField =
                CatalogMgr.class.getDeclaredField("nameToCatalog");
        nameToCatalogField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<String,
                CatalogIf<? extends DatabaseIf<? extends TableIf>>>
                nameToCatalog =
                (ConcurrentMap<String,
                        CatalogIf<? extends DatabaseIf<? extends TableIf>>>)
                        nameToCatalogField.get(catalogMgr);
        nameToCatalog.put(name, catalog);
    }

    private static ExternalCatalog mockExternalCatalog(long catalogId, String initialName) {
        AtomicReference<String> catalogName = new AtomicReference<>(initialName);
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getName()).thenAnswer(invocation -> catalogName.get());
        Mockito.doAnswer(invocation -> {
            catalogName.set(invocation.getArgument(0));
            return null;
        }).when(catalog).modifyCatalogName(Mockito.anyString());
        return catalog;
    }

    private static void assertReplacementRejected(
            CatalogMgr catalogMgr, String methodName,
            Class<?>[] parameterTypes, Object... arguments)
            throws Exception {
        Method method =
                CatalogMgr.class.getDeclaredMethod(
                        methodName, parameterTypes);
        method.setAccessible(true);
        InvocationTargetException exception =
                Assertions.assertThrows(
                        InvocationTargetException.class,
                        () -> method.invoke(catalogMgr, arguments));
        Assertions.assertInstanceOf(
                DdlException.class, exception.getCause());
        Assertions.assertTrue(
                exception.getCause().getMessage().contains(
                        "Catalog changed"));
    }

    @Test
    void testCatalogMutationRejectsReplacement() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        String catalogName = "replacement_test";
        CatalogIf<? extends DatabaseIf<? extends TableIf>>
                expectedCatalog =
                Mockito.mock(CatalogIf.class);
        CatalogIf<? extends DatabaseIf<? extends TableIf>>
                replacementCatalog =
                Mockito.mock(CatalogIf.class);
        addCatalogByName(
                catalogMgr, catalogName, replacementCatalog);

        assertReplacementRejected(
                catalogMgr, "dropCatalogInternal",
                new Class<?>[] {
                        String.class, boolean.class, CatalogIf.class
                },
                catalogName, false, expectedCatalog);
        assertReplacementRejected(
                catalogMgr, "alterCatalogNameInternal",
                new Class<?>[] {
                        String.class, String.class, CatalogIf.class
                },
                catalogName, "replacement_test_new",
                expectedCatalog);
        assertReplacementRejected(
                catalogMgr, "alterCatalogPropsInternal",
                new Class<?>[] {
                        String.class, Map.class, CatalogIf.class
                },
                catalogName, ImmutableMap.of("key", "value"),
                expectedCatalog);
    }

    @Test
    void testReplayDropCatalogInvalidatesDependentMtmvCaches() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        long catalogId = 41L;
        String catalogName = "drop_catalog_cache_test";
        CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getName()).thenReturn(catalogName);
        addCatalog(catalogMgr, catalog);
        addCatalogByName(catalogMgr, catalogName, catalog);

        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        List<TableNameInfo> affectedTables = List.of(
                new TableNameInfo(catalogName, "db", "base_table"),
                new TableNameInfo("internal", "db", "referencing_table"));
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(constraintManager.dropCatalogConstraints(catalogName)).thenReturn(affectedTables);
        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalogId);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);

            catalogMgr.replayDropCatalog(log);

            mtmvUtil.verify(() -> MTMVUtil.invalidateRewriteCachesByTableNamesBestEffort(
                    affectedTables, "after removing catalog " + catalogName));
        }
        Mockito.verify(constraintManager).dropCatalogConstraints(catalogName);
    }

    @Test
    void testAlterCatalogNameRenamesConstraintsWithoutDroppingThem() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        long catalogId = 42L;
        String oldCatalogName = "rename_catalog";
        String newCatalogName = "renamed_catalog";
        ExternalCatalog catalog = mockExternalCatalog(catalogId, oldCatalogName);
        addCatalog(catalogMgr, catalog);
        addCatalogByName(catalogMgr, oldCatalogName, catalog);

        ConstraintManager constraintManager = Mockito.spy(new ConstraintManager());
        TableNameInfo oldTable = new TableNameInfo(oldCatalogName, "db", "table");
        TableNameInfo newTable = new TableNameInfo(newCatalogName, "db", "table");
        constraintManager.addConstraint(oldTable, "pk",
                new PrimaryKeyConstraint("pk", ImmutableSet.of("key")), true);
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(env.getEditLog()).thenReturn(editLog);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);

            catalogMgr.alterCatalogName(oldCatalogName, newCatalogName);
        }

        Assertions.assertNull(catalogMgr.getCatalog(oldCatalogName));
        Assertions.assertSame(catalog, catalogMgr.getCatalog(newCatalogName));
        Assertions.assertTrue(constraintManager.getConstraints(oldTable).isEmpty());
        Assertions.assertNotNull(constraintManager.getConstraint(newTable, "pk"));
        Mockito.verify(constraintManager).renameCatalog(oldCatalogName, newCatalogName);
        Mockito.verify(constraintManager, Mockito.never()).dropCatalogConstraints(oldCatalogName);
        Mockito.verify(catalog).beginExclusiveConstraintMetadataMutation();
        Mockito.verify(editLog).logCatalogLog(
                Mockito.eq(OperationType.OP_ALTER_CATALOG_NAME), Mockito.any(CatalogLog.class));
    }

    @Test
    void testReplayAlterCatalogNameUsesSameConstraintMigration() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        long catalogId = 43L;
        String oldCatalogName = "replay_rename_catalog";
        String newCatalogName = "replayed_catalog";
        ExternalCatalog catalog = mockExternalCatalog(catalogId, oldCatalogName);
        addCatalog(catalogMgr, catalog);
        addCatalogByName(catalogMgr, oldCatalogName, catalog);

        ConstraintManager constraintManager = Mockito.spy(new ConstraintManager());
        TableNameInfo oldTable = new TableNameInfo(oldCatalogName, "db", "table");
        TableNameInfo newTable = new TableNameInfo(newCatalogName, "db", "table");
        constraintManager.addConstraint(oldTable, "pk",
                new PrimaryKeyConstraint("pk", ImmutableSet.of("key")), true);
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalogId);
        log.setNewCatalogName(newCatalogName);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);

            catalogMgr.replayAlterCatalogName(log);
        }

        Assertions.assertNull(catalogMgr.getCatalog(oldCatalogName));
        Assertions.assertSame(catalog, catalogMgr.getCatalog(newCatalogName));
        Assertions.assertTrue(constraintManager.getConstraints(oldTable).isEmpty());
        Assertions.assertNotNull(constraintManager.getConstraint(newTable, "pk"));
        Mockito.verify(constraintManager).renameCatalog(oldCatalogName, newCatalogName);
        Mockito.verify(constraintManager, Mockito.never()).dropCatalogConstraints(oldCatalogName);
        Mockito.verify(catalog).beginExclusiveConstraintMetadataMutation();
    }

    @Test
    void testAlterCatalogNameConflictDuringCleanupKeepsOldConstraints() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        long catalogId = 44L;
        String oldCatalogName = "rename_conflict_catalog";
        String newCatalogName = "rename_conflict_target";
        ExternalCatalog catalog = mockExternalCatalog(catalogId, oldCatalogName);
        CatalogIf<? extends DatabaseIf<? extends TableIf>> targetCatalog = Mockito.mock(CatalogIf.class);
        addCatalog(catalogMgr, catalog);
        addCatalogByName(catalogMgr, oldCatalogName, catalog);
        Mockito.doAnswer(invocation -> {
            addCatalogByName(catalogMgr, newCatalogName, targetCatalog);
            return null;
        }).when(catalog).onClose();

        ConstraintManager constraintManager = Mockito.spy(new ConstraintManager());
        TableNameInfo oldTable = new TableNameInfo(oldCatalogName, "db", "table");
        TableNameInfo newTable = new TableNameInfo(newCatalogName, "db", "table");
        constraintManager.addConstraint(oldTable, "pk",
                new PrimaryKeyConstraint("pk", ImmutableSet.of("key")), true);
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(env.getEditLog()).thenReturn(editLog);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);

            Assertions.assertThrows(DdlException.class,
                    () -> catalogMgr.alterCatalogName(oldCatalogName, newCatalogName));
        }

        Assertions.assertSame(catalog, catalogMgr.getCatalog(oldCatalogName));
        Assertions.assertSame(targetCatalog, catalogMgr.getCatalog(newCatalogName));
        Assertions.assertNotNull(constraintManager.getConstraint(oldTable, "pk"));
        Assertions.assertTrue(constraintManager.getConstraints(newTable).isEmpty());
        Mockito.verify(constraintManager, Mockito.never())
                .renameCatalog(oldCatalogName, newCatalogName);
        Mockito.verifyNoInteractions(editLog);
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
        long constraintMetadataBaseline = catalog.snapshotConstraintMetadata();
        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalog.getId());
        log.setNewProps(newProperties);
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
            Assertions.assertTrue(catalog.validationStarted.await(10, TimeUnit.SECONDS));

            Assertions.assertThrows(RuntimeException.class, catalog::makeSureInitialized);
            DdlException validationFailure = alterResult.get(10, TimeUnit.SECONDS);

            Assertions.assertNotNull(validationFailure);
            Assertions.assertEquals(oldProperties, catalog.propertiesSeenByInitialization);
            Assertions.assertEquals(oldProperties, catalog.getProperties());
            Assertions.assertNotEquals(
                    constraintMetadataBaseline, catalog.snapshotConstraintMetadata());
            try (ExternalCatalog.ConstraintMetadataReadGuard ignored =
                    catalog.lockConstraintMetadata(catalog.snapshotConstraintMetadata())) {
                // A failed property mutation must not leave the catalog marked as active.
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testAlterControllerCleanupRunsOutsideCatalogWriteLock() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        ExternalCatalog blockingCatalog = Mockito.mock(ExternalCatalog.class);
        ExternalCatalog otherCatalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(blockingCatalog.getId()).thenReturn(44L);
        Mockito.when(otherCatalog.getId()).thenReturn(45L);
        Mockito.when(blockingCatalog.validatePropertiesBeforeUpdate(Mockito.anyMap(), Mockito.anyMap()))
                .thenReturn(true);
        Mockito.when(otherCatalog.validatePropertiesBeforeUpdate(Mockito.anyMap(), Mockito.anyMap()))
                .thenReturn(true);
        addCatalog(catalogMgr, blockingCatalog);
        addCatalog(catalogMgr, otherCatalog);

        CountDownLatch cleanupStarted = new CountDownLatch(1);
        CountDownLatch allowCleanup = new CountDownLatch(1);
        Mockito.when(blockingCatalog.modifyCatalogPropsWithDeferredAccessControllerCleanup(Mockito.anyMap()))
                .thenReturn(() -> {
                    cleanupStarted.countDown();
                    try {
                        if (!allowCleanup.await(10, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("timed out waiting to release controller cleanup");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                });
        Mockito.when(otherCatalog.modifyCatalogPropsWithDeferredAccessControllerCleanup(Mockito.anyMap()))
                .thenReturn(() -> { });

        CatalogLog blockingLog = new CatalogLog();
        blockingLog.setCatalogId(44L);
        blockingLog.setNewProps(ImmutableMap.of("k", "v1"));
        CatalogLog otherLog = new CatalogLog();
        otherLog.setCatalogId(45L);
        otherLog.setNewProps(ImmutableMap.of("k", "v2"));
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<?> blockingAlter = executor.submit(() -> {
                catalogMgr.replayAlterCatalogProps(blockingLog, Collections.emptyMap(), false);
                return null;
            });
            Assertions.assertTrue(cleanupStarted.await(10, TimeUnit.SECONDS));

            Future<?> unrelatedAlter = executor.submit(() -> {
                catalogMgr.replayAlterCatalogProps(otherLog, Collections.emptyMap(), false);
                return null;
            });
            unrelatedAlter.get(2, TimeUnit.SECONDS);

            allowCleanup.countDown();
            blockingAlter.get(10, TimeUnit.SECONDS);
        } finally {
            allowCleanup.countDown();
            executor.shutdownNow();
        }
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
                Assertions.assertTrue(initializationReadProperties.await(10, TimeUnit.SECONDS));
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
}
