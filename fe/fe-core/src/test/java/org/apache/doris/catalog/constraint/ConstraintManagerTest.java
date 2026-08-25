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

package org.apache.doris.catalog.constraint;

import org.apache.doris.backup.BackupHandler;
import org.apache.doris.catalog.CatalogRecycleBin;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableAttributes;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Version;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Frontend;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for ConstraintManager, testing direct API methods
 * without requiring a full FE environment.
 * All mutations use replay=true to bypass table validation.
 */
class ConstraintManagerTest {
    private ConstraintManager mgr;

    private static final TableNameInfo T1 = new TableNameInfo("ctl", "db", "t1");
    private static final TableNameInfo T2 = new TableNameInfo("ctl", "db", "t2");
    private static final TableNameInfo T3 = new TableNameInfo("ctl", "db", "t3");

    @BeforeEach
    void setUp() {
        mgr = new ConstraintManager();
    }

    // ==================== isEmpty ====================

    @Test
    void isEmptyOnNewManager() {
        Assertions.assertTrue(mgr.isEmpty());
    }

    @Test
    void isEmptyAfterAdd() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        Assertions.assertFalse(mgr.isEmpty());
    }

    @Test
    void isEmptyAfterAddAndDrop() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.dropConstraint(T1, "pk", true);
        Assertions.assertTrue(mgr.isEmpty());
    }

    // ==================== addConstraint / getConstraint ====================

    @Test
    void addAndGetPrimaryKey() {
        PrimaryKeyConstraint pk = newPk("pk", "k1");
        mgr.addConstraint(T1, "pk", pk, true);
        Assertions.assertSame(pk, mgr.getConstraint(T1, "pk"));
    }

    @Test
    void addWithResolvedTableRevalidatesColumnsWithoutResolvingMetadata() {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        TableIf resolvedTable = Mockito.mock(TableIf.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(resolvedTable.getColumn("k1")).thenReturn(Mockito.mock(Column.class));
        PrimaryKeyConstraint pk = newPk("pk", "k1");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mgr.addConstraintWithResolvedTables(T1, "pk", pk, resolvedTable, null);
        }

        Assertions.assertSame(pk, mgr.getConstraint(T1, "pk"));
        Mockito.verify(resolvedTable).getColumn("k1");
        Mockito.verify(env).getEditLog();
        Mockito.verifyNoMoreInteractions(env);
    }

    @Test
    void addWithResolvedTableRejectsColumnRemovedAfterAnalysis() {
        TableIf resolvedTable = Mockito.mock(TableIf.class);
        PrimaryKeyConstraint pk = newPk("pk", "k1");

        Assertions.assertThrows(AnalysisException.class,
                () -> mgr.addConstraintWithResolvedTables(T1, "pk", pk, resolvedTable, null));
        Assertions.assertNull(mgr.getConstraint(T1, "pk"));
    }

    @Test
    void addDistributionMappingAllowsUniformFrontendVersions() {
        String currentVersion = Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH;
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Frontend frontend = Mockito.mock(Frontend.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        HashDistributionInfo distributionInfo = Mockito.mock(HashDistributionInfo.class);
        Column determinantColumn = Mockito.mock(Column.class);
        Column distributionColumn = Mockito.mock(Column.class);
        TableAttributes tableAttributes = Mockito.mock(TableAttributes.class);
        DistributionMappingConstraint mapping = new DistributionMappingConstraint(
                "mapping", "mapping_id", List.of("d1"), List.of("k1"));

        Mockito.when(env.getFrontends(null)).thenReturn(List.of(frontend));
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(frontend.getVersion()).thenReturn(currentVersion);
        Mockito.when(table.getColumn("d1")).thenReturn(determinantColumn);
        Mockito.when(table.getColumn("k1")).thenReturn(distributionColumn);
        Mockito.when(table.getDefaultDistributionInfo()).thenReturn(distributionInfo);
        Mockito.when(distributionInfo.getDistributionColumns()).thenReturn(List.of(distributionColumn));
        Mockito.when(distributionColumn.getName()).thenReturn("k1");
        Mockito.when(table.getTableAttributes()).thenReturn(tableAttributes);
        Mockito.when(tableAttributes.getConstraintsMap()).thenReturn(new HashMap<>());

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mgr.addConstraintWithResolvedTables(T1, mapping.getName(), mapping, table, null);
        }

        Assertions.assertSame(mapping, mgr.getConstraint(T1, mapping.getName()));
        Mockito.verify(env).getEditLog();
    }

    @Test
    void addDistributionMappingDoesNotReacquireHeldFrontendAdmissionFence() {
        String currentVersion = Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH;
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Frontend frontend = Mockito.mock(Frontend.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        HashDistributionInfo distributionInfo = Mockito.mock(HashDistributionInfo.class);
        Column determinantColumn = Mockito.mock(Column.class);
        Column distributionColumn = Mockito.mock(Column.class);
        TableAttributes tableAttributes = Mockito.mock(TableAttributes.class);
        DistributionMappingConstraint mapping = new DistributionMappingConstraint(
                "mapping", "mapping_id", List.of("d1"), List.of("k1"));
        ConstraintManager manager = Mockito.spy(mgr);

        Mockito.when(env.getFrontends(null)).thenReturn(List.of(frontend));
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(frontend.getVersion()).thenReturn(currentVersion);
        Mockito.when(table.getColumn("d1")).thenReturn(determinantColumn);
        Mockito.when(table.getColumn("k1")).thenReturn(distributionColumn);
        Mockito.when(table.getDefaultDistributionInfo()).thenReturn(distributionInfo);
        Mockito.when(distributionInfo.getDistributionColumns()).thenReturn(List.of(distributionColumn));
        Mockito.when(distributionColumn.getName()).thenReturn("k1");
        Mockito.when(table.getTableAttributes()).thenReturn(tableAttributes);
        Mockito.when(tableAttributes.getConstraintsMap()).thenReturn(new HashMap<>());

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            manager.acquireFrontendAdmissionForMapping();
            Mockito.clearInvocations(manager);
            try {
                manager.addConstraintWithResolvedTables(
                        T1, mapping.getName(), mapping, table, null);
            } finally {
                manager.releaseFrontendAdmissionFence();
            }
        }

        Mockito.verify(manager, Mockito.never()).acquireFrontendAdmissionForMapping();
        Assertions.assertSame(mapping, manager.getConstraint(T1, mapping.getName()));
    }

    @Test
    void addDistributionMappingRejectsMixedOrUnknownFrontendVersions() {
        String currentVersion = Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH;
        Env env = Mockito.mock(Env.class);
        Frontend currentFrontend = Mockito.mock(Frontend.class);
        Frontend oldFrontend = Mockito.mock(Frontend.class);
        Frontend unknownFrontend = Mockito.mock(Frontend.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        DistributionMappingConstraint mapping = new DistributionMappingConstraint(
                "mapping", "mapping_id", List.of("d1"), List.of("k1"));

        Mockito.when(env.getFrontends(null))
                .thenReturn(List.of(currentFrontend, oldFrontend, unknownFrontend));
        Mockito.when(currentFrontend.getVersion()).thenReturn(currentVersion);
        Mockito.when(oldFrontend.getNodeName()).thenReturn("old-fe");
        Mockito.when(oldFrontend.getVersion()).thenReturn("old-version");
        Mockito.when(unknownFrontend.getNodeName()).thenReturn("unknown-fe");

        AnalysisException exception;
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            exception = Assertions.assertThrows(AnalysisException.class,
                    () -> mgr.addConstraintWithResolvedTables(
                            T1, mapping.getName(), mapping, table, null));
        }

        Assertions.assertTrue(exception.getMessage().contains("old-fe(old-version)"));
        Assertions.assertTrue(exception.getMessage().contains("unknown-fe(null)"));
        Assertions.assertNull(mgr.getConstraint(T1, mapping.getName()));
        Mockito.verify(env, Mockito.never()).getEditLog();
        Mockito.verify(currentFrontend).getVersion();
        Mockito.verify(oldFrontend).getVersion();
        Mockito.verify(unknownFrontend).getVersion();
        Mockito.verify(oldFrontend, Mockito.never()).isAlive();
        Mockito.verify(unknownFrontend, Mockito.never()).isAlive();
    }

    @Test
    void concurrentFrontendAdmissionWaitsForMappingAddAndIsRejected() throws Exception {
        String currentVersion = Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH;
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Frontend frontend = Mockito.mock(Frontend.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        HashDistributionInfo distributionInfo = Mockito.mock(HashDistributionInfo.class);
        Column determinantColumn = Mockito.mock(Column.class);
        Column distributionColumn = Mockito.mock(Column.class);
        TableAttributes tableAttributes = Mockito.mock(TableAttributes.class);
        DistributionMappingConstraint mapping = new DistributionMappingConstraint(
                "mapping", "mapping_id", List.of("d1"), List.of("k1"));
        CountDownLatch versionCheckStarted = new CountDownLatch(1);
        CountDownLatch frontendAdmissionStarted = new CountDownLatch(1);

        Mockito.when(env.getFrontends(null)).thenAnswer(invocation -> {
            versionCheckStarted.countDown();
            Assertions.assertTrue(frontendAdmissionStarted.await(10, TimeUnit.SECONDS));
            return List.of(frontend);
        });
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(frontend.getVersion()).thenReturn(currentVersion);
        Mockito.when(table.getColumn("d1")).thenReturn(determinantColumn);
        Mockito.when(table.getColumn("k1")).thenReturn(distributionColumn);
        Mockito.when(table.getDefaultDistributionInfo()).thenReturn(distributionInfo);
        Mockito.when(distributionInfo.getDistributionColumns()).thenReturn(List.of(distributionColumn));
        Mockito.when(distributionColumn.getName()).thenReturn("k1");
        Mockito.when(table.getTableAttributes()).thenReturn(tableAttributes);
        Mockito.when(tableAttributes.getConstraintsMap()).thenReturn(new HashMap<>());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Future<DdlException> frontendAdmission = executor.submit(() -> {
                if (!versionCheckStarted.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("Timed out waiting for frontend version check");
                }
                frontendAdmissionStarted.countDown();
                try {
                    mgr.acquireFrontendAdmission();
                    mgr.releaseFrontendAdmissionFence();
                    return null;
                } catch (DdlException e) {
                    return e;
                }
            });

            mgr.addConstraintWithResolvedTables(T1, mapping.getName(), mapping, table, null);
            DdlException exception = frontendAdmission.get(10, TimeUnit.SECONDS);
            Assertions.assertNotNull(exception);
            Assertions.assertTrue(exception.getMessage()
                    .contains("Drop all distribution mapping constraints"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void frontendAdmissionIsRejectedWhileMappingExists() {
        DistributionMappingConstraint mapping = new DistributionMappingConstraint(
                "mapping", "mapping_id", List.of("d1"), List.of("k1"));
        mgr.addConstraint(T1, mapping.getName(), mapping, true);

        DdlException exception = Assertions.assertThrows(
                DdlException.class, mgr::acquireFrontendAdmission);

        Assertions.assertTrue(exception.getMessage()
                .contains("Drop all distribution mapping constraints"));
    }

    @Test
    void frontendAdmissionIsRejectedWhileRetainedJobContainsMapping() {
        Env env = Mockito.mock(Env.class);
        CatalogRecycleBin recycleBin = Mockito.mock(CatalogRecycleBin.class);
        BackupHandler backupHandler = Mockito.mock(BackupHandler.class);
        Mockito.when(backupHandler.containsDistributionMappingConstraint()).thenReturn(true);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedEnv.when(Env::getCurrentRecycleBin).thenReturn(recycleBin);
            Mockito.when(env.getBackupHandler()).thenReturn(backupHandler);

            DdlException exception = Assertions.assertThrows(
                    DdlException.class, mgr::acquireFrontendAdmission);

            Assertions.assertTrue(exception.getMessage().contains("backup or restore jobs"));
        }
    }

    @Test
    void syncMappingsSkipsExternalNonMappingEntriesBeforeMetadataResolution() {
        Env env = Mockito.mock(Env.class);
        mgr.addConstraint(
                new TableNameInfo("external", "db", "table"),
                "pk", newPk("pk", "k1"), true);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mgr.syncDistributionMappingsToTables();
        }

        Mockito.verifyNoInteractions(env);
    }

    @Test
    void canonicalizeExternalTableNameUsesPersistedSpelling() {
        TableNameInfo canonical = new TableNameInfo("ExtCtl", "DbOne", "Table_A");
        mgr.addConstraint(canonical, "pk", newPk("pk", "k1"), true);

        Assertions.assertEquals(canonical,
                mgr.canonicalizeExternalTableName(
                        new TableNameInfo("ExtCtl", "dbone", "table_a"),
                        "pk", true, true));
        Assertions.assertEquals(
                new TableNameInfo("ExtCtl", "dbone", "table_a"),
                mgr.canonicalizeExternalTableName(
                        new TableNameInfo("ExtCtl", "dbone", "table_a"),
                        "pk", true, false));

        TableNameInfo caseOnlyKey = new TableNameInfo("ExtCtl", "DbOne", "table_a");
        mgr.addConstraint(caseOnlyKey, "other_pk", newPk("other_pk", "k1"), true);
        Assertions.assertEquals(canonical,
                mgr.canonicalizeExternalTableName(caseOnlyKey, "pk", true, true));
    }

    @Test
    void addAndGetUniqueKey() {
        UniqueConstraint uk = new UniqueConstraint("uk", ImmutableSet.of("c1"));
        mgr.addConstraint(T1, "uk", uk, true);
        Assertions.assertSame(uk, mgr.getConstraint(T1, "uk"));
    }

    @Test
    void addDuplicateConstraintThrows() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        Assertions.assertThrows(AnalysisException.class,
                () -> mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true));
    }

    @Test
    void getConstraintNonExistentTableReturnsNull() {
        Assertions.assertNull(mgr.getConstraint(T1, "anything"));
    }

    @Test
    void getConstraintNonExistentNameReturnsNull() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        Assertions.assertNull(mgr.getConstraint(T1, "nonexistent"));
    }

    // ==================== getConstraints ====================

    @Test
    void getConstraintsReturnsImmutableCopy() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        Map<String, Constraint> result = mgr.getConstraints(T1);
        Assertions.assertEquals(1, result.size());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> result.put("x", newPk("x", "x")));
    }

    @Test
    void getConstraintsForNonExistentTableReturnsEmpty() {
        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
    }

    // ==================== Type-specific getters ====================

    @Test
    void getPrimaryKeyConstraints() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T1, "uk", new UniqueConstraint("uk", ImmutableSet.of("c1")), true);
        mgr.addConstraint(T2, "pk2", newPk("pk2", "k1"), true);
        mgr.addConstraint(T1, "fk", newFk("fk", T2, "c1", "k1"), true);

        Assertions.assertEquals(1, mgr.getPrimaryKeyConstraints(T1).size());
        Assertions.assertInstanceOf(PrimaryKeyConstraint.class,
                mgr.getPrimaryKeyConstraints(T1).get(0));
    }

    @Test
    void getForeignKeyConstraints() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "pk2", newPk("pk2", "k1"), true);
        mgr.addConstraint(T1, "fk", newFk("fk", T2, "c1", "k1"), true);

        Assertions.assertEquals(1, mgr.getForeignKeyConstraints(T1).size());
        Assertions.assertInstanceOf(ForeignKeyConstraint.class,
                mgr.getForeignKeyConstraints(T1).get(0));
    }

    @Test
    void getUniqueConstraints() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T1, "uk", new UniqueConstraint("uk", ImmutableSet.of("c1")), true);

        Assertions.assertEquals(1, mgr.getUniqueConstraints(T1).size());
        Assertions.assertInstanceOf(UniqueConstraint.class,
                mgr.getUniqueConstraints(T1).get(0));
    }

    @Test
    void typeSpecificGettersReturnEmptyForUnknownTable() {
        Assertions.assertTrue(mgr.getPrimaryKeyConstraints(T1).isEmpty());
        Assertions.assertTrue(mgr.getForeignKeyConstraints(T1).isEmpty());
        Assertions.assertTrue(mgr.getUniqueConstraints(T1).isEmpty());
    }

    // ==================== dropConstraint ====================

    @Test
    void dropConstraintRemoves() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.dropConstraint(T1, "pk", true);
        Assertions.assertNull(mgr.getConstraint(T1, "pk"));
    }

    @Test
    void dropNonExistentConstraintThrowsInNonReplay() {
        Assertions.assertThrows(AnalysisException.class,
                () -> mgr.dropConstraint(T1, "missing", false));
    }

    @Test
    void dropNonExistentConstraintSilentInReplay() {
        // Should not throw
        mgr.dropConstraint(T1, "missing", true);
    }

    @Test
    void dropConstraintRejectsChangedExpectedCascadeWithoutMutation() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk2", newFk("fk2", T1, "c1", "k1"), true);
        List<TableNameInfo> expectedCascadeDropTables = List.of(T2);
        mgr.addConstraint(T3, "fk3", newFk("fk3", T1, "c1", "k1"), true);

        Assertions.assertThrows(AnalysisException.class,
                () -> mgr.dropConstraintAndSubmit(
                        T1, "pk", expectedCascadeDropTables));

        Assertions.assertNotNull(mgr.getConstraint(T1, "pk"));
        Assertions.assertNotNull(mgr.getConstraint(T2, "fk2"));
        Assertions.assertNotNull(mgr.getConstraint(T3, "fk3"));
    }

    // ==================== FK bidirectional references ====================

    @Test
    void addForeignKeyRegistersBidirectionalReference() {
        PrimaryKeyConstraint pk = newPk("pk", "k1");
        mgr.addConstraint(T1, "pk", pk, true);
        ForeignKeyConstraint fk = newFk("fk", T1, "c1", "k1");
        mgr.addConstraint(T2, "fk", fk, true);

        // PK on T1 should have T2 in its foreign table list
        PrimaryKeyConstraint loadedPk = (PrimaryKeyConstraint) mgr.getConstraint(T1, "pk");
        Assertions.assertTrue(loadedPk.getForeignTableInfos().stream()
                .anyMatch(t -> t.getTbl().equals("t2")));
    }

    @Test
    void dropForeignKeyRemovesBidirectionalReference() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);
        mgr.dropConstraint(T2, "fk", true);

        PrimaryKeyConstraint loadedPk = (PrimaryKeyConstraint) mgr.getConstraint(T1, "pk");
        Assertions.assertTrue(loadedPk.getForeignTableInfos().isEmpty());
    }

    @Test
    void dropPrimaryKeyCascadesDropForeignKeys() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk1", newFk("fk1", T1, "c1", "k1"), true);
        mgr.addConstraint(T3, "fk2", newFk("fk2", T1, "c1", "k1"), true);

        mgr.dropConstraint(T1, "pk", true);

        // FK on T2 and T3 should also be removed
        Assertions.assertTrue(mgr.getConstraints(T2).isEmpty());
        Assertions.assertTrue(mgr.getConstraints(T3).isEmpty());
    }

    @Test
    void renameDatabaseUpdatesSelfReferencingForeignKeyAcrossImageRoundTrip()
            throws Exception {
        TableNameInfo oldTable = new TableNameInfo("ctl", "old_db", "t1");
        TableNameInfo newTable = new TableNameInfo("ctl", "new_db", "t1");
        mgr.addConstraint(oldTable, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(oldTable, "fk", newFk("fk", oldTable, "k1", "k1"), true);

        mgr.renameDatabase("ctl", "old_db", "new_db");
        assertSelfReference(mgr, newTable);

        ByteArrayOutputStream image = new ByteArrayOutputStream();
        mgr.write(new DataOutputStream(image));
        ConstraintManager loaded = ConstraintManager.read(
                new DataInputStream(new ByteArrayInputStream(image.toByteArray())));
        assertSelfReference(loaded, newTable);

        loaded.dropConstraint(newTable, "fk", true);
        loaded.dropConstraint(newTable, "pk", true);
        Assertions.assertTrue(loaded.getConstraints(newTable).isEmpty());
    }

    @Test
    void renameDatabaseUpdatesAllForeignKeyReferencesInOnePass() {
        TableNameInfo oldParent = new TableNameInfo("ctl", "old_db", "parent");
        TableNameInfo newParent = new TableNameInfo("ctl", "new_db", "parent");
        TableNameInfo externalChild = new TableNameInfo("ctl", "other_db", "child");
        mgr.addConstraint(oldParent, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(externalChild, "fk", newFk("fk", oldParent, "c1", "k1"), true);
        ImmutableList.Builder<TableNameInfo> oldChildren = ImmutableList.builder();
        ImmutableList.Builder<TableNameInfo> newChildren = ImmutableList.builder();
        for (int i = 0; i < 64; i++) {
            TableNameInfo oldChild = new TableNameInfo("ctl", "old_db", "child_" + i);
            TableNameInfo newChild = new TableNameInfo("ctl", "new_db", "child_" + i);
            mgr.addConstraint(oldChild, "fk", newFk("fk", oldParent, "c1", "k1"), true);
            oldChildren.add(oldChild);
            newChildren.add(newChild);
        }

        mgr.renameDatabase("ctl", "old_db", "new_db");

        Assertions.assertTrue(mgr.getConstraints(oldParent).isEmpty());
        for (TableNameInfo oldChild : oldChildren.build()) {
            Assertions.assertTrue(mgr.getConstraints(oldChild).isEmpty());
        }
        ImmutableList<TableNameInfo> renamedChildren = newChildren.build();
        for (TableNameInfo newChild : renamedChildren) {
            ForeignKeyConstraint renamedForeignKey =
                    (ForeignKeyConstraint) mgr.getConstraint(newChild, "fk");
            Assertions.assertEquals(newParent, renamedForeignKey.getReferencedTableName());
        }
        ForeignKeyConstraint externalForeignKey =
                (ForeignKeyConstraint) mgr.getConstraint(externalChild, "fk");
        Assertions.assertEquals(newParent, externalForeignKey.getReferencedTableName());
        PrimaryKeyConstraint primaryKey =
                (PrimaryKeyConstraint) mgr.getConstraint(newParent, "pk");
        primaryKey.addForeignTable(renamedChildren.get(0));
        Assertions.assertEquals(renamedChildren.size() + 1,
                primaryKey.getForeignTableInfos().size());
        Assertions.assertEquals(
                ImmutableSet.<TableNameInfo>builder()
                        .addAll(renamedChildren)
                        .add(externalChild)
                        .build(),
                ImmutableSet.copyOf(primaryKey.getForeignTableInfos()));
    }

    // ==================== dropTableConstraints ====================

    @Test
    void dropTableConstraintsRemovesAll() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T1, "uk", new UniqueConstraint("uk", ImmutableSet.of("c1")), true);
        mgr.dropTableConstraints(T1);
        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
    }

    @Test
    void dropTableConstraintsCascadesFKReferences() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);
        // Drop T1's constraints → PK dropped → FK on T2 cascade-dropped
        List<TableNameInfo> affectedTables = mgr.dropTableConstraints(T1);
        Assertions.assertTrue(mgr.getConstraints(T2).isEmpty());
        Assertions.assertEquals(ImmutableSet.of(T1, T2), ImmutableSet.copyOf(affectedTables));
    }

    @Test
    void dropTableConstraintsOnNonExistentTableIsNoop() {
        // Should not throw
        mgr.dropTableConstraints(new TableNameInfo("x", "y", "z"));
    }

    // ==================== checkAndDropTableConstraints ====================

    @Test
    void checkAndDropBlocksWhenFKExists() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);
        Assertions.assertThrows(DdlException.class,
                () -> mgr.checkAndDropTableConstraints(T1, true));
        // Constraints should still be intact
        Assertions.assertNotNull(mgr.getConstraint(T1, "pk"));
    }

    @Test
    void checkAndDropWithoutCheckDropsEvenWithFK() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);
        List<TableNameInfo> affectedTables = Assertions.assertDoesNotThrow(
                () -> mgr.checkAndDropTableConstraints(T1, false));
        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
        Assertions.assertTrue(mgr.getConstraints(T2).isEmpty());
        Assertions.assertEquals(ImmutableSet.of(T1, T2), ImmutableSet.copyOf(affectedTables));
    }

    @Test
    void checkAndDropSucceedsWhenNoFK() throws DdlException {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.checkAndDropTableConstraints(T1, true);
        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
    }

    @Test
    void batchDropAllowsReferencesInsideBatch() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);

        Assertions.assertDoesNotThrow(
                () -> mgr.checkAndDropTableConstraints(
                        ImmutableList.of(T1, T2), true));
        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
        Assertions.assertTrue(mgr.getConstraints(T2).isEmpty());
    }

    @Test
    void batchDropRejectsReferencesOutsideBatchWithoutMutation() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T3, "fk", newFk("fk", T1, "c1", "k1"), true);

        Assertions.assertThrows(DdlException.class,
                () -> mgr.checkAndDropTableConstraints(
                        ImmutableList.of(T1, T2), true));
        Assertions.assertNotNull(mgr.getConstraint(T1, "pk"));
        Assertions.assertNotNull(mgr.getConstraint(T3, "fk"));
    }

    // ==================== checkNoReferencingForeignKeys ====================

    @Test
    void checkNoReferencingForeignKeysPassesWithoutFK() throws DdlException {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.checkNoReferencingForeignKeys(T1); // no exception
    }

    @Test
    void checkNoReferencingForeignKeysThrowsWithFK() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);
        Assertions.assertThrows(DdlException.class,
                () -> mgr.checkNoReferencingForeignKeys(T1));
    }

    @Test
    void checkNoReferencingForeignKeysOnEmptyTableIsNoop() throws DdlException {
        mgr.checkNoReferencingForeignKeys(T1); // no exception
    }

    // ==================== findConstraintWithColumn ====================

    @Test
    void findConstraintWithColumnFindsPK() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        Assertions.assertEquals("pk", mgr.findConstraintWithColumn(T1, "k1"));
        Assertions.assertEquals("pk", mgr.findConstraintWithColumn(T1, "K1"));
    }

    @Test
    void findConstraintWithColumnFindsUnique() {
        mgr.addConstraint(T1, "uk", new UniqueConstraint("uk", ImmutableSet.of("c1")), true);
        Assertions.assertEquals("uk", mgr.findConstraintWithColumn(T1, "c1"));
        Assertions.assertEquals("uk", mgr.findConstraintWithColumn(T1, "C1"));
    }

    @Test
    void findConstraintWithColumnFindsFK() {
        mgr.addConstraint(T2, "pk2", newPk("pk2", "k1"), true);
        mgr.addConstraint(T1, "fk", newFk("fk", T2, "c1", "k1"), true);
        Assertions.assertEquals("fk", mgr.findConstraintWithColumn(T1, "c1"));
        Assertions.assertEquals("fk", mgr.findConstraintWithColumn(T1, "C1"));
    }

    @Test
    void findConstraintWithColumnReturnsNullForUnknownColumn() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        Assertions.assertNull(mgr.findConstraintWithColumn(T1, "nonexistent"));
    }

    @Test
    void findConstraintWithColumnReturnsNullForUnknownTable() {
        Assertions.assertNull(mgr.findConstraintWithColumn(T1, "k1"));
    }

    // ==================== dropCatalogConstraints ====================

    @Test
    void dropCatalogConstraintsRemovesOnlyMatchingCatalog() {
        TableNameInfo extT1 = new TableNameInfo("extCtl", "db", "t1");
        TableNameInfo extT2 = new TableNameInfo("extCtl", "db", "t2");
        mgr.addConstraint(extT1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(extT2, "uk", new UniqueConstraint("uk", ImmutableSet.of("c1")), true);
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);

        mgr.dropCatalogConstraints("extCtl");

        Assertions.assertTrue(mgr.getConstraints(extT1).isEmpty());
        Assertions.assertTrue(mgr.getConstraints(extT2).isEmpty());
        // T1 is in "ctl" catalog — should be unaffected
        Assertions.assertNotNull(mgr.getConstraint(T1, "pk"));
    }

    @Test
    void dropCatalogConstraintsCascadesFKsAcrossCatalogs() {
        TableNameInfo extT = new TableNameInfo("extCtl", "db", "t1");
        // PK on extT, FK on T1 referencing extT
        mgr.addConstraint(extT, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T1, "fk", newFk("fk", extT, "c1", "k1"), true);

        mgr.dropCatalogConstraints("extCtl");

        Assertions.assertTrue(mgr.getConstraints(extT).isEmpty());
        // FK on T1 referencing extT is cascade-dropped because the referenced PK was removed
        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty(),
                "FK on T1 should be cascade-dropped when referenced PK's catalog is dropped");
    }

    @Test
    void dropCatalogConstraintsOnNonExistentCatalogIsNoop() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.dropCatalogConstraints("nonExistent");
        Assertions.assertNotNull(mgr.getConstraint(T1, "pk"));
    }

    // ==================== dropDatabaseConstraints ====================

    @Test
    void dropDatabaseConstraintsRemovesAllInDatabase() {
        mgr.addConstraint(T1, "pk1", newPk("pk1", "k1"), true);
        mgr.addConstraint(T2, "pk2", newPk("pk2", "k1"), true);
        // T3 is in same db
        mgr.addConstraint(T3, "uk3", new UniqueConstraint("uk3", ImmutableSet.of("c1")), true);
        // Table in different database
        TableNameInfo otherDbTable = new TableNameInfo("ctl", "other_db", "t1");
        mgr.addConstraint(otherDbTable, "pk_other", newPk("pk_other", "k1"), true);

        List<TableNameInfo> affectedTables = mgr.dropDatabaseConstraints("ctl", "db");

        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
        Assertions.assertTrue(mgr.getConstraints(T2).isEmpty());
        Assertions.assertTrue(mgr.getConstraints(T3).isEmpty());
        // Other database unaffected
        Assertions.assertNotNull(mgr.getConstraint(otherDbTable, "pk_other"));
        Assertions.assertEquals(ImmutableSet.of(T1, T2, T3), ImmutableSet.copyOf(affectedTables));
    }

    @Test
    void dropDatabaseConstraintsCascadesFKsAcrossDatabase() {
        // PK in db, FK in other_db referencing the PK
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        TableNameInfo otherDbTable = new TableNameInfo("ctl", "other_db", "t1");
        mgr.addConstraint(otherDbTable, "fk", newFk("fk", T1, "c1", "k1"), true);

        List<TableNameInfo> affectedTables = mgr.dropDatabaseConstraints("ctl", "db");

        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
        // FK in other_db should be cascade-dropped because the referenced PK was removed
        Assertions.assertTrue(mgr.getConstraints(otherDbTable).isEmpty(),
                "FK in other_db should be cascade-dropped when referenced PK's database is dropped");
        Assertions.assertEquals(ImmutableSet.of(T1, otherDbTable), ImmutableSet.copyOf(affectedTables));
    }

    // ==================== renameTable ====================

    @Test
    void renameTableMovesConstraints() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        TableNameInfo newT = new TableNameInfo("ctl", "db", "t1_renamed");
        mgr.renameTable(T1, newT);

        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
        Assertions.assertNotNull(mgr.getConstraint(newT, "pk"));
    }

    @Test
    void renameTableUpdatesFKReferencesInOtherTables() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);

        TableNameInfo newT1 = new TableNameInfo("ctl", "db", "t1_renamed");
        mgr.renameTable(T1, newT1);

        ForeignKeyConstraint fk = (ForeignKeyConstraint) mgr.getConstraint(T2, "fk");
        Assertions.assertEquals("t1_renamed", fk.getReferencedTableName().getTbl());
    }

    @Test
    void renameTableUpdatesPKForeignTableListInOtherTables() {
        // T2 has PK, T1 has FK referencing T2. Rename T1.
        mgr.addConstraint(T2, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T1, "fk", newFk("fk", T2, "c1", "k1"), true);

        TableNameInfo newT1 = new TableNameInfo("ctl", "db", "t1_renamed");
        mgr.renameTable(T1, newT1);

        PrimaryKeyConstraint pk = (PrimaryKeyConstraint) mgr.getConstraint(T2, "pk");
        Assertions.assertTrue(pk.getForeignTableInfos().stream()
                .anyMatch(t -> t.getTbl().equals("t1_renamed")));
        Assertions.assertFalse(pk.getForeignTableInfos().stream()
                .anyMatch(t -> t.getTbl().equals("t1")));
    }

    @Test
    void renameNonExistentTableIsNoop() {
        TableNameInfo ghost = new TableNameInfo("ctl", "db", "ghost");
        TableNameInfo newGhost = new TableNameInfo("ctl", "db", "ghost2");
        mgr.renameTable(ghost, newGhost); // should not throw
        Assertions.assertTrue(mgr.isEmpty());
    }

    // ==================== swapTableConstraints ====================

    @Test
    void swapTableConstraintsExchangesMappings() {
        mgr.addConstraint(T1, "pk1", newPk("pk1", "k1"), true);
        mgr.addConstraint(T2, "uk2", new UniqueConstraint("uk2", ImmutableSet.of("c1")), true);

        mgr.swapTableConstraints(T1, T2);

        // pk1 should now be under T2
        Assertions.assertNotNull(mgr.getConstraint(T2, "pk1"));
        // uk2 should now be under T1
        Assertions.assertNotNull(mgr.getConstraint(T1, "uk2"));
    }

    @Test
    void swapTableConstraintsWhenOneSideEmpty() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);

        mgr.swapTableConstraints(T1, T2);

        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
        Assertions.assertNotNull(mgr.getConstraint(T2, "pk"));
    }

    @Test
    void swapTableConstraintsUpdatesFKReferences() {
        // T1 has PK, T3 has FK referencing T1. Swap T1 and T2.
        mgr.addConstraint(T1, "pk1", newPk("pk1", "k1"), true);
        mgr.addConstraint(T2, "uk2", new UniqueConstraint("uk2", ImmutableSet.of("c1")), true);
        mgr.addConstraint(T3, "fk", newFk("fk", T1, "c1", "k1"), true);

        mgr.swapTableConstraints(T1, T2);

        // T3's FK should now reference T2 (was T1)
        ForeignKeyConstraint fk = (ForeignKeyConstraint) mgr.getConstraint(T3, "fk");
        Assertions.assertEquals("t2", fk.getReferencedTableName().getTbl());
    }

    // ==================== dropAndRenameConstraints ====================

    @Test
    void dropAndRenameDropsOldAndMovesNew() {
        mgr.addConstraint(T1, "pk_old", newPk("pk_old", "k1"), true);
        mgr.addConstraint(T2, "pk_new", newPk("pk_new", "k2"), true);

        mgr.dropAndRenameConstraints(T1, T2);

        // T2's constraints should now be under T1's key
        Assertions.assertNotNull(mgr.getConstraint(T1, "pk_new"));
        // T1's old constraint should be gone
        Assertions.assertNull(mgr.getConstraint(T1, "pk_old"));
        // T2 should have no constraints
        Assertions.assertTrue(mgr.getConstraints(T2).isEmpty());
    }

    @Test
    void dropAndRenameUpdatesFKReferences() {
        // T2 has PK, T3 has FK referencing T2. Replace T1 with T2 (no swap).
        mgr.addConstraint(T2, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T3, "fk", newFk("fk", T2, "c1", "k1"), true);

        mgr.dropAndRenameConstraints(T1, T2);

        // T3's FK should now reference T1 (was T2, since T2 was renamed to T1)
        ForeignKeyConstraint fk = (ForeignKeyConstraint) mgr.getConstraint(T3, "fk");
        Assertions.assertEquals("t1", fk.getReferencedTableName().getTbl());
    }

    // ==================== migrateFromTable ====================

    @Test
    void migrateFromTableAddsConstraints() {
        PrimaryKeyConstraint pk = newPk("pk", "k1");
        Map<String, Constraint> existing = ImmutableMap.of("pk", pk);
        mgr.migrateFromTable(T1, existing);
        Assertions.assertSame(pk, mgr.getConstraint(T1, "pk"));
    }

    @Test
    void migrateFromTableWithEmptyMapIsNoop() {
        mgr.migrateFromTable(T1, ImmutableMap.of());
        Assertions.assertTrue(mgr.isEmpty());
    }

    @Test
    void migrateFromTableWithNullIsNoop() {
        mgr.migrateFromTable(T1, null);
        Assertions.assertTrue(mgr.isEmpty());
    }

    // ==================== rebuildForeignKeyReferences ====================

    @Test
    void rebuildForeignKeyReferencesWiresFKToPK() {
        // Simulate migration: PK on T1, FK on T2 referencing T1,
        // but PK doesn't know about T2 yet (as during per-table migration)
        PrimaryKeyConstraint pk = newPk("pk", "k1");
        mgr.addConstraint(T1, "pk", pk, true);
        // Add FK without registering bidirectional reference
        ForeignKeyConstraint fk = newFk("fk", T1, "c1", "k1");
        Map<String, Constraint> t2Map = new java.util.HashMap<>();
        t2Map.put("fk", fk);
        mgr.migrateFromTable(T2, t2Map);

        // Before rebuild: PK doesn't know about T2
        PrimaryKeyConstraint pkBefore = (PrimaryKeyConstraint) mgr.getConstraint(T1, "pk");
        Assertions.assertTrue(pkBefore.getForeignTableInfos().isEmpty(),
                "Before rebuild, PK should not know about T2");

        // After rebuild: PK should know about T2
        mgr.rebuildForeignKeyReferences();

        PrimaryKeyConstraint loadedPk = (PrimaryKeyConstraint) mgr.getConstraint(T1, "pk");
        Assertions.assertFalse(loadedPk.getForeignTableInfos().isEmpty());
    }

    @Test
    void rebuildForeignKeyReferencesDoesNotDuplicateEntries() {
        // PK on T1, FK on T2 referencing T1 — registered via addConstraint
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);

        // addConstraint already registered T2 in PK's foreignTableInfos
        PrimaryKeyConstraint pk = (PrimaryKeyConstraint) mgr.getConstraint(T1, "pk");
        Assertions.assertEquals(1, pk.getForeignTableInfos().size());

        // rebuild should NOT add duplicates
        mgr.rebuildForeignKeyReferences();
        Assertions.assertEquals(1, pk.getForeignTableInfos().size(),
                "rebuildForeignKeyReferences should not duplicate entries");
    }

    // ==================== Serialization ====================

    @Test
    void writeAndReadRoundTrip() throws Exception {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T1, "uk", new UniqueConstraint("uk", ImmutableSet.of("c1")), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);
        mgr.write(out);

        DataInput in = new DataInputStream(
                new ByteArrayInputStream(baos.toByteArray()));
        ConstraintManager loaded = ConstraintManager.read(in);

        Assertions.assertEquals(2, loaded.getConstraints(T1).size());
        Assertions.assertEquals(1, loaded.getConstraints(T2).size());
        Assertions.assertInstanceOf(PrimaryKeyConstraint.class,
                loaded.getConstraint(T1, "pk"));
        Assertions.assertInstanceOf(ForeignKeyConstraint.class,
                loaded.getConstraint(T2, "fk"));
    }

    @Test
    void writeAndReadEmptyManager() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);
        mgr.write(out);

        DataInput in = new DataInputStream(
                new ByteArrayInputStream(baos.toByteArray()));
        ConstraintManager loaded = ConstraintManager.read(in);

        Assertions.assertTrue(loaded.isEmpty());
    }

    // ==================== Helpers ====================

    private static PrimaryKeyConstraint newPk(String name, String... columns) {
        return new PrimaryKeyConstraint(name, ImmutableSet.copyOf(columns));
    }

    private static ForeignKeyConstraint newFk(String name, TableNameInfo refTable,
            String fkCol, String pkCol) {
        return new ForeignKeyConstraint(name,
                ImmutableList.of(fkCol), refTable, ImmutableList.of(pkCol));
    }

    private static void assertSelfReference(
            ConstraintManager manager, TableNameInfo tableNameInfo) {
        ForeignKeyConstraint foreignKey = (ForeignKeyConstraint) manager.getConstraint(
                tableNameInfo, "fk");
        PrimaryKeyConstraint primaryKey = (PrimaryKeyConstraint) manager.getConstraint(
                tableNameInfo, "pk");
        Assertions.assertEquals(tableNameInfo, foreignKey.getReferencedTableName());
        Assertions.assertEquals(List.of(tableNameInfo), primaryKey.getForeignTableInfos());
    }
}
