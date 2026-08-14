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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.test.TestExternalCatalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

class ConstraintCommandUtilsTest {
    @Test
    void lockCurrentDatabasesRejectsRecreatedDatabase() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogManager = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf resolvedDatabase = Mockito.mock(DatabaseIf.class);
        DatabaseIf recreatedDatabase = Mockito.mock(DatabaseIf.class);
        TableNameInfo tableNameInfo = new TableNameInfo("internal", "db", "tbl");
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogManager);
        Mockito.when(catalogManager.getCatalogOrDdlException("internal")).thenReturn(catalog);
        Mockito.when(catalogManager.getCatalog("internal")).thenReturn(catalog);
        Mockito.when(catalog.getDbOrDdlException("db")).thenReturn(resolvedDatabase);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(recreatedDatabase);
        Mockito.when(resolvedDatabase.getId()).thenReturn(1L);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            ConstraintCommandUtils.ExternalCatalogSnapshots snapshots =
                    ConstraintCommandUtils.snapshotExternalCatalogs(List.of(tableNameInfo));

            Assertions.assertThrows(DdlException.class,
                    () -> ConstraintCommandUtils.lockCurrentDatabases(
                            List.of(tableNameInfo), snapshots, List.of()));

            Mockito.verify(resolvedDatabase).readLock();
            Mockito.verify(resolvedDatabase).readUnlock();
        }
    }

    @Test
    void lockCurrentDatabasesUsesStableIdOrder() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogManager = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf firstDatabase = Mockito.mock(DatabaseIf.class);
        DatabaseIf secondDatabase = Mockito.mock(DatabaseIf.class);
        TableNameInfo firstTable = new TableNameInfo("internal", "db1", "tbl1");
        TableNameInfo secondTable = new TableNameInfo("internal", "db2", "tbl2");
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogManager);
        Mockito.when(catalogManager.getCatalogOrDdlException("internal")).thenReturn(catalog);
        Mockito.when(catalogManager.getCatalog("internal")).thenReturn(catalog);
        Mockito.when(catalog.getDbOrDdlException("db1")).thenReturn(firstDatabase);
        Mockito.when(catalog.getDbOrDdlException("db2")).thenReturn(secondDatabase);
        Mockito.when(catalog.getDbNullable("db1")).thenReturn(firstDatabase);
        Mockito.when(catalog.getDbNullable("db2")).thenReturn(secondDatabase);
        Mockito.when(firstDatabase.getId()).thenReturn(2L);
        Mockito.when(secondDatabase.getId()).thenReturn(1L);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            ConstraintCommandUtils.ExternalCatalogSnapshots snapshots =
                    ConstraintCommandUtils.snapshotExternalCatalogs(
                            List.of(firstTable, secondTable));

            try (ConstraintCommandUtils.LockedDatabases ignored =
                    ConstraintCommandUtils.lockCurrentDatabases(
                            List.of(firstTable, secondTable), snapshots, List.of())) {
                org.mockito.InOrder inOrder =
                        Mockito.inOrder(secondDatabase, firstDatabase);
                inOrder.verify(secondDatabase).readLock();
                inOrder.verify(firstDatabase).readLock();
            }

            org.mockito.InOrder unlockOrder =
                    Mockito.inOrder(firstDatabase, secondDatabase);
            unlockOrder.verify(firstDatabase).readUnlock();
            unlockOrder.verify(secondDatabase).readUnlock();
        }
    }

    @Test
    void lockCurrentTablesUsesStableOrderAndIdentityDeduplication() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogManager = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf firstDatabase = Mockito.mock(DatabaseIf.class);
        DatabaseIf secondDatabase = Mockito.mock(DatabaseIf.class);
        TableIf firstTable = Mockito.mock(TableIf.class);
        TableIf secondTable = Mockito.mock(TableIf.class);
        TableNameInfo firstTableInfo = new TableNameInfo("internal", "db1", "tbl1");
        TableNameInfo secondTableInfo = new TableNameInfo("internal", "db2", "tbl2");
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogManager);
        Mockito.when(catalogManager.getCatalogOrDdlException("internal")).thenReturn(catalog);
        Mockito.when(catalogManager.getCatalog("internal")).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(catalog.getDbOrDdlException("db1")).thenReturn(firstDatabase);
        Mockito.when(catalog.getDbOrDdlException("db2")).thenReturn(secondDatabase);
        Mockito.when(catalog.getDbNullable("db1")).thenReturn(firstDatabase);
        Mockito.when(catalog.getDbNullable("db2")).thenReturn(secondDatabase);
        Mockito.when(firstDatabase.getId()).thenReturn(2L);
        Mockito.when(firstDatabase.getFullName()).thenReturn("db1");
        Mockito.when(firstDatabase.getCatalog()).thenReturn(catalog);
        Mockito.when(firstDatabase.getTableNullable("tbl1")).thenReturn(firstTable);
        Mockito.when(secondDatabase.getId()).thenReturn(1L);
        Mockito.when(secondDatabase.getFullName()).thenReturn("db2");
        Mockito.when(secondDatabase.getCatalog()).thenReturn(catalog);
        Mockito.when(secondDatabase.getTableNullable("tbl2")).thenReturn(secondTable);
        Mockito.when(firstTable.getDatabase()).thenReturn(firstDatabase);
        Mockito.when(firstTable.getId()).thenReturn(2L);
        Mockito.when(firstTable.getName()).thenReturn("tbl1");
        Mockito.when(secondTable.getDatabase()).thenReturn(secondDatabase);
        Mockito.when(secondTable.getId()).thenReturn(1L);
        Mockito.when(secondTable.getName()).thenReturn("tbl2");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            ConstraintCommandUtils.ExternalCatalogSnapshots snapshots =
                    ConstraintCommandUtils.snapshotExternalCatalogs(
                            List.of(firstTableInfo, secondTableInfo));
            try (ConstraintCommandUtils.LockedDatabases lockedDatabases =
                    ConstraintCommandUtils.lockCurrentDatabases(
                            List.of(firstTableInfo, secondTableInfo), snapshots, List.of());
                    ConstraintCommandUtils.LockedTables ignored =
                            ConstraintCommandUtils.lockCurrentTables(
                                    lockedDatabases,
                                    List.of(firstTableInfo, secondTableInfo, firstTableInfo))) {
                Assertions.assertThrows(
                        DdlException.class,
                        () -> ignored.requireSame(
                                firstTableInfo,
                                Mockito.mock(TableIf.class)));
                ignored.requireSame(firstTableInfo, firstTable);
                org.mockito.InOrder lockOrder = Mockito.inOrder(secondTable, firstTable);
                lockOrder.verify(secondTable).writeLock();
                lockOrder.verify(firstTable).writeLock();
            }

            Mockito.verify(firstTable).writeLock();
            org.mockito.InOrder unlockOrder = Mockito.inOrder(firstTable, secondTable);
            unlockOrder.verify(firstTable).writeUnlock();
            unlockOrder.verify(secondTable).writeUnlock();
        }
    }

    @Test
    void tableLockHelpersDistinguishRequiredAndMissingTables() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogManager = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        TableIf existingTable = Mockito.mock(TableIf.class);
        TableNameInfo existingTableInfo = new TableNameInfo("internal", "db", "existing");
        TableNameInfo missingTableInfo = new TableNameInfo("internal", "db", "missing");
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogManager);
        Mockito.when(catalogManager.getCatalogOrDdlException("internal")).thenReturn(catalog);
        Mockito.when(catalogManager.getCatalog("internal")).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(catalog.getDbOrDdlException("db")).thenReturn(database);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(database);
        Mockito.when(database.getId()).thenReturn(1L);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getTableNullable("existing")).thenReturn(existingTable);
        Mockito.when(existingTable.getDatabase()).thenReturn(database);
        Mockito.when(existingTable.getId()).thenReturn(1L);
        Mockito.when(existingTable.getName()).thenReturn("existing");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            ConstraintCommandUtils.ExternalCatalogSnapshots snapshots =
                    ConstraintCommandUtils.snapshotExternalCatalogs(
                            List.of(existingTableInfo, missingTableInfo));
            try (ConstraintCommandUtils.LockedDatabases lockedDatabases =
                    ConstraintCommandUtils.lockCurrentDatabases(
                            List.of(existingTableInfo, missingTableInfo), snapshots, List.of())) {
                Assertions.assertThrows(DdlException.class,
                        () -> ConstraintCommandUtils.lockCurrentTables(
                                lockedDatabases,
                                List.of(existingTableInfo, missingTableInfo)));
                Mockito.verify(existingTable, Mockito.never()).writeLock();

                try (ConstraintCommandUtils.LockedTables lockedTables =
                        ConstraintCommandUtils.lockCurrentTablesIfPresent(
                                lockedDatabases,
                                List.of(existingTableInfo, missingTableInfo))) {
                    Assertions.assertSame(existingTable, lockedTables.get(existingTableInfo));
                    Assertions.assertNull(lockedTables.get(missingTableInfo));
                    Mockito.verify(existingTable).writeLock();
                }
                Mockito.verify(existingTable).writeUnlock();
            }
        }
    }

    @Test
    void externalTableUsesAnalyzedObjectWithoutCacheLookup() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogManager = Mockito.mock(CatalogMgr.class);
        ExternalCatalog catalog = newExternalCatalog();
        ExternalDatabase<ExternalTable> externalDatabase =
                Mockito.mock(ExternalDatabase.class);
        ExternalTable externalTable = Mockito.mock(ExternalTable.class);
        TableNameInfo tableNameInfo = new TableNameInfo("external", "db", "tbl");
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogManager);
        Mockito.when(catalogManager.getCatalogOrDdlException("external")).thenReturn(catalog);
        Mockito.when(catalogManager.getCatalog("external")).thenReturn(catalog);
        Mockito.when(externalDatabase.getFullName()).thenReturn("db");
        Mockito.when(externalDatabase.getCatalog()).thenReturn(catalog);
        Mockito.when(externalTable.getDatabase()).thenReturn(externalDatabase);
        Mockito.when(externalTable.getCatalog()).thenReturn(catalog);
        Mockito.when(externalTable.getName()).thenReturn("tbl");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            ConstraintCommandUtils.ExternalCatalogSnapshots snapshots =
                    ConstraintCommandUtils.snapshotExternalCatalogs(List.of(tableNameInfo));
            try (ConstraintCommandUtils.LockedDatabases lockedDatabases =
                    ConstraintCommandUtils.lockCurrentDatabases(
                            List.of(tableNameInfo), snapshots, List.of(externalTable));
                    ConstraintCommandUtils.LockedTables lockedTables =
                            ConstraintCommandUtils.lockCurrentTables(
                                    lockedDatabases, List.of(tableNameInfo))) {
                Assertions.assertSame(externalTable, lockedTables.get(tableNameInfo));
            }

            Mockito.verify(externalDatabase, Mockito.never()).getTableNullable("tbl");
            Mockito.verify(externalDatabase, Mockito.never()).getTableForReplay("tbl");
            Mockito.verify(externalDatabase, Mockito.never()).readLock();
            Mockito.verify(externalDatabase, Mockito.never()).readUnlock();
            Mockito.verify(externalTable, Mockito.never()).writeLock();
            Mockito.verify(externalTable, Mockito.never()).writeUnlock();
        }
    }

    @Test
    void activeExternalMutationRejectsConstraintLock() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogManager = Mockito.mock(CatalogMgr.class);
        ExternalCatalog catalog = newExternalCatalog();
        TableNameInfo tableNameInfo = new TableNameInfo("external", "db", "tbl");
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogManager);
        Mockito.when(catalogManager.getCatalogOrDdlException("external")).thenReturn(catalog);
        Mockito.when(catalogManager.getCatalog("external")).thenReturn(catalog);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            ConstraintCommandUtils.ExternalCatalogSnapshots snapshots =
                    ConstraintCommandUtils.snapshotExternalCatalogs(List.of(tableNameInfo));
            try (ExternalCatalog.ConstraintMetadataMutationGuard ignored =
                    catalog.beginConstraintMetadataMutation()) {
                Assertions.assertThrows(
                        DdlException.class,
                        () -> ConstraintCommandUtils.lockCurrentDatabases(
                                List.of(tableNameInfo), snapshots, List.of()));
            }
        }
    }

    @Test
    void completedExternalMutationRejectsOldSnapshot() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogManager = Mockito.mock(CatalogMgr.class);
        ExternalCatalog catalog = newExternalCatalog();
        TableNameInfo tableNameInfo = new TableNameInfo("external", "db", "tbl");
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogManager);
        Mockito.when(catalogManager.getCatalogOrDdlException("external")).thenReturn(catalog);
        Mockito.when(catalogManager.getCatalog("external")).thenReturn(catalog);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            ConstraintCommandUtils.ExternalCatalogSnapshots snapshots =
                    ConstraintCommandUtils.snapshotExternalCatalogs(List.of(tableNameInfo));
            try (ExternalCatalog.ConstraintMetadataMutationGuard ignored =
                    catalog.beginConstraintMetadataMutation()) {
                // Complete one mutation after the constraint analysis baseline.
            }
            Assertions.assertThrows(
                    DdlException.class,
                    () -> ConstraintCommandUtils.lockCurrentDatabases(
                            List.of(tableNameInfo), snapshots, List.of()));
        }
    }

    @Test
    void replacedExternalCatalogRejectsOldSnapshot() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogManager = Mockito.mock(CatalogMgr.class);
        ExternalCatalog original = newExternalCatalog();
        ExternalCatalog replacement = newExternalCatalog();
        TableNameInfo tableNameInfo = new TableNameInfo("external", "db", "tbl");
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogManager);
        Mockito.when(catalogManager.getCatalogOrDdlException("external"))
                .thenReturn(original);
        Mockito.when(catalogManager.getCatalog("external")).thenReturn(replacement);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            ConstraintCommandUtils.ExternalCatalogSnapshots snapshots =
                    ConstraintCommandUtils.snapshotExternalCatalogs(List.of(tableNameInfo));

            Assertions.assertThrows(
                    DdlException.class,
                    () -> ConstraintCommandUtils.lockCurrentDatabases(
                            List.of(tableNameInfo), snapshots, List.of()));
        }
    }

    private ExternalCatalog newExternalCatalog() {
        return new TestExternalCatalog(10L, "external", "",
                Map.of("catalog_provider.class", EmptyCatalogProvider.class.getName()), "");
    }

    public static class EmptyCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return Map.of();
        }
    }
}
