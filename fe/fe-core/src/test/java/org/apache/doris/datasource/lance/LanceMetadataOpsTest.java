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

package org.apache.doris.datasource.lance;

import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.Session;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.NamespaceAlreadyExistsException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableAlreadyExistsException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.AlterTableAddColumnsRequest;
import org.lance.namespace.model.CreateTableRequest;
import org.lance.namespace.model.DropNamespaceRequest;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class LanceMetadataOpsTest {
    @Test
    public void testAddColumnValidation() throws UserException {
        Column nullable = new Column("score", Type.INT, true);
        Assertions.assertDoesNotThrow(
                () -> LanceMetadataOps.validateAddColumn(nullable, null));

        Column required = new Column("required", Type.INT, false);
        assertRejected(() -> LanceMetadataOps.validateAddColumn(required, null),
                "only supports nullable columns");

        Column defaulted = new Column("defaulted", Type.INT, false, null, true, "1", "");
        assertRejected(() -> LanceMetadataOps.validateAddColumn(defaulted, null),
                "does not support default values");

        Column commented = new Column("commented", Type.INT, true, "comment");
        commented.setCommentSpecified(true);
        assertRejected(() -> LanceMetadataOps.validateAddColumn(commented, null),
                "does not support column comments");

        assertRejected(() -> LanceMetadataOps.validateAddColumn(
                        nullable, new ColumnPosition("id")),
                "does not support column positions");
    }

    @Test
    public void testModifyColumnValidation() throws UserException {
        Column column = new Column("score", Type.BIGINT, true);
        column.setNullableSpecified(true);
        Assertions.assertDoesNotThrow(
                () -> LanceMetadataOps.validateModifyColumn(column, null));

        column.setCommentSpecified(true);
        assertRejected(() -> LanceMetadataOps.validateModifyColumn(column, null),
                "does not support column comments");
    }

    @Test
    public void testCreateDatabaseIfNotExistsSkipsExistingDatabase() throws DdlException {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        LanceCatalogClient client = newClient(namespace, Mockito.mock(BufferAllocator.class));
        LanceExternalCatalog catalog = catalogWithClient(client);

        try {
            Assertions.assertTrue(new LanceMetadataOps(catalog).createDb(
                    "analytics", true, Collections.singletonMap("owner", "doris")));
        } finally {
            client.close();
        }

        Mockito.verify(namespace, Mockito.never()).createNamespace(Mockito.any());
        Mockito.verify(catalog).resetMetaCacheNames();
    }

    @Test
    public void testCreateDatabaseHandlesConcurrentCreate() throws DdlException {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.doThrow(new NamespaceNotFoundException("missing"))
                .when(namespace).namespaceExists(Mockito.any());
        Mockito.doThrow(new NamespaceAlreadyExistsException("created concurrently"))
                .when(namespace).createNamespace(Mockito.any());
        LanceCatalogClient client = newClient(namespace, Mockito.mock(BufferAllocator.class));
        LanceExternalCatalog catalog = catalogWithClient(client);
        LanceMetadataOps ops = new LanceMetadataOps(catalog);

        try {
            Assertions.assertTrue(ops.createDb("analytics", true, Collections.emptyMap()));
            DdlException exception = Assertions.assertThrows(DdlException.class,
                    () -> ops.createDb("analytics", false, Collections.emptyMap()));
            Assertions.assertTrue(exception.getMessage().contains("exist"));
        } finally {
            client.close();
        }

        Mockito.verify(namespace, Mockito.times(2)).createNamespace(Mockito.any());
        Mockito.verify(catalog).resetMetaCacheNames();
    }

    @Test
    public void testCreateTableIfNotExistsSkipsExistingRemoteTable() throws UserException {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        LanceCatalogClient client = newClient(namespace, Mockito.mock(BufferAllocator.class));
        LanceExternalCatalog catalog = catalogWithClient(client);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.doReturn(database).when(catalog).getDbNullable("local_db");
        Mockito.when(catalog.getDbForReplay("local_db")).thenReturn(Optional.of(database));
        Mockito.when(database.getRemoteName()).thenReturn("analytics");
        Mockito.when(database.getTableNullable("events")).thenReturn(null);
        CreateTableInfo createTableInfo = createTableInfo(true);

        try {
            Assertions.assertTrue(new LanceMetadataOps(catalog).createTable(createTableInfo));
        } finally {
            client.close();
        }

        Mockito.verify(namespace, Mockito.never())
                .createTable(Mockito.any(), Mockito.any(byte[].class));
        Mockito.verify(database).resetMetaCacheNames();
    }

    @Test
    public void testCreateTableHandlesConcurrentCreate() throws UserException {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.doThrow(new TableNotFoundException("missing"))
                .when(namespace).tableExists(Mockito.any());
        Mockito.doThrow(new TableAlreadyExistsException("created concurrently"))
                .when(namespace).createTable(Mockito.any(), Mockito.any(byte[].class));
        LanceCatalogClient client = newClient(namespace, new RootAllocator(1024 * 1024));
        LanceExternalCatalog catalog = catalogWithClient(client);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.doReturn(database).when(catalog).getDbNullable("local_db");
        Mockito.when(catalog.getDbForReplay("local_db")).thenReturn(Optional.of(database));
        Mockito.when(database.getRemoteName()).thenReturn("analytics");
        Mockito.when(database.getTableNullable("events")).thenReturn(null);
        LanceMetadataOps ops = new LanceMetadataOps(catalog);

        try {
            Assertions.assertTrue(ops.createTable(createTableInfo(true)));
            DdlException exception = Assertions.assertThrows(DdlException.class,
                    () -> ops.createTable(createTableInfo(false)));
            Assertions.assertTrue(exception.getMessage().contains("already exists"));
        } finally {
            client.close();
        }

        Mockito.verify(namespace, Mockito.times(2))
                .createTable(Mockito.any(), Mockito.any(byte[].class));
        Mockito.verify(database).resetMetaCacheNames();
    }

    @Test
    public void testCreateTableIgnoresStaleLocalCacheWhenRemoteTableIsGone() throws UserException {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.doThrow(new TableNotFoundException("missing"))
                .when(namespace).tableExists(Mockito.any());
        LanceCatalogClient client = newClient(namespace, new RootAllocator(1024 * 1024));
        LanceExternalCatalog catalog = catalogWithClient(client);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.doReturn(database).when(catalog).getDbNullable("local_db");
        Mockito.when(catalog.getDbForReplay("local_db")).thenReturn(Optional.of(database));
        Mockito.when(database.getRemoteName()).thenReturn("analytics");
        ExternalTable staleTable = table("local_db", "events", "analytics", "stale_events");
        Mockito.when(database.getTableNullable("events"))
                .thenReturn(staleTable, null);

        try {
            Assertions.assertFalse(new LanceMetadataOps(catalog).createTable(createTableInfo(true)));
        } finally {
            client.close();
        }

        ArgumentCaptor<CreateTableRequest> request = ArgumentCaptor.forClass(CreateTableRequest.class);
        Mockito.verify(namespace).createTable(request.capture(), Mockito.any(byte[].class));
        Assertions.assertEquals(Arrays.asList("tenant", "analytics", "events"),
                request.getValue().getId());
        Mockito.verify(database, Mockito.atLeastOnce()).resetMetaCacheNames();
        Mockito.verify(catalog).invalidateTableAccessCache();
    }

    @Test
    public void testCreateTableRejectsLocalNameConflictAfterRefresh() throws UserException {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.doThrow(new TableNotFoundException("missing"))
                .when(namespace).tableExists(Mockito.any());
        LanceCatalogClient client = newClient(namespace, new RootAllocator(1024 * 1024));
        LanceExternalCatalog catalog = catalogWithClient(client);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.doReturn(database).when(catalog).getDbNullable("local_db");
        Mockito.when(catalog.getDbForReplay("local_db")).thenReturn(Optional.of(database));
        Mockito.when(database.getRemoteName()).thenReturn("analytics");
        ExternalTable conflictingTable = table("local_db", "events", "analytics", "Events");
        Mockito.when(database.getTableNullable("events")).thenReturn(conflictingTable);
        LanceMetadataOps ops = new LanceMetadataOps(catalog);

        try {
            Assertions.assertTrue(ops.createTable(createTableInfo(true)));
            DdlException exception = Assertions.assertThrows(DdlException.class,
                    () -> ops.createTable(createTableInfo(false)));
            Assertions.assertTrue(exception.getMessage().contains("already exists"));
        } finally {
            client.close();
        }

        Mockito.verify(namespace, Mockito.never()).createTable(Mockito.any(), Mockito.any(byte[].class));
        Mockito.verify(database, Mockito.times(2)).resetMetaCacheNames();
    }

    @Test
    public void testIfExistsHandlesConcurrentDropAndRefreshesLocalNames() throws DdlException {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.doThrow(new NamespaceNotFoundException("dropped concurrently"))
                .when(namespace).dropNamespace(Mockito.any());
        Mockito.doThrow(new TableNotFoundException("dropped concurrently"))
                .when(namespace).dropTable(Mockito.any());
        LanceCatalogClient client = newClient(namespace, Mockito.mock(BufferAllocator.class));
        LanceExternalCatalog catalog = catalogWithClient(client);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.when(catalog.getDbForReplay("local_db")).thenReturn(Optional.of(database));
        ExternalTable table = table("local_db", "local_table", "analytics", "events");
        LanceMetadataOps ops = new LanceMetadataOps(catalog);

        try {
            Assertions.assertDoesNotThrow(() -> ops.dropDb("analytics", true, false));
            Assertions.assertThrows(DdlException.class,
                    () -> ops.dropDb("analytics", false, false));
            Assertions.assertDoesNotThrow(() -> ops.dropTable(table, true));
            Assertions.assertThrows(DdlException.class,
                    () -> ops.dropTable(table, false));
        } finally {
            client.close();
        }

        Mockito.verify(catalog).unregisterDatabase("analytics");
        Mockito.verify(database).unregisterTable("local_table");
    }

    @Test
    public void testDropDatabaseReturnsFalseForIfExistsNoOp() throws DdlException {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.doThrow(new NamespaceNotFoundException("missing"))
                .when(namespace).namespaceExists(Mockito.any());
        LanceCatalogClient client = newClient(namespace, Mockito.mock(BufferAllocator.class));
        LanceExternalCatalog catalog = catalogWithClient(client);
        LanceMetadataOps ops = new LanceMetadataOps(catalog);

        try {
            Assertions.assertFalse(ops.dropDb("missing_db", true, false));
        } finally {
            client.close();
        }

        Mockito.verify(namespace, Mockito.never()).dropNamespace(Mockito.any());
        Mockito.verify(catalog).unregisterDatabase("missing_db");
    }

    @Test
    public void testDropDatabaseUsesRemoteDatabaseName() throws DdlException {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        LanceCatalogClient client = newClient(namespace, Mockito.mock(BufferAllocator.class));
        LanceExternalCatalog catalog = catalogWithClient(client);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.doReturn(database).when(catalog).getDbNullable("sales_db");
        Mockito.when(database.getRemoteName()).thenReturn("Sales");
        LanceMetadataOps ops = new LanceMetadataOps(catalog);

        try {
            Assertions.assertTrue(ops.dropDb("sales_db", false, true));
        } finally {
            client.close();
        }

        ArgumentCaptor<DropNamespaceRequest> request = ArgumentCaptor.forClass(DropNamespaceRequest.class);
        Mockito.verify(namespace).dropNamespace(request.capture());
        Assertions.assertEquals(Arrays.asList("tenant", "Sales"), request.getValue().getId());
        Mockito.verify(catalog).unregisterDatabase("sales_db");
    }

    @Test
    public void testDropAndRenameInvalidateTableAccessCacheEvenWhenReplayCacheMisses() throws DdlException {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        LanceCatalogClient client = newClient(namespace, Mockito.mock(BufferAllocator.class));
        LanceExternalCatalog catalog = catalogWithClient(client);
        Mockito.when(catalog.getDbForReplay("local_db")).thenReturn(Optional.empty());
        ExternalTable table = table("local_db", "local_table", "analytics", "events");
        LanceMetadataOps ops = new LanceMetadataOps(catalog);

        try {
            ops.dropTable(table, false);
            ops.afterRenameTable("local_db", "local_table", "renamed_events");
        } finally {
            client.close();
        }

        Mockito.verify(catalog, Mockito.times(2)).invalidateTableAccessCache();
    }

    @Test
    public void testSuccessfulNamespaceAndTableMutationsRefreshLocalNames() throws UserException {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.doThrow(new NamespaceNotFoundException("missing")).doNothing()
                .when(namespace).namespaceExists(Mockito.any());
        Mockito.doThrow(new TableNotFoundException("missing"))
                .when(namespace).tableExists(Mockito.any());
        LanceCatalogClient client = newClient(namespace, new RootAllocator(1024 * 1024));
        LanceExternalCatalog catalog = catalogWithClient(client);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.doReturn(database).when(catalog).getDbNullable("local_db");
        Mockito.when(catalog.getDbForReplay("local_db")).thenReturn(Optional.of(database));
        Mockito.when(database.getRemoteName()).thenReturn("analytics");
        Mockito.when(database.getTableNullable("events")).thenReturn(null);
        ExternalTable table = table("local_db", "events", "analytics", "events");
        LanceMetadataOps ops = new LanceMetadataOps(catalog);

        try {
            Assertions.assertFalse(ops.createDb("analytics", false, Collections.emptyMap()));
            Assertions.assertFalse(ops.createTable(createTableInfo(false)));
            ops.dropTable(table, false);
            ops.dropDb("analytics", false, false);
        } finally {
            client.close();
        }

        Mockito.verify(catalog).resetMetaCacheNames();
        Mockito.verify(catalog).unregisterDatabase("analytics");
        Mockito.verify(database, Mockito.times(2)).resetMetaCacheNames();
        Mockito.verify(database).unregisterTable("events");
    }

    @Test
    public void testRenameTableIsRejected() {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        LanceCatalogClient client = newClient(namespace, Mockito.mock(BufferAllocator.class));
        LanceExternalCatalog catalog = catalogWithClient(client);

        try {
            DdlException exception = Assertions.assertThrows(DdlException.class,
                    () -> new LanceMetadataOps(catalog).renameTable("local_db", "events", "renamed_events"));
            Assertions.assertTrue(exception.getMessage().contains("not supported"));
        } finally {
            client.close();
        }

        Mockito.verify(namespace, Mockito.never()).renameTable(Mockito.any());
        Mockito.verify(catalog, Mockito.never()).invalidateTableAccessCache();
    }

    @Test
    public void testFailedColumnMutationDoesNotRefreshCache() {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.doThrow(new IllegalStateException("mutation failed"))
                .when(namespace).alterTableAddColumns(Mockito.any());
        LanceCatalogClient client = newClient(namespace, Mockito.mock(BufferAllocator.class));
        LanceExternalCatalog catalog = catalogWithClient(client);
        ExternalTable table = table("local_db", "local_table", "analytics", "events");
        Mockito.when(table.getFullSchema()).thenReturn(
                Collections.singletonList(new Column("id", Type.INT, false)));

        try {
            Assertions.assertThrows(DdlException.class, () -> new LanceMetadataOps(catalog).addColumn(
                    table, new Column("score", Type.BIGINT, true), null, 123L));
        } finally {
            client.close();
        }

        Mockito.verify(catalog, Mockito.never()).getDbForReplay(Mockito.anyString());
    }

    @Test
    public void testAllSuccessfulColumnMutationsRefreshCache() throws UserException {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        LanceCatalogClient client = newClient(namespace, Mockito.mock(BufferAllocator.class));
        LanceExternalCatalog catalog = catalogWithClient(client);
        ExternalTable table = table("local_db", "local_table", "analytics", "events");
        Column score = new Column("score", Type.INT, true);
        Mockito.when(table.getFullSchema()).thenReturn(
                Collections.singletonList(score));
        Mockito.when(table.getColumn("score")).thenReturn(score);
        Mockito.when(table.getColumn("ranking")).thenReturn(null);

        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.when(catalog.getDbForReplay("local_db")).thenReturn(Optional.of(database));
        Mockito.doReturn(Optional.of(table)).when(database).getTableForReplay("local_table");
        RefreshManager refreshManager = Mockito.mock(RefreshManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            LanceMetadataOps ops = new LanceMetadataOps(catalog);
            ops.addColumn(table, new Column("added", Type.BIGINT, true), null, 101L);
            ops.addColumns(table, Arrays.asList(
                    new Column("added_two", Type.INT, true),
                    new Column("added_three", Type.STRING, true)), 102L);
            ops.dropColumn(table, "score", 103L);
            ops.renameColumn(table, "score", "ranking", 104L);
            ops.modifyColumn(table, new Column("score", Type.BIGINT, true), null, 105L);
        } finally {
            client.close();
        }

        ArgumentCaptor<AlterTableAddColumnsRequest> request =
                ArgumentCaptor.forClass(AlterTableAddColumnsRequest.class);
        Mockito.verify(namespace, Mockito.times(2)).alterTableAddColumns(request.capture());
        Assertions.assertEquals(
                Arrays.asList("tenant", "analytics", "events"),
                request.getAllValues().get(0).getId());
        Assertions.assertEquals("CAST(NULL AS BIGINT)",
                request.getAllValues().get(0).getNewColumns().get(0).getExpression());
        Mockito.verify(namespace).alterTableDropColumns(Mockito.any());
        Mockito.verify(namespace, Mockito.times(2)).alterTableAlterColumns(Mockito.any());
        for (long updateTime = 101L; updateTime <= 105L; updateTime++) {
            Mockito.verify(refreshManager).refreshTableInternal(database, table, updateTime);
        }
    }

    private static LanceCatalogClient newClient(LanceNamespace namespace, BufferAllocator allocator) {
        return new LanceCatalogClient(namespace, allocator, Mockito.mock(Session.class),
                "filesystem", "default", Collections.singletonList("tenant"),
                Collections.emptyList(), Collections.emptyMap(), Collections.emptyList());
    }

    private static LanceExternalCatalog catalogWithClient(LanceCatalogClient client) {
        LanceExternalCatalog catalog = Mockito.mock(LanceExternalCatalog.class);
        Mockito.when(catalog.acquireClient()).thenAnswer(invocation -> client.acquire());
        return catalog;
    }

    private static CreateTableInfo createTableInfo(boolean ifNotExists) {
        CreateTableInfo createTableInfo = Mockito.mock(CreateTableInfo.class);
        Mockito.when(createTableInfo.getDbName()).thenReturn("local_db");
        Mockito.when(createTableInfo.getTableName()).thenReturn("events");
        Mockito.when(createTableInfo.isIfNotExists()).thenReturn(ifNotExists);
        Mockito.when(createTableInfo.getColumns()).thenReturn(
                Collections.singletonList(new Column("id", Type.INT, false)));
        Mockito.when(createTableInfo.getProperties()).thenReturn(Collections.emptyMap());
        return createTableInfo;
    }

    private static ExternalTable table(String dbName, String tableName, String remoteDbName, String remoteTableName) {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getDbName()).thenReturn(dbName);
        Mockito.when(table.getName()).thenReturn(tableName);
        Mockito.when(table.getRemoteDbName()).thenReturn(remoteDbName);
        Mockito.when(table.getRemoteName()).thenReturn(remoteTableName);
        return table;
    }

    private static void assertRejected(ThrowingRunnable action, String message) {
        UserException exception = Assertions.assertThrows(UserException.class, action::run);
        Assertions.assertTrue(exception.getMessage().contains(message));
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws UserException;
    }
}
