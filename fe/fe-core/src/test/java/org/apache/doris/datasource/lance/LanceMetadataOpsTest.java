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
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.Session;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.AlterTableAddColumnsRequest;
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
    public void testAddColumnUsesRemoteNamesAndRefreshesCache() throws UserException {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        BufferAllocator allocator = Mockito.mock(BufferAllocator.class);
        Session session = Mockito.mock(Session.class);
        LanceCatalogClient client = new LanceCatalogClient(namespace, allocator, session,
                "filesystem", "default", Collections.singletonList("tenant"),
                Collections.emptyList(), Collections.emptyMap(), Collections.emptyList());
        LanceExternalCatalog catalog = Mockito.mock(LanceExternalCatalog.class);
        Mockito.when(catalog.acquireClient()).thenAnswer(invocation -> client.acquire());

        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getDbName()).thenReturn("local_db");
        Mockito.when(table.getName()).thenReturn("local_table");
        Mockito.when(table.getRemoteDbName()).thenReturn("analytics");
        Mockito.when(table.getRemoteName()).thenReturn("events");
        Mockito.when(table.getFullSchema()).thenReturn(
                Collections.singletonList(new Column("id", Type.INT, false)));

        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.when(catalog.getDbForReplay("local_db")).thenReturn(Optional.of(database));
        Mockito.doReturn(Optional.of(table)).when(database).getTableForReplay("local_table");
        RefreshManager refreshManager = Mockito.mock(RefreshManager.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            new LanceMetadataOps(catalog).addColumn(
                    table, new Column("score", Type.BIGINT, true), null, 123L);
        } finally {
            client.close();
        }

        ArgumentCaptor<AlterTableAddColumnsRequest> request =
                ArgumentCaptor.forClass(AlterTableAddColumnsRequest.class);
        Mockito.verify(namespace).alterTableAddColumns(request.capture());
        Assertions.assertEquals(
                Arrays.asList("tenant", "analytics", "events"),
                request.getValue().getId());
        Assertions.assertEquals("CAST(NULL AS BIGINT)",
                request.getValue().getNewColumns().get(0).getExpression());
        Mockito.verify(refreshManager).refreshTableInternal(database, table, 123L);
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
