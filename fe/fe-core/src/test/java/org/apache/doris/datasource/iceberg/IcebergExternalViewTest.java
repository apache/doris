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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.mvcc.EmptyMvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccSnapshot;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.View;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IcebergExternalViewTest {
    private static final UUID VIEW_UUID = UUID.fromString("00000000-0000-0000-0000-000000000001");

    private IcebergExternalTable table;
    private IcebergExternalMetaCache cache;
    private View view;
    private MockedStatic<Env> mockedEnv;
    private ExecutorService executor;

    @BeforeEach
    public void setUp() {
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {});
        IcebergExternalDatabase database = new IcebergExternalDatabase(catalog, 2L, "db", "db");
        table = Mockito.spy(new IcebergExternalTable(3L, "view1", "view1", catalog, database));
        Mockito.doNothing().when(table).makeSureInitialized();
        Mockito.doReturn(true).when(table).isView();

        executor = Executors.newSingleThreadExecutor();
        cache = Mockito.spy(new IcebergExternalMetaCache(executor));
        cache.initCatalog(1L, Collections.emptyMap());
        view = Mockito.mock(View.class);
        Mockito.when(view.uuid()).thenReturn(VIEW_UUID);
        Mockito.when(view.schema()).thenReturn(new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())));
        Mockito.doReturn(view).when(cache).getIcebergView(table);
        // A valid view can be rejected by the catalog's table endpoint.
        Mockito.doThrow(tableEndpointError()).when(cache).getSnapshotCache(table);
        Mockito.doThrow(tableEndpointError()).when(cache)
                .withSnapshotCacheValue(Mockito.eq(table), Mockito.any());
        Mockito.doThrow(tableEndpointError()).when(cache).getIcebergTable(table);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr manager = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(manager);
        Mockito.when(manager.iceberg(1L)).thenReturn(cache);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
        executor.shutdownNow();
    }

    @Test
    public void testSchemaCacheReadsViewSchema() {
        IcebergSchemaCacheValue value = (IcebergSchemaCacheValue) table.getSchemaCacheValue().get();
        assertViewSchema(value.getSchema());
        Assertions.assertTrue(value.getPartitionColumns().isEmpty());
        Assertions.assertSame(value, table.getSchemaCacheValue().get());
        verifyNoTableMetadata();
    }

    @Test
    public void testFullSchemaDoesNotLoadTableOrAppendTableMetadataColumns() {
        Mockito.doReturn(true).when(table).needInternalHiddenColumns();
        assertViewSchema(table.getFullSchema(Optional.of(new EmptyMvccSnapshot())));
        assertViewSchema(table.getFullSchema());
        Mockito.verify(table, Mockito.never()).getIcebergTable();
        verifyNoTableMetadata();
    }

    @Test
    public void testSchemaUtilityReadsViewSchema() {
        assertViewSchema(IcebergUtils.getIcebergSchema(table));
        assertViewSchema(IcebergUtils.getIcebergSchema(table, Optional.of(new EmptyMvccSnapshot())));
        verifyNoTableMetadata();
    }

    @Test
    public void testViewHasNoTablePartitions() throws Exception {
        Optional<MvccSnapshot> snapshot = Optional.of(new EmptyMvccSnapshot());
        Assertions.assertFalse(table.supportInternalPartitionPruned());
        Assertions.assertFalse(table.isValidRelatedTable());
        Assertions.assertEquals(PartitionType.UNPARTITIONED, table.getPartitionType(snapshot));
        Assertions.assertTrue(table.getPartitionColumns(snapshot).isEmpty());
        Assertions.assertTrue(table.getPartitionColumnNames(snapshot).isEmpty());
        Assertions.assertTrue(table.getNameToPartitionItems(snapshot).isEmpty());
        Assertions.assertTrue(table.getAndCopyPartitionItems(snapshot).isEmpty());
        verifyNoTableMetadata();
    }

    @Test
    public void testRefreshedViewSchemaIsReadFromViewCache() {
        assertViewSchema(table.getFullSchema(Optional.empty()));
        View refreshedView = Mockito.mock(View.class);
        Mockito.when(refreshedView.uuid()).thenReturn(VIEW_UUID);
        Mockito.when(refreshedView.schema()).thenReturn(new Schema(1,
                Types.NestedField.optional(3, "evolved", Types.LongType.get())));
        Mockito.when(cache.getIcebergView(table)).thenReturn(refreshedView);

        List<Column> columns = table.getFullSchema(Optional.empty());
        Assertions.assertEquals(1, columns.size());
        Assertions.assertEquals("evolved", columns.get(0).getName());
        Assertions.assertEquals(PrimitiveType.BIGINT, columns.get(0).getType().getPrimitiveType());
        verifyNoTableMetadata();
    }

    @Test
    public void testRecreatedViewDoesNotReuseOldSchema() {
        assertViewSchema(table.getFullSchema(Optional.empty()));
        View recreatedView = Mockito.mock(View.class);
        Mockito.when(recreatedView.uuid()).thenReturn(UUID.fromString("00000000-0000-0000-0000-000000000002"));
        Mockito.when(recreatedView.schema()).thenReturn(new Schema(
                Types.NestedField.optional(3, "recreated", Types.LongType.get())));
        Mockito.when(cache.getIcebergView(table)).thenReturn(recreatedView);

        List<Column> columns = table.getFullSchema(Optional.empty());
        Assertions.assertEquals(1, columns.size());
        Assertions.assertEquals("recreated", columns.get(0).getName());
        verifyNoTableMetadata();
    }

    @Test
    public void testSessionViewSchemaBypassesSharedCache() {
        IcebergRestExternalCatalog sessionCatalog = Mockito.mock(IcebergRestExternalCatalog.class);
        Mockito.when(sessionCatalog.getId()).thenReturn(1L);
        Mockito.when(sessionCatalog.useSessionCatalog(Mockito.any())).thenReturn(true);
        Mockito.when(sessionCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {});
        IcebergMetadataOps ops = Mockito.mock(IcebergMetadataOps.class);
        Mockito.when(sessionCatalog.getMetadataOps()).thenReturn(ops);
        View sessionView = Mockito.mock(View.class);
        Mockito.when(sessionView.schema()).thenReturn(new Schema(
                Types.NestedField.optional(1, "session_column", Types.LongType.get())));
        Mockito.when(ops.loadView(Mockito.any(), Mockito.eq("db"), Mockito.eq("view1")))
                .thenReturn(sessionView);
        Mockito.doReturn(sessionCatalog).when(table).getCatalog();

        List<Column> schema = table.getSchemaCacheValue().get().getSchema();
        Assertions.assertEquals(1, schema.size());
        Assertions.assertEquals("session_column", schema.get(0).getName());
        Assertions.assertEquals("session_column", table.getFullSchema().get(0).getName());
        Assertions.assertEquals("session_column", IcebergUtils.getIcebergSchema(table).get(0).getName());
        Mockito.verify(cache, Mockito.never()).getIcebergView(table);
        Mockito.verify(cache, Mockito.never()).getIcebergViewSchemaCacheValue(Mockito.any(), Mockito.anyLong());
        verifyNoTableMetadata();
    }

    @Test
    public void testSessionViewWithoutCredentialRejectsSharedCacheFallback() {
        IcebergRestExternalCatalog sessionCatalog = Mockito.mock(IcebergRestExternalCatalog.class);
        Mockito.when(sessionCatalog.getId()).thenReturn(1L);
        Mockito.when(sessionCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {});
        Mockito.when(sessionCatalog.useSessionCatalog(Mockito.any()))
                .thenThrow(new IllegalStateException("Session credential is required"));
        Mockito.doReturn(sessionCatalog).when(table).getCatalog();

        Assertions.assertThrows(IllegalStateException.class, table::getSchemaCacheValue);
        Assertions.assertThrows(IllegalStateException.class, table::getFullSchema);
        Assertions.assertThrows(IllegalStateException.class, () -> IcebergUtils.getIcebergSchema(table));
        Mockito.verify(cache, Mockito.never()).getIcebergView(table);
        Mockito.verify(cache, Mockito.never()).getIcebergViewSchemaCacheValue(Mockito.any(), Mockito.anyLong());
        verifyNoTableMetadata();
    }

    @Test
    public void testViewSchemaStillRejectsDuplicateColumns() {
        Mockito.when(view.schema()).thenReturn(new Schema(
                Types.NestedField.optional(1, "ID", Types.IntegerType.get()),
                Types.NestedField.optional(2, "id", Types.IntegerType.get())));

        RuntimeException error = Assertions.assertThrows(RuntimeException.class, table::getSchemaCacheValue);
        Assertions.assertTrue(ExceptionUtils.getRootCauseMessage(error).contains("Duplicate column name"));
        verifyNoTableMetadata();
    }

    @Test
    public void testDeletedViewFailureIsNotTreatedAsATable() {
        Mockito.when(cache.getIcebergView(table)).thenThrow(new NoSuchViewException("View was deleted"));
        RuntimeException error = Assertions.assertThrows(RuntimeException.class, table::getSchemaCacheValue);
        Assertions.assertInstanceOf(NoSuchViewException.class, error.getCause());
        verifyNoTableMetadata();
    }

    @Test
    public void testOrdinaryTableMetadataFailureStillPropagates() {
        Mockito.doReturn(false).when(table).isView();
        Assertions.assertThrows(BadRequestException.class, table::getSchemaCacheValue);
        Mockito.verify(cache).withSnapshotCacheValue(Mockito.eq(table), Mockito.any());
        Mockito.verify(cache, Mockito.never()).getIcebergView(table);
    }

    private void verifyNoTableMetadata() {
        Mockito.verify(cache, Mockito.never()).getSnapshotCache(table);
        Mockito.verify(cache, Mockito.never()).withSnapshotCacheValue(Mockito.eq(table), Mockito.any());
        Mockito.verify(cache, Mockito.never()).getIcebergTable(table);
        Mockito.verify(cache, Mockito.never()).getTableCacheValue(table);
    }

    private static void assertViewSchema(List<Column> columns) {
        Assertions.assertEquals(2, columns.size());
        Assertions.assertEquals("id", columns.get(0).getName());
        Assertions.assertEquals(PrimitiveType.INT, columns.get(0).getType().getPrimitiveType());
        Assertions.assertEquals("name", columns.get(1).getName());
        Assertions.assertEquals(PrimitiveType.STRING, columns.get(1).getType().getPrimitiveType());
    }

    private static BadRequestException tableEndpointError() {
        return new BadRequestException("Table 'db.view1' is not an Iceberg compatible table");
    }
}
