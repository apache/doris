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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.proc.IndexInfoProcDir;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.commands.ShowColumnsCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.thrift.TDescribeTablesParams;
import org.apache.doris.thrift.TDescribeTablesResult;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class IcebergSchemaDisplayTest {
    private IcebergExternalCatalog catalog;
    private IcebergExternalDatabase database;
    private IcebergExternalTable table;
    private TableOperations operations;
    private TableMetadata metadata;
    private Schema schema;
    private List<Column> scanColumns;
    private ConnectContext context;

    @BeforeEach
    void setUp() {
        context = new ConnectContext();
        context.setThreadLocalInfo();
        schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "value", Types.StringType.get(), "value doc"),
                Types.NestedField.optional(3, "event_time", Types.TimestampType.withoutZone()),
                Types.NestedField.required(4, "payload", Types.StructType.of(
                        Types.NestedField.required(5, "child", Types.IntegerType.get(), "nested doc"))),
                Types.NestedField.optional(6, "zoned_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(7, "bytes", Types.BinaryType.get()));
        metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                "file:/tmp/iceberg-schema-display", Collections.singletonMap(TableProperties.FORMAT_VERSION, "2"));
        operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenReturn(metadata);
        Mockito.when(operations.io()).thenReturn(Mockito.mock(FileIO.class));
        Mockito.when(operations.locationProvider()).thenReturn(Mockito.mock(LocationProvider.class));

        catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() { });
        Mockito.when(catalog.getEnableMappingVarbinary()).thenReturn(true);
        database = Mockito.spy(new IcebergExternalDatabase(catalog, 2L, "db", "db"));
        table = Mockito.spy(new IcebergExternalTable(3, "required_tbl", "required_tbl", catalog, database));
        Mockito.doNothing().when(table).makeSureInitialized();
        Mockito.doReturn(new BaseTable(operations, "required_tbl")).when(table).getIcebergTable();
        // Model the cached scan schema. Displaying a table must never mutate these shared columns.
        scanColumns = IcebergUtils.parseSchema(schema, true, false);
        Mockito.doReturn(scanColumns).when(table).getFullSchema();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void testDescribeAndDdlPreserveDeclaredNullabilityWithoutChangingScanColumns() throws Exception {
        context.getSessionVariable().showColumnCommentInDescribe = true;
        List<List<String>> rows = describe();
        Assertions.assertEquals("No", rows.get(0).get(2));
        Assertions.assertEquals("No", rows.get(1).get(2));
        Assertions.assertEquals("Yes", rows.get(2).get(2));
        Assertions.assertEquals("No", rows.get(3).get(2));
        Assertions.assertEquals("value doc", rows.get(1).get(6));
        Assertions.assertTrue(rows.get(3).get(1).contains("child:int not null comment 'nested doc'"));
        Assertions.assertEquals("WITH_TIMEZONE", rows.get(4).get(5));
        Assertions.assertTrue(rows.get(5).get(1).startsWith("varbinary"));

        String ddl = showCreate();
        Assertions.assertTrue(ddl.contains("`id` bigint NOT NULL"));
        Assertions.assertTrue(ddl.contains("`value` text NOT NULL"));
        Assertions.assertTrue(ddl.contains("`event_time` datetimev2(6) NULL"));
        Assertions.assertTrue(ddl.contains("struct<child:int not null comment 'nested doc'> NOT NULL"));
        Assertions.assertEquals(rows, describe());
        Assertions.assertEquals(ddl, showCreate());
        Assertions.assertTrue(scanColumns.stream().allMatch(Column::isAllowNull));
        Assertions.assertTrue(scanColumns.get(3).getChildren().get(0).isAllowNull());
        Assertions.assertTrue(SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(),
                table, scanColumns.get(0), Collections.emptyList()).nullable());
    }

    @Test
    void testSchemaOnlyChangeUsesCurrentTableSchema() throws Exception {
        Assertions.assertNull(metadata.currentSnapshot());
        Assertions.assertEquals("No", describe().get(1).get(2));
        List<Types.NestedField> fields = new ArrayList<>(schema.columns());
        fields.set(1, Types.NestedField.optional(2, "value", Types.StringType.get(), "value doc"));
        TableMetadata evolved = metadata.updateSchema(new Schema(fields));
        Mockito.when(operations.current()).thenReturn(evolved);

        Assertions.assertNull(evolved.currentSnapshot());
        Assertions.assertNotEquals(metadata.currentSchemaId(), evolved.currentSchemaId());
        Assertions.assertEquals("Yes", describe().get(1).get(2));
        Assertions.assertTrue(showCreate().contains("`value` text NULL"));
        Assertions.assertTrue(showCreate().contains("`id` bigint NOT NULL"));
    }

    @Test
    void testDisplayRetainsOneMetadataGeneration() {
        List<Types.NestedField> fields = new ArrayList<>(schema.columns());
        fields.set(1, Types.NestedField.optional(2, "renamed_value", Types.StringType.get()));
        TableMetadata evolved = metadata.updateSchema(new Schema(fields))
                .upgradeToFormatVersion(3);
        Mockito.when(operations.current()).thenReturn(metadata, evolved);
        Mockito.clearInvocations(operations);

        List<Column> displayed = table.getBaseSchemaForDisplay(true);
        Assertions.assertEquals(schema.columns().size(), displayed.size());
        Assertions.assertEquals("value", displayed.get(1).getName());
        Assertions.assertFalse(displayed.get(1).isAllowNull());
        Mockito.verify(operations, Mockito.times(1)).current();
    }

    @Test
    void testHiddenColumnsFollowExistingVisibilityRules() throws Exception {
        Mockito.when(operations.current()).thenReturn(metadata.upgradeToFormatVersion(3));
        Assertions.assertEquals(schema.columns().size(), describe().size());
        context.getSessionVariable().setShowHiddenColumns(true);
        List<List<String>> rows = describe();
        Assertions.assertEquals(schema.columns().size() + 3, rows.size());
        Assertions.assertTrue(rows.stream().anyMatch(row -> row.get(0).equals(IcebergUtils.ICEBERG_ROW_ID_COL)));
        Assertions.assertTrue(rows.stream().anyMatch(row -> row.get(0).equals(
                IcebergUtils.ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL)));
        Assertions.assertEquals(schema.columns().size(), table.getBaseSchemaForDisplay(false).size());
        Assertions.assertFalse(showCreate().contains("`" + IcebergUtils.ICEBERG_ROW_ID_COL + "`"));
        Assertions.assertFalse(showCreate().contains("`" + IcebergRowId.createHiddenColumn().getName() + "`"));
    }

    @Test
    void testViewsKeepTheirExistingSchemaPath() throws Exception {
        Mockito.doReturn(true).when(table).isView();
        Assertions.assertEquals("Yes", describe().get(0).get(2));
        Mockito.verify(table, Mockito.never()).getIcebergTable();
    }

    @Test
    void testOtherTablesKeepTheirExistingSchemaPath() throws Exception {
        TableIf otherTable = Mockito.mock(TableIf.class);
        Mockito.when(otherTable.getBaseSchema()).thenReturn(scanColumns);
        List<List<String>> rows = new IndexInfoProcDir(database, otherTable).lookup("4").fetchResult().getRows();
        Assertions.assertEquals("Yes", rows.get(0).get(2));
        Mockito.verify(otherTable).getBaseSchema();
    }

    @Test
    void testShowColumnsUsesDeclaredNullability() throws Exception {
        try (MockedStatic<Env> ignored = mockMetadataEnv()) {
            for (boolean full : new boolean[] {false, true}) {
                int nullIndex = full ? 3 : 2;
                ShowColumnsCommand command = new ShowColumnsCommand(full,
                        new TableNameInfo("iceberg", "db", "required_tbl"), null, null, null);
                List<List<String>> rows = command.doRun(context, null).getResultRows();
                Assertions.assertEquals("NO", rows.get(0).get(nullIndex));
                Assertions.assertEquals("NO", rows.get(1).get(nullIndex));
                Assertions.assertEquals("YES", rows.get(2).get(nullIndex));
                ShowColumnsCommand filtered = new ShowColumnsCommand(full,
                        new TableNameInfo("iceberg", "db", "required_tbl"), null, "id", null);
                List<List<String>> filteredRows = filtered.doRun(context, null).getResultRows();
                Assertions.assertEquals(1, filteredRows.size());
                Assertions.assertEquals("id", filteredRows.get(0).get(0));
                Assertions.assertEquals("NO", filteredRows.get(0).get(nullIndex));
            }
        }
        Assertions.assertTrue(scanColumns.stream().allMatch(Column::isAllowNull));
    }

    @Test
    void testDescribeTablesUsesDeclaredNullabilityWithoutSessionContext() throws Exception {
        try (MockedStatic<Env> ignored = mockMetadataEnv()) {
            // information_schema requests arrive on an RPC thread, without a SQL session context.
            ConnectContext.remove();
            TDescribeTablesResult result = describeTables("required_tbl");
            Assertions.assertEquals(Collections.singletonList(scanColumns.size()), result.getTablesOffset());
            Assertions.assertFalse(result.getColumns().get(0).getColumnDesc().isIsAllowNull());
            Assertions.assertFalse(result.getColumns().get(1).getColumnDesc().isIsAllowNull());
            Assertions.assertTrue(result.getColumns().get(2).getColumnDesc().isIsAllowNull());
            Assertions.assertEquals("value doc", result.getColumns().get(1).getComment());
        }
        Assertions.assertTrue(scanColumns.stream().allMatch(Column::isAllowNull));
        Assertions.assertTrue(scanColumns.get(3).getChildren().get(0).isAllowNull());
    }

    @Test
    void testDescribeTablesKeepsOffsetsWhenDisplaySchemaFails() throws Exception {
        try (MockedStatic<Env> ignored = mockMetadataEnv()) {
            IcebergExternalTable unavailable = Mockito.mock(IcebergExternalTable.class);
            Mockito.when(unavailable.getBaseSchemaForDisplay()).thenThrow(new RuntimeException("schema unavailable"));
            Mockito.doReturn(unavailable).when(database).getTableNullableIfException("unavailable");
            TDescribeTablesResult result = describeTables("unavailable", "required_tbl");
            Assertions.assertEquals(Arrays.asList(0, scanColumns.size()), result.getTablesOffset());
            Assertions.assertEquals(scanColumns.size(), result.getColumns().size());
            Assertions.assertFalse(result.getColumns().get(0).getColumnDesc().isIsAllowNull());
        }
    }

    @Test
    void testDescribeTablesChecksPrivilegesBeforeLoadingSchema() throws Exception {
        try (MockedStatic<Env> ignored = mockMetadataEnv()) {
            Mockito.when(Env.getCurrentEnv().getAccessManager().checkTblPriv(Mockito.any(UserIdentity.class),
                    Mockito.eq("iceberg"), Mockito.eq("db"), Mockito.eq("required_tbl"),
                    Mockito.eq(PrivPredicate.SHOW))).thenReturn(false);
            TDescribeTablesResult result = describeTables("required_tbl");
            Assertions.assertTrue(result.getColumns().isEmpty());
            Mockito.verify(table, Mockito.never()).getBaseSchemaForDisplay();
        }
    }

    private MockedStatic<Env> mockMetadataEnv() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalogOrAnalysisException("iceberg");
        Mockito.doReturn(catalog).when(catalogMgr).getCatalogOrException(Mockito.eq("iceberg"), Mockito.any());
        Mockito.doReturn(database).when(catalog).getDbOrAnalysisException("db");
        Mockito.doReturn(database).when(catalog).getDbNullable("db");
        Mockito.doReturn(table).when(database).getTableOrAnalysisException("required_tbl");
        Mockito.doReturn(table).when(database).getTableNullableIfException("required_tbl");
        Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.eq(PrivPredicate.SHOW))).thenReturn(true);
        Mockito.when(accessManager.checkTblPriv(Mockito.any(UserIdentity.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.eq(PrivPredicate.SHOW))).thenReturn(true);
        MockedStatic<Env> mocked = Mockito.mockStatic(Env.class);
        mocked.when(Env::getCurrentEnv).thenReturn(env);
        return mocked;
    }

    private TDescribeTablesResult describeTables(String... names) throws Exception {
        TDescribeTablesParams params = new TDescribeTablesParams();
        params.setCatalog("iceberg");
        params.setDb("db");
        params.setTablesName(Arrays.asList(names));
        params.setCurrentUserIdent(UserIdentity.ROOT.toThrift());
        // The RPC handler does not need the background report thread started by its constructor.
        return Mockito.mock(FrontendServiceImpl.class, Mockito.CALLS_REAL_METHODS).describeTables(params);
    }

    private List<List<String>> describe() throws Exception {
        return new IndexInfoProcDir(database, table).lookup("3").fetchResult().getRows();
    }

    private String showCreate() {
        List<String> statements = new ArrayList<>();
        Env.getDdlStmt(table, statements, null, null, false, true, -1L);
        return statements.get(0);
    }
}
