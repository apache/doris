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

package org.apache.doris.arrowflight;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.thrift.TColumnDef;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TDescribeTablesParams;
import org.apache.doris.thrift.TDescribeTablesResult;
import org.apache.doris.thrift.TGetDbsParams;
import org.apache.doris.thrift.TGetDbsResult;
import org.apache.doris.thrift.TGetTablesParams;
import org.apache.doris.thrift.TListTableStatusResult;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TTableStatus;

import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FlightSqlMetadataOrderTest {
    private boolean savedRunningUnitTest;

    @BeforeEach
    public void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() {
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void catalogsUseShowVisibilityAndSessionRoles(boolean mappedRole) throws Exception {
        ConnectContext ctx = context(mappedRole);
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogs = Mockito.mock(CatalogMgr.class, Mockito.CALLS_REAL_METHODS);
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        Map<String, CatalogIf> byName = new LinkedHashMap<>();
        for (String name : Arrays.asList("zulu", "internal", "apple", "hidden", "role_catalog")) {
            CatalogIf catalog = Mockito.mock(CatalogIf.class);
            Mockito.when(catalog.getName()).thenReturn(name);
            byName.put(name, catalog);
        }
        Deencapsulation.setField(catalogs, "nameToCatalog", byName);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogs);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(access.checkCtlPriv(Mockito.any(UserIdentity.class), Mockito.eq("apple"),
                Mockito.eq(PrivPredicate.SHOW))).thenReturn(true);
        Mockito.when(access.checkCtlPriv(Mockito.any(UserIdentity.class), Mockito.eq("zulu"),
                Mockito.eq(PrivPredicate.SHOW))).thenReturn(true);
        Mockito.when(access.checkCtlPriv(Mockito.any(UserIdentity.class), Mockito.eq("role_catalog"),
                Mockito.eq(PrivPredicate.SHOW))).thenAnswer(invocation ->
                        ((UserIdentity) invocation.getArgument(0)).getAuthenticatedRoles().contains("mapped_role"));
        try (MockedStatic<Env> environments = Mockito.mockStatic(Env.class);
                MockedStatic<ExecuteEnv> executeEnv = Mockito.mockStatic(ExecuteEnv.class);
                MockedConstruction<FrontendServiceImpl> services = Mockito.mockConstruction(FrontendServiceImpl.class);
                RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_CATALOGS_SCHEMA, allocator)) {
            environments.when(Env::getCurrentEnv).thenReturn(env);
            root.allocateNew();
            new FlightSqlSchemaHelper(ctx).getCatalogs(root);
            Assertions.assertEquals(mappedRole ? Arrays.asList("apple", "role_catalog", "zulu")
                    : Arrays.asList("apple", "zulu"), values(root, "catalog_name"));
            Mockito.verify(catalogs).listCatalogsWithCheckPriv(Mockito.argThat(identity ->
                    identity.equals(ctx.getCurrentUserIdentity())
                            && identity.getAuthenticatedRoles().equals(ctx.getAuthenticatedRoles())));
            Assertions.assertTrue(ctx.getCurrentUserIdentity().getAuthenticatedRoles().isEmpty());
            Mockito.verifyNoInteractions(services.constructed().get(0));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void schemasOrderCatalogAndSchemaTogether(boolean hasCatalogs) throws Exception {
        try (MockedStatic<ExecuteEnv> executeEnv = Mockito.mockStatic(ExecuteEnv.class);
                MockedConstruction<FrontendServiceImpl> services = Mockito.mockConstruction(FrontendServiceImpl.class,
                        (service, construction) -> Mockito.when(service.getDbNames(Mockito.any(TGetDbsParams.class)))
                                .thenReturn(databases(hasCatalogs)));
                RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_SCHEMAS_SCHEMA, allocator)) {
            FlightSqlSchemaHelper helper = new FlightSqlSchemaHelper(context(true));
            helper.setParameterForGetDbSchemas(CommandGetDbSchemas.newBuilder().setCatalog("internal").build());
            root.allocateNew();
            helper.getSchemas(root);
            Assertions.assertEquals(hasCatalogs ? Arrays.asList("apple", "apple", "zulu")
                    : Arrays.asList("", "", ""), values(root, "catalog_name"));
            Assertions.assertEquals(Arrays.asList("a_db", "z_db", "z_db"), values(root, "db_schema_name"));
            Mockito.verify(services.constructed().get(0)).getDbNames(Mockito.argThat(params ->
                    params.getCurrentUserIdent().getAuthenticatedRoles().equals(Set.of("mapped_role"))));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void tablesOrderBeforeDescribingAndKeepSchemaAssociations(boolean includeSchema) throws Exception {
        ConnectContext ctx = context(true);
        try (MockedStatic<ExecuteEnv> executeEnv = Mockito.mockStatic(ExecuteEnv.class);
                MockedConstruction<FrontendServiceImpl> services = Mockito.mockConstruction(FrontendServiceImpl.class,
                        (service, construction) -> {
                            Mockito.when(service.getDbNames(Mockito.any(TGetDbsParams.class)))
                                    .thenReturn(databases(true));
                            Mockito.when(service.listTableStatus(Mockito.any(TGetTablesParams.class)))
                                    .thenAnswer(invocation -> new TListTableStatusResult().setTables(Arrays.asList(
                                            new TTableStatus().setName("z_table").setType("BASE TABLE"),
                                            new TTableStatus().setName("a_view").setType("VIEW"),
                                            new TTableStatus().setName("a_table").setType("BASE TABLE"))));
                            Mockito.when(service.describeTables(Mockito.any(TDescribeTablesParams.class)))
                                    .thenAnswer(invocation -> {
                                        TDescribeTablesParams params = invocation.getArgument(0);
                                        Assertions.assertEquals(Arrays.asList("a_table", "z_table"),
                                                params.getTablesName());
                                        Assertions.assertEquals(ctx.getCurrentUserIdentityForRpc(),
                                                params.getCurrentUserIdent());
                                        List<TColumnDef> columns = new ArrayList<>();
                                        List<Integer> offsets = new ArrayList<>();
                                        for (String table : params.getTablesName()) {
                                            String column = params.getCatalog() + "_" + params.getDb() + "_" + table;
                                            columns.add(new TColumnDef(new TColumnDesc(column, TPrimitiveType.INT)));
                                            offsets.add(columns.size());
                                        }
                                        return new TDescribeTablesResult().setColumns(columns).setTablesOffset(offsets);
                                    });
                        });
                RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(includeSchema
                        ? Schemas.GET_TABLES_SCHEMA : Schemas.GET_TABLES_SCHEMA_NO_SCHEMA, allocator)) {
            FlightSqlSchemaHelper helper = new FlightSqlSchemaHelper(ctx);
            helper.setParameterForGetTables(CommandGetTables.newBuilder().setCatalog("internal")
                    .addTableTypes("table").setIncludeSchema(includeSchema).build());
            root.allocateNew();
            helper.getTables(root);
            Assertions.assertEquals(Arrays.asList("apple", "apple", "apple", "apple", "zulu", "zulu"),
                    values(root, "catalog_name"));
            Assertions.assertEquals(Arrays.asList("a_db", "a_db", "z_db", "z_db", "z_db", "z_db"),
                    values(root, "db_schema_name"));
            Assertions.assertEquals(Arrays.asList("a_table", "z_table", "a_table", "z_table", "a_table", "z_table"),
                    values(root, "table_name"));
            Assertions.assertEquals(Collections.nCopies(6, "BASE TABLE"), values(root, "table_type"));
            for (int row = 0; includeSchema && row < root.getRowCount(); row++) {
                byte[] bytes = ((VarBinaryVector) root.getVector("table_schema")).get(row);
                String column = root.getVector("catalog_name").getObject(row) + "_"
                        + root.getVector("db_schema_name").getObject(row) + "_"
                        + root.getVector("table_name").getObject(row);
                Assertions.assertEquals(column, MessageSerializer.deserializeSchema(
                        new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes))))
                        .getFields().get(0).getName());
            }
            Mockito.verify(services.constructed().get(0), Mockito.times(3)).listTableStatus(Mockito.argThat(params ->
                    params.getCurrentUserIdent().equals(ctx.getCurrentUserIdentityForRpc())
                            && params.getRequiredColumns().equals(Set.of("TABLE_NAME", "TABLE_TYPE"))));
            Mockito.verify(services.constructed().get(0), Mockito.times(includeSchema ? 3 : 0))
                    .describeTables(Mockito.any(TDescribeTablesParams.class));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void emptyDiscoveryStaysEmpty(boolean tables) throws Exception {
        try (MockedStatic<ExecuteEnv> executeEnv = Mockito.mockStatic(ExecuteEnv.class);
                MockedConstruction<FrontendServiceImpl> services = Mockito.mockConstruction(FrontendServiceImpl.class,
                        (service, construction) -> Mockito.when(service.getDbNames(Mockito.any(TGetDbsParams.class)))
                                .thenReturn(new TGetDbsResult().setDbs(Collections.emptyList())));
                RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(tables
                        ? Schemas.GET_TABLES_SCHEMA : Schemas.GET_SCHEMAS_SCHEMA, allocator)) {
            FlightSqlSchemaHelper helper = new FlightSqlSchemaHelper(context(false));
            root.allocateNew();
            if (tables) {
                helper.setParameterForGetTables(CommandGetTables.newBuilder().setIncludeSchema(true).build());
                helper.getTables(root);
            } else {
                helper.setParameterForGetDbSchemas(CommandGetDbSchemas.getDefaultInstance());
                helper.getSchemas(root);
            }
            Assertions.assertEquals(0, root.getRowCount());
            Mockito.verify(services.constructed().get(0), Mockito.never())
                    .listTableStatus(Mockito.any(TGetTablesParams.class));
            Mockito.verify(services.constructed().get(0), Mockito.never())
                    .describeTables(Mockito.any(TDescribeTablesParams.class));
        }
    }

    private static ConnectContext context(boolean mappedRole) {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("metadata_user", "%"));
        ctx.setAuthenticatedRoles(mappedRole ? Set.of("mapped_role") : Collections.emptySet());
        return ctx;
    }

    private static TGetDbsResult databases(boolean hasCatalogs) {
        TGetDbsResult result = new TGetDbsResult().setDbs(Arrays.asList("z_db", "z_db", "a_db"));
        if (hasCatalogs) {
            result.setCatalogs(Arrays.asList("zulu", "apple", "apple"));
        }
        return result;
    }

    private static List<String> values(VectorSchemaRoot root, String column) {
        List<String> values = new ArrayList<>();
        for (int row = 0; row < root.getRowCount(); row++) {
            values.add(root.getVector(column).getObject(row).toString());
        }
        return values;
    }
}
