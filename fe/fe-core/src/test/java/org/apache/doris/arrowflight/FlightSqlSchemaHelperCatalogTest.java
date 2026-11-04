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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlightSqlSchemaHelperCatalogTest {
    private static Stream<Arguments> catalogFilters() {
        return Stream.of(
                Arguments.of(null, Arrays.asList("internal", "external")),
                Arguments.of("internal", Collections.singletonList("internal")),
                Arguments.of("external", Collections.singletonList("external")),
                Arguments.of("", Collections.emptyList()));
    }

    @ParameterizedTest
    @MethodSource("catalogFilters")
    public void schemasRespectCatalogPresence(String catalog, List<String> expectedCatalogs) throws Exception {
        checkMetadata(catalog, expectedCatalogs, false, false);
    }

    @ParameterizedTest
    @MethodSource("catalogFilters")
    public void tablesRespectCatalogPresence(String catalog, List<String> expectedCatalogs) throws Exception {
        checkMetadata(catalog, expectedCatalogs, true, false);
    }

    @ParameterizedTest
    @MethodSource("catalogFilters")
    public void tableSchemasRespectCatalogPresence(String catalog, List<String> expectedCatalogs) throws Exception {
        checkMetadata(catalog, expectedCatalogs, true, true);
    }

    private void checkMetadata(String catalog, List<String> expectedCatalogs, boolean tables, boolean includeSchema)
            throws Exception {
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        try (MockedStatic<ExecuteEnv> executeEnv = Mockito.mockStatic(ExecuteEnv.class);
                MockedConstruction<FrontendServiceImpl> services = Mockito.mockConstruction(
                        FrontendServiceImpl.class, (service, construction) -> {
                            Mockito.when(service.getDbNames(Mockito.any(TGetDbsParams.class))).thenAnswer(invocation -> {
                                TGetDbsParams params = invocation.getArgument(0);
                                Assertions.assertEquals("test%", params.getPattern());
                                Assertions.assertEquals(UserIdentity.ROOT.toThrift(), params.getCurrentUserIdent());
                                List<String> catalogs = Stream.of("internal", "external")
                                        .filter(name -> !params.isSetCatalog() || params.getCatalog().isEmpty()
                                                || name.equals(params.getCatalog()))
                                        .collect(Collectors.toList());
                                return new TGetDbsResult().setCatalogs(catalogs)
                                        .setDbs(Collections.nCopies(catalogs.size(), "test_db"));
                            });
                            Mockito.when(service.listTableStatus(Mockito.any(TGetTablesParams.class)))
                                    .thenAnswer(invocation -> {
                                        TGetTablesParams params = invocation.getArgument(0);
                                        Assertions.assertTrue(expectedCatalogs.contains(params.getCatalog()));
                                        Assertions.assertEquals("test_db", params.getDb());
                                        Assertions.assertEquals("visible%", params.getPattern());
                                        Assertions.assertEquals(UserIdentity.ROOT.toThrift(),
                                                params.getCurrentUserIdent());
                                        return new TListTableStatusResult().setTables(Collections.singletonList(
                                                new TTableStatus().setName("visible_table").setType("BASE TABLE")));
                                    });
                            Mockito.when(service.describeTables(Mockito.any(TDescribeTablesParams.class)))
                                    .thenAnswer(invocation -> {
                                        TDescribeTablesParams params = invocation.getArgument(0);
                                        Assertions.assertTrue(expectedCatalogs.contains(params.getCatalog()));
                                        Assertions.assertEquals("test_db", params.getDb());
                                        Assertions.assertEquals(Collections.singletonList("visible_table"),
                                                params.getTablesName());
                                        Assertions.assertEquals(UserIdentity.ROOT.toThrift(),
                                                params.getCurrentUserIdent());
                                        TColumnDesc item = new TColumnDesc("item", TPrimitiveType.INT)
                                                .setIsAllowNull(true);
                                        TColumnDesc array = new TColumnDesc("items", TPrimitiveType.ARRAY)
                                                .setIsAllowNull(true).setChildren(Collections.singletonList(item));
                                        return new TDescribeTablesResult()
                                                .setColumns(Collections.singletonList(new TColumnDef(array)))
                                                .setTablesOffset(Collections.singletonList(1));
                                    });
                        });
                RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(tables
                        ? (includeSchema ? Schemas.GET_TABLES_SCHEMA : Schemas.GET_TABLES_SCHEMA_NO_SCHEMA)
                        : Schemas.GET_SCHEMAS_SCHEMA, allocator)) {
            FlightSqlSchemaHelper helper = new FlightSqlSchemaHelper(ctx);
            if (tables) {
                CommandGetTables.Builder command = CommandGetTables.newBuilder().setDbSchemaFilterPattern("test%")
                        .setTableNameFilterPattern("visible%").setIncludeSchema(includeSchema);
                if (catalog != null) {
                    command.setCatalog(catalog);
                }
                helper.setParameterForGetTables(command.build());
            } else {
                CommandGetDbSchemas.Builder command = CommandGetDbSchemas.newBuilder()
                        .setDbSchemaFilterPattern("test%");
                if (catalog != null) {
                    command.setCatalog(catalog);
                }
                helper.setParameterForGetDbSchemas(command.build());
            }
            root.allocateNew();
            if (tables) {
                helper.getTables(root);
            } else {
                helper.getSchemas(root);
            }
            List<String> actualCatalogs = new ArrayList<>();
            for (int row = 0; row < root.getRowCount(); row++) {
                actualCatalogs.add(root.getVector("catalog_name").getObject(row).toString());
                Assertions.assertEquals("test_db", root.getVector("db_schema_name").getObject(row).toString());
                if (tables) {
                    Assertions.assertEquals("visible_table", root.getVector("table_name").getObject(row).toString());
                }
                if (includeSchema) {
                    byte[] bytes = ((VarBinaryVector) root.getVector("table_schema")).get(row);
                    Schema schema = MessageSerializer.deserializeSchema(
                            new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes))));
                    Assertions.assertEquals(1, schema.getFields().size());
                    Assertions.assertEquals("items", schema.getFields().get(0).getName());
                    Assertions.assertEquals(new ArrowType.List(), schema.getFields().get(0).getType());
                    Assertions.assertEquals(new ArrowType.Int(32, true),
                            schema.getFields().get(0).getChildren().get(0).getType());
                }
            }
            Assertions.assertEquals(expectedCatalogs, actualCatalogs);
            FrontendServiceImpl service = services.constructed().get(0);
            Mockito.verify(service, Mockito.times("".equals(catalog) ? 0 : 1))
                    .getDbNames(Mockito.argThat(params -> !params.isIncludeDbIds()));
            Mockito.verify(service, Mockito.times(tables ? expectedCatalogs.size() : 0))
                    .listTableStatus(Mockito.any(TGetTablesParams.class));
            Mockito.verify(service, Mockito.times(includeSchema ? expectedCatalogs.size() : 0))
                    .describeTables(Mockito.any(TDescribeTablesParams.class));
        }
    }
}
