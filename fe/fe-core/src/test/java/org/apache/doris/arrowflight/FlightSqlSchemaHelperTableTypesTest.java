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
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
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

public class FlightSqlSchemaHelperTableTypesTest {
    private static Stream<Arguments> tableFilters() {
        return Stream.of(false, true).flatMap(includeSchema -> Stream.of(
                Arguments.of(includeSchema, Collections.emptyList(), Arrays.asList("base", "system", "view")),
                Arguments.of(includeSchema, Arrays.asList("VIEW", "BASE TABLE"), Arrays.asList("base", "view")),
                Arguments.of(includeSchema, Arrays.asList("BASE TABLE", "VIEW"), Arrays.asList("base", "view")),
                Arguments.of(includeSchema, Arrays.asList("BASE TABLE"), Arrays.asList("base")),
                Arguments.of(includeSchema, Arrays.asList("SYSTEM VIEW"), Arrays.asList("system")),
                Arguments.of(includeSchema, Arrays.asList("VIEW"), Arrays.asList("view")),
                Arguments.of(includeSchema, Arrays.asList("UNKNOWN"), Collections.emptyList()),
                Arguments.of(includeSchema, Arrays.asList("UNKNOWN", "VIEW", "VIEW"), Arrays.asList("view"))));
    }

    @ParameterizedTest
    @MethodSource("tableFilters")
    public void filtersAuthorizedTablesBeforeDescribing(boolean includeSchema, List<String> types,
            List<String> expectedNames) throws Exception {
        checkTables("internal", includeSchema, types, expectedNames);
    }

    @Test
    public void retainsExternalCatalogForFilteredSchemas() throws Exception {
        checkTables("external", true, Arrays.asList("VIEW", "BASE TABLE"), Arrays.asList("base", "view"));
    }

    private void checkTables(String catalog, boolean includeSchema, List<String> types,
            List<String> expectedNames) throws Exception {
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        List<TTableStatus> authorizedTables = Arrays.asList(
                new TTableStatus().setName("base").setType("BASE TABLE"),
                new TTableStatus().setName("system").setType("SYSTEM VIEW"),
                new TTableStatus().setName("view").setType("VIEW"));
        List<String> describedNames = new ArrayList<>();
        try (MockedStatic<ExecuteEnv> executeEnv = Mockito.mockStatic(ExecuteEnv.class);
                MockedConstruction<FrontendServiceImpl> services = Mockito.mockConstruction(
                        FrontendServiceImpl.class, (service, construction) -> {
                            Mockito.when(service.getDbNames(Mockito.any(TGetDbsParams.class))).thenAnswer(invocation -> {
                                TGetDbsParams params = invocation.getArgument(0);
                                Assertions.assertEquals(catalog, params.getCatalog());
                                Assertions.assertEquals("db%", params.getPattern());
                                Assertions.assertEquals(UserIdentity.ROOT.toThrift(), params.getCurrentUserIdent());
                                return new TGetDbsResult().setDbs(Arrays.asList("db"))
                                        .setCatalogs(Arrays.asList(catalog));
                            });
                            Mockito.when(service.listTableStatus(Mockito.any(TGetTablesParams.class)))
                                    .thenAnswer(invocation -> {
                                        TGetTablesParams params = invocation.getArgument(0);
                                        Assertions.assertEquals(catalog, params.getCatalog());
                                        Assertions.assertEquals("db", params.getDb());
                                        Assertions.assertEquals("%", params.getPattern());
                                        Assertions.assertEquals(UserIdentity.ROOT.toThrift(),
                                                params.getCurrentUserIdent());
                                        // The service only recognizes VIEW; other types return all authorized tables.
                                        return new TListTableStatusResult().setTables(authorizedTables.stream()
                                                .filter(table -> !"VIEW".equals(params.getType())
                                                        || "VIEW".equals(table.getType()))
                                                .collect(Collectors.toList()));
                                    });
                            Mockito.when(service.describeTables(Mockito.any(TDescribeTablesParams.class)))
                                    .thenAnswer(invocation -> {
                                        TDescribeTablesParams params = invocation.getArgument(0);
                                        describedNames.addAll(params.getTablesName());
                                        Assertions.assertEquals(catalog, params.getCatalog());
                                        Assertions.assertEquals(UserIdentity.ROOT.toThrift(),
                                                params.getCurrentUserIdent());
                                        List<TColumnDef> columns = new ArrayList<>();
                                        List<Integer> offsets = new ArrayList<>();
                                        for (String name : params.getTablesName()) {
                                            columns.add(new TColumnDef(new TColumnDesc(name + "_id", TPrimitiveType.INT)));
                                            offsets.add(columns.size());
                                        }
                                        return new TDescribeTablesResult().setColumns(columns).setTablesOffset(offsets);
                                    });
                        });
                RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(includeSchema
                        ? Schemas.GET_TABLES_SCHEMA : Schemas.GET_TABLES_SCHEMA_NO_SCHEMA, allocator)) {
            FlightSqlSchemaHelper helper = new FlightSqlSchemaHelper(ctx);
            helper.setParameterForGetTables(CommandGetTables.newBuilder().setCatalog(catalog)
                    .setDbSchemaFilterPattern("db%").setTableNameFilterPattern("%")
                    .addAllTableTypes(types).setIncludeSchema(includeSchema).build());
            root.allocateNew();
            helper.getTables(root);
            List<String> actualNames = new ArrayList<>();
            for (int row = 0; row < root.getRowCount(); row++) {
                actualNames.add(root.getVector("table_name").getObject(row).toString());
                if (includeSchema) {
                    byte[] bytes = ((VarBinaryVector) root.getVector("table_schema")).get(row);
                    Schema schema = MessageSerializer.deserializeSchema(
                            new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes))));
                    Assertions.assertEquals(1, schema.getFields().size());
                    Assertions.assertEquals(actualNames.get(row) + "_id", schema.getFields().get(0).getName());
                    Assertions.assertEquals(new ArrowType.Int(32, true), schema.getFields().get(0).getType());
                }
            }
            Assertions.assertEquals(expectedNames, actualNames);
            Assertions.assertEquals(includeSchema ? expectedNames : Collections.emptyList(), describedNames);
            Mockito.verify(services.constructed().get(0)).listTableStatus(Mockito.any(TGetTablesParams.class));
        }
    }
}
