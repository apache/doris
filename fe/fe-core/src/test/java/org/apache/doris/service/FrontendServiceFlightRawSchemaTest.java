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

package org.apache.doris.service;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.arrowflight.FlightSqlSchemaHelper;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;

import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Set;

public class FrontendServiceFlightRawSchemaTest {
    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    public void dottedExternalSchemaRemainsRaw(boolean showFullName, boolean includeSchema) throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        TableIf table = Mockito.mock(TableIf.class);
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setAuthenticatedRoles(Set.of("flight_role"));
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogs);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(catalogs.getCatalog("external")).thenReturn(catalog);
        Mockito.when(catalogs.getCatalogOrException(Mockito.eq("external"), Mockito.any())).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("external");
        Mockito.when(catalog.getDbNames()).thenReturn(List.of("a.b"));
        Mockito.when(catalog.getDbNullable("a.b")).thenReturn(database);
        Mockito.when(database.getTablesIgnoreException()).thenReturn(List.of(table));
        Mockito.when(database.getTableNullableIfException("base_table")).thenReturn(table);
        Mockito.when(table.getName()).thenReturn("base_table");
        Mockito.when(table.getMysqlType()).thenReturn("BASE TABLE");
        Mockito.when(table.getBaseSchemaForDisplayOrEmpty()).thenReturn(List.of(new Column("id", Type.INT)));
        Mockito.when(access.checkDbPriv(Mockito.any(UserIdentity.class), Mockito.eq("external"), Mockito.eq("a.b"),
                Mockito.eq(PrivPredicate.SHOW))).thenReturn(true);
        Mockito.when(access.checkTblPriv(Mockito.any(UserIdentity.class), Mockito.eq("external"), Mockito.eq("a.b"),
                Mockito.eq("base_table"), Mockito.eq(PrivPredicate.SHOW))).thenAnswer(invocation -> {
                    UserIdentity user = invocation.getArgument(0);
                    Assertions.assertEquals(UserIdentity.ROOT, user);
                    return true;
                });
        boolean original = GlobalVariable.showFullDbNameInInfoSchemaDb;
        GlobalVariable.showFullDbNameInInfoSchemaDb = showFullName;
        try (MockedStatic<Env> environments = Mockito.mockStatic(Env.class);
                MockedStatic<ExecuteEnv> executeEnv = Mockito.mockStatic(ExecuteEnv.class);
                MockedConstruction<FrontendServiceImpl> services = Mockito.mockConstruction(FrontendServiceImpl.class,
                        Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));
                RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(includeSchema
                        ? Schemas.GET_TABLES_SCHEMA : Schemas.GET_TABLES_SCHEMA_NO_SCHEMA, allocator)) {
            environments.when(Env::getCurrentEnv).thenReturn(env);
            FlightSqlSchemaHelper helper = new FlightSqlSchemaHelper(ctx);
            helper.setParameterForGetTables(CommandGetTables.newBuilder().setCatalog("external")
                    .setDbSchemaFilterPattern("a.b").setIncludeSchema(includeSchema).build());
            root.allocateNew();
            helper.getTables(root);
            Assertions.assertEquals(1, root.getRowCount());
            Assertions.assertEquals("a.b", root.getVector("db_schema_name").getObject(0).toString());
            Assertions.assertEquals("base_table", root.getVector("table_name").getObject(0).toString());
            if (includeSchema) {
                byte[] bytes = ((VarBinaryVector) root.getVector("table_schema")).get(0);
                Assertions.assertEquals("id", MessageSerializer.deserializeSchema(new ReadChannel(
                        Channels.newChannel(new ByteArrayInputStream(bytes)))).getFields().get(0).getName());
            }
            Mockito.verify(catalog, Mockito.never()).getDbNullable("b");
        } finally {
            GlobalVariable.showFullDbNameInInfoSchemaDb = original;
        }
    }
}
