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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

class CreateTableCommandTest {

    @Test
    void temporaryCtasChecksSessionScopedTableName() {
        CreateTableInfo createTableInfo = Mockito.mock(CreateTableInfo.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
        List<String> tableNameParts = ImmutableList.of("catalog", "database", "target");
        List<String> qualifiedName = ImmutableList.of("catalog", "database", "target");
        String innerTableName = "session-id_#TEMP#_target";

        Mockito.when(createTableInfo.getTableNameParts()).thenReturn(tableNameParts);
        Mockito.when(createTableInfo.isTemp()).thenReturn(true);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog("catalog")).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable("database")).thenReturn(database);
        Mockito.when(database.isTableExist(innerTableName)).thenReturn(false);
        CreateTableCommand command = new CreateTableCommand(Optional.empty(), createTableInfo);

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<RelationUtil> relationMock = Mockito.mockStatic(RelationUtil.class);
                MockedStatic<Util> utilMock = Mockito.mockStatic(Util.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            relationMock.when(() -> RelationUtil.getQualifierName(context, tableNameParts))
                    .thenReturn(qualifiedName);
            utilMock.when(() -> Util.generateTempTableInnerName("target")).thenReturn(innerTableName);

            org.junit.jupiter.api.Assertions.assertFalse(command.targetTableExists(context));

            Mockito.verify(database).isTableExist(innerTableName);
            Mockito.verify(database, Mockito.never()).isTableExist("target");
        }
    }

    @Test
    void ctasIfNotExistsReturnsBeforeUnsupportedSinkValidation() throws Exception {
        LogicalPlan query = Mockito.mock(LogicalPlan.class);
        CreateTableInfo createTableInfo = Mockito.mock(CreateTableInfo.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Env env = Mockito.mock(Env.class);
        List<String> tableNameParts = ImmutableList.of("catalog", "database", "target");

        Mockito.when(createTableInfo.getTableNameParts()).thenReturn(tableNameParts);
        Mockito.when(createTableInfo.isIfNotExists()).thenReturn(true);
        Mockito.when(env.createTable(createTableInfo)).thenReturn(true);
        CreateTableCommand command = Mockito.spy(
                new CreateTableCommand(Optional.of(query), createTableInfo));
        Mockito.doNothing().when(command).validateCreateTableAsSelect(context, query);

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<UnboundTableSinkCreator> sinkMock =
                        Mockito.mockStatic(UnboundTableSinkCreator.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);

            org.junit.jupiter.api.Assertions.assertDoesNotThrow(
                    () -> command.run(context, executor));

            sinkMock.verifyNoInteractions();
            Mockito.verify(env).createTable(createTableInfo);
        }
    }

    @Test
    void ctasValidatesSinkBeforePublishingMetadata() throws Exception {
        LogicalPlan query = Mockito.mock(LogicalPlan.class);
        CreateTableInfo createTableInfo = Mockito.mock(CreateTableInfo.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Env env = Mockito.mock(Env.class);
        List<String> tableNameParts = ImmutableList.of("catalog", "database", "target");

        Mockito.when(createTableInfo.getTableNameParts()).thenReturn(tableNameParts);
        CreateTableCommand command = Mockito.spy(
                new CreateTableCommand(Optional.of(query), createTableInfo));
        Mockito.doNothing().when(command).validateCreateTableAsSelect(context, query);
        Mockito.doReturn(false).when(command).targetTableExists(context);

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<UnboundTableSinkCreator> sinkMock =
                        Mockito.mockStatic(UnboundTableSinkCreator.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            sinkMock.when(() -> UnboundTableSinkCreator.createUnboundTableSink(
                    tableNameParts, ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), query))
                    .thenThrow(new UserException("unsupported sink"));

            org.junit.jupiter.api.Assertions.assertThrows(
                    Exception.class, () -> command.run(context, executor));

            Mockito.verify(env, Mockito.never()).createTable(createTableInfo);
            Mockito.verify(env, Mockito.never()).dropTable(
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                    Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                    Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyBoolean());
        }
    }

    @Test
    void existingCtasTargetPrecedesUnsupportedSinkValidation() throws Exception {
        LogicalPlan query = Mockito.mock(LogicalPlan.class);
        CreateTableInfo createTableInfo = Mockito.mock(CreateTableInfo.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Env env = Mockito.mock(Env.class);
        List<String> tableNameParts = ImmutableList.of("catalog", "database", "target");

        Mockito.when(createTableInfo.getTableNameParts()).thenReturn(tableNameParts);
        Mockito.when(createTableInfo.getTableName()).thenReturn("target");
        CreateTableCommand command = Mockito.spy(
                new CreateTableCommand(Optional.of(query), createTableInfo));
        Mockito.doNothing().when(command).validateCreateTableAsSelect(context, query);
        Mockito.doReturn(true).when(command).targetTableExists(context);

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<UnboundTableSinkCreator> sinkMock =
                        Mockito.mockStatic(UnboundTableSinkCreator.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            Exception exception = org.junit.jupiter.api.Assertions.assertThrows(
                    Exception.class, () -> command.run(context, executor));

            org.junit.jupiter.api.Assertions.assertTrue(exception.getMessage().contains("already exists"));
            sinkMock.verifyNoInteractions();
            Mockito.verify(env, Mockito.never()).createTable(createTableInfo);
            Mockito.verify(env, Mockito.never()).dropTable(
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                    Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                    Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyBoolean());
        }
    }
}
