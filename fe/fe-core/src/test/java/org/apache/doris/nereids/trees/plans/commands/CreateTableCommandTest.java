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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class CreateTableCommandTest {

    @Test
    void ctasInheritsOnlyForResolvedInternalOlapTarget() throws Exception {
        for (boolean external : new boolean[] {false, true}) {
            LogicalPlan query = new NereidsParser().parseSingle("SELECT 1 AS k");
            CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
            ConnectContext context = Mockito.mock(ConnectContext.class);
            StatementContext statement = Mockito.mock(StatementContext.class);
            OlapTable source = Mockito.mock(OlapTable.class);
            Mockito.when(source.getVariantEscapeDotsInKeys()).thenReturn(true);
            Mockito.when(statement.getTables()).thenReturn(
                    Map.of(ImmutableList.of("internal", "database", "source"), source));
            Mockito.when(context.getStatementContext()).thenReturn(statement);
            Mockito.when(context.getSessionVariable()).thenReturn(new SessionVariable());
            Mockito.when(info.getTableNameParts()).thenReturn(ImmutableList.of("catalog", "database", "target"));
            Mockito.when(info.getProperties()).thenReturn(new HashMap<>());
            Mockito.when(info.getCtasColumns()).thenReturn(null);
            // These values are resolved by CreateTableInfo.validateCreateTableAsSelect before inheritance.
            Mockito.when(info.isExternal()).thenReturn(external);
            Mockito.when(info.getEngineName()).thenReturn(external ? null : CreateTableInfo.ENGINE_OLAP);
            Plan plan = Mockito.mock(Plan.class);
            Mockito.when(plan.getOutput()).thenReturn(ImmutableList.of(new SlotReference("k", IntegerType.INSTANCE)));
            try (MockedConstruction<NereidsPlanner> planners = Mockito.mockConstruction(NereidsPlanner.class,
                    (planner, mockContext) -> Mockito.when(planner.planWithLock(
                            Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(plan))) {
                new CreateTableCommand(Optional.of(query), info).validateCreateTableAsSelect(context, query);
                if (external) {
                    Mockito.verify(info, Mockito.never()).setProperties(Mockito.anyMap());
                    Assertions.assertFalse(info.getProperties().containsKey(
                            PropertyAnalyzer.PROPERTIES_VARIANT_ESCAPE_DOTS_IN_KEYS));
                } else {
                    Mockito.verify(info).setProperties(Map.of(
                            PropertyAnalyzer.PROPERTIES_VARIANT_ESCAPE_DOTS_IN_KEYS, "true"));
                }
                Mockito.verify(info).validateCreateTableAsSelect(Mockito.anyList(), Mockito.anyList(),
                        Mockito.same(context));
            }
        }
    }

    @Test
    void externalTargetStillRejectsExplicitEscapeProperty() {
        CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class,
                Mockito.CALLS_REAL_METHODS);
        for (String value : new String[] {"true", "false"}) {
            Mockito.when(info.getProperties()).thenReturn(Map.of(
                    PropertyAnalyzer.PROPERTIES_VARIANT_ESCAPE_DOTS_IN_KEYS, value));
            DdlException error = Assertions.assertThrows(DdlException.class,
                    () -> catalog.validateCreateTableProperties(info));
            Assertions.assertTrue(error.getMessage().contains("variant_escape_dots_in_keys"), error.getMessage());
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
            Mockito.verify(command, Mockito.never()).validateCreateTableProperties(context);
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
        Mockito.doNothing().when(command).validateCreateTableProperties(context);
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
        Mockito.doNothing().when(command).validateCreateTableProperties(context);
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

    @Test
    void ctasConfigurationPreflightPrecedesTargetLookup() throws Exception {
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
        Mockito.doThrow(new UserException("unsupported catalog configuration"))
                .when(command).validateCreateTableProperties(context);

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<UnboundTableSinkCreator> sinkMock =
                        Mockito.mockStatic(UnboundTableSinkCreator.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            Exception exception = org.junit.jupiter.api.Assertions.assertThrows(
                    Exception.class, () -> command.run(context, executor));

            org.junit.jupiter.api.Assertions.assertTrue(
                    exception.getMessage().contains("unsupported catalog configuration"));
            Mockito.verify(command, Mockito.never()).targetTableExists(context);
            sinkMock.verifyNoInteractions();
            Mockito.verify(env, Mockito.never()).createTable(createTableInfo);
            Mockito.verify(env, Mockito.never()).dropTable(
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                    Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                    Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyBoolean());
        }
    }
}
