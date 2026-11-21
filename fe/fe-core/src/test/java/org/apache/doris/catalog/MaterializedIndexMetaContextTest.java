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

package org.apache.doris.catalog;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateMaterializedViewCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TStorageType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

class MaterializedIndexMetaContextTest {
    private static final String SQL = "create materialized view mv as select k from t";

    private ConnectContext previous;
    private boolean wasUnitTest;
    private Env env;
    private MockedStatic<Env> environment;

    @BeforeEach
    void setUp() {
        previous = ConnectContext.get();
        ConnectContext.remove();
        wasUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(env.getInternalCatalog().getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getCatalogMgr().getCatalog(InternalCatalog.INTERNAL_CATALOG_NAME).getDb("db"))
                .thenReturn(Optional.empty());
        environment = Mockito.mockStatic(Env.class);
        environment.when(Env::getCurrentEnv).thenReturn(env);
    }

    @AfterEach
    void tearDown() {
        environment.close();
        FeConstants.runningUnitTest = wasUnitTest;
        if (previous == null) {
            ConnectContext.remove();
        } else {
            previous.setThreadLocalInfo();
        }
    }

    private static MaterializedIndexMeta index(OriginStatement definition, Map<String, String> variables) {
        return new MaterializedIndexMeta(1, List.of(new Column("k", Type.INT)),
                1, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS, definition, List.of(), "db", variables);
    }

    @Test
    void preservesFailureFromContextInitialization() {
        RuntimeException failure = new IllegalStateException("namespace lookup failed");
        Mockito.when(env.getCatalogMgr().getCatalog(InternalCatalog.INTERNAL_CATALOG_NAME)).thenThrow(failure);
        MaterializedIndexMeta index = index(new OriginStatement(SQL, 0), Map.of());
        FeConstants.runningUnitTest = false;
        Assertions.assertSame(failure, Assertions.assertThrows(IOException.class, index::parseStmt).getCause());
        Assertions.assertNull(ConnectContext.get());
        ArgumentCaptor<String> session = ArgumentCaptor.forClass(String.class);
        Mockito.verify(env).registerSessionInfo(session.capture());
        Mockito.verify(env).unregisterSessionInfo(session.getValue());
    }

    @Test
    void indexWithoutDefinitionDoesNotCreateSessionForStoredVariables() throws Exception {
        MaterializedIndexMeta index = index(null, Map.of(SessionVariable.SQL_MODE, "2"));
        try (MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            index.parseStmt();
            Assertions.assertTrue(contexts.constructed().isEmpty());
            Assertions.assertNull(ConnectContext.get());
        }
    }

    @ParameterizedTest
    @CsvSource({"true, false", "true, true", "false, false", "false, true"})
    void isolatesReplayFromClientOrAlterJob(boolean client, boolean fail) throws Exception {
        ConnectContext caller = new ConnectContext();
        if (client) {
            caller.setEnv(env);
            caller.setCurrentUserIdentity(UserIdentity.ROOT);
            caller.restoreNameSpaceContext(new NameSpaceContext("internal", "client_db", 42));
        }
        caller.setThreadLocalInfo();
        SessionVariable variables = caller.getSessionVariable();
        variables.setSqlMode(1);
        StatementContext outer = Mockito.mock(StatementContext.class);
        caller.setStatementContext(outer);
        MysqlStateType state = caller.getState().getStateType();
        RuntimeException failure = new IllegalStateException("validation failed");
        AtomicReference<ConnectContext> replay = new AtomicReference<>();
        AtomicReference<StatementContext> parsed = new AtomicReference<>();
        CreateMaterializedViewCommand command = Mockito.mock(CreateMaterializedViewCommand.class);
        Mockito.doAnswer(call -> {
            ConnectContext validating = call.getArgument(0);
            Assertions.assertSame(replay.get(), validating);
            Assertions.assertSame(validating, ConnectContext.get());
            Assertions.assertSame(parsed.get(), validating.getStatementContext());
            Assertions.assertSame(client ? UserIdentity.ROOT : UserIdentity.ADMIN, validating.getCurrentUserIdentity());
            Assertions.assertEquals(client ? "client_db" : "db", validating.getDatabase());
            Assertions.assertEquals(client ? 1 : 2, validating.getSessionVariable().getSqlMode());
            validating.getSessionVariable().setSqlMode(3);
            if (fail) {
                validating.getState().setError("validation failed");
                throw failure;
            }
            return null;
        }).when(command).validate(Mockito.any(ConnectContext.class));
        try (MockedConstruction<NereidsParser> parsers = Mockito.mockConstruction(NereidsParser.class,
                (parser, construction) -> Mockito.when(parser.parseSingle(SQL)).thenAnswer(call -> {
                    ConnectContext parsing = ConnectContext.get();
                    Assertions.assertNotSame(caller, parsing);
                    replay.set(parsing);
                    parsed.set(parsing.getStatementContext());
                    Assertions.assertNotSame(outer, parsed.get());
                    Assertions.assertSame(parsing, parsed.get().getConnectContext());
                    return command;
                }))) {
            MaterializedIndexMeta index = index(new OriginStatement(SQL, 0),
                    client ? Map.of() : Map.of(SessionVariable.SQL_MODE, "2"));
            if (fail) {
                Assertions.assertSame(failure, Assertions.assertThrows(IOException.class, index::parseStmt).getCause());
            } else {
                index.parseStmt();
            }
            Mockito.verify(command).validate(replay.get());
            Assertions.assertTrue(replay.get().isClosingOrClosed());
            Assertions.assertSame(caller, ConnectContext.get());
            Assertions.assertSame(outer, caller.getStatementContext());
            Assertions.assertSame(variables, caller.getSessionVariable());
            Assertions.assertEquals(1, variables.getSqlMode());
            Assertions.assertEquals(state, caller.getState().getStateType());
            Assertions.assertSame(client ? UserIdentity.ROOT : null, caller.getCurrentUserIdentity());
            Assertions.assertSame(client ? env : null, caller.getEnv());
            Assertions.assertEquals(client ? "client_db" : "", caller.getDatabase());
            Mockito.verify(outer, Mockito.never()).close();
        } finally {
            caller.cleanup();
        }
    }
}
