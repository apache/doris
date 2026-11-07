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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.View;
import org.apache.doris.common.FeConstants;
import org.apache.doris.connector.ConnectorStatementScopeImpl;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TSchemaTableRequestParams;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

class MetadataGeneratorContextLifecycleTest {
    @Test
    void materializedViewMetadataDoesNotCreateSession() throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Database database = Mockito.mock(Database.class);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(catalog.getAllDbs()).thenReturn(List.of(database));
        MTMV view = Mockito.mock(MTMV.class);
        Mockito.when(view.getName()).thenReturn("mv");
        Mockito.when(view.getType()).thenReturn(TableType.MATERIALIZED_VIEW);
        Mockito.when(view.getRelation()).thenReturn(new MTMVRelation(
                Set.of(), Set.of(), Set.of(), Set.of(), Set.of()));
        Mockito.when(database.getTables()).thenReturn(List.of(view));
        try (MockedStatic<Env> environment = Mockito.mockStatic(Env.class);
                MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            environment.when(Env::getCurrentEnv).thenReturn(env);
            Method method = MetadataGenerator.class.getDeclaredMethod(
                    "viewDependencyMetadataResult", TSchemaTableRequestParams.class);
            method.setAccessible(true);
            TFetchSchemaTableDataResult result = (TFetchSchemaTableDataResult) method.invoke(null,
                    new TSchemaTableRequestParams().setCurrentUserIdent(UserIdentity.ROOT.toThrift()));
            Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
            Assertions.assertEquals(0, result.getDataBatchSize());
            Assertions.assertTrue(contexts.constructed().isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void reusesPrivateContextAcrossViewsIncludingDanglingViews(boolean firstViewFails) throws Exception {
        boolean savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Database database = Mockito.mock(Database.class);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(catalog.getAllDbs()).thenReturn(Collections.singletonList(database));
        View first = view("first");
        View second = view("second");
        Mockito.when(database.getTables()).thenReturn(Arrays.asList(first, second));
        Table base = Mockito.mock(Table.class);
        Mockito.when(base.getType()).thenReturn(TableType.OLAP);
        List<ConnectContext> contexts = new ArrayList<>();
        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<PlanUtils> plans = Mockito.mockStatic(PlanUtils.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            ConnectContext caller = new ConnectContext();
            caller.setThreadLocalInfo();
            try {
                ConnectContext borrowed = caller;
                plans.when(() -> PlanUtils.tableCollect(Mockito.anyString(), Mockito.any()))
                        .thenAnswer(call -> {
                            Assertions.assertSame(borrowed, ConnectContext.get());
                            ConnectContext context = call.getArgument(1);
                            Assertions.assertNotSame(borrowed, context);
                            Assertions.assertFalse(context.isClosingOrClosed());
                            Assertions.assertNull(context.getStatementContext());
                            contexts.add(context);
                            if (firstViewFails && contexts.size() == 1) {
                                throw new IllegalStateException("missing base table");
                            }
                            return Collections.singletonMap(Arrays.asList("internal", "db", "base"), base);
                        });
                Method method = MetadataGenerator.class.getDeclaredMethod(
                        "viewDependencyMetadataResult", TSchemaTableRequestParams.class);
                method.setAccessible(true);
                TFetchSchemaTableDataResult result = (TFetchSchemaTableDataResult) method.invoke(null,
                        new TSchemaTableRequestParams().setCurrentUserIdent(UserIdentity.ROOT.toThrift()));
                Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
                Assertions.assertEquals(firstViewFails ? 1 : 2, result.getDataBatchSize());
                Assertions.assertEquals(2, contexts.size());
                Assertions.assertSame(contexts.get(0), contexts.get(1));
                Assertions.assertTrue(contexts.get(0).isClosingOrClosed());
                Assertions.assertSame(caller, ConnectContext.get());
                Assertions.assertFalse(caller.isClosingOrClosed());
            } finally {
                caller.cleanup();
            }
        } finally {
            ConnectContext.remove();
            FeConstants.runningUnitTest = savedRunningUnitTest;
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void tableCollectionClosesItsStatementWithoutReplacingBorrowedState(boolean danglingView) {
        ConnectContext caller = new ConnectContext();
        StatementContext original = new StatementContext(caller, new OriginStatement("outer query", 0));
        caller.setStatementContext(original);
        caller.setThreadLocalInfo();
        AtomicReference<ConnectorStatementScopeImpl> statementScope = new AtomicReference<>();
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        try (MockedStatic<Env> environment = Mockito.mockStatic(Env.class);
                MockedStatic<CascadesContext> contexts = Mockito.mockStatic(
                        CascadesContext.class, Mockito.CALLS_REAL_METHODS)) {
            environment.when(Env::getCurrentEnv).thenReturn(env);
            caller.setEnv(env);
            contexts.when(() -> CascadesContext.initContext(Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenAnswer(call -> {
                        StatementContext statement = call.getArgument(0);
                        statementScope.set((ConnectorStatementScopeImpl)
                                statement.getOrCreateConnectorStatementScope());
                        return call.callRealMethod();
                    });
            if (danglingView) {
                Assertions.assertThrows(RuntimeException.class,
                        () -> PlanUtils.tableCollect("select * from internal.missing_db.missing_table", caller));
            } else {
                Assertions.assertTrue(PlanUtils.tableCollect("select 1", caller).isEmpty());
            }
            Assertions.assertTrue(statementScope.get().isClosed());
            Assertions.assertSame(original, caller.getStatementContext());
            Assertions.assertSame(caller, ConnectContext.get());
            Assertions.assertFalse(caller.isClosingOrClosed());
        } finally {
            caller.cleanup();
        }
    }

    private static View view(String name) {
        View view = Mockito.mock(View.class);
        Mockito.when(view.getName()).thenReturn(name);
        Mockito.when(view.getType()).thenReturn(TableType.VIEW);
        Mockito.when(view.getInlineViewDef()).thenReturn(name);
        return view;
    }
}
