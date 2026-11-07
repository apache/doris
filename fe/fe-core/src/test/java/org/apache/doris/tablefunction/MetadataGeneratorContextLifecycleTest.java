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
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.View;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TSchemaTableRequestParams;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class MetadataGeneratorContextLifecycleTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closesEachViewContextIncludingDanglingViews(boolean firstViewFails) throws Exception {
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
        List<StatementContext> statements = new ArrayList<>();
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
                            if (!contexts.isEmpty()) {
                                Assertions.assertTrue(contexts.get(0).isClosingOrClosed());
                            }
                            ConnectContext context = call.getArgument(1);
                            StatementContext statement = Mockito.mock(StatementContext.class);
                            context.setStatementContext(statement);
                            contexts.add(context);
                            statements.add(statement);
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
                Assertions.assertNotSame(contexts.get(0), contexts.get(1));
                for (int i = 0; i < contexts.size(); i++) {
                    Assertions.assertTrue(contexts.get(i).isClosingOrClosed());
                    Mockito.verify(statements.get(i)).close();
                }
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

    private static View view(String name) {
        View view = Mockito.mock(View.class);
        Mockito.when(view.getName()).thenReturn(name);
        Mockito.when(view.getType()).thenReturn(TableType.VIEW);
        Mockito.when(view.getInlineViewDef()).thenReturn(name);
        return view;
    }
}
