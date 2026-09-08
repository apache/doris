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
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.trees.plans.commands.execute.ExecuteAction;
import org.apache.doris.nereids.trees.plans.commands.execute.ExecuteActionFactory;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

public class ExecuteActionCommandTest {

    @Test
    public void testCommandDoesNotRepeatActionOwnedMutationFence() throws Exception {
        Fixture fixture = new Fixture();

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ExecuteActionFactory> factoryStatic = Mockito.mockStatic(ExecuteActionFactory.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(fixture.env);
            factoryStatic.when(() -> ExecuteActionFactory.createAction(
                            Mockito.anyString(), Mockito.anyMap(), Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenReturn(fixture.action);

            fixture.command.run(fixture.context, fixture.executor);

            Mockito.verify(fixture.action).execute(fixture.table);
            Mockito.verifyNoInteractions(fixture.refreshManager, fixture.executor);
        }
    }

    @Test
    public void testFailedMutationSkipsPostCommitRefresh() throws Exception {
        Fixture fixture = new Fixture();
        Mockito.when(fixture.action.execute(fixture.table)).thenThrow(new UserException("remote failure"));

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ExecuteActionFactory> factoryStatic = Mockito.mockStatic(ExecuteActionFactory.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(fixture.env);
            factoryStatic.when(() -> ExecuteActionFactory.createAction(
                            Mockito.anyString(), Mockito.anyMap(), Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenReturn(fixture.action);

            DdlException thrown = Assertions.assertThrows(
                    DdlException.class, () -> fixture.command.run(fixture.context, fixture.executor));
            Assertions.assertTrue(thrown.getMessage().contains("remote failure"));
            Mockito.verifyNoInteractions(fixture.refreshManager, fixture.executor);
        }
    }

    private static final class Fixture {
        private final Env env = Mockito.mock(Env.class);
        private final CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        private final RefreshManager refreshManager = Mockito.mock(RefreshManager.class);
        private final ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        private final ExternalDatabase database = Mockito.mock(ExternalDatabase.class);
        private final ExternalTable table = Mockito.mock(ExternalTable.class);
        private final ExecuteAction action = Mockito.mock(ExecuteAction.class);
        private final ConnectContext context = Mockito.mock(ConnectContext.class);
        private final StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        private final ExecuteActionCommand command = new ExecuteActionCommand(
                new TableNameInfo("ctl", "db", "tbl"), "action", Collections.emptyMap(),
                Optional.empty(), Optional.empty());

        private Fixture() throws UserException {
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);
            Mockito.when(catalogMgr.getCatalog("ctl")).thenReturn(catalog);
            Mockito.when(catalog.getDbNullable("db")).thenReturn(database);
            Mockito.when(database.getTableNullable("tbl")).thenReturn(table);
            Mockito.when(action.isSupported(table)).thenReturn(true);
            Mockito.when(action.execute(table)).thenReturn(null);
        }
    }
}
