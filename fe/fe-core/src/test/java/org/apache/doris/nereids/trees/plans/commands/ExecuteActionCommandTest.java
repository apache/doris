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
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.execute.ExecuteAction;
import org.apache.doris.nereids.trees.plans.commands.execute.ExecuteActionFactory;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

class ExecuteActionCommandTest {

    @Test
    void executesActionWithinCatalogAuthenticationScope() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
        IcebergExternalCatalog externalCatalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        ExecuteAction action = Mockito.mock(ExecuteAction.class);
        TableNameInfo tableName = Mockito.mock(TableNameInfo.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        RecordingAuthenticator authenticator = new RecordingAuthenticator();
        AtomicBoolean actionExecutedInScope = new AtomicBoolean();

        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(tableName.getCtl()).thenReturn("iceberg");
        Mockito.when(tableName.getDb()).thenReturn("db");
        Mockito.when(tableName.getTbl()).thenReturn("tbl");
        Mockito.when(catalogMgr.getCatalog("iceberg")).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(database);
        Mockito.when(database.getTableNullable("tbl")).thenReturn(table);
        Mockito.when(table.getCatalog()).thenReturn(externalCatalog);
        Mockito.when(table.getDbName()).thenReturn("db");
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(externalCatalog.getExecutionAuthenticator()).thenReturn(authenticator);
        Mockito.when(action.isSupported(table)).thenReturn(true);
        Mockito.when(action.execute(table)).thenAnswer(invocation -> {
            actionExecutedInScope.set(authenticator.inScope);
            return null;
        });

        ExecuteActionCommand command = new ExecuteActionCommand(tableName, "expire_snapshots",
                Collections.emptyMap(), Optional.empty(), Optional.empty());

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<ExecuteActionFactory> factoryMock = Mockito.mockStatic(ExecuteActionFactory.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            factoryMock.when(() -> ExecuteActionFactory.createAction(
                    "expire_snapshots", Collections.emptyMap(), Optional.empty(), Optional.empty(), table))
                    .thenReturn(action);

            command.run(context, executor);
        }

        Assertions.assertEquals(1, authenticator.executionCount);
        Assertions.assertTrue(actionExecutedInScope.get());
    }

    private static class RecordingAuthenticator implements ExecutionAuthenticator {
        private boolean inScope;
        private int executionCount;

        @Override
        public <T> T execute(Callable<T> task) throws Exception {
            executionCount++;
            inScope = true;
            try {
                return task.call();
            } finally {
                inScope = false;
            }
        }
    }
}
