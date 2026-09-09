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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalMetaCache;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMetadataOps;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.execute.ExecuteAction;
import org.apache.doris.nereids.trees.plans.commands.execute.ExecuteActionFactory;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class ExecuteActionCommandTest {

    @Test
    void executesRollbackWithinCatalogAuthenticationScope() throws Exception {
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
        Map<String, String> properties = Collections.singletonMap("snapshot_id", "123");

        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(tableName.getCtl()).thenReturn("iceberg_catalog");
        Mockito.when(tableName.getDb()).thenReturn("test_db");
        Mockito.when(tableName.getTbl()).thenReturn("test_table");
        Mockito.when(catalogMgr.getCatalog("iceberg_catalog")).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable("test_db")).thenReturn(database);
        Mockito.when(database.getTableNullable("test_table")).thenReturn(table);
        Mockito.when(table.getCatalog()).thenReturn(externalCatalog);
        Mockito.when(table.getDbName()).thenReturn("test_db");
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(externalCatalog.getExecutionAuthenticator()).thenReturn(authenticator);
        Mockito.when(action.isSupported(table)).thenReturn(true);
        Mockito.when(action.execute(table)).thenAnswer(invocation -> {
            actionExecutedInScope.set(authenticator.inScope);
            return null;
        });

        ExecuteActionCommand command = new ExecuteActionCommand(tableName, "rollback_to_snapshot",
                properties, Optional.empty(), Optional.empty());

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<ExecuteActionFactory> factoryMock = Mockito.mockStatic(ExecuteActionFactory.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            factoryMock.when(() -> ExecuteActionFactory.createAction(
                    "rollback_to_snapshot", properties, Optional.empty(), Optional.empty(), table))
                    .thenReturn(action);

            command.run(context, executor);
        }

        Assertions.assertEquals(1, authenticator.executionCount);
        Assertions.assertTrue(actionExecutedInScope.get());
        Assertions.assertFalse(authenticator.inScope);
    }

    @Test
    void retriesRollbackOnCatalogAuthenticationGenerationChange() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf<?> commandCatalog = Mockito.mock(CatalogIf.class);
        DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
        IcebergExternalCatalog externalCatalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        IcebergMetadataOps generationOneOps = Mockito.mock(IcebergMetadataOps.class);
        IcebergMetadataOps generationTwoOps = Mockito.mock(IcebergMetadataOps.class);
        Table generationTwoTable = Mockito.mock(Table.class);
        Snapshot targetSnapshot = Mockito.mock(Snapshot.class);
        Snapshot previousSnapshot = Mockito.mock(Snapshot.class);
        ManageSnapshots manageSnapshots = Mockito.mock(ManageSnapshots.class);
        TableNameInfo tableName = Mockito.mock(TableNameInfo.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        UserIdentity userIdentity = Mockito.mock(UserIdentity.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        ExternalMetaCacheMgr externalMetaCacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        AtomicReference<ExecutionAuthenticator> currentAuthenticator = new AtomicReference<>();
        AtomicReference<IcebergMetadataOps> currentOps = new AtomicReference<>(generationOneOps);
        AtomicReference<ExecutionAuthenticator> activeAuthenticator = new AtomicReference<>();
        RecordingAuthenticator generationTwo = new RecordingAuthenticator(activeAuthenticator, null);
        RecordingAuthenticator generationOne = new RecordingAuthenticator(activeAuthenticator, () -> {
            currentOps.set(generationTwoOps);
            currentAuthenticator.set(generationTwo);
        });
        currentAuthenticator.set(generationOne);
        Map<String, String> properties = Collections.singletonMap("snapshot_id", "123");
        NameMapping mapping = new NameMapping(1L, "test_db", "test_table", "remote_db", "remote_table");
        ExecutorService cacheExecutor = Executors.newSingleThreadExecutor();
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(cacheExecutor) {
            @Override
            protected CatalogIf<?> getCatalog(long catalogId) {
                return externalCatalog;
            }
        };

        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(externalMetaCacheMgr);
        Mockito.when(tableName.getCtl()).thenReturn("iceberg_catalog");
        Mockito.when(tableName.getDb()).thenReturn("test_db");
        Mockito.when(tableName.getTbl()).thenReturn("test_table");
        Mockito.when(catalogMgr.getCatalog("iceberg_catalog")).thenReturn(commandCatalog);
        Mockito.when(commandCatalog.getDbNullable("test_db")).thenReturn(database);
        Mockito.when(database.getTableNullable("test_table")).thenReturn(table);
        Mockito.when(table.getCatalog()).thenReturn(externalCatalog);
        Mockito.when(externalCatalog.getId()).thenReturn(1L);
        Mockito.when(table.getDbName()).thenReturn("test_db");
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(table.getOrBuildNameMapping()).thenReturn(mapping);
        Mockito.when(context.getCurrentUserIdentity()).thenReturn(userIdentity);
        Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.eq(PrivPredicate.ALTER)))
                .thenReturn(true);
        Mockito.when(externalCatalog.getExecutionAuthenticator()).thenAnswer(
                invocation -> currentAuthenticator.get());
        Mockito.when(externalCatalog.getMetadataOps()).thenAnswer(invocation -> currentOps.get());
        Mockito.when(generationOneOps.getExecutionAuthenticator()).thenReturn(generationOne);
        Mockito.when(generationTwoOps.getExecutionAuthenticator()).thenReturn(generationTwo);
        Mockito.when(generationTwoOps.loadTable("remote_db", "remote_table")).thenAnswer(invocation -> {
            Assertions.assertSame(generationTwo, activeAuthenticator.get());
            return generationTwoTable;
        });
        Mockito.when(generationTwoTable.snapshot(123L)).thenReturn(targetSnapshot);
        Mockito.when(generationTwoTable.currentSnapshot()).thenReturn(previousSnapshot);
        Mockito.when(previousSnapshot.snapshotId()).thenReturn(456L);
        Mockito.when(generationTwoTable.manageSnapshots()).thenReturn(manageSnapshots);
        Mockito.when(manageSnapshots.rollbackTo(123L)).thenReturn(manageSnapshots);
        ExecuteActionCommand command = new ExecuteActionCommand(tableName, "rollback_to_snapshot",
                properties, Optional.empty(), Optional.empty());

        try {
            cache.initCatalog(1L, Collections.emptyMap());
            Mockito.when(externalMetaCacheMgr.iceberg(1L)).thenReturn(cache);
            try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
                envMock.when(Env::getCurrentEnv).thenReturn(env);

                command.run(context, executor);
            }

            Assertions.assertEquals(1, generationOne.executionCount);
            Assertions.assertEquals(2, generationTwo.executionCount);
            Mockito.verify(generationOneOps, Mockito.never()).loadTable(Mockito.anyString(), Mockito.anyString());
            Mockito.verify(generationTwoOps).loadTable("remote_db", "remote_table");
            Mockito.verify(manageSnapshots).commit();
        } finally {
            cache.close();
            cacheExecutor.shutdownNow();
        }
    }

    private static class RecordingAuthenticator implements ExecutionAuthenticator {
        private boolean inScope;
        private int executionCount;
        private final AtomicReference<ExecutionAuthenticator> activeAuthenticator;
        private final Runnable beforeTask;

        private RecordingAuthenticator() {
            this(null, null);
        }

        private RecordingAuthenticator(AtomicReference<ExecutionAuthenticator> activeAuthenticator,
                Runnable beforeTask) {
            this.activeAuthenticator = activeAuthenticator;
            this.beforeTask = beforeTask;
        }

        @Override
        public <T> T execute(Callable<T> task) throws Exception {
            executionCount++;
            inScope = true;
            ExecutionAuthenticator previousAuthenticator = null;
            if (activeAuthenticator != null) {
                previousAuthenticator = activeAuthenticator.get();
                activeAuthenticator.set(this);
            }
            try {
                if (beforeTask != null) {
                    beforeTask.run();
                }
                return task.call();
            } finally {
                inScope = false;
                if (activeAuthenticator != null) {
                    activeAuthenticator.set(previousAuthenticator);
                }
            }
        }
    }
}
