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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMetadataOps;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdatePartitionStatistics;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

class IcebergComputePartitionStatsCommandTest {
    private ConnectContext context;
    private StmtExecutor executor;
    private IcebergExternalTable dorisTable;
    private Table table;
    private TableNameInfo name;
    private AccessControllerManager access;
    private EditLog editLog;
    private ExternalMetaCacheMgr cache;
    private IcebergMetadataOps metadataOps;
    private AtomicBoolean authenticated;
    private MockedStatic<Env> mockedEnv;
    private MockedStatic<ConnectContext> mockedContext;
    private MockedStatic<IcebergUtils> mockedUtils;

    @BeforeEach
    void setUp() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        metadataOps = Mockito.mock(IcebergMetadataOps.class);
        ExecutionAuthenticator authenticator = Mockito.mock(ExecutionAuthenticator.class);
        authenticated = new AtomicBoolean();
        Mockito.doAnswer(invocation -> {
            boolean previous = authenticated.getAndSet(true);
            try {
                return ((Callable<?>) invocation.getArgument(0)).call();
            } finally {
                authenticated.set(previous);
            }
        }).when(authenticator).execute(Mockito.<Callable<Object>>any());
        IcebergExternalDatabase database = Mockito.mock(IcebergExternalDatabase.class);
        dorisTable = Mockito.mock(IcebergExternalTable.class);
        table = Mockito.mock(Table.class, Mockito.withSettings().extraInterfaces(HasTableOperations.class));
        Mockito.when(table.name()).thenReturn("test_table");
        Mockito.when(table.io()).thenReturn(Mockito.mock(FileIO.class));
        Mockito.when(((HasTableOperations) table).operations()).thenReturn(Mockito.mock(TableOperations.class));
        context = Mockito.mock(ConnectContext.class);
        executor = Mockito.mock(StmtExecutor.class);
        access = Mockito.mock(AccessControllerManager.class);
        editLog = Mockito.mock(EditLog.class);
        cache = Mockito.mock(ExternalMetaCacheMgr.class);
        name = Mockito.mock(TableNameInfo.class);
        Mockito.when(name.getCtl()).thenReturn("ctl");
        Mockito.when(name.getDb()).thenReturn("db");
        Mockito.when(name.getTbl()).thenReturn("tbl");
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogs);
        Mockito.when(catalogs.getCatalog("ctl")).thenReturn(catalog);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(catalog.getMetadataOps()).thenReturn(metadataOps);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(authenticator);
        Mockito.when(metadataOps.getExecutionAuthenticator()).thenReturn(authenticator);
        Mockito.doReturn(database).when(catalog).getDbNullable("db");
        Mockito.when(database.getTableNullable("tbl")).thenReturn(dorisTable);
        Mockito.when(dorisTable.getCatalog()).thenReturn(catalog);
        Mockito.when(dorisTable.getDbName()).thenReturn("db");
        Mockito.when(dorisTable.getName()).thenReturn("tbl");
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cache);
        Mockito.when(context.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        Mockito.when(context.getState()).thenReturn(new QueryState());
        Mockito.when(context.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(context.getRemoteIP()).thenReturn("127.0.0.1");
        Mockito.when(access.checkTblPriv(context, "ctl", "db", "tbl", PrivPredicate.ALTER)).thenReturn(true);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedContext = Mockito.mockStatic(ConnectContext.class);
        mockedContext.when(ConnectContext::get).thenReturn(context);
        mockedUtils = Mockito.mockStatic(IcebergUtils.class);
        mockedUtils.when(() -> IcebergUtils.getWritableIcebergTable(dorisTable, metadataOps)).thenAnswer(invocation -> {
            Assertions.assertTrue(authenticated.get());
            return table;
        });
    }

    @AfterEach
    void tearDown() {
        mockedUtils.close();
        mockedContext.close();
        mockedEnv.close();
    }

    @Test
    void testParserUsesExistingExecuteGrammar() {
        ExecuteActionCommand parsed = (ExecuteActionCommand) new NereidsParser()
                .parseSingle("ALTER TABLE ctl.db.tbl EXECUTE compute_partition_stats()");
        Assertions.assertEquals("compute_partition_stats", parsed.getActionName());
        Assertions.assertTrue(parsed.getProperties().isEmpty());
    }

    @Test
    void testEmptyResultIsSentAfterRefreshLog() throws Exception {
        command(Collections.emptyMap()).run(context, executor);
        ArgumentCaptor<ResultSet> result = ArgumentCaptor.forClass(ResultSet.class);
        ArgumentCaptor<ExternalObjectLog> log = ArgumentCaptor.forClass(ExternalObjectLog.class);
        InOrder order = Mockito.inOrder(editLog, executor);
        order.verify(editLog).logRefreshExternalTable(log.capture());
        order.verify(executor).sendResultSet(result.capture());
        Assertions.assertEquals("db", log.getValue().getDbName());
        Assertions.assertEquals("tbl", log.getValue().getTableName());
        Assertions.assertEquals(1, result.getValue().getMetaData().getColumnCount());
        Assertions.assertTrue(result.getValue().getResultRows().isEmpty());
        Mockito.verifyNoInteractions(cache);
    }

    @Test
    void testAlterPermissionIsRequiredBeforeLoadingTable() {
        Mockito.when(access.checkTblPriv(context, "ctl", "db", "tbl", PrivPredicate.ALTER)).thenReturn(false);
        Assertions.assertThrows(UserException.class, () -> command(Collections.emptyMap()).run(context, executor));
        mockedUtils.verifyNoInteractions();
        Mockito.verifyNoInteractions(editLog, executor);
    }

    @Test
    void testValidationFailureDoesNotSendSuccessOrRefresh() {
        Assertions.assertThrows(UserException.class,
                () -> command(Collections.singletonMap("unknown", "1")).run(context, executor));
        Assertions.assertThrows(UserException.class,
                () -> command(Collections.singletonMap("snapshot_id", "123")).run(context, executor));
        Mockito.verifyNoInteractions(editLog, executor, cache);
    }

    @Test
    void testCommitLocalInvalidationAndRefreshLogOrder() throws Exception {
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.snapshotId()).thenReturn(42L);
        Mockito.when(table.currentSnapshot()).thenReturn(snapshot);
        PartitionStatisticsFile file = Mockito.mock(PartitionStatisticsFile.class);
        Mockito.when(file.path()).thenReturn("metadata/partition-stats-42.parquet");
        UpdatePartitionStatistics update = Mockito.mock(UpdatePartitionStatistics.class);
        Mockito.when(table.updatePartitionStatistics()).thenReturn(update);
        Mockito.when(update.setPartitionStatistics(file)).thenReturn(update);
        try (MockedStatic<PartitionStatsHandler> handler = Mockito.mockStatic(PartitionStatsHandler.class)) {
            handler.when(() -> PartitionStatsHandler.computeAndWriteStatsFile(
                    Mockito.any(Table.class), Mockito.eq(42L))).thenAnswer(invocation -> {
                        Assertions.assertTrue(authenticated.get());
                        return file;
                    });
            Mockito.doAnswer(invocation -> {
                Assertions.assertTrue(authenticated.get());
                return null;
            }).when(update).commit();
            command(Collections.emptyMap()).run(context, executor);
            InOrder order = Mockito.inOrder(update, cache, editLog, executor);
            order.verify(update).commit();
            order.verify(cache).invalidateTableCache(dorisTable);
            order.verify(editLog).logRefreshExternalTable(Mockito.any(ExternalObjectLog.class));
            order.verify(executor).sendResultSet(Mockito.any(ResultSet.class));
        }
    }

    @Test
    void testUnknownCommitStateDoesNotRetryOrSendSuccess() throws Exception {
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.snapshotId()).thenReturn(42L);
        Mockito.when(table.currentSnapshot()).thenReturn(snapshot);
        PartitionStatisticsFile file = Mockito.mock(PartitionStatisticsFile.class);
        UpdatePartitionStatistics update = Mockito.mock(UpdatePartitionStatistics.class);
        Mockito.when(table.updatePartitionStatistics()).thenReturn(update);
        Mockito.when(update.setPartitionStatistics(file)).thenReturn(update);
        CommitStateUnknownException failure = new CommitStateUnknownException(new IOException("lost commit response"));
        Mockito.doThrow(failure).when(update).commit();
        try (MockedStatic<PartitionStatsHandler> handler = Mockito.mockStatic(PartitionStatsHandler.class)) {
            handler.when(() -> PartitionStatsHandler.computeAndWriteStatsFile(
                    Mockito.any(Table.class), Mockito.eq(42L))).thenReturn(file);
            UserException error = Assertions.assertThrows(UserException.class,
                    () -> command(Collections.emptyMap()).run(context, executor));
            Assertions.assertTrue(error.getCause() instanceof UserException);
            Assertions.assertSame(failure, error.getCause().getCause());
            handler.verify(() -> PartitionStatsHandler.computeAndWriteStatsFile(
                    Mockito.any(Table.class), Mockito.eq(42L)), Mockito.times(1));
            Mockito.verify(update, Mockito.times(1)).commit();
            mockedUtils.verify(() -> IcebergUtils.getWritableIcebergTable(dorisTable, metadataOps), Mockito.times(1));
            Mockito.verifyNoInteractions(cache, editLog, executor);
        }
    }

    private ExecuteActionCommand command(Map<String, String> properties) {
        return new ExecuteActionCommand(name, "compute_partition_stats", properties, Optional.empty(), Optional.empty());
    }
}
