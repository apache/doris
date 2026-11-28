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

package org.apache.doris.load;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.load.NereidsStreamLoadPlanner;
import org.apache.doris.nereids.load.NereidsStreamLoadTask;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterOpExecutor;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TGroupCommitInfo;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TOlapTableIndexTablets;
import org.apache.doris.thrift.TOlapTableLocationParam;
import org.apache.doris.thrift.TOlapTablePartition;
import org.apache.doris.thrift.TOlapTablePartitionParam;
import org.apache.doris.thrift.TOlapTableSink;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;
import org.apache.doris.thrift.TTabletLocation;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.function.FailableRunnable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamLoadHandlerTest {
    @Test
    public void testJournalSyncChecksTableOnlyWhenRequested() throws Exception {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.checkFeExist("follower", 9010)).thenReturn(Mockito.mock(Frontend.class));
        Mockito.when(env.getMaxJournalId()).thenReturn(200L);
        TGroupCommitInfo info = new TGroupCommitInfo().setTableName("tbl");
        TMasterOpRequest request = new TMasterOpRequest().setClientNodeHost("follower").setClientNodePort(9010)
                .setDb("db").setSyncJournalOnly(true).setGroupCommitInfo(info);
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class, Mockito.CALLS_REAL_METHODS);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals(200L, service.forward(request).getMaxJournalId());
            info.setCheckTableBlocked(false);
            Assertions.assertEquals(200L, service.forward(request).getMaxJournalId());
            info.setCheckTableBlocked(true);
            info.unsetTableName();
            TMasterOpResult missingName = service.forward(request);
            Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR.getValue(), missingName.getStatusCode());
            Assertions.assertEquals(200L, missingName.getMaxJournalId());
            Mockito.verify(env, Mockito.never()).getInternalCatalog();
        }
    }

    @Test
    public void testMasterChecksTableBeforeReturningGlobalJournalId() throws Exception {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.checkFeExist("follower", 9010)).thenReturn(Mockito.mock(Frontend.class));
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(db);
        Mockito.when(db.getTableOrMetaException("tbl", TableType.OLAP)).thenReturn(table);
        Mockito.when(table.getId()).thenReturn(42L);
        GroupCommitManager manager = Mockito.mock(GroupCommitManager.class);
        Mockito.when(env.getGroupCommitManager()).thenReturn(manager);
        Mockito.when(manager.isBlock(42L)).thenReturn(true, false);
        Mockito.when(env.getMaxJournalId()).thenReturn(200L);
        Mockito.when(table.getState()).thenReturn(OlapTable.OlapTableState.ROLLUP);
        TMasterOpRequest request = new TMasterOpRequest().setClientNodeHost("follower").setClientNodePort(9010)
                .setDb("db").setSyncJournalOnly(true)
                .setGroupCommitInfo(new TGroupCommitInfo().setTableName("tbl").setCheckTableBlocked(true));
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class, Mockito.CALLS_REAL_METHODS);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            TMasterOpResult blocked = service.forward(request);
            Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR.getValue(), blocked.getStatusCode());
            Assertions.assertTrue(blocked.getErrMessage().contains("insert table 42 is blocked on schema change"));
            Assertions.assertEquals(200L, blocked.getMaxJournalId());
            TMasterOpResult accepted = service.forward(request);
            Assertions.assertTrue(accepted.isSetStatusCode());
            Assertions.assertEquals(TStatusCode.OK.getValue(), accepted.getStatusCode());
            Assertions.assertEquals(200L, accepted.getMaxJournalId());
            table.isDropped = true;
            Assertions.assertEquals(TStatusCode.NOT_FOUND.getValue(), service.forward(request).getStatusCode());
            table.isDropped = false;
            Mockito.when(db.getTableOrMetaException("tbl", TableType.OLAP))
                    .thenThrow(new MetaNotFoundException("unknown table"));
            TMasterOpResult missing = service.forward(request);
            Assertions.assertEquals(TStatusCode.NOT_FOUND.getValue(), missing.getStatusCode());
            Assertions.assertEquals(200L, missing.getMaxJournalId());
            Mockito.verify(table, Mockito.never()).tryReadLock(Mockito.anyLong(), Mockito.any());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFollowerUsesReplayedLoadPrivilegeBeforeReportingTableStatus(boolean granted) throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Auth auth = Mockito.mock(Auth.class);
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.isMaster()).thenReturn(false);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("loader", "%");
        Mockito.doAnswer(invocation -> {
            List<UserIdentity> users = invocation.getArgument(3);
            users.add(user);
            return null;
        }).when(auth).checkPlainPassword(Mockito.eq("loader"), Mockito.eq("client"), Mockito.eq("pwd"),
                Mockito.anyList());
        TStreamLoadPutRequest request = new TStreamLoadPutRequest().setUser("loader").setUserIp("client")
                .setPasswd("pwd").setDb("db").setTbl("tbl").setGroupCommitMode("sync_mode");
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class, Mockito.CALLS_REAL_METHODS);
        AtomicBoolean hasLoadPrivilege = new AtomicBoolean(!granted);
        Mockito.when(access.checkTblPriv(user, InternalCatalog.INTERNAL_CATALOG_NAME, "db", "tbl", PrivPredicate.LOAD))
                .thenAnswer(invocation -> hasLoadPrivilege.get());
        TMasterOpResult missing = new TMasterOpResult().setStatusCode(TStatusCode.NOT_FOUND.getValue())
                .setErrMessage("unknown table");
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedConstruction<MasterOpExecutor> executors = Mockito.mockConstruction(MasterOpExecutor.class,
                        (executor, context) -> Mockito.when(
                                executor.syncJournalAndGetGroupCommitTableStatus("db", "tbl", 5000))
                                .thenAnswer(invocation -> {
                                    hasLoadPrivilege.set(granted);
                                    return missing;
                                }))) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            TStreamLoadPutResult result = service.streamLoadPut(request);
            Assertions.assertEquals(granted ? TStatusCode.NOT_FOUND : TStatusCode.ANALYSIS_ERROR,
                    result.getStatus().getStatusCode());
            Assertions.assertEquals(granted, result.getStatus().getErrorMsgs().get(0).contains("unknown table"));
            Assertions.assertEquals(1, executors.constructed().size());
            Mockito.verify(catalog, Mockito.never()).getDbNullable("db");
        }
    }

    @ParameterizedTest
    @EnumSource(OlapTable.OlapTableState.class)
    public void testGroupCommitPlanningWithTableReadLock(OlapTable.OlapTableState state) throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        TStreamLoadPutRequest request = new TStreamLoadPutRequest().setDb("db").setTbl("tbl")
                .setGroupCommitMode("async_mode");
        TStreamLoadPutResult result = new TStreamLoadPutResult();
        StreamLoadHandler handler = Mockito.spy(new StreamLoadHandler(request, null, result));
        Mockito.doNothing().when(handler).setCloudCluster();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(catalog.getDbNullable("db")).thenReturn(db);
            Mockito.when(db.getTableOrMetaException("tbl", TableType.OLAP)).thenReturn(table);
            Mockito.when(table.getId()).thenReturn(42L);
            Mockito.when(table.getState()).thenReturn(state);
            Mockito.when(table.tryReadLock(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS))).thenReturn(true);
            Mockito.when(table.getBaseSchemaVersion()).thenReturn(8);
            Mockito.when(table.getTableProperty()).thenReturn(Mockito.mock(TableProperty.class));
            Mockito.when(table.getTableProperty().getUseSchemaLightChange()).thenReturn(true);
            GroupCommitManager manager = Mockito.mock(GroupCommitManager.class);
            Mockito.when(env.getGroupCommitManager()).thenReturn(manager);
            withMockedStreamLoadPlanner(table, request, handler::generatePlan);
            InOrder order = Mockito.inOrder(catalog, db, table);
            order.verify(catalog).getDbNullable("db");
            order.verify(db).getTableOrMetaException("tbl", TableType.OLAP);
            order.verify(table).tryReadLock(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS));
            Mockito.verify(table).readUnlock();
            Assertions.assertEquals(42L, result.getTableId());
            Assertions.assertEquals(8, result.getBaseSchemaVersion());
        }
    }

    @Test
    public void testNonMasterPlannerReturnsJournalSyncErrorBeforeCatalogLookup() throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        TStreamLoadPutRequest request = new TStreamLoadPutRequest().setDb("db").setTbl("tbl")
                .setUser("").setToken("test-token").setGroupCommitMode("sync_mode");
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(true).when(service).checkToken("test-token");
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedConstruction<MasterOpExecutor> validators = Mockito.mockConstruction(MasterOpExecutor.class,
                        (validator, context) -> Mockito.doThrow(new DdlException("planner has not replayed the ALTER"))
                                .when(validator).syncJournalAndGetGroupCommitTableStatus("db", "tbl", 5000))) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            TStreamLoadPutResult result = service.streamLoadPut(request);
            Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR, result.getStatus().getStatusCode());
            Assertions.assertTrue(result.getStatus().getErrorMsgs().get(0)
                    .contains("planner has not replayed the ALTER"));
            Assertions.assertFalse(result.isSetPipelineParams());
            Assertions.assertEquals(1, validators.constructed().size());
            Mockito.verify(catalog, Mockito.never()).getDbNullable("db");
        }
    }

    @Test
    public void testSchemaVersionLockTimeoutDoesNotReadMetadata() throws Exception {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(table.tryReadLock(1, TimeUnit.MILLISECONDS)).thenReturn(false);
        verifySchemaVersionReadFailure(table, UserException.class);
        Mockito.verify(table).tryReadLock(1, TimeUnit.MILLISECONDS);
        Mockito.verify(table, Mockito.never()).getBaseSchemaVersion();
        Mockito.verify(table, Mockito.never()).readUnlock();
    }

    @Test
    public void testSchemaVersionReadFailureReleasesLock() throws Exception {
        OlapTable table = Mockito.spy(new OlapTable());
        Mockito.doThrow(new IllegalStateException("schema read failed")).when(table).getBaseSchemaVersion();
        verifySchemaVersionReadFailure(table, IllegalStateException.class);
        MonitoredReentrantReadWriteLock lock = Deencapsulation.getField(table, "rwLock");
        Assertions.assertEquals(0, lock.getReadHoldCount());
        Mockito.verify(table).readUnlock();
    }

    private void verifySchemaVersionReadFailure(OlapTable table, Class<? extends Throwable> failure) throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(database);
        Mockito.when(database.getTableOrMetaException("tbl", TableType.OLAP)).thenReturn(table);
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setDb("db");
        request.setTbl("tbl");
        request.setThriftRpcTimeoutMs(1);
        StreamLoadHandler handler = new StreamLoadHandler(request, null, new TStreamLoadPutResult());
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Deencapsulation.invoke(handler, "setDbAndTable");
            Assertions.assertThrows(failure, () -> generateMetadataPlan(handler, table, request));
        }
    }

    @Test
    public void testSchemaVersionReadWaitsForBaseIndexReplacement() throws Exception {
        OlapTable table = Mockito.spy(new OlapTable());
        table.setBaseIndexId(1L);
        MaterializedIndexMeta oldIndexMeta = Mockito.mock(MaterializedIndexMeta.class);
        Mockito.when(oldIndexMeta.getSchemaVersion()).thenReturn(6);
        table.getMutableIndexIdToMeta().put(1L, oldIndexMeta);
        MaterializedIndexMeta newIndexMeta = Mockito.mock(MaterializedIndexMeta.class);
        Mockito.when(newIndexMeta.getSchemaVersion()).thenReturn(7);
        table.getMutableIndexIdToMeta().put(2L, newIndexMeta);
        Mockito.doAnswer(invocation -> {
            MonitoredReentrantReadWriteLock lock = Deencapsulation.getField(table, "rwLock");
            Assertions.assertEquals(1, lock.getReadHoldCount());
            return invocation.callRealMethod();
        }).when(table).getBaseSchemaVersion();
        CountDownLatch oldIndexRemoved = new CountDownLatch(1);
        CountDownLatch readerStarted = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            readerStarted.countDown();
            return invocation.callRealMethod();
        }).when(table).tryReadLock(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS));

        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(database);
        Mockito.when(database.getTableOrMetaException("tbl", TableType.OLAP)).thenReturn(table);
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setDb("db");
        request.setTbl("tbl");
        TStreamLoadPutResult result = new TStreamLoadPutResult();
        StreamLoadHandler handler = new StreamLoadHandler(request, null, result);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> schemaChange = executor.submit(() -> {
            table.writeLock();
            try {
                // Schema change removes the old base index before switching to the shadow index.
                table.getMutableIndexIdToMeta().remove(1L);
                oldIndexRemoved.countDown();
                Assertions.assertTrue(readerStarted.await(5, TimeUnit.SECONDS));
                table.setBaseIndexId(2L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                table.writeUnlock();
            }
        });
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertTrue(oldIndexRemoved.await(5, TimeUnit.SECONDS));
            Deencapsulation.invoke(handler, "setDbAndTable");
            // Catalog lookup must not touch index metadata or acquire an extra read lock.
            Mockito.verify(table, Mockito.never()).getBaseSchemaVersion();
            generateMetadataPlan(handler, table, request);
            Assertions.assertEquals(7, result.getBaseSchemaVersion());
            Mockito.verify(table, Mockito.times(1)).tryReadLock(5000, TimeUnit.MILLISECONDS);
        } finally {
            readerStarted.countDown();
            try {
                schemaChange.get(5, TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
            }
        }
    }

    private void generateMetadataPlan(StreamLoadHandler handler, OlapTable table,
            TStreamLoadPutRequest request) throws Exception {
        withMockedStreamLoadPlanner(table, request, () -> handler.generatePlan(table));
    }

    private void withMockedStreamLoadPlanner(OlapTable table, TStreamLoadPutRequest request,
            FailableRunnable<Exception> action) throws Exception {
        NereidsStreamLoadTask task = Mockito.mock(NereidsStreamLoadTask.class);
        Mockito.when(task.getGroupCommit()).thenReturn("sync_mode");
        Mockito.doReturn(false).when(table).getEnableUniqueKeyMergeOnWrite();
        Mockito.doReturn(false).when(table).enableTso();
        TPipelineFragmentParams plan = buildStreamLoadParams(new TOlapTableSink());
        plan.setQueryOptions(new TQueryOptions());
        try (MockedStatic<NereidsStreamLoadTask> tasks = Mockito.mockStatic(NereidsStreamLoadTask.class);
                MockedConstruction<NereidsStreamLoadPlanner> planners = Mockito.mockConstruction(
                        NereidsStreamLoadPlanner.class, (planner, context) ->
                                Mockito.when(planner.plan(Mockito.any(), Mockito.anyInt())).thenReturn(plan))) {
            tasks.when(() -> NereidsStreamLoadTask.fromTStreamLoadPutRequest(request)).thenReturn(task);
            action.run();
        }
    }

    @Test
    public void testResponseKeepsFirstTableMetadata() throws Exception {
        Database database = new Database(17, "db");
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        TStreamLoadPutResult result = new TStreamLoadPutResult();
        StreamLoadHandler handler = new StreamLoadHandler(request, null, result);
        Deencapsulation.setField(handler, "db", database);
        OlapTable first = Mockito.spy(new OlapTable());
        Mockito.doReturn(41L).when(first).getId();
        Mockito.doReturn(7).when(first).getBaseSchemaVersion();
        generateMetadataPlan(handler, first, request);
        OlapTable second = Mockito.spy(new OlapTable());
        Mockito.doReturn(42L).when(second).getId();
        generateMetadataPlan(handler, second, request);
        Assertions.assertEquals(17, result.getDbId());
        Assertions.assertEquals(41, result.getTableId());
        Assertions.assertEquals(7, result.getBaseSchemaVersion());
        Mockito.verify(second, Mockito.never()).getBaseSchemaVersion();
    }

    @Test
    public void testPlanningWithoutResponseDoesNotReadExtraSchemaMetadata() throws Exception {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        StreamLoadHandler handler = new StreamLoadHandler(request, null, null);
        OlapTable table = Mockito.spy(new OlapTable());
        generateMetadataPlan(handler, table, request);
        Mockito.verify(table, Mockito.never()).getBaseSchemaVersion();
        Mockito.verify(table, Mockito.times(1)).tryReadLock(5000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testSelectBackendSkipsDecommissioningBackend() throws Exception {
        SystemInfoService originalSystemInfoService = Env.getCurrentSystemInfo();
        Backend decommissioningBackend = createBackend(10001L, "127.0.0.1");
        decommissioningBackend.setDecommissioning(true);
        Backend selectedBackend = createBackend(10002L, "127.0.0.2");
        CloudSystemInfoService systemInfoService =
                new TestCloudSystemInfoService(Arrays.asList(decommissioningBackend, selectedBackend));

        try {
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", systemInfoService);

            Assertions.assertEquals(selectedBackend.getId(), StreamLoadHandler.selectBackend("cluster0").getId());
        } finally {
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfoService);
        }
    }

    @Test
    public void testSetCloudClusterUsesBackendComputeGroup() throws Exception {
        SystemInfoService originalSystemInfoService = Env.getCurrentSystemInfo();
        String originalCloudUniqueId = Config.cloud_unique_id;
        Backend backend = createBackend(10001L, "127.0.0.1");
        backend.setCloudClusterName("backend_compute_group");
        CloudSystemInfoService systemInfoService =
                new TestCloudSystemInfoService(Arrays.asList(backend));
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setUser("");
        request.setBackendId(backend.getId());
        request.setCloudCluster("header_compute_group");

        try {
            Config.cloud_unique_id = "test_cloud_unique_id";
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", systemInfoService);
            ConnectContext.remove();

            StreamLoadHandler handler = new StreamLoadHandler(
                    request, null, new TStreamLoadPutResult());
            handler.setCloudCluster();

            Assertions.assertEquals("backend_compute_group",
                    ConnectContext.get().getSessionVariable().getCloudCluster());
            Assertions.assertEquals("backend_compute_group", request.getCloudCluster());
        } finally {
            ConnectContext.remove();
            Config.cloud_unique_id = originalCloudUniqueId;
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfoService);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGroupCommitUnknownDatabasePreservesAnalysisError(boolean planOnMaster) throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.checkFeExist("follower", 9010)).thenReturn(Mockito.mock(Frontend.class));
        Mockito.when(env.getMaxJournalId()).thenReturn(200L);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(null);
        FrontendServiceImpl service = Mockito.spy(new FrontendServiceImpl(null));
        Mockito.doReturn(true).when(service).checkToken("valid");
        Mockito.doReturn(false).when(service).checkToken("invalid");
        TStreamLoadPutRequest request = new TStreamLoadPutRequest().setDb("db").setTbl("tbl")
                .setUser("").setToken("invalid").setGroupCommitMode("async_mode");
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            TMasterOpRequest blockCheck = new TMasterOpRequest().setClientNodeHost("follower").setClientNodePort(9010)
                    .setDb(request.getDb()).setSyncJournalOnly(true)
                    .setGroupCommitInfo(new TGroupCommitInfo().setTableName(request.getTbl())
                            .setCheckTableBlocked(true));
            TMasterOpResult masterResult = service.forward(blockCheck);
            Assertions.assertEquals(200L, masterResult.getMaxJournalId());
            Mockito.when(env.isMaster()).thenReturn(planOnMaster);
            try (MockedConstruction<MasterOpExecutor> executors = Mockito.mockConstruction(MasterOpExecutor.class,
                    (executor, context) -> Mockito.when(
                            executor.syncJournalAndGetGroupCommitTableStatus("db", "tbl", 5000))
                            .thenReturn(masterResult))) {
                TStreamLoadPutResult denied = service.streamLoadPut(request);
                Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR, denied.getStatus().getStatusCode());
                Assertions.assertTrue(denied.getStatus().getErrorMsgs().get(0).contains("Invalid token"));
                Assertions.assertTrue(executors.constructed().isEmpty());

                request.setToken("valid");
                TStreamLoadPutResult missingDb = service.streamLoadPut(request);
                Assertions.assertEquals(planOnMaster ? 0 : 1, executors.constructed().size());
                if (!planOnMaster) {
                    Mockito.verify(executors.constructed().get(0))
                            .syncJournalAndGetGroupCommitTableStatus("db", "tbl", 5000);
                }
                Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR, missingDb.getStatus().getStatusCode());
                Assertions.assertTrue(missingDb.getStatus().getErrorMsgs().get(0).contains("unknown database"));
            }
        }
    }

    @Test
    public void testStreamLoadPutRejectsMissingBackend() throws Exception {
        SystemInfoService originalSystemInfoService = Env.getCurrentSystemInfo();
        String originalCloudUniqueId = Config.cloud_unique_id;
        CloudSystemInfoService systemInfoService =
                new TestCloudSystemInfoService(Collections.emptyList());
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setUser("");
        request.setToken("test_token");
        request.setBackendId(10001L);
        request.setCloudCluster("header_compute_group");

        try {
            Config.cloud_unique_id = "test_cloud_unique_id";
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", systemInfoService);
            ConnectContext.remove();

            TStreamLoadPutResult result = new FrontendServiceImpl(null).streamLoadPut(request);

            Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR, result.getStatus().getStatusCode());
            Assertions.assertTrue(result.getStatus().getErrorMsgs().get(0).contains("10001"));
            Assertions.assertFalse(result.isSetPipelineParams());
            Assertions.assertNull(ConnectContext.get());
        } finally {
            ConnectContext.remove();
            Config.cloud_unique_id = originalCloudUniqueId;
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfoService);
        }
    }

    @Test
    public void testGroupCommitValidatesBackendComputeGroupPrivilege() throws Exception {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Backend backend = createBackend(10001L, "127.0.0.1");
        backend.setCloudClusterName("backend_compute_group");
        CloudSystemInfoService systemInfoService =
                new TestCloudSystemInfoService(Arrays.asList(backend));
        CloudEnv cloudEnv = Mockito.mock(CloudEnv.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Auth auth = Mockito.mock(Auth.class);
        Mockito.when(cloudEnv.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(cloudEnv.getAuth()).thenReturn(auth);
        Mockito.doAnswer(invocation -> {
            List<UserIdentity> currentUser = invocation.getArgument(3);
            currentUser.add(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));
            return null;
        }).when(auth).checkPlainPassword(Mockito.eq("test_user"), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyList());
        Mockito.doThrow(new DdlException("USAGE denied"))
                .when(cloudEnv).changeCloudCluster(
                        Mockito.eq("backend_compute_group"), Mockito.any(ConnectContext.class));

        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setUser("test_user");
        request.setUserIp("127.0.0.1");
        request.setPasswd("test_password");
        request.setBackendId(backend.getId());
        request.setCloudCluster("header_compute_group");
        request.setAuthCode(0);
        request.setGroupCommitMode("sync_mode");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Config.cloud_unique_id = "test_cloud_unique_id";
            mockedEnv.when(Env::getCurrentEnv).thenReturn(cloudEnv);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            ConnectContext.remove();

            StreamLoadHandler handler = new StreamLoadHandler(
                    request, null, new TStreamLoadPutResult());
            try {
                handler.setCloudCluster();
                Assertions.fail("group commit should validate compute group privilege");
            } catch (DdlException e) {
                Assertions.assertTrue(e.getMessage().contains("USAGE denied"));
            }

            Mockito.verify(cloudEnv).changeCloudCluster(
                    Mockito.eq("backend_compute_group"), Mockito.any(ConnectContext.class));
        } finally {
            ConnectContext.remove();
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    @Test
    public void testAssignAdaptiveRandomBucketRoutesPartitionToTabletOwner() {
        // The load runs on backend 20, but the only tablet of the partition lives on backend 10,
        // so the sink has to route the partition to backend 10 instead of assuming self ownership.
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setBackendId(20L);
        StreamLoadHandler handler = new StreamLoadHandler(
                request, null, new TStreamLoadPutResult());

        TPipelineFragmentParams params = buildStreamLoadParams(createSingleBucketSink());
        handler.assignAdaptiveRandomBucket(params);

        TOlapTablePartition partition = getSinglePartition(params);
        Assertions.assertEquals(10L, partition.getBucketBeId());
        Assertions.assertEquals(0L, partition.getLoadTabletIdx());
        Assertions.assertEquals(Lists.newArrayList(0), partition.getLocalBucketSeqs());
        Assertions.assertEquals(10L, partition.getIndexes().get(0).getBucketBeId());
        Assertions.assertEquals(Lists.newArrayList(0), partition.getIndexes().get(0).getLocalBucketSeqs());
    }

    @Test
    public void testAssignAdaptiveRandomBucketKeepsLocalBucketWhenSinkOwnsTablet() {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setBackendId(10L);
        StreamLoadHandler handler = new StreamLoadHandler(
                request, null, new TStreamLoadPutResult());

        TPipelineFragmentParams params = buildStreamLoadParams(createSingleBucketSink());
        handler.assignAdaptiveRandomBucket(params);

        TOlapTablePartition partition = getSinglePartition(params);
        Assertions.assertEquals(10L, partition.getBucketBeId());
        Assertions.assertEquals(Lists.newArrayList(0), partition.getLocalBucketSeqs());
    }

    @Test
    public void testAssignAdaptiveRandomBucketFallsBackWithoutBackendId() {
        // An old client does not report the executing backend, so no assignment consistent with the
        // receiver side can be computed. Adaptive mode must be turned off to keep the legacy routing.
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setDb("test_db");
        request.setTbl("test_tbl");
        StreamLoadHandler handler = new StreamLoadHandler(
                request, null, new TStreamLoadPutResult());

        TPipelineFragmentParams params = buildStreamLoadParams(createSingleBucketSink());
        handler.assignAdaptiveRandomBucket(params);

        TOlapTableSink sink = params.getFragment().getOutputSink().getOlapTableSink();
        Assertions.assertFalse(sink.isSetEnableAdaptiveRandomBucket());
        TOlapTablePartition partition = getSinglePartition(params);
        Assertions.assertFalse(partition.isSetBucketBeId());
        Assertions.assertFalse(partition.getIndexes().get(0).isSetBucketBeId());
    }

    @Test
    public void testAssignAdaptiveRandomBucketFallsBackWhenNothingCanBeAssigned() {
        // A partition the FE cannot assign (here: without a load tablet index) would leave the sink
        // in adaptive mode without any routing, so adaptive mode has to be turned off.
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setBackendId(20L);
        StreamLoadHandler handler = new StreamLoadHandler(
                request, null, new TStreamLoadPutResult());

        TOlapTableSink sink = createSingleBucketSink();
        sink.getPartition().getPartitions().get(0).unsetLoadTabletIdx();
        TPipelineFragmentParams params = buildStreamLoadParams(sink);
        handler.assignAdaptiveRandomBucket(params);

        Assertions.assertFalse(sink.isSetEnableAdaptiveRandomBucket());
    }

    @Test
    public void testAssignAdaptiveRandomBucketKeepsAdaptiveForFakePartitions() {
        // Auto partition tables get their real partitions, and their assignments, while the load
        // runs, so an empty plan time assignment must keep adaptive mode enabled.
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setBackendId(20L);
        StreamLoadHandler handler = new StreamLoadHandler(
                request, null, new TStreamLoadPutResult());

        TOlapTableSink sink = createSingleBucketSink();
        sink.getPartition().setPartitionsIsFake(true);
        sink.getPartition().getPartitions().get(0).unsetLoadTabletIdx();
        TPipelineFragmentParams params = buildStreamLoadParams(sink);
        handler.assignAdaptiveRandomBucket(params);

        Assertions.assertTrue(sink.isSetEnableAdaptiveRandomBucket());
        Assertions.assertTrue(sink.isEnableAdaptiveRandomBucket());
    }

    @Test
    public void testAssignAdaptiveRandomBucketIgnoresNonOlapTableSink() {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setBackendId(20L);
        StreamLoadHandler handler = new StreamLoadHandler(
                request, null, new TStreamLoadPutResult());

        TPipelineFragmentParams params = new TPipelineFragmentParams();
        TPlanFragment fragment = new TPlanFragment();
        fragment.setOutputSink(new TDataSink(TDataSinkType.DATA_STREAM_SINK));
        params.setFragment(fragment);

        handler.assignAdaptiveRandomBucket(params);
        Assertions.assertNull(params.getFragment().getOutputSink().getOlapTableSink());
    }

    /**
     * Creates a random distribution sink with one bucket, whose only tablet is located on backend 10.
     */
    private static TOlapTableSink createSingleBucketSink() {
        TOlapTablePartition partition = new TOlapTablePartition();
        partition.setId(1000L);
        partition.setNumBuckets(1);
        partition.setLoadTabletIdx(0);
        partition.addToIndexes(new TOlapTableIndexTablets(1L, Lists.newArrayList(100L)));
        TOlapTablePartitionParam partitionParam = new TOlapTablePartitionParam();
        partitionParam.addToPartitions(partition);

        TOlapTableLocationParam locationParam = new TOlapTableLocationParam();
        locationParam.addToTablets(new TTabletLocation(100L, Lists.newArrayList(10L)));

        TOlapTableSink sink = new TOlapTableSink();
        sink.setEnableAdaptiveRandomBucket(true);
        sink.setLoadToSingleTablet(false);
        sink.setPartition(partitionParam);
        sink.setLocation(locationParam);
        return sink;
    }

    private static TPipelineFragmentParams buildStreamLoadParams(TOlapTableSink sink) {
        TPipelineFragmentParams params = new TPipelineFragmentParams();
        TPlanFragment fragment = new TPlanFragment();
        TDataSink tDataSink = new TDataSink(TDataSinkType.OLAP_TABLE_SINK);
        tDataSink.setOlapTableSink(sink);
        fragment.setOutputSink(tDataSink);
        params.setFragment(fragment);
        return params;
    }

    private static TOlapTablePartition getSinglePartition(TPipelineFragmentParams params) {
        return params.getFragment().getOutputSink().getOlapTableSink()
                .getPartition().getPartitions().get(0);
    }

    private Backend createBackend(long id, String host) {
        Backend backend = new Backend(id, host, 9050);
        backend.setAlive(true);
        return backend;
    }

    private static class TestCloudSystemInfoService extends CloudSystemInfoService {
        private final List<Backend> backends;

        private TestCloudSystemInfoService(List<Backend> backends) {
            this.backends = backends;
        }

        @Override
        public List<Backend> getBackendsByClusterName(final String clusterName) {
            return backends;
        }

        @Override
        public Backend getBackend(long backendId) {
            return backends.stream().filter(backend -> backend.getId() == backendId).findFirst().orElse(null);
        }
    }
}
