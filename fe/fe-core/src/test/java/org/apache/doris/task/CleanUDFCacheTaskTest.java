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

package org.apache.doris.task;

import org.apache.doris.analysis.SetType;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionName;
import org.apache.doris.catalog.FunctionSearchDesc;
import org.apache.doris.catalog.FunctionUtil;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.GenericPool;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.master.MasterImpl;
import org.apache.doris.master.ReportHandler;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.DropFunctionCommand;
import org.apache.doris.nereids.trees.plans.commands.info.FunctionArgTypesInfo;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TAgentTaskRequest;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CleanUDFCacheTaskTest {
    private static final long BACKEND_ID = 10000L;
    private static final long FUNCTION_ID = 20000L;
    private static final String HOST = "127.0.0.1";
    private static final int BE_PORT = 9060;

    private final List<AgentBatchTask> submissions = new ArrayList<>();
    private Backend backend;
    private MasterImpl master;
    private MockedStatic<Env> envMock;
    private MockedStatic<AgentTaskExecutor> executorMock;
    private MockedConstruction<ReportHandler> reportHandlerMock;

    @BeforeEach
    public void setUp() {
        MetricRepo.init();
        AgentTaskQueue.clearAllTasks();
        SystemInfoService systemInfo = new SystemInfoService();
        backend = new Backend(BACKEND_ID, HOST, 9050);
        backend.setBePort(BE_PORT);
        systemInfo.addBackend(backend);
        envMock = Mockito.mockStatic(Env.class);
        envMock.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
        executorMock = Mockito.mockStatic(AgentTaskExecutor.class);
        executorMock.when(() -> AgentTaskExecutor.submit(Mockito.any(AgentBatchTask.class)))
                .thenAnswer(invocation -> {
                    submissions.add(invocation.getArgument(0));
                    return null;
                });
        reportHandlerMock = Mockito.mockConstruction(ReportHandler.class,
                (mock, context) -> Mockito.doNothing().when(mock).start());
        master = new MasterImpl();
    }

    @AfterEach
    public void tearDown() {
        reportHandlerMock.close();
        executorMock.close();
        envMock.close();
        AgentTaskQueue.clearAllTasks();
    }

    @Test
    public void unavailableBackendCleanupIsRetriedByTaskReportAfterRecovery() throws Exception {
        FunctionUtil.cleanUdfCache(Map.of(FUNCTION_ID, "sum(INT)"));
        AgentTask queued = AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID);
        Assertions.assertNotNull(queued);
        Assertions.assertFalse(backend.isAlive());
        Assertions.assertEquals(1, submissions.size());
        submissions.get(0).run(); // AgentBatchTask skips the unavailable backend.
        Assertions.assertSame(queued,
                AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID));
        Assertions.assertEquals(1, queued.getFailedTimes());

        backend.setAlive(true);
        reportTasks();
        Assertions.assertEquals(2, submissions.size());
        Assertions.assertEquals(List.of(queued), submissions.get(1).getAllTasks());
        TAgentTaskRequest request = submissions.get(1).toAgentTaskRequest(queued);
        Assertions.assertEquals(FUNCTION_ID, request.getSignature());
        Assertions.assertEquals(FUNCTION_ID, request.getCleanUdfCacheReq().getFunctionId());

        Assertions.assertEquals(TStatusCode.OK, finish(FUNCTION_ID, TStatusCode.OK));
        Assertions.assertNull(AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void rpcSubmissionFailureKeepsCleanupForTheNextReport() throws Exception {
        backend.setAlive(true);
        GenericPool<BackendService.Client> originalPool = ClientPool.backendPool;
        GenericPool<BackendService.Client> failingPool = Mockito.mock(GenericPool.class);
        Mockito.when(failingPool.borrowObject(Mockito.any(TNetworkAddress.class)))
                .thenThrow(new Exception("simulated submit RPC failure"));
        ClientPool.backendPool = failingPool;
        try {
            FunctionUtil.cleanUdfCache(Map.of(FUNCTION_ID, "sum(INT)"));
            AgentTask queued = AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID);
            Assertions.assertNotNull(queued);
            submissions.get(0).run();
            Mockito.verify(failingPool).borrowObject(Mockito.any(TNetworkAddress.class));
            Assertions.assertSame(queued,
                    AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID));
            reportTasks();
            Assertions.assertEquals(List.of(queued), submissions.get(1).getAllTasks());
        } finally {
            ClientPool.backendPool = originalPool;
        }
    }

    @Test
    public void duplicateCleanupKeepsPendingGenerationAndDistinctIdsDoNotCollide() {
        FunctionUtil.cleanUdfCache(Map.of(FUNCTION_ID, "sum(INT)", FUNCTION_ID + 1, "sum(INT)"));
        AgentTask first = AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID);
        AgentTask second = AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID + 1);
        Assertions.assertNotNull(first);
        Assertions.assertNotNull(second);
        Assertions.assertNotSame(first, second);
        FunctionUtil.cleanUdfCache(Map.of(FUNCTION_ID, "sum(INT)"));
        Assertions.assertEquals(1, submissions.size());
        Assertions.assertSame(first,
                AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID));
    }

    @Test
    public void failedAcknowledgementKeepsTaskAndSuccessOnlyRemovesItsGeneration() {
        FunctionUtil.cleanUdfCache(Map.of(FUNCTION_ID, "sum(INT)", FUNCTION_ID + 1, "sum(INT)"));
        AgentTask first = AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID);
        Assertions.assertNotNull(first);
        finish(FUNCTION_ID, TStatusCode.INTERNAL_ERROR);
        Assertions.assertSame(first,
                AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID));
        Assertions.assertEquals(1, first.getFailedTimes());
        Assertions.assertEquals(TStatusCode.OK, finish(FUNCTION_ID, TStatusCode.OK));
        Assertions.assertNull(AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID));
        Assertions.assertNotNull(AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID + 1));
        // The task is already removed when the same execution result arrives again.
        Assertions.assertEquals(TStatusCode.CANCELLED, finish(FUNCTION_ID, TStatusCode.OK));
    }

    @Test
    public void recycledDatabaseSnapshotOnlyQueuesPythonFunctions() {
        Database database = Mockito.mock(Database.class);
        Function python = Mockito.mock(Function.class);
        Mockito.when(python.getBinaryType()).thenReturn(Function.BinaryType.PYTHON_UDF);
        Mockito.when(python.getId()).thenReturn(FUNCTION_ID);
        Mockito.when(python.signatureString()).thenReturn("sum(INT)");
        Function java = Mockito.mock(Function.class);
        Mockito.when(java.getBinaryType()).thenReturn(Function.BinaryType.JAVA_UDF);
        Mockito.when(database.getFunctions()).thenReturn(List.of(python, java));
        FunctionUtil.cleanUdfCache(database);
        Assertions.assertNotNull(AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID));
        Assertions.assertEquals(1, submissions.get(0).getTaskNum());
        Mockito.verify(database).readLock();
        Mockito.verify(database).readUnlock();
    }

    @Test
    public void legacyTaskKeepsTheOneShotTaskSignature() {
        CleanUDFCacheTask task = new CleanUDFCacheTask(BACKEND_ID, "sum(INT)", FUNCTION_ID);
        Assertions.assertEquals(-1L, task.getSignature());
        Assertions.assertEquals(FUNCTION_ID, task.toThrift().getFunctionId());
        Assertions.assertEquals(-1L, new AgentBatchTask().toAgentTaskRequest(task).getSignature());
    }

    @Test
    public void localDropRetriesAfterBackendRecovery() throws Exception {
        dropAndRetry(SetType.DEFAULT);
    }

    @Test
    public void globalDropRetriesAfterBackendRecovery() throws Exception {
        dropAndRetry(SetType.GLOBAL);
    }

    private void dropAndRetry(SetType scope) throws Exception {
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        envMock.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getAccessManager().checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN))).thenReturn(true);
        List<Long> functionIds = List.of(FUNCTION_ID, FUNCTION_ID + 1);
        if (scope == SetType.GLOBAL) {
            Mockito.when(env.getGlobalFunctionMgr().dropFunction(
                    Mockito.any(FunctionSearchDesc.class), Mockito.eq(false))).thenReturn(functionIds);
        } else {
            InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
            envMock.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            Database database = Mockito.mock(Database.class);
            Mockito.when(catalog.getDbNullable("cleanup_db")).thenReturn(database);
            Mockito.when(database.dropFunction(Mockito.any(FunctionSearchDesc.class), Mockito.eq(false)))
                    .thenReturn(functionIds);
        }
        DropFunctionCommand command = new DropFunctionCommand(scope, false,
                new FunctionName("cleanup_db", "sum"),
                new FunctionArgTypesInfo(List.of(IntegerType.INSTANCE), false));
        command.run(new ConnectContext(), null);
        Assertions.assertEquals(1, submissions.size());
        submissions.get(0).run();
        for (long functionId : functionIds) {
            AgentTask task = AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, functionId);
            Assertions.assertNotNull(task);
            Assertions.assertEquals(1, task.getFailedTimes());
            Assertions.assertEquals(functionId, ((CleanUDFCacheTask) task).toThrift().getFunctionId());
        }
        backend.setAlive(true);
        reportTasks();
        Assertions.assertEquals(2, submissions.get(1).getTaskNum());
        for (long functionId : functionIds) {
            Assertions.assertEquals(TStatusCode.OK, finish(functionId, TStatusCode.OK));
            Assertions.assertNull(AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, functionId));
        }
    }

    private void reportTasks() throws Exception {
        Method taskReport = ReportHandler.class.getDeclaredMethod("taskReport", long.class, Map.class, long.class);
        taskReport.setAccessible(true);
        taskReport.invoke(null, BACKEND_ID, Map.of(), 0L);
    }

    private TStatusCode finish(long functionId, TStatusCode status) {
        TFinishTaskRequest request = new TFinishTaskRequest(new TBackend(HOST, BE_PORT, 0),
                TTaskType.CLEAN_UDF_CACHE, functionId, new TStatus(status));
        return master.finishTask(request).getStatus().getStatusCode();
    }
}
