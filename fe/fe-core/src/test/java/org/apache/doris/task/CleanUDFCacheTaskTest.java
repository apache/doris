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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionUtil;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.GenericPool;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.master.MasterImpl;
import org.apache.doris.master.ReportHandler;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TAgentTaskRequest;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;

import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CleanUDFCacheTaskTest {
    private static final long BACKEND_ID = 10000L;
    private static final long FUNCTION_ID = 101L;
    private static final int BE_PORT = 9060;
    private Backend backend;
    private MasterImpl master;
    private MockedStatic<Env> mockedEnv;
    private MockedStatic<AgentTaskExecutor> mockedExecutor;
    private MockedConstruction<ReportHandler> mockedReportHandler;
    private GenericPool<BackendService.Client> originalPool;
    private boolean originalRunningUnitTest;
    private final List<AgentBatchTask> submitted = new ArrayList<>();

    @BeforeEach
    void setUp() {
        MetricRepo.init();
        AgentTaskQueue.clearAllTasks();
        originalPool = ClientPool.backendPool;
        originalRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        SystemInfoService systemInfo = new SystemInfoService();
        backend = new Backend(BACKEND_ID, "127.0.0.1", 9050);
        backend.setBePort(BE_PORT);
        systemInfo.addBackend(backend);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
        mockedExecutor = Mockito.mockStatic(AgentTaskExecutor.class);
        mockedExecutor.when(() -> AgentTaskExecutor.submit(Mockito.any(AgentBatchTask.class)))
                .thenAnswer(invocation -> {
                    submitted.add(invocation.getArgument(0));
                    return null;
                });
        mockedReportHandler = Mockito.mockConstruction(ReportHandler.class);
        master = new MasterImpl();
    }

    @AfterEach
    void tearDown() {
        AgentTaskQueue.clearAllTasks();
        ClientPool.backendPool = originalPool;
        FeConstants.runningUnitTest = originalRunningUnitTest;
        mockedReportHandler.close();
        mockedExecutor.close();
        mockedEnv.close();
    }

    @Test
    void testWireSignatureRemainsUntrackedForOldBackends() {
        CleanUDFCacheTask task = new CleanUDFCacheTask(BACKEND_ID, "f(INT)", FUNCTION_ID);
        TAgentTaskRequest wire = new AgentBatchTask().toAgentTaskRequest(task);
        Assertions.assertEquals(FUNCTION_ID, task.getSignature());
        Assertions.assertEquals(-1, wire.getSignature());
        Assertions.assertEquals(FUNCTION_ID, wire.getCleanUdfCacheReq().getFunctionId());
        Assertions.assertTrue(wire.getCleanUdfCacheReq().isNeedAck());
    }

    @Test
    void testBackendWithoutAcknowledgementsKeepsCleanupPendingAcrossReports() {
        FunctionUtil.cleanUdfCache(Map.of(FUNCTION_ID, "f(INT)"));
        reportNoRunningTasks();
        reportNoRunningTasks();
        Assertions.assertEquals(3, submitted.size());
        Assertions.assertNotNull(queued(FUNCTION_ID));
        for (AgentBatchTask batch : submitted) {
            Assertions.assertEquals(-1, batch.toAgentTaskRequest(batch.getAllTasks().get(0)).getSignature());
        }
    }

    @Test
    void testDifferentFunctionsRemainQueuedAndDuplicateDropsAreCoalesced() {
        FunctionUtil.cleanUdfCache(Map.of(FUNCTION_ID, "f(INT)", FUNCTION_ID + 1, "g(INT)"));
        Assertions.assertNotNull(queued(FUNCTION_ID));
        Assertions.assertNotNull(queued(FUNCTION_ID + 1));
        Assertions.assertEquals(FUNCTION_ID, queued(FUNCTION_ID).getSignature());
        Assertions.assertEquals(FUNCTION_ID + 1, queued(FUNCTION_ID + 1).getSignature());
        FunctionUtil.cleanUdfCache(Map.of(FUNCTION_ID, "f(INT)", FUNCTION_ID + 1, "g(INT)"));
        Assertions.assertEquals(1, submitted.size());
        Assertions.assertEquals(2, submitted.get(0).getTaskNum());
    }

    @Test
    void testDeadBackendCleanupResendsOnTaskReportAfterRecovery() {
        backend.setAlive(false);
        FunctionUtil.cleanUdfCache(Map.of(FUNCTION_ID, "f(INT)"));
        Assertions.assertNotNull(queued(FUNCTION_ID));
        submitted.get(0).run();
        Assertions.assertEquals(1, queued(FUNCTION_ID).getFailedTimes());
        backend.setAlive(true);
        reportNoRunningTasks();
        Assertions.assertEquals(2, submitted.size());
        Assertions.assertSame(queued(FUNCTION_ID), submitted.get(1).getAllTasks().get(0));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testRpcFailureResendsAndOnlySuccessfulCompletionRemovesTask() throws Exception {
        backend.setAlive(true);
        GenericPool<BackendService.Client> pool = Mockito.mock(GenericPool.class);
        BackendService.Client client = Mockito.mock(BackendService.Client.class);
        ClientPool.backendPool = pool;
        Mockito.when(pool.borrowObject(Mockito.any())).thenReturn(client);
        Mockito.doThrow(new TException("test submit RPC failure")).when(client).submitTasks(Mockito.anyList());
        FunctionUtil.cleanUdfCache(Map.of(FUNCTION_ID, "f(INT)"));
        Assertions.assertNotNull(queued(FUNCTION_ID));
        submitted.get(0).run();
        Assertions.assertEquals(1, queued(FUNCTION_ID).getFailedTimes());
        Mockito.doReturn(null).when(client).submitTasks(Mockito.anyList());
        reportNoRunningTasks();
        submitted.get(1).run();
        Assertions.assertNotNull(queued(FUNCTION_ID));
        Mockito.verify(client, Mockito.times(2)).submitTasks(Mockito.anyList());
        master.finishTask(finishRequest(FUNCTION_ID, TStatusCode.OK));
        Assertions.assertNull(queued(FUNCTION_ID));
        reportNoRunningTasks();
        Assertions.assertEquals(2, submitted.size());
    }

    @Test
    void testFailedCompletionKeepsTaskForRetry() {
        CleanUDFCacheTask task = new CleanUDFCacheTask(BACKEND_ID, "f(INT)", FUNCTION_ID);
        AgentTaskQueue.addTask(task);
        master.finishTask(finishRequest(task.getSignature(), TStatusCode.INTERNAL_ERROR));
        Assertions.assertSame(task, AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE,
                task.getSignature()));
        Assertions.assertEquals(1, task.getFailedTimes());
    }

    @Test
    void testSuccessCompletionRemovesOnlyItsFunction() {
        CleanUDFCacheTask first = new CleanUDFCacheTask(BACKEND_ID, "f(INT)", FUNCTION_ID);
        CleanUDFCacheTask second = new CleanUDFCacheTask(BACKEND_ID, "g(INT)", FUNCTION_ID + 1);
        Assertions.assertTrue(AgentTaskQueue.addTask(first));
        Assertions.assertTrue(AgentTaskQueue.addTask(second));
        master.finishTask(finishRequest(first.getSignature(), TStatusCode.OK));
        Assertions.assertNull(queued(FUNCTION_ID));
        Assertions.assertSame(second, queued(FUNCTION_ID + 1));
        Assertions.assertEquals(TStatusCode.OK,
                master.finishTask(finishRequest(first.getSignature(), TStatusCode.OK)).getStatus().getStatusCode());
    }

    private AgentTask queued(long functionId) {
        return AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, functionId);
    }

    private void reportNoRunningTasks() {
        Deencapsulation.invoke(ReportHandler.class, "taskReport", BACKEND_ID, Map.of(), 0L);
    }

    private TFinishTaskRequest finishRequest(long functionId, TStatusCode status) {
        TBackend reportBackend = new TBackend();
        reportBackend.setHost(backend.getHost());
        reportBackend.setBePort(BE_PORT);
        TFinishTaskRequest request = new TFinishTaskRequest();
        request.setBackend(reportBackend);
        request.setTaskType(TTaskType.CLEAN_UDF_CACHE);
        request.setSignature(functionId);
        request.setTaskStatus(new TStatus(status));
        return request;
    }
}
