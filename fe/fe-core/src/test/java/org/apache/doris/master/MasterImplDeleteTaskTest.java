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

package org.apache.doris.master;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class MasterImplDeleteTaskTest {
    private static final long BACKEND_ID = 10000L;
    private static final long DB_ID = 20000L;
    private static final long TABLE_ID = 30000L;
    private static final long PARTITION_ID = 40000L;
    private static final long INDEX_ID = 50000L;
    private static final long TABLET_ID = 60000L;
    private static final long REPLICA_ID = 70000L;
    private static final long TRANSACTION_ID = 80000L;
    private static final long SIGNATURE = 90000L;
    private static final String HOST = "127.0.0.1";
    private static final int HEARTBEAT_PORT = 9050;
    private static final int BE_PORT = 9060;

    private MasterImpl masterImpl;
    private MockedStatic<Env> mockedEnvStatic;
    private MockedConstruction<ReportHandler> mockedReportHandlerConstruction;

    @Before
    public void setUp() {
        AgentTaskQueue.clearAllTasks();

        SystemInfoService systemInfoService = new SystemInfoService();
        Backend backend = new Backend(BACKEND_ID, HOST, HEARTBEAT_PORT);
        backend.setBePort(BE_PORT);
        systemInfoService.addBackend(backend);

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

        mockedReportHandlerConstruction = Mockito.mockConstruction(ReportHandler.class,
                (mock, context) -> {
                    Mockito.doNothing().when(mock).start();
                });

        masterImpl = new MasterImpl();
    }

    @After
    public void tearDown() {
        AgentTaskQueue.clearAllTasks();
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
        if (mockedReportHandlerConstruction != null) {
            mockedReportHandlerConstruction.close();
        }
    }

    @Test
    public void testDeletePushGenericFailureCountsDownSingleMark() {
        MarkedCountDownLatch<Long, Long> latch = new MarkedCountDownLatch<Long, Long>(2);
        latch.addMark(BACKEND_ID, TABLET_ID);
        latch.addMark(BACKEND_ID + 1, TABLET_ID + 1);

        PushTask pushTask = newDeletePushTask(latch);
        AgentTaskQueue.addTask(pushTask);

        masterImpl.finishTask(newFinishTaskRequest(TStatusCode.INTERNAL_ERROR));

        Assert.assertEquals(1, latch.getCount());
        Assert.assertEquals(TStatusCode.INTERNAL_ERROR, latch.getStatus().getErrorCode());
        Assert.assertEquals(1, pushTask.getFailedTimes());
        Assert.assertNull(AgentTaskQueue.getTask(BACKEND_ID, TTaskType.REALTIME_PUSH, SIGNATURE));
    }

    @Test
    public void testDeletePushInvalidArgumentCountsDownToZero() {
        MarkedCountDownLatch<Long, Long> latch = new MarkedCountDownLatch<Long, Long>(2);
        latch.addMark(BACKEND_ID, TABLET_ID);
        latch.addMark(BACKEND_ID + 1, TABLET_ID + 1);

        PushTask pushTask = newDeletePushTask(latch);
        AgentTaskQueue.addTask(pushTask);

        masterImpl.finishTask(newFinishTaskRequest(TStatusCode.INVALID_ARGUMENT));

        Assert.assertEquals(0, latch.getCount());
        Assert.assertEquals(TStatusCode.INVALID_ARGUMENT, latch.getStatus().getErrorCode());
        Assert.assertEquals(1, pushTask.getFailedTimes());
        Assert.assertNull(AgentTaskQueue.getTask(BACKEND_ID, TTaskType.REALTIME_PUSH, SIGNATURE));
    }

    @Test
    public void testDeletePushFailedWithMsgKeepsFailureStatus() {
        MarkedCountDownLatch<Long, Long> latch = new MarkedCountDownLatch<Long, Long>(1);
        latch.addMark(BACKEND_ID, TABLET_ID);

        PushTask pushTask = newDeletePushTask(latch);
        pushTask.failedWithMsg("submit failed");

        Assert.assertEquals(0, latch.getCount());
        Assert.assertEquals(TStatusCode.CANCELLED, latch.getStatus().getErrorCode());
        Assert.assertEquals("submit failed", latch.getStatus().getErrorMsg());
    }

    private PushTask newDeletePushTask(MarkedCountDownLatch<Long, Long> latch) {
        PushTask pushTask = new PushTask(null, BACKEND_ID, DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID,
                TABLET_ID, REPLICA_ID, 1, 1L, null, 0, 60, -1, TPushType.DELETE, null,
                false, TPriority.NORMAL, TTaskType.REALTIME_PUSH, TRANSACTION_ID, SIGNATURE,
                null, null, null, 0, null);
        pushTask.setCountDownLatch(latch);
        return pushTask;
    }

    private TFinishTaskRequest newFinishTaskRequest(TStatusCode statusCode) {
        TStatus taskStatus = new TStatus(statusCode);
        taskStatus.setErrorMsgs(Lists.newArrayList("delete failed"));
        TBackend tBackend = new TBackend(HOST, BE_PORT, 0);
        TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.REALTIME_PUSH, SIGNATURE, taskStatus);
        request.setReportVersion(1L);
        return request;
    }
}
