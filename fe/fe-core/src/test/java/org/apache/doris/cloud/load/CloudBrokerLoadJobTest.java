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

package org.apache.doris.cloud.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.BrokerFileGroupAggInfo;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TxnStateCallbackFactory;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CloudBrokerLoadJobTest {

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    public void testMultiClusterSyncDoesNotCreateContext(boolean hasCaller, boolean enabled) {
        boolean previousEnabled = SessionVariable.cloudEnableMultiClusterSyncLoad;
        SessionVariable.cloudEnableMultiClusterSyncLoad = enabled;
        ConnectContext.remove();
        ConnectContext caller = hasCaller ? new ConnectContext() : null;
        CloudBrokerLoadJob job = new CloudBrokerLoadJob();
        Deencapsulation.setField(job, "cloudClusterId", "loading");
        CloudSystemInfoService infoService = Mockito.mock(CloudSystemInfoService.class);
        List<Backend> backends = Collections.singletonList(Mockito.mock(Backend.class));
        Mockito.when(infoService.getCloudClusterIds()).thenReturn(Arrays.asList("loading", "other"));
        Mockito.when(infoService.getBackendsByClusterId("other")).thenReturn(backends);
        OlapTable table = Mockito.mock(OlapTable.class);
        List<Long> tablets = Collections.singletonList(123L);
        Mockito.when(table.getAllTabletIds()).thenReturn(tablets);

        try (AutoCloseConnectContext scope = hasCaller ? new AutoCloseConnectContext(caller) : null;
                MockedStatic<Env> environment = Mockito.mockStatic(Env.class);
                MockedStatic<StmtExecutor> executor = Mockito.mockStatic(StmtExecutor.class);
                MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            environment.when(Env::getCurrentSystemInfo).thenReturn(infoService);
            job.afterLoadingTaskCommitTransaction(Collections.singletonList(table));
            executor.verify(() -> StmtExecutor.syncLoadForTablets(Collections.singletonList(backends), tablets),
                    Mockito.times(enabled ? 1 : 0));
            Assertions.assertTrue(contexts.constructed().isEmpty());
            Assertions.assertSame(caller, ConnectContext.get());
            if (hasCaller) {
                Assertions.assertFalse(caller.isClosingOrClosed());
            }
        } finally {
            SessionVariable.cloudEnableMultiClusterSyncLoad = previousEnabled;
        }
    }

    @Test
    public void testRetryStartsNewTransactionAfterAbort() throws Exception {
        GlobalTransactionMgrIface transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);
        BrokerFileGroupAggInfo fileGroupAggInfo = Mockito.mock(BrokerFileGroupAggInfo.class);
        CloudBrokerLoadJob job = new CloudBrokerLoadJob();
        Deencapsulation.setField(job, "id", 1001L);
        Deencapsulation.setField(job, "dbId", 2001L);
        Deencapsulation.setField(job, "label", "cloud_broker_load_retry");
        Deencapsulation.setField(job, "transactionId", 3001L);
        Deencapsulation.setField(job, "fileGroupAggInfo", fileGroupAggInfo);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactionMgr);
            Mockito.when(transactionMgr.getCallbackFactory()).thenReturn(callbackFactory);
            Mockito.when(fileGroupAggInfo.getAllTableIds()).thenReturn(Sets.newHashSet(4001L));
            Mockito.when(transactionMgr.beginTransaction(Mockito.anyLong(), Mockito.anyList(),
                    Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.anyLong(), Mockito.anyLong())).thenReturn(5001L);

            job.unprotectedExecuteRetry(new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL, "rpc failed"));
            Assertions.assertEquals(0L, job.getTransactionId());
            job.beginTxn();
        }

        Assertions.assertEquals(5001L, job.getTransactionId());
        Assertions.assertEquals(JobState.RETRY, job.getState());
        Mockito.verify(transactionMgr).abortTransaction(2001L, "cloud_broker_load_retry", "rpc failed");
    }

    @Test
    public void testRetryClearsTransactionIdWhenAbortFails() throws Exception {
        GlobalTransactionMgrIface transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);
        CloudBrokerLoadJob job = new CloudBrokerLoadJob();
        Deencapsulation.setField(job, "id", 1002L);
        Deencapsulation.setField(job, "dbId", 2002L);
        Deencapsulation.setField(job, "label", "cloud_broker_load_abort_failed");
        Deencapsulation.setField(job, "transactionId", 3002L);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactionMgr);
            Mockito.when(transactionMgr.getCallbackFactory()).thenReturn(callbackFactory);
            Mockito.doThrow(new UserException("abort rpc failed"))
                    .when(transactionMgr).abortTransaction(2002L, "cloud_broker_load_abort_failed", "rpc failed");

            job.unprotectedExecuteRetry(new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL, "rpc failed"));
        }

        Assertions.assertEquals(0L, job.getTransactionId());
        Assertions.assertEquals(JobState.RETRY, job.getState());
    }

    @Test
    public void testStaleRetryDoesNotReviveFinishedJob() throws Exception {
        GlobalTransactionMgrIface transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);
        CloudBrokerLoadJob job = new CloudBrokerLoadJob();
        LoadTask failedTask = Mockito.mock(LoadTask.class);
        LoadTask otherTask = Mockito.mock(LoadTask.class);
        Deencapsulation.setField(job, "id", 1003L);
        Deencapsulation.setField(job, "dbId", 2003L);
        Deencapsulation.setField(job, "label", "cloud_broker_load_finished_during_retry");
        Deencapsulation.setField(job, "transactionId", 3003L);
        Deencapsulation.setField(job, "cloudClusterId", "cluster_id");
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(job, "idToTasks");
        idToTasks.put(4003L, failedTask);
        idToTasks.put(4004L, otherTask);
        Mockito.when(otherTask.isDone()).thenAnswer(invocation -> {
            Deencapsulation.setField(job, "state", JobState.FINISHED);
            return true;
        });

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactionMgr);
            Mockito.when(transactionMgr.getCallbackFactory()).thenReturn(callbackFactory);

            job.onTaskFailed(4003L, new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL, "rpc failed"));
        }

        Assertions.assertEquals(JobState.FINISHED, job.getState());
        Mockito.verify(callbackFactory).removeCallback(1003L);
        Mockito.verify(callbackFactory, Mockito.never()).addCallback(job);
    }
}
