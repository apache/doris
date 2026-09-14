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
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.BrokerFileGroupAggInfo;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TxnStateCallbackFactory;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class CloudBrokerLoadJobTest {

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
            Assert.assertEquals(0L, job.getTransactionId());
            job.beginTxn();
        }

        Assert.assertEquals(5001L, job.getTransactionId());
        Assert.assertEquals(JobState.RETRY, job.getState());
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

        Assert.assertEquals(0L, job.getTransactionId());
        Assert.assertEquals(JobState.RETRY, job.getState());
    }
}
