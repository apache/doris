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

package org.apache.doris.load.loadv2;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.MasterTaskExecutor;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TxnStateCallbackFactory;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

public class LoadJobTest {

    @BeforeClass
    public static void start() {
        MetricRepo.init();
    }

    @Test
    public void testSetJobPropertiesWithErrorTimeout() {
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(LoadCommand.TIMEOUT_PROPERTY, "abc");
        LoadJob loadJob = new BrokerLoadJob();
        try {
            loadJob.setJobProperties(jobProperties);
            Assert.fail();
        } catch (DdlException e) {
            // CHECKSTYLE IGNORE THIS LINE
        }
    }

    @Test
    public void testSetJobProperties() {
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(LoadCommand.TIMEOUT_PROPERTY, "1000");
        jobProperties.put(LoadCommand.MAX_FILTER_RATIO_PROPERTY, "0.1");
        jobProperties.put(LoadCommand.EXEC_MEM_LIMIT, "1024");
        jobProperties.put(LoadCommand.STRICT_MODE, "True");

        LoadJob loadJob = new BrokerLoadJob();
        try {
            loadJob.setJobProperties(jobProperties);
            Assert.assertEquals(1000, loadJob.getTimeout());
            Assert.assertEquals(0.1, loadJob.getMaxFilterRatio(), 0);
            Assert.assertEquals(1024, loadJob.getExecMemLimit());
            Assert.assertTrue(loadJob.isStrictMode());
        } catch (DdlException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testExecute() {
        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            MasterTaskExecutor taskScheduler = Mockito.mock(MasterTaskExecutor.class);
            GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);

            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);
            Mockito.when(env.getPendingLoadTaskScheduler()).thenReturn(taskScheduler);

            LoadJob loadJob = new BrokerLoadJob();
            try {
                loadJob.execute();
            } catch (LoadException e) {
                Assert.fail(e.getMessage());
            }
            Assert.assertEquals(JobState.PENDING, loadJob.getState());
        }
    }

    @Test
    public void testProcessTimeoutWithCompleted() {
        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "state", JobState.FINISHED);

        loadJob.processTimeout();
        Assert.assertEquals(JobState.FINISHED, loadJob.getState());
    }

    @Test
    public void testProcessTimeoutWithIsCommitting() {
        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "isCommitting", true);
        Deencapsulation.setField(loadJob, "state", JobState.LOADING);

        loadJob.processTimeout();
        Assert.assertEquals(JobState.LOADING, loadJob.getState());
    }

    @Test
    public void testProcessTimeoutWithLongTimeoutSecond() {
        LoadJob loadJob = new BrokerLoadJob();
        loadJob.setTimeout(1000L);
        loadJob.processTimeout();
        Assert.assertEquals(JobState.PENDING, loadJob.getState());
    }

    @Test
    public void testProcessTimeout() {
        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            EditLog editLog = Mockito.mock(EditLog.class);
            GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
            TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);

            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(txnMgr.getCallbackFactory()).thenReturn(callbackFactory);

            LoadJob loadJob = new BrokerLoadJob();
            loadJob.setTimeout(0);
            Deencapsulation.setField(loadJob, "createTimestamp", 0L);

            loadJob.processTimeout();
            Assert.assertEquals(JobState.CANCELLED, loadJob.getState());
        }
    }

    @Test
    public void testUpdateStateToLoading() {
        LoadJob loadJob = new BrokerLoadJob();
        loadJob.updateState(JobState.LOADING);
        Assert.assertEquals(JobState.LOADING, loadJob.getState());
        Assert.assertNotEquals(-1, (long) Deencapsulation.getField(loadJob, "loadStartTimestamp"));
    }

    @Test
    public void testUpdateStateToFinished() {
        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
            TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);
            LoadTask loadTask1 = Mockito.mock(LoadTask.class);

            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);
            Mockito.when(txnMgr.getCallbackFactory()).thenReturn(callbackFactory);

            LoadJob loadJob = new BrokerLoadJob();
            loadJob.idToTasks.put(1L, loadTask1);

            Assert.assertEquals(1, loadJob.idToTasks.size());
            loadJob.updateState(JobState.FINISHED);
            Assert.assertEquals(JobState.FINISHED, loadJob.getState());
            Assert.assertNotEquals(-1, (long) Deencapsulation.getField(loadJob, "finishTimestamp"));
            Assert.assertEquals(100, (int) Deencapsulation.getField(loadJob, "progress"));
            Assert.assertEquals(0, loadJob.idToTasks.size());
        }
    }
}
