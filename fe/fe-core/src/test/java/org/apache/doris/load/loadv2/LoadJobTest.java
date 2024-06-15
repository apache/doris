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

import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.metric.LongCounterMetric;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.MasterTaskExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TransactionState;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class LoadJobTest {

    @BeforeClass
    public static void start() {
        MetricRepo.init();
    }

    @Test
    public void testSetJobPropertiesWithErrorTimeout() {
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(LoadStmt.TIMEOUT_PROPERTY, "abc");
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
        jobProperties.put(LoadStmt.TIMEOUT_PROPERTY, "1000");
        jobProperties.put(LoadStmt.MAX_FILTER_RATIO_PROPERTY, "0.1");
        jobProperties.put(LoadStmt.EXEC_MEM_LIMIT, "1024");
        jobProperties.put(LoadStmt.STRICT_MODE, "True");

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
    public void testExecute(@Mocked GlobalTransactionMgr globalTransactionMgr,
                            @Mocked MasterTaskExecutor masterTaskExecutor)
            throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException, InterruptedException {
        LoadJob loadJob = new BrokerLoadJob();
        new Expectations() {
            {
                globalTransactionMgr.beginTransaction(anyLong, Lists.newArrayList(), anyString, (TUniqueId) any,
                        (TransactionState.TxnCoordinator) any,
                        (TransactionState.LoadJobSourceType) any, anyLong, anyLong);
                minTimes = 0;
                result = 1;
            }
        };

        try {
            loadJob.execute();
        } catch (LoadException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals(JobState.PENDING, loadJob.getState());
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
    public void testProcessTimeout(@Mocked Env env, @Mocked EditLog editLog) {
        LoadJob loadJob = new BrokerLoadJob();
        loadJob.setTimeout(0);
        new Expectations() {
            {
                env.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        loadJob.processTimeout();
        Assert.assertEquals(JobState.CANCELLED, loadJob.getState());
    }

    @Test
    public void testUpdateStateToLoading() {
        LoadJob loadJob = new BrokerLoadJob();
        loadJob.updateState(JobState.LOADING);
        Assert.assertEquals(JobState.LOADING, loadJob.getState());
        Assert.assertNotEquals(-1, (long) Deencapsulation.getField(loadJob, "loadStartTimestamp"));
    }

    @Test
    public void testUpdateStateToFinished(@Mocked MetricRepo metricRepo,
                                          @Injectable LoadTask loadTask1,
                                          @Mocked LongCounterMetric longCounterMetric) {
        LoadJob loadJob = new BrokerLoadJob();
        loadJob.idToTasks.put(1L, loadTask1);

        // TxnStateCallbackFactory factory = Catalog.getCurrentEnv().getGlobalTransactionMgr().getCallbackFactory();
        Env env = Env.getCurrentEnv();
        GlobalTransactionMgr mgr = new GlobalTransactionMgr(env);
        Deencapsulation.setField(env, "globalTransactionMgr", mgr);
        Assert.assertEquals(1, loadJob.idToTasks.size());
        loadJob.updateState(JobState.FINISHED);
        Assert.assertEquals(JobState.FINISHED, loadJob.getState());
        Assert.assertNotEquals(-1, (long) Deencapsulation.getField(loadJob, "finishTimestamp"));
        Assert.assertEquals(100, (int) Deencapsulation.getField(loadJob, "progress"));
        Assert.assertEquals(0, loadJob.idToTasks.size());
    }

    @Test
    public void testSerialization() throws IOException, AnalysisException {
        // 1. Write objects to file
        final Path path = Files.createTempFile("loadjobv2", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(LoadStmt.TIMEOUT_PROPERTY, "1000");
        jobProperties.put(LoadStmt.MAX_FILTER_RATIO_PROPERTY, "0.1");
        jobProperties.put(LoadStmt.EXEC_MEM_LIMIT, "1024");
        jobProperties.put(LoadStmt.STRICT_MODE, "True");

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

        loadJob.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        LoadJob loadJob2 = LoadJob.read(in);

        Assert.assertEquals(loadJob.dbId, loadJob2.dbId);
        Assert.assertEquals(loadJob.state, loadJob2.state);
        Assert.assertEquals(loadJob.getTimeout(), loadJob2.getTimeout());
        Assert.assertEquals(loadJob.getExecMemLimit(), loadJob2.getExecMemLimit());
        Assert.assertEquals(loadJob.isStrictMode(), loadJob2.isStrictMode());

        // 3. delete files
        in.close();
        Files.delete(path);

    }
}
