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
//

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.metric.LongCounterMetric;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.MasterTaskExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TransactionState;

import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Mocked;

public class LoadJobTest {

    @Test
    public void testGetDbNotExists(@Mocked Catalog catalog) {
        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "dbId", 1L);
        new Expectations() {
            {
                catalog.getDb(1L);
                result = null;
            }
        };

        try {
            loadJob.getDb();
            Assert.fail();
        } catch (MetaNotFoundException e) {
        }
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
            Assert.assertEquals(1000, (long) Deencapsulation.getField(loadJob, "timeoutSecond"));
            Assert.assertEquals(0.1, Deencapsulation.getField(loadJob, "maxFilterRatio"), 0);
            Assert.assertEquals(1024, (long) Deencapsulation.getField(loadJob, "execMemLimit"));
            Assert.assertTrue(Deencapsulation.getField(loadJob, "strictMode"));
        } catch (DdlException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testExecute(@Mocked GlobalTransactionMgr globalTransactionMgr,
                            @Mocked MasterTaskExecutor masterTaskExecutor)
            throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException, DuplicatedRequestException {
        LoadJob loadJob = new BrokerLoadJob();
        new Expectations() {
            {
                globalTransactionMgr.beginTransaction(anyLong, anyString, (TUniqueId) any, anyString,
                        (TransactionState.LoadJobSourceType) any, anyLong, anyLong);
                result = 1;
            }
        };

        loadJob.execute();
        Assert.assertEquals(JobState.LOADING, loadJob.getState());
        Assert.assertEquals(1, loadJob.getTransactionId());

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
        Deencapsulation.setField(loadJob, "timeoutSecond", 1000L);

        loadJob.processTimeout();
        Assert.assertEquals(JobState.PENDING, loadJob.getState());
    }

    @Test
    public void testProcessTimeout(@Mocked Catalog catalog, @Mocked EditLog editLog) {
        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "timeoutSecond", 0);
        new Expectations() {
            {
                catalog.getEditLog();
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
            @Mocked LongCounterMetric longCounterMetric) {
        
        MetricRepo.COUNTER_LOAD_FINISHED = longCounterMetric;
        LoadJob loadJob = new BrokerLoadJob();
        
        // TxnStateCallbackFactory factory = Catalog.getCurrentCatalog().getGlobalTransactionMgr().getCallbackFactory();
        Catalog catalog = Catalog.getCurrentCatalog();
        GlobalTransactionMgr mgr = new GlobalTransactionMgr(catalog);
        Deencapsulation.setField(catalog, "globalTransactionMgr", mgr);
        loadJob.updateState(JobState.FINISHED);
        Assert.assertEquals(JobState.FINISHED, loadJob.getState());
        Assert.assertNotEquals(-1, (long) Deencapsulation.getField(loadJob, "finishTimestamp"));
        Assert.assertEquals(100, (int)Deencapsulation.getField(loadJob, "progress"));
    }
}
