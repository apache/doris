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

package org.apache.doris.job.manager;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

public class JobManagerTest {
    @Test
    public void testJobAuth() throws IOException, AnalysisException {
        UserIdentity user1 = new UserIdentity("testJobAuthUser", "%");
        user1.analyze();
        ConnectContext ctx = TestWithFeService.createCtx(user1, "%");
        try (MockedStatic<ConnectContext> mocked = Mockito.mockStatic(ConnectContext.class)) {
            mocked.when(ConnectContext::get).thenReturn(ctx);
            JobManager manager = new JobManager();
            HashSet<String> tableNames = Sets.newHashSet();
            try {
                // should check db auth
                manager.checkJobAuth("ctl1", "db1", tableNames);
                throw new RuntimeException("should exception");
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Admin_priv,Load_priv"));
                Assert.assertTrue(e.getMessage().contains("db1"));
            }
            tableNames.add("table1");
            try {
                // should check db auth
                manager.checkJobAuth("ctl1", "db1", tableNames);
                throw new RuntimeException("should exception");
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Admin_priv,Load_priv"));
                Assert.assertTrue(e.getMessage().contains("table1"));
            }
        }
    }

    private static AbstractJob mockJob(long id, String name, JobExecuteType type) {
        AbstractJob job = Mockito.mock(AbstractJob.class);
        Mockito.when(job.getJobId()).thenReturn(id);
        Mockito.when(job.getJobName()).thenReturn(name);
        JobExecutionConfiguration cfg = new JobExecutionConfiguration();
        cfg.setExecuteType(type);
        Mockito.when(job.getJobConfig()).thenReturn(cfg);
        return job;
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testCancelTaskByIdNotBlockedByOtherStreamingJob() throws JobException {
        JobManager manager = new JobManager();
        AbstractJob streamingJob = mockJob(1L, "streaming_job", JobExecuteType.STREAMING);
        AbstractJob batchJob = mockJob(2L, "batch_job", JobExecuteType.RECURRING);
        Map<Long, AbstractJob> jobMap = (Map<Long, AbstractJob>) Deencapsulation.getField(manager, "jobMap");
        jobMap.put(1L, streamingJob);
        jobMap.put(2L, batchJob);

        // Cancelling the batch job must not be blocked by the unrelated streaming job in jobMap.
        manager.cancelTaskById("batch_job", 100L);
        Mockito.verify(batchJob).cancelTaskById(100L);

        // Cancelling the streaming job itself still rejected.
        try {
            manager.cancelTaskById("streaming_job", 100L);
            Assert.fail("expected JobException for streaming job");
        } catch (JobException e) {
            Assert.assertTrue(e.getMessage().contains("streaming job not support"));
        }
    }
}
