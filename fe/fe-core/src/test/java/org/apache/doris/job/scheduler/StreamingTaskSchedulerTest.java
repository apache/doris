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

package org.apache.doris.job.scheduler;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.extensions.insert.streaming.AbstractStreamingTask;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.job.manager.StreamingTaskManager;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class StreamingTaskSchedulerTest {

    @Test
    public void testRecoveredSourceFinishesOrContinuesAfterProbe() throws Exception {
        Env env = Mockito.mock(Env.class);
        JobManager<?, ?> jobManager = Mockito.mock(JobManager.class);
        StreamingTaskManager taskManager = Mockito.mock(StreamingTaskManager.class);
        StreamingInsertJob exhaustedJob = Mockito.mock(StreamingInsertJob.class);
        AbstractStreamingTask exhaustedTask = Mockito.mock(AbstractStreamingTask.class);
        StreamingInsertJob continuedJob = Mockito.mock(StreamingInsertJob.class);
        AbstractStreamingTask continuedTask = Mockito.mock(AbstractStreamingTask.class);

        Mockito.when(env.getJobManager()).thenReturn(jobManager);
        Mockito.when(jobManager.getStreamingTaskManager()).thenReturn(taskManager);
        Mockito.when(exhaustedTask.getJobId()).thenReturn(1L);
        Mockito.doReturn(exhaustedJob).when(jobManager).getJob(1L);
        Mockito.when(exhaustedJob.needScheduleTask()).thenReturn(true);
        Mockito.when(exhaustedJob.hasMoreDataToConsume()).thenReturn(false);
        Mockito.when(exhaustedJob.hasReachedEnd()).thenReturn(true);
        Mockito.when(continuedTask.getJobId()).thenReturn(2L);
        Mockito.doReturn(continuedJob).when(jobManager).getJob(2L);
        Mockito.when(continuedJob.needScheduleTask()).thenReturn(true);
        Mockito.when(continuedJob.hasMoreDataToConsume()).thenReturn(true);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            StreamingTaskScheduler scheduler = new StreamingTaskScheduler();

            Deencapsulation.invoke(scheduler, "scheduleOneTask", exhaustedTask);
            Mockito.verify(exhaustedJob).updateJobStatus(JobStatus.FINISHED);
            Mockito.verify(exhaustedJob).logUpdateOperation();
            Mockito.verify(exhaustedTask, Mockito.never()).execute();

            Deencapsulation.invoke(scheduler, "scheduleOneTask", continuedTask);
            Mockito.verify(taskManager).addRunningTask(continuedTask);
            Mockito.verify(continuedTask).execute();
            Mockito.verify(continuedJob, Mockito.never()).updateJobStatus(JobStatus.FINISHED);
        }
    }
}
