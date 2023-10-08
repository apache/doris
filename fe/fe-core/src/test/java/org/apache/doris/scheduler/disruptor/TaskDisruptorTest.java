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

package org.apache.doris.scheduler.disruptor;

import org.apache.doris.catalog.Env;
import org.apache.doris.scheduler.constants.JobCategory;
import org.apache.doris.scheduler.executor.JobExecutor;
import org.apache.doris.scheduler.job.ExecutorResult;
import org.apache.doris.scheduler.job.Job;
import org.apache.doris.scheduler.manager.TimerJobManager;
import org.apache.doris.scheduler.manager.TransientTaskManager;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Tested;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class TaskDisruptorTest {

    @Tested
    private TaskDisruptor taskDisruptor;

    @Injectable
    private TimerJobManager timerJobManager;

    @Injectable
    private TransientTaskManager transientTaskManager;

    private static boolean testEventExecuteFlag = false;

    @Mocked
    Env env;

    @BeforeEach
    public void init() {
        taskDisruptor = new TaskDisruptor(timerJobManager, transientTaskManager);
    }

    @Test
    void testPublishEventAndConsumer() {
        Job job = new Job("test", 6000L, null,
                null, new TestExecutor());
        job.setJobCategory(JobCategory.COMMON);
        new Expectations() {{
                timerJobManager.getJob(anyLong);
                result = job;
            }};
        taskDisruptor.tryPublish(job.getJobId(), 1L);
        Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> testEventExecuteFlag);
        Assertions.assertTrue(testEventExecuteFlag);
    }


    class TestExecutor implements JobExecutor<Boolean> {
        @Override
        public ExecutorResult execute(Job job) {
            testEventExecuteFlag = true;
            return new ExecutorResult(true, true, null, "null");
        }
    }

    @AfterEach
    public void after() {
        taskDisruptor.close();
    }
}
