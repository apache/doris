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
import org.apache.doris.common.DdlException;
import org.apache.doris.persist.EditLog;
import org.apache.doris.scheduler.constants.JobCategory;
import org.apache.doris.scheduler.constants.JobType;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.JobExecutor;
import org.apache.doris.scheduler.job.ExecutorResult;
import org.apache.doris.scheduler.job.Job;
import org.apache.doris.scheduler.job.JobTask;
import org.apache.doris.scheduler.manager.TimerJobManager;
import org.apache.doris.scheduler.manager.TransientTaskManager;

import lombok.extern.slf4j.Slf4j;
import mockit.Expectations;
import mockit.Mocked;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TimerJobManagerTest {

    TimerJobManager timerJobManager;

    @Mocked
    EditLog editLog;

    private static AtomicInteger testExecuteCount = new AtomicInteger(0);
    Job job = new Job("test", 6000L, null,
            null, new TestExecutor());
    JobTask jobTask = new JobTask(job.getJobId(), 1L, System.currentTimeMillis());

    @BeforeEach
    public void init() {
        job.setJobType(JobType.RECURRING);
        job.setJobCategory(JobCategory.COMMON);
        testExecuteCount.set(0);
        timerJobManager = new TimerJobManager();
        TransientTaskManager transientTaskManager = new TransientTaskManager();
        TaskDisruptor taskDisruptor = new TaskDisruptor(this.timerJobManager, transientTaskManager);
        this.timerJobManager.setDisruptor(taskDisruptor);
        taskDisruptor.start();
        timerJobManager.start();
    }

    @Test
    public void testCycleScheduler(@Mocked Env env) throws DdlException {
        setContext(env);
        timerJobManager.registerJob(job);
        //consider the time of the first execution and give some buffer time
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> testExecuteCount.get() >= 3);
    }

    private void setContext(Env env) {
        new Expectations() {
            {
                env.getEditLog();
                result = editLog;
                editLog.logCreateJob((Job) any);
            }
        };
    }

    @Test
    public void testCycleSchedulerAndStop(@Mocked Env env) throws DdlException {
        setContext(env);
        timerJobManager.registerJob(job);
        long startTime = System.currentTimeMillis();
        Awaitility.await().atMost(8, TimeUnit.SECONDS).until(() -> testExecuteCount.get() >= 1);
        timerJobManager.unregisterJob(job.getJobId());
        //consider the time of the first execution and give some buffer time
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> System.currentTimeMillis() >= startTime + 13000L);
        Assertions.assertEquals(1, testExecuteCount.get());
    }

    @Test
    public void testCycleSchedulerWithIncludeStartTimeAndEndTime(@Mocked Env env) throws DdlException {
        setContext(env);
        job.setStartTimeMs(System.currentTimeMillis() + 6000L);
        long endTimestamp = System.currentTimeMillis() + 19000L;
        job.setEndTimeMs(endTimestamp);
        timerJobManager.registerJob(job);
        //consider the time of the first execution and give some buffer time

        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> System.currentTimeMillis()
                >= endTimestamp + 12000L);
        Assertions.assertEquals(3, testExecuteCount.get());
    }

    @Test
    public void testCycleSchedulerWithIncludeEndTime(@Mocked Env env) throws DdlException {
        setContext(env);
        long endTimestamp = System.currentTimeMillis() + 13000;
        job.setEndTimeMs(endTimestamp);
        timerJobManager.registerJob(job);

        //consider the time of the first execution and give some buffer time
        Awaitility.await().atMost(36, TimeUnit.SECONDS).until(() -> System.currentTimeMillis()
                >= endTimestamp + 12000L);
        Assertions.assertEquals(2, testExecuteCount.get());
    }

    @Test
    public void testCycleSchedulerWithIncludeStartTime(@Mocked Env env) throws DdlException {
        setContext(env);

        long startTimestamp = System.currentTimeMillis() + 6000L;
        job.setStartTimeMs(startTimestamp);
        timerJobManager.registerJob(job);
        //consider the time of the first execution and give some buffer time
        Awaitility.await().atMost(14, TimeUnit.SECONDS).until(() -> System.currentTimeMillis()
                >= startTimestamp + 7000L);
        Assertions.assertEquals(2, testExecuteCount.get());
    }

    @Test
    public void testCycleSchedulerWithImmediatelyStart(@Mocked Env env) throws DdlException {
        setContext(env);
        long startTimestamp = System.currentTimeMillis();
        job.setImmediatelyStart(true);
        timerJobManager.registerJob(job);
        //consider the time of the first execution and give some buffer time
        Awaitility.await().atMost(16, TimeUnit.SECONDS).until(() -> System.currentTimeMillis()
                >= startTimestamp + 15000L);
        Assertions.assertEquals(3, testExecuteCount.get());
    }

    @Test
    public void testOneTimeJob(@Mocked Env env) throws DdlException {
        setContext(env);

        long startTimestamp = System.currentTimeMillis() + 3000L;
        job.setIntervalMs(0L);
        job.setStartTimeMs(startTimestamp);
        job.setJobType(JobType.ONE_TIME);
        timerJobManager.registerJob(job);
        //consider the time of the first execution and give some buffer time
        Awaitility.await().atMost(14, TimeUnit.SECONDS).until(() -> System.currentTimeMillis()
                >= startTimestamp + 7000L);
        Assertions.assertEquals(1, testExecuteCount.get());
    }

    @AfterEach
    public void after() throws IOException {
        timerJobManager.close();
    }

    class TestExecutor implements JobExecutor<Boolean, String> {

        @Override
        public ExecutorResult<Boolean> execute(Job job, String dataContext) throws JobException {
            log.info("test execute count:{}", testExecuteCount.incrementAndGet());
            return new ExecutorResult<>(true, true, null, "");
        }
    }
}
