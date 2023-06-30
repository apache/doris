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

import org.apache.doris.scheduler.executor.JobExecutor;
import org.apache.doris.scheduler.job.AsyncJobManager;
import org.apache.doris.scheduler.job.Job;

import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class AsyncJobManagerTest {

    AsyncJobManager asyncJobManager;

    private static AtomicInteger testExecuteCount = new AtomicInteger(0);
    Job job = new Job("test", 6000L, null,
            null, new TestExecutor());

    @BeforeEach
    public void init() {
        testExecuteCount.set(0);
        asyncJobManager = new AsyncJobManager();
    }

    @Test
    public void testCycleScheduler() {
        asyncJobManager.registerJob(job);
        //consider the time of the first execution and give some buffer time
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> testExecuteCount.get() >= 3);
    }

    @Test
    public void testCycleSchedulerAndStop() {
        asyncJobManager.registerJob(job);
        long startTime = System.currentTimeMillis();
        Awaitility.await().atMost(8, TimeUnit.SECONDS).until(() -> testExecuteCount.get() >= 1);
        asyncJobManager.unregisterJob(job.getJobId());
        //consider the time of the first execution and give some buffer time
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> System.currentTimeMillis() >= startTime + 13000L);
        Assertions.assertEquals(1, testExecuteCount.get());

    }

    @Test
    public void testCycleSchedulerWithIncludeStartTimeAndEndTime() {
        job.setStartTimestamp(System.currentTimeMillis() + 6000L);
        long endTimestamp = System.currentTimeMillis() + 19000L;
        job.setEndTimestamp(endTimestamp);
        asyncJobManager.registerJob(job);
        //consider the time of the first execution and give some buffer time

        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> System.currentTimeMillis()
                >= endTimestamp + 12000L);
        Assertions.assertEquals(2, testExecuteCount.get());
    }

    @Test
    public void testCycleSchedulerWithIncludeEndTime() {
        long endTimestamp = System.currentTimeMillis() + 13000;
        job.setEndTimestamp(endTimestamp);
        asyncJobManager.registerJob(job);
        //consider the time of the first execution and give some buffer time
        Awaitility.await().atMost(36, TimeUnit.SECONDS).until(() -> System.currentTimeMillis()
                >= endTimestamp + 12000L);
        Assertions.assertEquals(2, testExecuteCount.get());
    }

    @Test
    public void testCycleSchedulerWithIncludeStartTime() {

        long startTimestamp = System.currentTimeMillis() + 6000L;
        job.setStartTimestamp(startTimestamp);
        asyncJobManager.registerJob(job);
        //consider the time of the first execution and give some buffer time
        Awaitility.await().atMost(14, TimeUnit.SECONDS).until(() -> System.currentTimeMillis()
                >= startTimestamp + 7000L);
        Assertions.assertEquals(1, testExecuteCount.get());
    }

    @AfterEach
    public void after() throws IOException {
        asyncJobManager.close();
    }

    class TestExecutor implements JobExecutor<Boolean> {
        @Override
        public Boolean execute() {
            log.info("test execute count:{}", testExecuteCount.incrementAndGet());
            return true;
        }
    }
}
