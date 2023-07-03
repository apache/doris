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

import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TimerTaskDisruptorTest {

    @Tested
    private TimerTaskDisruptor timerTaskDisruptor;

    @Injectable
    private AsyncJobManager asyncJobManager;

    private static boolean testEventExecuteFlag = false;

    @BeforeEach
    public void init() {
        timerTaskDisruptor = new TimerTaskDisruptor(asyncJobManager);
    }

    @Test
    void testPublishEventAndConsumer() {
        Job job = new Job("test", 6000L, null,
                null, new TestExecutor());
        new Expectations() {{
                asyncJobManager.getJob(anyLong);
                result = job;
            }};
        timerTaskDisruptor.tryPublish(job.getJobId(), UUID.randomUUID().getMostSignificantBits());
        Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> testEventExecuteFlag);
        Assertions.assertTrue(testEventExecuteFlag);
    }


    class TestExecutor implements JobExecutor<Boolean> {
        @Override
        public Boolean execute() {
            testEventExecuteFlag = true;
            return true;
        }
    }

    @AfterEach
    public void after() {
        timerTaskDisruptor.close();
    }
}
