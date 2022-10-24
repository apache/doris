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

package org.apache.doris.mtmv;

import org.apache.doris.mtmv.MtmvUtils.TaskRetryPolicy;
import org.apache.doris.mtmv.MtmvUtils.TaskState;
import org.apache.doris.mtmv.metadata.MtmvJob;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class MtmvTaskExecutorTest extends TestWithFeService {
    @Test
    public void testSubmitTask() throws InterruptedException, ExecutionException {
        MtmvTaskExecutorPool pool = new MtmvTaskExecutorPool();
        MtmvTaskExecutor executor = new MtmvTaskExecutor();
        executor.setProcessor(new MtmvTaskProcessor());
        executor.setJob(MtmvUtilsTest.createDummyJob());
        executor.initTask(UUID.randomUUID().toString(), System.currentTimeMillis());
        pool.executeTask(executor);
        executor.getFuture().get();
        Assertions.assertEquals(TaskState.SUCCESS, executor.getTask().getState());
    }


    @Test
    public void testFailTask() throws InterruptedException, ExecutionException {
        MtmvTaskExecutorPool pool = new MtmvTaskExecutorPool();
        MtmvTaskExecutor executor = new MtmvTaskExecutor();
        executor.setProcessor(new MtmvTaskProcessorTest(1));
        executor.setJob(MtmvUtilsTest.createDummyJob());
        executor.initTask(UUID.randomUUID().toString(), System.currentTimeMillis());
        pool.executeTask(executor);
        executor.getFuture().get();
        Assertions.assertEquals(TaskState.FAILED, executor.getTask().getState());
        Assertions.assertEquals("java.lang.Exception: my define error 1", executor.getTask().getErrorMessage());
    }

    @Test
    public void testRetryTask() throws InterruptedException, ExecutionException {
        MtmvTaskExecutorPool pool = new MtmvTaskExecutorPool();

        MtmvTaskExecutor executor = new MtmvTaskExecutor();
        executor.setProcessor(new MtmvTaskProcessorTest(3));
        MtmvJob job = MtmvUtilsTest.createDummyJob();
        job.setRetryPolicy(TaskRetryPolicy.TIMES);
        executor.setJob(job);
        executor.initTask(UUID.randomUUID().toString(), System.currentTimeMillis());
        pool.executeTask(executor);
        executor.getFuture().get();
        Assertions.assertEquals(TaskState.SUCCESS, executor.getTask().getState());
    }

    @Test
    public void testRetryFailTask() throws InterruptedException, ExecutionException {
        MtmvTaskExecutorPool pool = new MtmvTaskExecutorPool();

        MtmvTaskExecutor executor = new MtmvTaskExecutor();
        executor.setProcessor(new MtmvTaskProcessorTest(4));
        MtmvJob job = MtmvUtilsTest.createDummyJob();
        job.setRetryPolicy(TaskRetryPolicy.TIMES);
        executor.setJob(job);
        executor.initTask(UUID.randomUUID().toString(), System.currentTimeMillis());
        pool.executeTask(executor);
        executor.getFuture().get();
        Assertions.assertEquals(TaskState.FAILED, executor.getTask().getState());
        Assertions.assertEquals("java.lang.Exception: my define error 4", executor.getTask().getErrorMessage());
    }

    public static class MtmvTaskProcessorTest extends MtmvTaskProcessor {
        private final int times;
        private int runTimes = 0;

        public MtmvTaskProcessorTest(int times) {
            this.times = times;
        }

        void process(MtmvTaskContext context) throws Exception {
            if (runTimes < times) {
                runTimes++;
                throw new Exception("my define error " + runTimes);
            }
        }
    }
}
