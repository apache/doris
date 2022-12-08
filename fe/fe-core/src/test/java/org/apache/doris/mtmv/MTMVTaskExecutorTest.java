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

import org.apache.doris.mtmv.MTMVUtils.TaskRetryPolicy;
import org.apache.doris.mtmv.MTMVUtils.TaskState;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class MTMVTaskExecutorTest extends TestWithFeService {
    @Test
    public void testSubmitTask() throws InterruptedException, ExecutionException {
        MTMVTaskExecutorPool pool = new MTMVTaskExecutorPool();
        MTMVTaskExecutor executor = new MTMVTaskExecutor();
        executor.setProcessor(new MTMVTaskProcessor());
        executor.setJob(MTMVUtilsTest.createDummyJob());
        executor.initTask(UUID.randomUUID().toString(), System.currentTimeMillis());
        pool.executeTask(executor);
        executor.getFuture().get();
        Assertions.assertEquals(TaskState.SUCCESS, executor.getTask().getState());
    }


    @Test
    public void testFailTask() throws InterruptedException, ExecutionException {
        MTMVTaskExecutorPool pool = new MTMVTaskExecutorPool();
        MTMVTaskExecutor executor = new MTMVTaskExecutor();
        executor.setProcessor(new MTMVTaskProcessorTest(1));
        executor.setJob(MTMVUtilsTest.createDummyJob());
        executor.initTask(UUID.randomUUID().toString(), System.currentTimeMillis());
        pool.executeTask(executor);
        executor.getFuture().get();
        Assertions.assertEquals(TaskState.FAILED, executor.getTask().getState());
        Assertions.assertEquals("java.lang.Exception: my define error 1", executor.getTask().getMessage());
    }

    @Test
    public void testRetryTask() throws InterruptedException, ExecutionException {
        MTMVTaskExecutorPool pool = new MTMVTaskExecutorPool();

        MTMVTaskExecutor executor = new MTMVTaskExecutor();
        executor.setProcessor(new MTMVTaskProcessorTest(3));
        MTMVJob job = MTMVUtilsTest.createDummyJob();
        job.setRetryPolicy(TaskRetryPolicy.TIMES);
        executor.setJob(job);
        executor.initTask(UUID.randomUUID().toString(), System.currentTimeMillis());
        pool.executeTask(executor);
        executor.getFuture().get();
        Assertions.assertEquals(TaskState.SUCCESS, executor.getTask().getState());
    }

    @Test
    public void testRetryFailTask() throws InterruptedException, ExecutionException {
        MTMVTaskExecutorPool pool = new MTMVTaskExecutorPool();

        MTMVTaskExecutor executor = new MTMVTaskExecutor();
        executor.setProcessor(new MTMVTaskProcessorTest(4));
        MTMVJob job = MTMVUtilsTest.createDummyJob();
        job.setRetryPolicy(TaskRetryPolicy.TIMES);
        executor.setJob(job);
        executor.initTask(UUID.randomUUID().toString(), System.currentTimeMillis());
        pool.executeTask(executor);
        executor.getFuture().get();
        Assertions.assertEquals(TaskState.FAILED, executor.getTask().getState());
        Assertions.assertEquals("java.lang.Exception: my define error 4", executor.getTask().getMessage());
    }

    public static class MTMVTaskProcessorTest extends MTMVTaskProcessor {
        private final int times;
        private int runTimes = 0;

        public MTMVTaskProcessorTest(int times) {
            this.times = times;
        }

        void process(MTMVTaskContext context) throws Exception {
            if (runTimes < times) {
                runTimes++;
                throw new Exception("my define error " + runTimes);
            }
        }
    }
}
