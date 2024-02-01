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

package org.apache.doris.task;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;

public class PriorityMasterTaskExecutorTest {
    private static final Logger LOG = LoggerFactory.getLogger(PriorityMasterTaskExecutorTest.class);

    // save the running order
    public static List<MasterTask> runningOrderList = Lists.newCopyOnWriteArrayList();

    private static final int THREAD_NUM = 1;

    private PriorityMasterTaskExecutor<TestMasterTask> executor;

    @Before
    public void setUp() {
        Comparator<TestMasterTask> comparator = Comparator.comparing(TestMasterTask::getPriority)
                .thenComparingLong(TestMasterTask::getSignature);
        executor = new PriorityMasterTaskExecutor("priority_master_task_executor_test", THREAD_NUM, comparator,
                TestMasterTask.class, false);
        executor.start();
    }

    @After
    public void tearDown() {
        if (executor != null) {
            executor.close();
        }
    }

    @Test
    public void testSubmit() {

        MasterTask errorTask = new ErrorMasterTask();
        try {
            executor.submit(errorTask);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RejectedExecutionException);
            Assert.assertTrue(("Task must be an instance of [" + TestMasterTask.class.getName() + "]").equals(e.getMessage()));
        }


        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(5);
        // submit task
        MasterTask task1 = new TestMasterTask(1L, 0, startLatch, finishLatch);
        Assert.assertTrue(executor.submit(task1));
        Assert.assertEquals(1, executor.getTaskNum());

        // submit same running task error
        Assert.assertFalse(executor.submit(task1));
        Assert.assertEquals(1, executor.getTaskNum());

        // submit some task with priority
        MasterTask task5 = new TestMasterTask(5L, 1, startLatch, finishLatch);
        executor.submit(task5);

        MasterTask task2 = new TestMasterTask(2L, 1, startLatch, finishLatch);
        executor.submit(task2);

        MasterTask task4 = new TestMasterTask(4L, 0, startLatch, finishLatch);
        executor.submit(task4);

        MasterTask task3 = new TestMasterTask(3L, 0, startLatch, finishLatch);
        executor.submit(task3);

        // start running tasks
        startLatch.countDown();

        // wait for all tasks finish
        try {
            finishLatch.await();
        } catch (InterruptedException interruptedException) {
            interruptedException.printStackTrace();
            Assert.fail();
        }

        // compare priority value first, the lower the higher priority
        // then compare signature value, the lower the higher priority
        Assert.assertTrue(runningOrderList.size() == 5);
        Assert.assertTrue(runningOrderList.get(0) == task1);
        Assert.assertTrue(runningOrderList.get(1) == task3);
        Assert.assertTrue(runningOrderList.get(2) == task4);
        Assert.assertTrue(runningOrderList.get(3) == task2);
        Assert.assertTrue(runningOrderList.get(4) == task5);
    }

    private class ErrorMasterTask extends MasterTask {

        @Override
        protected void exec() {
            // do nothing
        }
    }

    private class TestMasterTask extends MasterTask {

        private final long priority;
        private final CountDownLatch startLatch;
        private final CountDownLatch finishLatch;

        public TestMasterTask(long signature, long priority, CountDownLatch startLatch, CountDownLatch finishLatch) {
            this.signature = signature;
            this.priority = priority;
            this.startLatch = startLatch;
            this.finishLatch = finishLatch;
        }

        public long getPriority() {
            return priority;
        }

        public long getSignature() {
            return signature;
        }

        @Override
        protected void exec() {
            runningOrderList.add(this);
            LOG.info("run exec. signature: {}", signature);
            try {
                startLatch.await();
                Thread.sleep(1000);
            } catch (InterruptedException interruptedException) {
                throw new RuntimeException(interruptedException);
            } finally {
                finishLatch.countDown();
            }
            LOG.info("run finish. signature: {}", signature);
        }

    }
}
