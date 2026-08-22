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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class StreamingInsertTaskResourceTest {

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void closeReleasesExactAttemptStatementContextAndWorkerContext() throws Exception {
        StreamingInsertTask task = new StreamingInsertTask(
                1L, 2L, "", null, "", null, Collections.emptyMap(), null, null);
        ConnectContext taskContext = new ConnectContext();
        TUniqueId queryId = new TUniqueId(10L, 20L);
        taskContext.setQueryId(queryId);
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        taskContext.setStatementContext(statementContext);
        taskContext.setThreadLocalInfo();
        Field contextField = StreamingInsertTask.class.getDeclaredField("ctx");
        contextField.setAccessible(true);
        contextField.set(task, taskContext);

        task.closeOrReleaseResources();
        task.closeOrReleaseResources();

        Mockito.verify(statementContext).close();
        Assertions.assertNull(task.getCtx());
        Assertions.assertNull(ConnectContext.get());
    }

    @Test
    void cancellationLeavesAttemptCleanupToExecutionOwner() throws Exception {
        CountDownLatch planningStarted = new CountDownLatch(1);
        CountDownLatch finishPlanning = new CountDownLatch(1);
        AtomicInteger closeCalls = new AtomicInteger();
        AtomicBoolean cleanupRanOnWorker = new AtomicBoolean();
        Thread[] workerRef = new Thread[1];
        AbstractStreamingTask task = new AbstractStreamingTask(1L, 2L, null) {
            @Override
            public void before() throws Exception {
                planningStarted.countDown();
                finishPlanning.await(10, TimeUnit.SECONDS);
            }

            @Override
            public void run() {
            }

            @Override
            public boolean onSuccess() {
                return false;
            }

            @Override
            public void closeOrReleaseResources() {
                closeCalls.incrementAndGet();
                cleanupRanOnWorker.set(Thread.currentThread() == workerRef[0]);
            }
        };
        Thread worker = new Thread(() -> {
            workerRef[0] = Thread.currentThread();
            try {
                task.execute();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        worker.start();
        Assertions.assertTrue(planningStarted.await(10, TimeUnit.SECONDS));

        task.cancel(false);
        Assertions.assertEquals(0, closeCalls.get());
        finishPlanning.countDown();
        worker.join(TimeUnit.SECONDS.toMillis(10));

        Assertions.assertFalse(worker.isAlive());
        Assertions.assertEquals(1, closeCalls.get());
        Assertions.assertTrue(cleanupRanOnWorker.get());
    }

    @Test
    void terminalFailureCanCancelFromExecutionOwnerWithoutSelfWait() throws Exception {
        AtomicBoolean failed = new AtomicBoolean();
        StreamingInsertTask task = new StreamingInsertTask(
                1L, 2L, "", null, "", null, Collections.emptyMap(), null, null) {
            @Override
            public void before() {
                setStatus(org.apache.doris.job.common.TaskStatus.RUNNING);
                noRetry = true;
            }

            @Override
            public void run() throws org.apache.doris.job.exception.JobException {
                throw new org.apache.doris.job.exception.JobException("expected");
            }

            @Override
            public synchronized void closeOrReleaseResources() {
            }

            @Override
            protected void onFail(String errMsg) {
                setStatus(org.apache.doris.job.common.TaskStatus.FAILED);
                cancel(true);
                failed.set(true);
            }
        };
        Thread worker = new Thread(() -> {
            try {
                task.execute();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });

        worker.start();
        worker.join(TimeUnit.SECONDS.toMillis(10));

        Assertions.assertFalse(worker.isAlive());
        Assertions.assertTrue(failed.get());
    }

    @Test
    void cleanupFailureIsHandledByTaskFailureStateMachine() throws Exception {
        AtomicBoolean failed = new AtomicBoolean();
        AtomicBoolean successCalled = new AtomicBoolean();
        AbstractStreamingTask task = new AbstractStreamingTask(1L, 2L, null) {
            @Override
            public void before() {
                setStatus(org.apache.doris.job.common.TaskStatus.RUNNING);
                noRetry = true;
            }

            @Override
            public void run() {
            }

            @Override
            public boolean onSuccess() {
                successCalled.set(true);
                return true;
            }

            @Override
            public void closeOrReleaseResources() {
                throw new IllegalStateException("cleanup failed");
            }

            @Override
            protected void onFail(String errMsg) {
                Assertions.assertEquals("cleanup failed", errMsg);
                failed.set(true);
            }
        };

        task.execute();

        Assertions.assertTrue(failed.get());
        Assertions.assertFalse(successCalled.get());
    }
}
