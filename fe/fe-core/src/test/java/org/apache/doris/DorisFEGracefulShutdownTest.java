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

package org.apache.doris;

import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.QueryAdmissionController;
import org.apache.doris.transaction.TransactionEntry;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DorisFEGracefulShutdownTest {
    @Test
    public void testIdleRollbackCannotBlockShutdownDeadline() throws Exception {
        CountDownLatch rollbackStarted = new CountDownLatch(1);
        CountDownLatch finishRollback = new CountDownLatch(1);
        CountDownLatch rollbackFinished = new CountDownLatch(1);
        AtomicReference<Thread> rollbackThread = new AtomicReference<>();
        TransactionEntry transaction = Mockito.mock(TransactionEntry.class);
        Mockito.when(transaction.isTxnModel()).thenReturn(true);
        Mockito.when(transaction.abortTransaction()).thenAnswer(invocation -> {
            rollbackThread.set(Thread.currentThread());
            rollbackStarted.countDown();
            // Even a rollback that ignores interruption must not hold up the shutdown hook.
            Uninterruptibles.awaitUninterruptibly(finishRollback);
            rollbackFinished.countDown();
            return 1L;
        });
        TestContext first = new TestContext();
        first.setTxnEntry(transaction);
        TestContext second = new TestContext();
        ConnectScheduler scheduler = scheduler(first, second);
        try {
            long started = System.nanoTime();
            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(3),
                    () -> DorisFE.waitForGracefulShutdown(scheduler,
                            System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500)));
            Assertions.assertTrue(rollbackStarted.await(1, TimeUnit.SECONDS));
            Assertions.assertTrue(rollbackThread.get().isDaemon());
            Assertions.assertEquals(1L, rollbackFinished.getCount());
            Assertions.assertTrue(System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(400),
                    "pending idle transaction cleanup must count as work until the deadline");
            Assertions.assertFalse(second.closed, "do not start the next rollback after the deadline");
        } finally {
            finishRollback.countDown();
        }
        Assertions.assertTrue(rollbackFinished.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testCompletedIdleCleanupDoesNotConsumeWholeGracePeriod() {
        TestContext context = new TestContext();
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(3),
                () -> DorisFE.waitForGracefulShutdown(scheduler(context),
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(10)));
        Assertions.assertTrue(context.closed);
        Assertions.assertSame(context, context.closingContext, "rollback must use the session's thread context");
    }

    @Test
    public void testRescanAfterLastRequestFinishes() throws InterruptedException {
        TestContext context = new TestContext();
        ConnectScheduler scheduler = scheduler(context);
        context.startMysqlCommand();
        AtomicInteger scans = new AtomicInteger();
        AtomicBoolean requestFinished = new AtomicBoolean();
        QueryAdmissionController.INSTANCE.acquireContinuation();
        Mockito.doAnswer(invocation -> {
            invocation.callRealMethod();
            if (scans.incrementAndGet() == 1) {
                // The scan skipped a busy session, but the request ends before the next count sample.
                context.finishMysqlCommand();
                QueryAdmissionController.INSTANCE.release();
                requestFinished.set(true);
            }
            return null;
        }).when(scheduler).closeIdleMysqlConnections(Mockito.anyLong());
        try {
            DorisFE.waitForGracefulShutdown(scheduler, System.nanoTime() + TimeUnit.SECONDS.toNanos(5));
            Assertions.assertTrue(context.closed, "the now-idle transaction needs another cleanup pass");
            Assertions.assertTrue(scans.get() >= 2);
        } finally {
            if (!requestFinished.get()) {
                QueryAdmissionController.INSTANCE.release();
            }
        }
    }

    @Test
    public void testExpiredDeadlineDoesNotStartIdleCleanup() throws InterruptedException {
        TestContext context = new TestContext();
        DorisFE.waitForGracefulShutdown(scheduler(context), System.nanoTime());
        Assertions.assertFalse(context.closed);
    }

    private static ConnectScheduler scheduler(TestContext... contexts) {
        Map<Integer, ConnectContext> connections = new LinkedHashMap<>();
        for (int i = 0; i < contexts.length; ++i) {
            contexts[i].setCommand(MysqlCommand.COM_SLEEP);
            connections.put(i, contexts[i]);
        }
        ConnectScheduler scheduler = Mockito.mock(ConnectScheduler.class);
        Mockito.when(scheduler.getConnectionMap()).thenReturn(connections);
        Mockito.doCallRealMethod().when(scheduler).closeIdleMysqlConnections(Mockito.anyLong());
        return scheduler;
    }

    private static class TestContext extends ConnectContext {
        private volatile boolean closed;
        private volatile ConnectContext closingContext;

        @Override
        public void cleanup() {
            // Only model the blocking socket close here; GracefulMysqlConnectionTest
            // exercises the full session cleanup, including temporary tables.
            closeChannel();
        }

        @Override
        protected void closeChannel() {
            closingContext = ConnectContext.get();
            closed = true;
            // Model XNIO's synchronous close listener, which unregisters the session and aborts its txn.
            closeTxn();
        }
    }
}
