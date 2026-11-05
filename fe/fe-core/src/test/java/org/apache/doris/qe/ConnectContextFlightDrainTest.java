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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.arrowflight.protocol.FlightProtocolAdapter;
import org.apache.doris.arrowflight.results.FlightSqlChannel;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.qe.ConnectContext.CloseMode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Ticker;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Timeout(30)
public class ConnectContextFlightDrainTest {
    private boolean savedRunningUnitTest;
    private int savedDeferredTimeout;
    private ConnectContext ctx;
    private ConnectPoolMgr pool;
    private AtomicLong tickerNanos;

    @BeforeEach
    public void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        savedDeferredTimeout = Config.arrow_flight_deferred_query_idle_timeout_second;
        FeConstants.runningUnitTest = true;
        tickerNanos = new AtomicLong();
        FlightSqlChannel channel = new FlightSqlChannel(new Ticker() {
            @Override
            public long read() {
                return tickerNanos.get();
            }
        });
        ctx = Mockito.spy(new ConnectContext(new FlightProtocolAdapter("draining-token", channel)));
        ctx.setEnv(ConnectPoolTestSupport.envAllowing(10));
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        pool = Mockito.spy(new ConnectPoolMgr(10));
        Assertions.assertEquals(-1, ctx.registerClientSession(pool));
        ctx.setCommand(MysqlCommand.COM_SLEEP);
    }

    @AfterEach
    public void tearDown() {
        ctx.cleanup();
        ConnectContext.remove();
        Config.arrow_flight_deferred_query_idle_timeout_second = savedDeferredTimeout;
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    private void leaveResultForDoGet() {
        ctx.runCommand(() -> {
            ctx.getFlightSqlChannel().addOKResult("result", "KILL CONNECTION");
            ctx.requestClose(CloseMode.AFTER_RESPONSE);
        });
        Assertions.assertTrue(ctx.isClosingAfterResponse());
        Assertions.assertSame(ctx, pool.getContext(ctx.getConnectionId()));
        Mockito.verify(ctx, Mockito.never()).releaseProtocolSession();
    }

    private void verifyReleasedOnce() {
        Assertions.assertEquals(0, pool.getConnectionNum());
        Assertions.assertEquals(0, ctx.getFlightSqlChannel().getAllocatedMemory());
        Mockito.verify(pool).unregisterConnection(ctx);
        Mockito.verify(ctx).releaseProtocolSession();
        Mockito.verify(ctx).closeTxn();
        Mockito.verify(ctx).deleteTempTable();
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    private static void stop(ExecutorService worker) throws InterruptedException {
        worker.shutdownNow();
        Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
    }

    private static void awaitOwnerWait(Thread reader) throws InterruptedException {
        // Observe the actual ownership wait, rather than assuming the reader has run after a delay.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (reader.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
            Assertions.assertTrue(reader.isAlive(), "result entry exited before waiting for its owner");
            Thread.sleep(1);
        }
        Assertions.assertEquals(Thread.State.TIMED_WAITING, reader.getState());
    }

    @Test
    public void testPendingResultRetainsSessionUntilDoGetReturns() {
        leaveResultForDoGet();
        Assertions.assertThrows(ConnectContext.SessionClosedException.class,
                () -> ctx.executeCommand(0, () -> "another SQL"));
        Assertions.assertFalse(ctx.runCommand(() -> Assertions.fail("closing session admitted a command")));
        Assertions.assertEquals("0", ctx.executeResult(0, () -> {
            VectorSchemaRoot root = ctx.getFlightSqlChannel().getResult("result").getVectorSchemaRoot();
            try {
                Assertions.assertSame(ctx, ConnectContext.get());
                Assertions.assertEquals(1, root.getRowCount());
                return root.getVector(0).getObject(0).toString();
            } finally {
                ctx.getFlightSqlChannel().invalidate("result");
                // Finishing the result does not close the context until its owner exits.
                Assertions.assertSame(ctx, pool.getContext(ctx.getConnectionId()));
                Mockito.verify(ctx, Mockito.never()).releaseProtocolSession();
            }
        }));
        Assertions.assertThrows(ConnectContext.SessionClosedException.class,
                () -> ctx.executeResult(0, () -> "another DoGet"));
        verifyReleasedOnce();
    }

    @Test
    public void testIdleDeferredCloseRetainsExistingResult() {
        ctx.runCommand(() -> ctx.getFlightSqlChannel().addOKResult("result", "SET"));
        ctx.requestClose(CloseMode.AFTER_RESPONSE);
        ctx.requestClose(CloseMode.AFTER_RESPONSE);
        Assertions.assertTrue(ctx.isClosingAfterResponse());
        Assertions.assertSame(ctx, pool.getContext(ctx.getConnectionId()));
        ctx.executeResult(0, () -> {
            ctx.getFlightSqlChannel().invalidate("result");
            return null;
        });
        verifyReleasedOnce();
    }

    @Test
    public void testDoGetWaitsForTheClosingCommandToYieldExecution() throws Exception {
        CountDownLatch responseReady = new CountDownLatch(1);
        CountDownLatch finishCommand = new CountDownLatch(1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        FutureTask<String> result = new FutureTask<>(() -> ctx.executeResult(10_000, () -> {
            try {
                return ctx.getFlightSqlChannel().getResult("result").getVectorSchemaRoot()
                        .getVector(0).getObject(0).toString();
            } finally {
                ctx.getFlightSqlChannel().invalidate("result");
            }
        }));
        Thread reader = new Thread(result, "flight-draining-result");
        try {
            Future<?> command = worker.submit(() -> ctx.runCommand(() -> {
                ctx.getFlightSqlChannel().addOKResult("result", "KILL CONNECTION");
                ctx.requestClose(CloseMode.AFTER_RESPONSE);
                responseReady.countDown();
                await(finishCommand);
            }));
            await(responseReady);
            reader.start();
            awaitOwnerWait(reader);
            Assertions.assertSame(ctx, pool.getContext(ctx.getConnectionId()));
            Mockito.verify(ctx, Mockito.never()).releaseProtocolSession();
            finishCommand.countDown();
            command.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals("0", result.get(10, TimeUnit.SECONDS));
            verifyReleasedOnce();
        } finally {
            finishCommand.countDown();
            reader.interrupt();
            reader.join(10_000);
            Assertions.assertFalse(reader.isAlive());
            stop(worker);
        }
    }

    @Test
    public void testWaitingDoGetIsRejectedBeforeSlowGracefulCleanupFinishes() throws Exception {
        leaveResultForDoGet();
        CountDownLatch sending = new CountDownLatch(1);
        CountDownLatch finishSend = new CountDownLatch(1);
        CountDownLatch releasing = new CountDownLatch(1);
        CountDownLatch finishRelease = new CountDownLatch(1);
        AtomicBoolean releaseAllowed = new AtomicBoolean();
        Mockito.doAnswer(call -> {
            releasing.countDown();
            releaseAllowed.set(finishRelease.await(10, TimeUnit.SECONDS));
            return call.callRealMethod();
        }).when(ctx).releaseProtocolSession();
        FutureTask<ConnectContext.SessionClosedException> duplicate = new FutureTask<>(() ->
                Assertions.assertThrows(ConnectContext.SessionClosedException.class,
                        () -> ctx.executeResult(10_000, () -> Assertions.fail("duplicate DoGet ran"))));
        Thread waiter = new Thread(duplicate, "flight-duplicate-result");
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> stream = worker.submit(() -> ctx.executeResult(0, () -> {
                sending.countDown();
                await(finishSend);
                ctx.getFlightSqlChannel().invalidate("result");
                return null;
            }));
            await(sending);
            waiter.start();
            awaitOwnerWait(waiter);
            finishSend.countDown();
            await(releasing);
            Assertions.assertNotNull(duplicate.get(10, TimeUnit.SECONDS));
            Assertions.assertFalse(releaseAllowed.get());
            Assertions.assertEquals(0, pool.getConnectionNum());
            finishRelease.countDown();
            stream.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(releaseAllowed.get());
            verifyReleasedOnce();
        } finally {
            finishSend.countDown();
            finishRelease.countDown();
            waiter.interrupt();
            waiter.join(10_000);
            Assertions.assertFalse(waiter.isAlive());
            stop(worker);
        }
    }

    @Test
    public void testOpenSessionMaintenanceDoesNotRefreshIdleDeadline() {
        ctx.startTime = 1000;
        ctx.checkTimeout(1001);
        Assertions.assertEquals(1000, ctx.getStartTime());
        Assertions.assertFalse(ctx.isClosingOrClosed());
    }

    @Test
    public void testRejectedTicketDoesNotRefreshDeadlineOrConsumeOtherResult() {
        leaveResultForDoGet();
        ctx.startTime = 1000;
        ctx.checkTimeout(1001);
        Assertions.assertEquals(1000, ctx.getStartTime());
        Assertions.assertThrows(IllegalArgumentException.class, () -> ctx.executeResult(0, () -> {
            Assertions.assertNull(ctx.getFlightSqlChannel().getResult("unknown"));
            throw new IllegalArgumentException("Unknown result ticket");
        }));
        Assertions.assertEquals(1000, ctx.getStartTime());
        Assertions.assertNotNull(ctx.getFlightSqlChannel().getResult("result"));
        Assertions.assertSame(ctx, pool.getContext(ctx.getConnectionId()));
        Mockito.verify(ctx, Mockito.never()).releaseProtocolSession();
        ctx.getSessionVariable().waitTimeoutS = 1;
        ctx.checkTimeout(2001);
        verifyReleasedOnce();
    }

    @Test
    public void testImmediateCloseWaitsForResultOwnerBeforeReleasingMemory() throws Exception {
        verifyCloseDuringDoGet(false);
    }

    @Test
    public void testHardTimeoutWaitsForResultOwnerBeforeReleasingMemory() throws Exception {
        verifyCloseDuringDoGet(true);
    }

    @Test
    public void testExpiredUnconsumedResultMakesGracefulCleanupReady() {
        leaveResultForDoGet();
        tickerNanos.set(TimeUnit.MINUTES.toNanos(10) + 1);
        ctx.checkTimeout(ctx.getStartTime() + TimeUnit.MINUTES.toMillis(10) + 1);
        verifyReleasedOnce();
    }

    @Test
    public void testExpiryMaintenanceDoesNotReleaseMemoryUsedByDoGet() throws Exception {
        leaveResultForDoGet();
        CountDownLatch sending = new CountDownLatch(1);
        CountDownLatch finishSend = new CountDownLatch(1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> stream = worker.submit(() -> ctx.executeResult(0, () -> {
                VectorSchemaRoot root = ctx.getFlightSqlChannel().getResult("result").getVectorSchemaRoot();
                try {
                    sending.countDown();
                    await(finishSend);
                    Assertions.assertEquals("0", root.getVector(0).getObject(0).toString());
                } finally {
                    ctx.getFlightSqlChannel().invalidate("result");
                }
                return null;
            }));
            await(sending);
            tickerNanos.set(TimeUnit.MINUTES.toNanos(10) + 1);
            ctx.checkTimeout(ctx.getStartTime() + TimeUnit.MINUTES.toMillis(10) + 1);
            Assertions.assertTrue(ctx.isClosingAfterResponse());
            Assertions.assertSame(ctx, pool.getContext(ctx.getConnectionId()));
            Assertions.assertTrue(ctx.getFlightSqlChannel().getAllocatedMemory() > 0);
            Mockito.verify(ctx, Mockito.never()).releaseProtocolSession();
            finishSend.countDown();
            stream.get(10, TimeUnit.SECONDS);
            verifyReleasedOnce();
        } finally {
            finishSend.countDown();
            stop(worker);
        }
    }

    private void verifyCloseDuringDoGet(boolean timeout) throws Exception {
        leaveResultForDoGet();
        ctx.getSessionVariable().waitTimeoutS = 1;
        long deadline = ctx.getStartTime() + 1001;
        CountDownLatch sending = new CountDownLatch(1);
        CountDownLatch finishSend = new CountDownLatch(1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> stream = worker.submit(() -> ctx.executeResult(0, () -> {
                VectorSchemaRoot root = ctx.getFlightSqlChannel().getResult("result").getVectorSchemaRoot();
                try {
                    sending.countDown();
                    await(finishSend);
                    Assertions.assertEquals("0", root.getVector(0).getObject(0).toString());
                } finally {
                    ctx.getFlightSqlChannel().invalidate("result");
                }
                return null;
            }));
            await(sending);
            if (timeout) {
                ctx.checkTimeout(deadline);
            } else {
                ctx.cleanup();
            }
            Assertions.assertFalse(ctx.isClosingAfterResponse());
            Assertions.assertEquals(0, pool.getConnectionNum());
            Assertions.assertTrue(ctx.getFlightSqlChannel().getAllocatedMemory() > 0);
            Mockito.verify(ctx, Mockito.never()).releaseProtocolSession();
            Assertions.assertThrows(ConnectContext.SessionClosedException.class,
                    () -> ctx.executeResult(0, () -> "late result"));
            finishSend.countDown();
            stream.get(10, TimeUnit.SECONDS);
            verifyReleasedOnce();
        } finally {
            finishSend.countDown();
            stop(worker);
        }
    }

    @Test
    public void testReentrantCloseDuringResultAndCleanupReleasesOnce() {
        leaveResultForDoGet();
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.doAnswer(call -> {
            ctx.cleanup();
            return null;
        }).when(executor).cancel(Mockito.any(Status.class), Mockito.eq(false));
        Mockito.doAnswer(call -> {
            ctx.cleanup();
            ctx.requestClose(CloseMode.AFTER_RESPONSE);
            return call.callRealMethod();
        }).when(ctx).releaseProtocolSession();
        ctx.executeResult(0, () -> {
            ctx.setExecutor(executor);
            ctx.cleanup();
            ctx.cleanup();
            ctx.requestClose(CloseMode.AFTER_RESPONSE);
            Assertions.assertTrue(ctx.getFlightSqlChannel().getAllocatedMemory() > 0);
            Mockito.verify(ctx, Mockito.never()).releaseProtocolSession();
            ctx.getFlightSqlChannel().invalidate("result");
            return null;
        });
        Mockito.verify(executor).cancel(Mockito.any(Status.class), Mockito.eq(false));
        verifyReleasedOnce();
    }

    @Test
    public void testFailedResultStillReleasesAndRestoresCallerContext() {
        leaveResultForDoGet();
        ConnectContext caller = new ConnectContext();
        caller.setThreadLocalInfo();
        IOException failure = new IOException("stream failed");
        try {
            Assertions.assertSame(failure, Assertions.assertThrows(IOException.class,
                    () -> ctx.executeResult(0, () -> {
                        try {
                            throw failure;
                        } finally {
                            ctx.getFlightSqlChannel().invalidate("result");
                        }
                    })));
            Assertions.assertSame(caller, ConnectContext.get());
            verifyReleasedOnce();
        } finally {
            caller.cleanup();
        }
    }

    private StmtExecutor leaveDeferredQuery() {
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        ctx.runCommand(() -> {
            long started = ctx.getStartTime();
            Mockito.when(executor.getDeferredQueryId()).thenReturn(new TUniqueId(1, 2));
            Mockito.when(executor.getDeferredExecTimeoutS()).thenReturn(5);
            Mockito.when(executor.getDeferredStartTimeMs()).thenReturn(started);
            ctx.addFlightSqlDeferredExecutor(executor);
            ctx.requestClose(CloseMode.AFTER_RESPONSE);
        });
        Assertions.assertTrue(ctx.isClosingAfterResponse());
        return executor;
    }

    @Test
    public void testDeferredQueryExpiryMakesGracefulCleanupReady() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 7;
        StmtExecutor executor = leaveDeferredQuery();
        long started = ctx.getStartTime();
        ctx.checkTimeout(started + 7000);
        Mockito.verify(executor, Mockito.never()).finalizeArrowFlightQuery();
        Assertions.assertSame(ctx, pool.getContext(ctx.getConnectionId()));
        ctx.checkTimeout(started + 7001);
        Mockito.verify(executor).finalizeArrowFlightQuery();
        verifyReleasedOnce();
        ctx.checkTimeout(started + 8000);
        Mockito.verify(executor).finalizeArrowFlightQuery();
    }

    @Test
    public void testDisabledDeferredExpiryStillHasSessionTimeout() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 0;
        ctx.getSessionVariable().waitTimeoutS = 1;
        StmtExecutor executor = leaveDeferredQuery();
        ctx.checkTimeout(ctx.getStartTime() + 1001);
        Mockito.verify(executor).finalizeArrowFlightQuery();
        verifyReleasedOnce();
    }
}
