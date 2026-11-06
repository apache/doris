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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.ConnectContext.CloseMode;
import org.apache.doris.qe.protocol.ProtocolAdapter;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Timeout(30)
public class ConnectContextLifecycleTest {
    private boolean savedRunningUnitTest;
    private Env env;
    private ProtocolAdapter adapter;
    private ConnectContext ctx;
    private ConnectPoolMgr pool;

    @BeforeEach
    public void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        env = ConnectPoolTestSupport.envAllowing(10);
        adapter = Mockito.mock(ProtocolAdapter.class);
        Mockito.when(adapter.type()).thenReturn(ConnectContext.ConnectType.MYSQL);
        ctx = Mockito.spy(new ConnectContext(adapter));
        ctx.setEnv(env);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        pool = new ConnectPoolMgr(10);
    }

    @AfterEach
    public void tearDown() {
        ctx.cleanup();
        ConnectContext.remove();
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    private void register() {
        Assertions.assertEquals(-1, ctx.registerClientSession(pool));
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    private static void stop(ExecutorService workers) throws InterruptedException {
        workers.shutdownNow();
        Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
    }

    private void verifyReleasedOnce() {
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter).releaseSession(ctx);
        Mockito.verify(ctx).closeTxn();
        Mockito.verify(ctx).deleteTempTable();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCloseAndQueryIdPublicationLeaveNoTraceIndex(boolean publishBeforeClose) throws Exception {
        ConnectScheduler scheduler = Mockito.mock(ConnectScheduler.class);
        Mockito.when(scheduler.getConnectPoolMgr()).thenReturn(pool);
        ctx.setConnectScheduler(scheduler);
        ctx.setTraceId("closing-query-trace");
        register();
        TUniqueId queryId = new TUniqueId(1, 2);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch exit = new CountDownLatch(1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> request = worker.submit(() -> ctx.runCommand(() -> {
                if (publishBeforeClose) {
                    ctx.setQueryId(queryId);
                }
                entered.countDown();
                await(exit);
                if (!publishBeforeClose) {
                    ctx.setQueryId(queryId);
                }
            }));
            await(entered);
            Assertions.assertEquals(publishBeforeClose ? DebugUtil.printId(queryId) : "",
                    pool.getQueryIdByTraceId("closing-query-trace"));
            ctx.cleanup();
            Assertions.assertEquals(0, pool.getConnectionNum());
            exit.countDown();
            request.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(queryId, ctx.queryId());
            Assertions.assertEquals("", pool.getQueryIdByTraceId("closing-query-trace"));
            verifyReleasedOnce();
        } finally {
            exit.countDown();
            stop(worker);
        }
    }

    @Test
    public void testPrivateContextNeedsNoActivation() {
        Assertions.assertTrue(ctx.runCommand(() -> Assertions.assertSame(ctx, ConnectContext.get())));
        Assertions.assertNull(ConnectContext.get());
        Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
        ctx.cleanup();
        Assertions.assertFalse(ctx.runCommand(() -> Assertions.fail("closed request ran")));
        verifyReleasedOnce();
    }

    @Test
    public void testRegistrationAndReentrantCloseReleaseOnce() {
        register();
        Assertions.assertThrows(IllegalStateException.class, () -> ctx.registerClientSession(pool));
        Mockito.doAnswer(call -> {
            ctx.cleanup();
            return null;
        }).when(adapter).closeConnection(ctx);
        Mockito.doAnswer(call -> {
            ctx.cleanup();
            ctx.requestClose(CloseMode.AFTER_RESPONSE);
            return null;
        }).when(adapter).releaseSession(ctx);
        ctx.cleanup();
        ctx.kill(true);
        ctx.cleanup();
        Assertions.assertEquals(0, pool.getConnectionNum());
        Assertions.assertEquals(0, pool.getUserConnectionMap().get(UserIdentity.ROOT.getQualifiedUser()).get());
        Assertions.assertThrows(ConnectContext.SessionClosedException.class, () -> ctx.registerClientSession(pool));
        Assertions.assertThrows(IllegalStateException.class, () -> ctx.setExecutor(Mockito.mock(StmtExecutor.class)));
        verifyReleasedOnce();
    }

    @Test
    public void testInitializationFailureDoesNotUnregisterAnotherContext() {
        ConnectContext registered = ConnectPoolTestSupport.mysqlConnection(env, UserIdentity.ROOT);
        Assertions.assertEquals(-1, registered.registerClientSession(pool));
        try {
            ctx.runCommand(() -> {
                ctx.cleanup();
                Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
                Assertions.assertThrows(ConnectContext.SessionClosedException.class,
                        () -> ctx.registerClientSession(pool));
            });
            Assertions.assertSame(registered, pool.getContext(0));
            Assertions.assertEquals(1, pool.getConnectionNum());
            verifyReleasedOnce();
        } finally {
            registered.cleanup();
        }
    }

    @Test
    public void testQuotaRefusalUsesOwnedExit() {
        pool = new ConnectPoolMgr(0);
        ctx.runCommand(() -> {
            Assertions.assertEquals(0, ctx.registerClientSession(pool));
            ctx.cleanup();
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
        });
        Assertions.assertEquals(0, pool.getConnectionNum());
        verifyReleasedOnce();
    }

    @Test
    public void testImmediateCloseDefersCleanupAndCancelsLateExecutor() {
        register();
        ctx.runCommand(() -> {
            StmtExecutor running = Mockito.mock(StmtExecutor.class);
            ctx.setExecutor(running);
            ctx.cleanup();
            Assertions.assertSame(ctx, ConnectContext.get());
            Assertions.assertNull(pool.getContext(ctx.getConnectionId()));
            Mockito.verify(running).cancel(Mockito.any(Status.class), Mockito.eq(false));
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
            StmtExecutor late = Mockito.mock(StmtExecutor.class);
            ctx.setExecutor(late);
            Mockito.verify(late).cancel(Mockito.any(Status.class), Mockito.eq(false));
        });
        Assertions.assertNull(ConnectContext.get());
        Assertions.assertNull(ctx.getExecutor());
        verifyReleasedOnce();
    }

    @Test
    public void testDeferredClosePreservesResponseAndSkipsRearm() {
        register();
        Runnable rearm = Mockito.mock(Runnable.class);
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        ctx.runCommand(() -> {
            ctx.setExecutor(running);
            ctx.requestClose(CloseMode.AFTER_RESPONSE);
            ctx.requestClose(CloseMode.AFTER_RESPONSE);
            Assertions.assertTrue(ctx.isClosingAfterResponse());
            Assertions.assertEquals(1, pool.getConnectionNum());
            Mockito.verify(adapter, Mockito.never()).closeConnection(ctx);
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
            StmtExecutor late = Mockito.mock(StmtExecutor.class);
            ctx.setExecutor(late);
            Mockito.verifyNoInteractions(late);
        }, rearm);
        Mockito.verifyNoInteractions(running, rearm);
        Assertions.assertFalse(ctx.isClosingAfterResponse());
        verifyReleasedOnce();
    }

    @Test
    public void testImmediateCloseUpgradesDeferredCloseBeforeOwnerExit() {
        register();
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        ctx.runCommand(() -> {
            ctx.setExecutor(running);
            ctx.requestClose(CloseMode.AFTER_RESPONSE);
            ctx.cleanup();
            ctx.requestClose(CloseMode.AFTER_RESPONSE);
            Assertions.assertFalse(ctx.isClosingAfterResponse());
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
        });
        Mockito.verify(running).cancel(Mockito.any(Status.class), Mockito.eq(false));
        verifyReleasedOnce();
    }

    @Test
    public void testIdleDeferredCloseRunsInline() {
        register();
        ctx.requestClose(CloseMode.AFTER_RESPONSE);
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Assertions.assertEquals(0, pool.getConnectionNum());
        verifyReleasedOnce();
    }

    @ParameterizedTest
    @EnumSource(CloseMode.class)
    public void testCleanupDoesNotReadReleasedProtocolState(CloseMode mode) {
        register();
        AtomicBoolean released = new AtomicBoolean();
        AtomicBoolean readAfterRelease = new AtomicBoolean();
        Mockito.doAnswer(call -> {
            released.set(true);
            return null;
        }).when(adapter).releaseSession(ctx);
        Mockito.when(adapter.hasPendingResponse()).thenAnswer(call -> {
            if (released.get()) {
                readAfterRelease.set(true);
            }
            return false;
        });

        ctx.runCommand(() -> ctx.requestClose(mode));

        verifyReleasedOnce();
        Assertions.assertFalse(readAfterRelease.get(), "Cleanup consulted protocol state after releasing it");
        Assertions.assertEquals(0, pool.getConnectionNum());
    }

    @Test
    public void testRequestExitsBeforeCancellationHandsOffCleanup() throws Exception {
        register();
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch exitRequest = new CountDownLatch(1);
        CountDownLatch cancelling = new CountDownLatch(1);
        CountDownLatch finishCancellation = new CountDownLatch(1);
        AtomicBoolean cancellationFinished = new AtomicBoolean();
        AtomicReference<Thread> closer = new AtomicReference<>();
        AtomicReference<Thread> releaser = new AtomicReference<>();
        Mockito.doAnswer(call -> {
            ctx.cleanup(); // Reentrant cancellation must not recursively run cleanup.
            cancelling.countDown();
            cancellationFinished.set(finishCancellation.await(10, TimeUnit.SECONDS));
            return null;
        }).when(running).cancel(Mockito.any(Status.class), Mockito.eq(false));
        Mockito.doAnswer(call -> {
            releaser.set(Thread.currentThread());
            return null;
        }).when(adapter).releaseSession(ctx);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> request = workers.submit(() -> ctx.runCommand(() -> {
                ctx.setExecutor(running);
                ctx.requestClose(CloseMode.AFTER_RESPONSE);
                entered.countDown();
                await(exitRequest);
            }));
            await(entered);
            Future<?> close = workers.submit(() -> {
                closer.set(Thread.currentThread());
                ctx.cleanup();
            });
            await(cancelling);
            exitRequest.countDown();
            request.get(10, TimeUnit.SECONDS);
            ctx.cleanup();
            Mockito.verify(adapter).closeConnection(ctx);
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
            finishCancellation.countDown();
            close.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(cancellationFinished.get());
            Assertions.assertSame(closer.get(), releaser.get());
            verifyReleasedOnce();
        } finally {
            exitRequest.countDown();
            finishCancellation.countDown();
            stop(workers);
        }
    }

    @Test
    public void testImmediateCloseDuringResponseCheckWaitsForCancellation() throws Exception {
        register();
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        CountDownLatch checkingResponse = new CountDownLatch(1);
        CountDownLatch finishResponseCheck = new CountDownLatch(1);
        CountDownLatch cancelling = new CountDownLatch(1);
        CountDownLatch finishCancellation = new CountDownLatch(1);
        AtomicBoolean responseCheckFinished = new AtomicBoolean();
        AtomicBoolean cancellationFinished = new AtomicBoolean();
        AtomicReference<Thread> closer = new AtomicReference<>();
        AtomicReference<Thread> releaser = new AtomicReference<>();
        Mockito.when(adapter.hasPendingResponse()).thenAnswer(call -> {
            checkingResponse.countDown();
            responseCheckFinished.set(finishResponseCheck.await(10, TimeUnit.SECONDS));
            return false;
        });
        Mockito.doAnswer(call -> {
            cancelling.countDown();
            cancellationFinished.set(finishCancellation.await(10, TimeUnit.SECONDS));
            return null;
        }).when(running).cancel(Mockito.any(Status.class), Mockito.eq(false));
        Mockito.doAnswer(call -> {
            releaser.set(Thread.currentThread());
            return null;
        }).when(adapter).releaseSession(ctx);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> request = workers.submit(() -> ctx.runCommand(() -> {
                ctx.setExecutor(running);
                ctx.requestClose(CloseMode.AFTER_RESPONSE);
            }));
            await(checkingResponse);
            Future<?> close = workers.submit(() -> {
                closer.set(Thread.currentThread());
                ctx.cleanup();
            });
            await(cancelling);
            finishResponseCheck.countDown();
            request.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(responseCheckFinished.get());
            Assertions.assertEquals(0, pool.getConnectionNum());
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);

            finishCancellation.countDown();
            close.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(cancellationFinished.get());
            Assertions.assertSame(closer.get(), releaser.get());
            verifyReleasedOnce();
        } finally {
            finishResponseCheck.countDown();
            finishCancellation.countDown();
            stop(workers);
        }
    }

    @Test
    public void testCancellationFinishesBeforeRequestHandsOff() throws Exception {
        register();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch exitRequest = new CountDownLatch(1);
        AtomicReference<Thread> requestThread = new AtomicReference<>();
        AtomicReference<Thread> releaser = new AtomicReference<>();
        Mockito.doAnswer(call -> {
            releaser.set(Thread.currentThread());
            return null;
        }).when(adapter).releaseSession(ctx);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> request = worker.submit(() -> ctx.runCommand(() -> {
                requestThread.set(Thread.currentThread());
                entered.countDown();
                await(exitRequest);
            }));
            await(entered);
            ctx.cleanup();
            Mockito.verify(adapter).closeConnection(ctx);
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
            exitRequest.countDown();
            request.get(10, TimeUnit.SECONDS);
            Assertions.assertSame(requestThread.get(), releaser.get());
            verifyReleasedOnce();
        } finally {
            exitRequest.countDown();
            stop(worker);
        }
    }

    @RepeatedTest(10)
    public void testDeferredCloseRacesWithOwnerExit() throws Exception {
        pool = Mockito.spy(pool);
        register();
        CyclicBarrier handoff = new CyclicBarrier(2);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> request = worker.submit(() -> ctx.runCommand(() -> handoff.await(10, TimeUnit.SECONDS)));
            handoff.await(10, TimeUnit.SECONDS);
            ctx.requestClose(CloseMode.AFTER_RESPONSE);
            request.get(10, TimeUnit.SECONDS);
            Mockito.verify(pool).unregisterConnection(ctx);
            verifyReleasedOnce();
        } finally {
            stop(worker);
        }
    }

    @Test
    public void testConcurrentCloseDuringGracefulCleanupDoesNotCancelOrReleaseTwice() throws Exception {
        register();
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch finishClose = new CountDownLatch(1);
        AtomicBoolean transportClosed = new AtomicBoolean();
        Mockito.doAnswer(call -> {
            closing.countDown();
            transportClosed.set(finishClose.await(10, TimeUnit.SECONDS));
            return null;
        }).when(adapter).closeConnection(ctx);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> close = worker.submit(() -> ctx.requestClose(CloseMode.AFTER_RESPONSE));
            await(closing);
            ctx.cleanup();
            Assertions.assertFalse(ctx.runCommand(() -> Assertions.fail("closed request ran")));
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
            finishClose.countDown();
            close.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(transportClosed.get());
            verifyReleasedOnce();
        } finally {
            finishClose.countDown();
            stop(worker);
        }
    }

    @Test
    public void testFailuresStillHandOffCleanupAndAttemptRemainingResources() {
        pool = Mockito.spy(pool);
        register();
        Mockito.doAnswer(call -> {
            call.callRealMethod();
            throw new IllegalStateException("unregister failed");
        }).when(pool).unregisterConnection(ctx);
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        Mockito.doThrow(new IllegalStateException("cancel failed"))
                .when(running).cancel(Mockito.any(Status.class), Mockito.eq(false));
        Mockito.doThrow(new IllegalStateException("transport failed")).when(adapter).closeConnection(ctx);
        Mockito.doAnswer(call -> {
            ctx.cleanup();
            throw new IllegalStateException("release failed");
        }).when(adapter).releaseSession(ctx);
        ctx.runCommand(() -> {
            ctx.setExecutor(running);
            ctx.cleanup();
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
        });
        ctx.cleanup();
        Assertions.assertEquals(0, pool.getConnectionNum());
        Mockito.verify(pool).unregisterConnection(ctx);
        Mockito.verify(running).cancel(Mockito.any(Status.class), Mockito.eq(false));
        verifyReleasedOnce();
    }

    @Test
    public void testDeferredUnregistrationFailureStillHandsOff() {
        pool = Mockito.spy(pool);
        register();
        Mockito.doAnswer(call -> {
            call.callRealMethod();
            throw new IllegalStateException("unregister failed");
        }).when(pool).unregisterConnection(ctx);
        ctx.runCommand(() -> ctx.requestClose(CloseMode.AFTER_RESPONSE));
        Mockito.verify(pool).unregisterConnection(ctx);
        verifyReleasedOnce();
    }

    @Test
    public void testBodyExceptionRestoresBindingAndReturnsExecution() {
        ConnectContext caller = new ConnectContext();
        caller.setThreadLocalInfo();
        IOException failure = new IOException("command failed");
        try {
            Assertions.assertSame(failure, Assertions.assertThrows(IOException.class,
                    () -> ctx.runCommand(() -> {
                        throw failure;
                    })));
            Assertions.assertSame(caller, ConnectContext.get());
            Assertions.assertTrue(ctx.runCommand(() -> Assertions.assertSame(ctx, ConnectContext.get())));
            Assertions.assertSame(caller, ConnectContext.get());
        } finally {
            caller.cleanup();
        }
    }

    @Test
    public void testRearmRunsAfterBusinessFinallyAndBindingRestore() {
        ConnectContext caller = new ConnectContext();
        caller.setThreadLocalInfo();
        StringBuilder order = new StringBuilder();
        try {
            ctx.runCommand(() -> {
                try {
                    order.append("body/");
                } finally {
                    order.append("finally/");
                }
            }, () -> {
                Assertions.assertSame(caller, ConnectContext.get());
                order.append("rearm");
            });
            Assertions.assertEquals("body/finally/rearm", order.toString());
            Assertions.assertTrue(ctx.runCommand(() -> { }));
        } finally {
            caller.cleanup();
        }
    }

    @Test
    public void testRearmFailureClosesAndDoesNotStrandExecution() {
        RuntimeException failure = new RuntimeException("resume failed");
        Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class,
                () -> ctx.runCommand(() -> { }, () -> {
                    throw failure;
                })));
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Assertions.assertFalse(ctx.runCommand(() -> Assertions.fail("closed request ran")));
        verifyReleasedOnce();
    }

    @Test
    public void testNestedEntryIsRejectedWithoutDamagingOwner() {
        ctx.runCommand(() -> {
            Assertions.assertThrows(IllegalStateException.class, () -> ctx.runCommand(() -> { }));
            Assertions.assertSame(ctx, ConnectContext.get());
            ctx.requestClose(CloseMode.AFTER_RESPONSE);
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
        });
        verifyReleasedOnce();
    }

    @Test
    public void testFinalCleanupBindsClosingContextAndRestoresCaller() {
        ConnectContext caller = new ConnectContext();
        caller.setThreadLocalInfo();
        AtomicReference<ConnectContext> rollbackContext = new AtomicReference<>();
        Mockito.doAnswer(call -> {
            rollbackContext.set(ConnectContext.get());
            return null;
        }).when(ctx).closeTxn();
        ctx.cleanup();
        Assertions.assertSame(ctx, rollbackContext.get());
        Assertions.assertSame(caller, ConnectContext.get());
        caller.cleanup();
        Assertions.assertNull(ConnectContext.get());
        verifyReleasedOnce();
    }

    @Test
    public void testRegistrationAndCloseCannotLeavePoolEntry() throws Exception {
        CountDownLatch reserving = new CountDownLatch(1);
        CountDownLatch finishRegistration = new CountDownLatch(1);
        Mockito.when(env.getAuth().getMaxConn(Mockito.anyString())).thenAnswer(call -> {
            reserving.countDown();
            await(finishRegistration);
            return 10L;
        });
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> registration = workers.submit(() -> {
                try {
                    Assertions.assertEquals(-1, ctx.registerClientSession(pool));
                } catch (ConnectContext.SessionClosedException closed) {
                    // Closing may now win while the property lookup runs outside the gate.
                    Assertions.assertTrue(ctx.isClosingOrClosed());
                }
            });
            await(reserving);
            Future<?> close = workers.submit(ctx::cleanup);
            finishRegistration.countDown();
            registration.get(10, TimeUnit.SECONDS);
            close.get(10, TimeUnit.SECONDS);
            Assertions.assertNull(pool.getContext(ctx.getConnectionId()));
            Assertions.assertEquals(0, pool.getConnectionNum());
            verifyReleasedOnce();
        } finally {
            finishRegistration.countDown();
            stop(workers);
        }
    }

    @Test
    public void testFlightMetadataIsNotIdleWhileItOwnsExecution() {
        Mockito.when(adapter.type()).thenReturn(ConnectContext.ConnectType.ARROW_FLIGHT_SQL);
        Mockito.doReturn("metadata-token").when(ctx).getPeerIdentity();
        register();
        ctx.getSessionVariable().waitTimeoutS = 1;
        ctx.runCommand(() -> {
            ctx.checkTimeout(ctx.getStartTime() + 1001);
            Assertions.assertFalse(ctx.isClosingOrClosed());
        });
        ctx.checkTimeout(ctx.getStartTime() + 1001);
        Assertions.assertTrue(ctx.isClosingOrClosed());
        verifyReleasedOnce();
    }

    @Test
    public void testMysqlIncompletePacketCanTimeOutWhileReading() {
        register();
        ctx.getSessionVariable().waitTimeoutS = 1;
        ctx.runCommand(() -> {
            ctx.checkTimeout(ctx.getStartTime() + 1001);
            Assertions.assertTrue(ctx.isClosingOrClosed());
            Mockito.verify(adapter).closeConnection(ctx);
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
        });
        verifyReleasedOnce();
    }

    @Test
    public void testNewCommandRefreshesIdleDeadline() {
        register();
        ctx.getSessionVariable().waitTimeoutS = 1;
        ctx.startTime = 1000;
        ctx.runCommand(() -> ctx.checkTimeout(2001));
        Assertions.assertFalse(ctx.isClosingOrClosed());
    }

    @Test
    public void testQueryTimeoutAndKillQueryRetainSession() {
        register();
        ctx.setCommand(MysqlCommand.COM_QUERY);
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        ctx.setExecutor(running);
        ctx.checkTimeout(ctx.getStartTime() + ctx.getExecTimeoutS() * 1000L + 1);
        ArgumentCaptor<Status> reason = ArgumentCaptor.forClass(Status.class);
        Mockito.verify(running).cancel(reason.capture(), Mockito.eq(false));
        Assertions.assertEquals(TStatusCode.TIMEOUT, reason.getValue().getErrorCode());
        ctx.kill(false);
        Mockito.verify(running).cancel(Mockito.any(Status.class));
        Assertions.assertFalse(ctx.isClosingOrClosed());
        Assertions.assertSame(ctx, pool.getContext(ctx.getConnectionId()));
    }

    @Test
    public void testCloseInterruptsInitializationWhileQuotaLookupIsBlocked() throws Exception {
        CountDownLatch lookup = new CountDownLatch(1);
        CountDownLatch returnProperty = new CountDownLatch(1);
        Auth auth = env.getAuth();
        Mockito.doAnswer(call -> {
            lookup.countDown();
            await(returnProperty);
            return 10L;
        }).when(auth).getMaxConn(UserIdentity.ROOT.getQualifiedUser());
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> initialize = workers.submit(() -> ctx.runCommand(() ->
                    Assertions.assertThrows(ConnectContext.SessionClosedException.class,
                            () -> ctx.registerClientSession(pool))));
            await(lookup);
            workers.submit(ctx::cleanup).get(5, TimeUnit.SECONDS);
            Mockito.verify(adapter).closeConnection(ctx);
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
            Assertions.assertTrue(ctx.isClosingOrClosed());
            returnProperty.countDown();
            initialize.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(0, pool.getConnectionNum());
            verifyReleasedOnce();
        } finally {
            returnProperty.countDown();
            stop(workers);
        }
    }

    @Test
    public void testCloseInterruptsQueryWhileTimeoutPropertyLookupIsBlocked() throws Exception {
        register();
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch exit = new CountDownLatch(1);
        CountDownLatch lookup = new CountDownLatch(1);
        CountDownLatch returnProperty = new CountDownLatch(1);
        Auth auth = env.getAuth();
        Mockito.doAnswer(call -> {
            lookup.countDown();
            await(returnProperty);
            return 1;
        }).when(auth).getQueryTimeout(UserIdentity.ROOT.getQualifiedUser());
        ExecutorService workers = Executors.newFixedThreadPool(3);
        try {
            Future<?> query = workers.submit(() -> ctx.runCommand(() -> {
                ctx.setCommand(MysqlCommand.COM_QUERY);
                ctx.setExecutor(running);
                entered.countDown();
                await(exit);
            }));
            await(entered);
            long now = ctx.getStartTime() + 2000;
            Future<?> timeout = workers.submit(() -> ctx.checkTimeout(now));
            await(lookup);
            workers.submit(ctx::cleanup).get(5, TimeUnit.SECONDS);
            Mockito.verify(adapter).closeConnection(ctx);
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
            returnProperty.countDown();
            timeout.get(10, TimeUnit.SECONDS);
            // Only immediate close cancels: the stale timeout decision observes STOPPING and exits.
            ArgumentCaptor<Status> reason = ArgumentCaptor.forClass(Status.class);
            Mockito.verify(running).cancel(reason.capture(), Mockito.eq(false));
            Assertions.assertEquals(TStatusCode.CANCELLED, reason.getValue().getErrorCode());
            exit.countDown();
            query.get(10, TimeUnit.SECONDS);
            verifyReleasedOnce();
        } finally {
            returnProperty.countDown();
            exit.countDown();
            stop(workers);
        }
    }

    @Test
    public void testTimeoutLookupDoesNotApplyPreviousQueryLimitToNextStatement() throws Exception {
        register();
        StmtExecutor earlier = Mockito.mock(StmtExecutor.class);
        StmtExecutor next = Mockito.mock(StmtExecutor.class);
        Mockito.when(next.isSyncLoadKindStmt()).thenReturn(true);
        ctx.setCommand(MysqlCommand.COM_QUERY);
        ctx.setExecutor(earlier);
        long start = ctx.getStartTime();
        CountDownLatch lookup = new CountDownLatch(1);
        CountDownLatch returnProperty = new CountDownLatch(1);
        Auth auth = env.getAuth();
        Mockito.doAnswer(call -> {
            lookup.countDown();
            await(returnProperty);
            return 1;
        }).when(auth).getQueryTimeout(UserIdentity.ROOT.getQualifiedUser());
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> timeout = workers.submit(() -> ctx.checkTimeout(start + 2000));
            await(lookup);
            workers.submit(() -> ctx.runCommand(() -> {
                ctx.setExecutor(next);
                // Consecutive short statements can share a millisecond start time. Identity must
                // still be rechecked; the preceding SELECT's limit is not this INSERT's limit.
                ctx.startTime = start;
                ctx.getSessionVariable().setInsertTimeoutS(100);
            })).get(5, TimeUnit.SECONDS);
            returnProperty.countDown();
            timeout.get(10, TimeUnit.SECONDS);
            Mockito.verify(earlier, Mockito.never()).cancel(Mockito.any(Status.class), Mockito.anyBoolean());
            Mockito.verify(next, Mockito.never()).cancel(Mockito.any(Status.class), Mockito.anyBoolean());
            Assertions.assertFalse(ctx.isClosingOrClosed());
        } finally {
            returnProperty.countDown();
            stop(workers);
        }
    }

}
