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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.spi.ConnectorOperationAbortedException;
import org.apache.doris.connector.spi.ConnectorOperationControl;

import shade.doris.hive.org.apache.thrift.TException;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Bridges one logical client invocation to every raw HMS attempt made by RetryingMetaStoreClient. */
final class HmsRemoteCallTracking {

    private static final long CONTROL_CHECK_MILLIS = 100L;
    private static final ThreadLocal<Context> CURRENT = new ThreadLocal<>();
    private static final ScheduledExecutorService CONTROL_WATCHDOG =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "hms-operation-control-watchdog");
                thread.setDaemon(true);
                return thread;
            });

    private HmsRemoteCallTracking() {
    }

    static <T> T withTracker(HmsPartitionBatchLoader.RemoteCallTracker tracker, int itemCount,
            ConnectorOperationControl operationControl, Callable<T> clientInvocation) throws Exception {
        Context previous = CURRENT.get();
        operationControl.checkActive();
        Context context = new Context(tracker, itemCount, operationControl, Thread.currentThread());
        CURRENT.set(context);
        ScheduledFuture<?> watchdog = operationControl == ConnectorOperationControl.NONE
                ? null : CONTROL_WATCHDOG.scheduleWithFixedDelay(
                        context::checkOperation, CONTROL_CHECK_MILLIS, CONTROL_CHECK_MILLIS, TimeUnit.MILLISECONDS);
        try {
            try {
                T result = clientInvocation.call();
                operationControl.checkActive();
                return result;
            } catch (Exception e) {
                ConnectorOperationAbortedException abort = context.getAbort();
                if (abort != null) {
                    throw abort;
                }
                if (causedByInterruptedException(e)) {
                    // RetryingMetaStoreClient's retry delay uses Thread.sleep. A direct Future.cancel(true)
                    // can interrupt that sleep before the watchdog observes the caller control. The Hive dynamic
                    // proxy wraps the undeclared InterruptedException in UndeclaredThrowableException, and sleep
                    // clears the flag while throwing. Restore it and preserve cancellation semantics; after a
                    // failed wire attempt the pooled client is ambiguous and the specialized abort taints it.
                    Thread.currentThread().interrupt();
                    throw context.interruptedAbort();
                }
                throw e;
            }
        } finally {
            context.finish();
            if (watchdog != null) {
                watchdog.cancel(false);
            }
            if (context.wasInterruptedByWatchdog()) {
                Thread.interrupted();
            }
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }

    private static boolean causedByInterruptedException(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof InterruptedException) {
                return true;
            }
        }
        return false;
    }

    static <T> T trackWireAttempt(ThriftCall<T> wireAttempt) throws TException {
        Context context = CURRENT.get();
        if (context == null) {
            return wireAttempt.call();
        }
        context.operationControl.checkActive();
        context.startWireAttempt();
        try {
            T result = context.tracker.call(context.itemCount, wireAttempt::call);
            context.finishWireAttempt(false);
            context.operationControl.checkActive();
            return result;
        } catch (TException e) {
            context.finishWireAttempt(true);
            throw e;
        } catch (RuntimeException e) {
            context.finishWireAttempt(false);
            throw e;
        } catch (Exception e) {
            context.finishWireAttempt(false);
            throw new TException(e);
        }
    }

    static void checkReconnectActive() {
        Context context = CURRENT.get();
        if (context != null) {
            context.checkRetryPhaseActive();
        }
    }

    static void markReconnectFailure() {
        Context context = CURRENT.get();
        if (context != null) {
            context.markReconnectFailure();
        }
    }

    static void markReconnectSuccess() {
        Context context = CURRENT.get();
        if (context != null) {
            context.markReconnectSuccess();
        }
    }

    static boolean shouldTaintClient(ConnectorOperationAbortedException abort) {
        return abort instanceof RetryPhaseOperationAbortedException;
    }

    @FunctionalInterface
    interface ThriftCall<T> {
        T call() throws TException;
    }

    private static final class Context {
        private final HmsPartitionBatchLoader.RemoteCallTracker tracker;
        private final int itemCount;
        private final ConnectorOperationControl operationControl;
        private final Thread invocationThread;
        private ConnectorOperationAbortedException abort;
        private boolean finished;
        private boolean interruptedByWatchdog;
        private boolean wireCallActive;
        private boolean retryingAfterWireFailure;
        private boolean clientUnsafeAfterReconnectFailure;

        private Context(HmsPartitionBatchLoader.RemoteCallTracker tracker, int itemCount,
                ConnectorOperationControl operationControl, Thread invocationThread) {
            this.tracker = tracker;
            this.itemCount = itemCount;
            this.operationControl = operationControl;
            this.invocationThread = invocationThread;
        }

        private void checkOperation() {
            try {
                operationControl.checkActive();
            } catch (ConnectorOperationAbortedException e) {
                synchronized (this) {
                    if (finished || abort != null) {
                        return;
                    }
                    if ((retryingAfterWireFailure || clientUnsafeAfterReconnectFailure) && !wireCallActive) {
                        abort = new RetryPhaseOperationAbortedException(e.getReason(), e.getMessage());
                        interruptedByWatchdog = true;
                        invocationThread.interrupt();
                    } else {
                        // Do not interrupt an active Thrift call: the transport may ignore interruption while the
                        // pooled client becomes ambiguous. The post-call check will return the abort as soon as the
                        // synchronous wire call finishes. Retry sleep/reconnect is interruptible because the first
                        // failed wire attempt has already made that pooled client unsafe to reuse.
                        abort = e;
                    }
                }
            }
        }

        private synchronized void startWireAttempt() {
            wireCallActive = true;
            retryingAfterWireFailure = false;
        }

        private synchronized void finishWireAttempt(boolean failed) {
            wireCallActive = false;
            retryingAfterWireFailure = failed;
            if (failed && abort != null && !interruptedByWatchdog) {
                abort = new RetryPhaseOperationAbortedException(abort.getReason(), abort.getMessage());
                interruptedByWatchdog = true;
                invocationThread.interrupt();
            }
        }

        private synchronized void markReconnectFailure() {
            clientUnsafeAfterReconnectFailure = true;
            if (abort != null && !interruptedByWatchdog) {
                abort = new RetryPhaseOperationAbortedException(abort.getReason(), abort.getMessage());
                interruptedByWatchdog = true;
                invocationThread.interrupt();
            }
        }

        private synchronized void markReconnectSuccess() {
            clientUnsafeAfterReconnectFailure = false;
        }

        private void checkRetryPhaseActive() {
            try {
                operationControl.checkActive();
            } catch (ConnectorOperationAbortedException e) {
                synchronized (this) {
                    if (retryingAfterWireFailure || clientUnsafeAfterReconnectFailure) {
                        throw new RetryPhaseOperationAbortedException(e.getReason(), e.getMessage());
                    }
                }
                // Hive may reconnect before the first wire attempt only because the configured socket lifetime
                // expired. Cancellation in that lifecycle reconnect does not make the healthy client ambiguous.
                throw e;
            }
        }

        private synchronized ConnectorOperationAbortedException getAbort() {
            return abort;
        }

        private synchronized ConnectorOperationAbortedException interruptedAbort() {
            ConnectorOperationAbortedException interrupted =
                    (retryingAfterWireFailure || clientUnsafeAfterReconnectFailure)
                    ? new RetryPhaseOperationAbortedException(
                            ConnectorOperationAbortedException.Reason.CANCELLED,
                            "HMS retry was interrupted")
                    : new ConnectorOperationAbortedException(
                            ConnectorOperationAbortedException.Reason.CANCELLED,
                            "HMS operation was interrupted");
            abort = interrupted;
            return interrupted;
        }

        private synchronized void finish() {
            finished = true;
        }

        private synchronized boolean wasInterruptedByWatchdog() {
            return interruptedByWatchdog;
        }
    }

    private static final class RetryPhaseOperationAbortedException
            extends ConnectorOperationAbortedException {
        private RetryPhaseOperationAbortedException(Reason reason, String message) {
            super(reason, message);
        }
    }
}
