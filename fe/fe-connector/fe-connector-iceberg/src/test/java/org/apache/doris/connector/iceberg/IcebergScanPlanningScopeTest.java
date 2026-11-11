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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.scan.ConnectorScanContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class IcebergScanPlanningScopeTest {
    @Test
    public void queuedCompletableFutureAndSubmitSettleWithoutWorkerAvailability() throws Exception {
        RecordingExecutor workers = new RecordingExecutor();
        ConnectorScanContext context = new ConnectorScanContext();
        IcebergScanPlanningScope scope = new IcebergScanPlanningScope(context, workers);
        AtomicInteger reads = new AtomicInteger();
        CompletableFuture<Integer> async = CompletableFuture.supplyAsync(() -> {
            // Mirrors the SDK AsyncSupply, whose supplier reaches the guarded FileIO.
            scope.checkCancelled();
            return reads.incrementAndGet();
        }, scope);
        Future<Integer> submitted = scope.submit(reads::incrementAndGet);
        context.cancel();
        Assertions.assertInstanceOf(CancellationException.class,
                Assertions.assertThrows(ExecutionException.class, () -> async.get(10, TimeUnit.SECONDS)).getCause());
        Assertions.assertInstanceOf(CancellationException.class,
                Assertions.assertThrows(ExecutionException.class, () -> submitted.get(10, TimeUnit.SECONDS)).getCause());
        scope.close();
        Assertions.assertTrue(scope.isTerminated());
        Assertions.assertEquals(0, reads.get());
        // Simulate the borrowed executor eventually dequeuing wrappers that cleanup already claimed.
        workers.queued.forEach(Runnable::run);
        Assertions.assertEquals(0, reads.get());
        Assertions.assertThrows(CancellationException.class, () -> scope.submit(reads::incrementAndGet));
    }

    @Test
    public void cancelledFutureDoesNotCompleteActualTermination() throws Exception {
        ExecutorService workers = Executors.newSingleThreadExecutor();
        ExecutorService closer = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ConnectorScanContext context = new ConnectorScanContext();
        IcebergScanPlanningScope scope = new IcebergScanPlanningScope(context, workers);
        try {
            Future<?> job = scope.submit(() -> {
                entered.countDown();
                Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                return null;
            });
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            job.cancel(false);
            context.cancel();
            Future<?> closing = closer.submit(scope::close);
            Assertions.assertFalse(scope.awaitTermination(50, TimeUnit.MILLISECONDS));
            Assertions.assertFalse(closing.isDone());
            release.countDown();
            closing.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(scope.isTerminated());
            Assertions.assertFalse(workers.isShutdown());
        } finally {
            release.countDown();
            workers.shutdownNow();
            closer.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
            Assertions.assertTrue(closer.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void cancellationRacingSubmissionReturnsBeforeBorrowedExecute() throws Exception {
        CountDownLatch submitting = new CountDownLatch(1);
        CountDownLatch enqueue = new CountDownLatch(1);
        RecordingExecutor workers = new RecordingExecutor() {
            @Override
            public void execute(Runnable task) {
                submitting.countDown();
                await(enqueue);
                super.execute(task);
            }
        };
        ConnectorScanContext context = new ConnectorScanContext();
        IcebergScanPlanningScope scope = new IcebergScanPlanningScope(context, workers);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        AtomicInteger reads = new AtomicInteger();
        try {
            Future<Future<Integer>> submission = caller.submit(() -> scope.submit(reads::incrementAndGet));
            Assertions.assertTrue(submitting.await(10, TimeUnit.SECONDS));
            context.cancel();
            Assertions.assertTrue(scope.awaitTermination(10, TimeUnit.SECONDS));
            Assertions.assertFalse(submission.isDone(), "cancel must not wait for borrowed execute to return");
            enqueue.countDown();
            Future<Integer> job = submission.get(10, TimeUnit.SECONDS);
            Assertions.assertInstanceOf(CancellationException.class,
                    Assertions.assertThrows(ExecutionException.class, job::get).getCause());
            scope.close();
            workers.queued.forEach(Runnable::run);
            Assertions.assertEquals(0, reads.get());
        } finally {
            enqueue.countDown();
            caller.shutdownNow();
            Assertions.assertTrue(caller.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void partialSubmissionFailureSettlesPreviouslyAcceptedSdkFutures() throws Exception {
        RecordingExecutor workers = new RecordingExecutor() {
            @Override
            public void execute(Runnable task) {
                if (!queued.isEmpty()) {
                    throw new RejectedExecutionException("controlled rejection");
                }
                super.execute(task);
            }
        };
        IcebergScanPlanningScope scope = new IcebergScanPlanningScope(new ConnectorScanContext(), workers);
        CompletableFuture<Integer> accepted = CompletableFuture.supplyAsync(() -> {
            scope.checkCancelled();
            return 1;
        }, scope);
        Assertions.assertThrows(RejectedExecutionException.class, () -> scope.submit(() -> 2));
        scope.close();
        Assertions.assertInstanceOf(CancellationException.class,
                Assertions.assertThrows(CompletionException.class, accepted::join).getCause());
        workers.queued.forEach(Runnable::run);
    }

    @Test
    public void sdkTaskPinsAndRestoresContextClassLoader() throws Exception {
        RecordingExecutor workers = new RecordingExecutor();
        IcebergScanPlanningScope scope = new IcebergScanPlanningScope(new ConnectorScanContext(), workers);
        Future<ClassLoader> loader = scope.submit(() -> Thread.currentThread().getContextClassLoader());
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        ClassLoader sentinel = new ClassLoader(previous) {};
        try {
            thread.setContextClassLoader(sentinel);
            workers.queued.get(0).run();
            Assertions.assertSame(sentinel, thread.getContextClassLoader());
            Assertions.assertSame(IcebergScanPlanningScope.class.getClassLoader(), loader.get());
        } finally {
            thread.setContextClassLoader(previous);
            scope.close();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static class RecordingExecutor extends AbstractExecutorService {
        protected final List<Runnable> queued = new ArrayList<>();

        @Override
        public void execute(Runnable task) {
            queued.add(task);
        }

        @Override
        public void shutdown() {
            Assertions.fail("borrowed executor must not be shut down");
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new AssertionError("borrowed executor must not be shut down");
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return false;
        }
    }
}
