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

package org.apache.doris.common.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Timeout(30)
class FairTaskExecutorTest {
    private FairTaskExecutor executor(int workers) {
        return new FairTaskExecutor(workers, task -> {
            Thread thread = new Thread(task);
            thread.setDaemon(true);
            return thread;
        });
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void groupsRoundRobinInsteadOfDrainingTheFirstQuery() throws Exception {
        try (FairTaskExecutor pool = executor(1)) {
            FairTaskExecutor.Group first = pool.newGroup(100);
            FairTaskExecutor.Group second = pool.newGroup(100);
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            first.execute(() -> {
                started.countDown();
                await(release);
            });
            await(started);
            List<String> order = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch done = new CountDownLatch(20);
            for (int i = 0; i < 10; i++) {
                first.execute(() -> {
                    order.add("A");
                    done.countDown();
                });
                second.execute(() -> {
                    order.add("B");
                    done.countDown();
                });
            }
            release.countDown();
            await(done);
            for (int i = 0; i < 20; i++) {
                Assertions.assertEquals(i % 2 == 0 ? "A" : "B", order.get(i));
            }
        }
    }

    @Test
    void oneGroupCanUseAllWorkers() {
        try (FairTaskExecutor pool = executor(4)) {
            FairTaskExecutor.Group group = pool.newGroup(4);
            CountDownLatch started = new CountDownLatch(4);
            CountDownLatch release = new CountDownLatch(1);
            for (int i = 0; i < 4; i++) {
                group.execute(() -> {
                    started.countDown();
                    await(release);
                });
            }
            try {
                await(started);
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void independentSingleWorkerPoolsCannotStarveChildren() throws Exception {
        try (FairTaskExecutor parents = executor(1); FairTaskExecutor children = executor(1)) {
            FairTaskExecutor.Group parent = parents.newGroup(1);
            FairTaskExecutor.Group child = children.newGroup(1);
            CompletableFuture<Integer> result = new CompletableFuture<>();
            parent.execute(() -> {
                CompletableFuture<Integer> value = new CompletableFuture<>();
                child.tryExecute(() -> value.complete(42), value::completeExceptionally, null);
                result.complete(value.join());
            });
            Assertions.assertEquals(42, result.get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void fullQueueResumesWithoutLosingWakeupsOrDuplicatingTasks() {
        try (FairTaskExecutor pool = executor(4)) {
            FairTaskExecutor.Group group = pool.newGroup(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            int lanes = 12;
            int rounds = 2000;
            CountDownLatch done = new CountDownLatch(lanes);
            AtomicInteger executions = new AtomicInteger();
            class Lane implements Runnable {
                int remaining = rounds;
                final Runnable admission = this::submit;

                void submit() {
                    group.tryExecute(this, failure::set, admission);
                }

                @Override
                public void run() {
                    executions.incrementAndGet();
                    if (--remaining == 0) {
                        done.countDown();
                    } else {
                        submit();
                    }
                }
            }

            for (int i = 0; i < lanes; i++) {
                new Lane().submit();
            }
            await(done);
            Assertions.assertNull(failure.get());
            Assertions.assertEquals(lanes * rounds, executions.get());
            Assertions.assertEquals(0, group.queuedTasks());
        }
    }

    @Test
    void closeRejectsQueuedWorkAndWakesCapacityWaitersExactlyOnce() {
        try (FairTaskExecutor pool = executor(1)) {
            FairTaskExecutor.Group group = pool.newGroup(1);
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            group.execute(() -> {
                started.countDown();
                await(release);
            });
            await(started);
            AtomicInteger rejected = new AtomicInteger();
            AtomicInteger resumed = new AtomicInteger();
            Assertions.assertTrue(group.tryExecute(() -> Assertions.fail("cancelled task ran"),
                    failure -> rejected.incrementAndGet(), null));
            Assertions.assertFalse(group.tryExecute(() -> Assertions.fail("unaccepted task ran"),
                    failure -> Assertions.fail(failure), resumed::incrementAndGet));
            try {
                group.close();
                group.close();
                Assertions.assertEquals(1, rejected.get());
                Assertions.assertEquals(1, resumed.get());
                Assertions.assertEquals(0, group.queuedTasks());
                Assertions.assertThrows(java.util.concurrent.RejectedExecutionException.class,
                        () -> group.execute(() -> { }));
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void admissionRacingCloseHasExactlyOneTerminalOutcome() throws Exception {
        try (FairTaskExecutor pool = executor(4)) {
            for (int round = 0; round < 100; round++) {
                FairTaskExecutor.Group group = pool.newGroup(8);
                AtomicInteger accepted = new AtomicInteger();
                AtomicInteger finished = new AtomicInteger();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch admitted = new CountDownLatch(1);
                List<Thread> submitters = new ArrayList<>();
                for (int lane = 0; lane < 4; lane++) {
                    Thread submitter = new Thread(() -> {
                        await(start);
                        for (int task = 0; task < 100; task++) {
                            java.util.concurrent.atomic.AtomicBoolean terminal =
                                    new java.util.concurrent.atomic.AtomicBoolean();
                            Runnable complete = () -> {
                                if (!terminal.compareAndSet(false, true)) {
                                    failure.set(new AssertionError("task completed twice"));
                                }
                                finished.incrementAndGet();
                            };
                            try {
                                if (group.tryExecute(complete, error -> complete.run(), null)) {
                                    accepted.incrementAndGet();
                                    admitted.countDown();
                                }
                            } catch (java.util.concurrent.RejectedExecutionException e) {
                                return;
                            }
                        }
                    });
                    submitter.start();
                    submitters.add(submitter);
                }
                start.countDown();
                await(admitted);
                group.close();
                for (Thread submitter : submitters) {
                    submitter.join(5000);
                    Assertions.assertFalse(submitter.isAlive());
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (finished.get() != accepted.get() && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                Assertions.assertNull(failure.get());
                Assertions.assertEquals(accepted.get(), finished.get());
                Assertions.assertEquals(0, group.queuedTasks());
            }
        }
    }

    @Test
    void failureIsDeliveredAndDoesNotRemoveTheWorker() throws Exception {
        try (FairTaskExecutor pool = executor(1)) {
            FairTaskExecutor.Group group = pool.newGroup(2);
            CompletableFuture<Throwable> failure = new CompletableFuture<>();
            AssertionError expected = new AssertionError("task failure");
            group.tryExecute(() -> {
                throw expected;
            }, failure::complete, null);
            Assertions.assertSame(expected, failure.get(10, TimeUnit.SECONDS));
            CompletableFuture<Boolean> next = new CompletableFuture<>();
            group.execute(() -> next.complete(true));
            Assertions.assertTrue(next.get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void closeCompletesRunningFuturesWithoutInterruptingSharedIo() throws Exception {
        FairTaskExecutor pool = executor(1);
        FairTaskExecutor.Group group = pool.newGroup(1);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        FutureTask<Integer> task = new FutureTask<>(() -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                failure.set(e);
            }
            return 1;
        });
        try {
            group.execute(task);
            await(started);
            group.close();
            Assertions.assertThrows(java.util.concurrent.CancellationException.class,
                    () -> task.get(1, TimeUnit.SECONDS));
            Assertions.assertNull(failure.get());
            pool.close();
            Assertions.assertFalse(pool.awaitTermination(1, TimeUnit.MILLISECONDS), "shared IO is still running");
        } finally {
            release.countDown();
            pool.close();
            Assertions.assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void idleWorkersWakeForRepeatedIsolatedArrivals() throws Exception {
        try (FairTaskExecutor pool = executor(4)) {
            FairTaskExecutor.Group group = pool.newGroup(1);
            for (int i = 0; i < 1000; i++) {
                CompletableFuture<Boolean> result = new CompletableFuture<>();
                group.execute(() -> result.complete(true));
                Assertions.assertTrue(result.get(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void serialContinuationsRetainTheWorkerWithoutRunningConcurrently() throws Exception {
        List<Thread> workers = new ArrayList<>();
        try (FairTaskExecutor pool = new FairTaskExecutor(4, task -> {
            Thread thread = new Thread(task);
            thread.setDaemon(true);
            workers.add(thread);
            return thread;
        })) {
            FairTaskExecutor.Group group = pool.newGroup(1);
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            CompletableFuture<Integer> done = new CompletableFuture<>();
            AtomicReference<Thread> owner = new AtomicReference<>();
            AtomicInteger runs = new AtomicInteger();
            AtomicInteger active = new AtomicInteger();
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    Assertions.assertEquals(1, active.incrementAndGet());
                    try {
                        if (runs.getAndIncrement() == 0) {
                            owner.set(Thread.currentThread());
                            started.countDown();
                            await(release);
                        }
                        Assertions.assertSame(owner.get(), Thread.currentThread());
                    } finally {
                        active.decrementAndGet();
                    }
                    if (runs.get() == 1000) {
                        done.complete(runs.get());
                    } else {
                        Assertions.assertTrue(group.tryExecuteContinuation(this, done::completeExceptionally, null));
                    }
                }
            };
            group.tryExecute(task, done::completeExceptionally, null);
            await(started);
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (workers.stream().anyMatch(thread -> thread != owner.get()
                        && thread.getState() != Thread.State.WAITING) && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                Assertions.assertTrue(workers.stream().allMatch(thread -> thread == owner.get()
                        || thread.getState() == Thread.State.WAITING));
            } finally {
                release.countDown();
            }
            Assertions.assertEquals(1000, done.get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void continuationGoesBehindOtherReadyQueries() throws Exception {
        try (FairTaskExecutor pool = executor(1)) {
            FairTaskExecutor.Group first = pool.newGroup(1);
            FairTaskExecutor.Group second = pool.newGroup(1);
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            CompletableFuture<Boolean> done = new CompletableFuture<>();
            List<String> order = new ArrayList<>();
            first.tryExecute(new Runnable() {
                int runs;

                @Override
                public void run() {
                    order.add("A");
                    if (++runs == 1) {
                        started.countDown();
                        await(release);
                        first.tryExecuteContinuation(this, done::completeExceptionally, null);
                    } else {
                        done.complete(true);
                    }
                }
            }, done::completeExceptionally, null);
            await(started);
            second.execute(() -> order.add("B"));
            release.countDown();
            Assertions.assertTrue(done.get(10, TimeUnit.SECONDS));
            Assertions.assertEquals(java.util.Arrays.asList("A", "B", "A"), order);
        }
    }

    @Test
    void continuationAdmissionFromAnotherPoolStillWakesAWorker() throws Exception {
        try (FairTaskExecutor parents = executor(1); FairTaskExecutor children = executor(1)) {
            FairTaskExecutor.Group parent = parents.newGroup(1);
            FairTaskExecutor.Group child = children.newGroup(1);
            CompletableFuture<Integer> result = new CompletableFuture<>();
            parent.execute(() -> {
                CompletableFuture<Integer> value = new CompletableFuture<>();
                child.tryExecuteContinuation(() -> value.complete(42), value::completeExceptionally, null);
                result.complete(value.join());
            });
            Assertions.assertEquals(42, result.get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void fullQueueContinuationResumesExactlyOnce() throws Exception {
        try (FairTaskExecutor pool = executor(1)) {
            FairTaskExecutor.Group group = pool.newGroup(1);
            CompletableFuture<Integer> done = new CompletableFuture<>();
            AtomicInteger resumed = new AtomicInteger();
            group.tryExecute(new Runnable() {
                int runs;
                final Runnable retry = () -> {
                    resumed.incrementAndGet();
                    group.tryExecute(this, done::completeExceptionally, null);
                };

                @Override
                public void run() {
                    if (++runs == 1) {
                        group.execute(() -> { });
                        Assertions.assertFalse(group.tryExecuteContinuation(this, done::completeExceptionally, retry));
                    } else {
                        done.complete(runs);
                    }
                }
            }, done::completeExceptionally, null);
            Assertions.assertEquals(2, done.get(10, TimeUnit.SECONDS));
            Assertions.assertEquals(1, resumed.get());
            Assertions.assertEquals(0, group.queuedTasks());
        }
    }

    @Test
    void closeRejectsAnAcceptedContinuationExactlyOnce() throws Exception {
        try (FairTaskExecutor pool = executor(1)) {
            FairTaskExecutor.Group group = pool.newGroup(1);
            CountDownLatch accepted = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger runs = new AtomicInteger();
            AtomicInteger rejected = new AtomicInteger();
            group.execute(new Runnable() {
                @Override
                public void run() {
                    runs.incrementAndGet();
                    group.tryExecuteContinuation(this, failure -> rejected.incrementAndGet(), null);
                    accepted.countDown();
                    // Simulate preemption between the final submission and return.
                    await(release);
                }
            });
            await(accepted);
            try {
                group.close();
                group.close();
                Assertions.assertEquals(1, rejected.get());
                Assertions.assertEquals(1, runs.get());
                Assertions.assertEquals(0, group.queuedTasks());
            } finally {
                release.countDown();
            }
        }
    }


    @Test
    void manyGroupsWithBoundedContinuationsMakeProgress() {
        try (FairTaskExecutor pool = executor(4)) {
            int groups = 8;
            int lanes = 4;
            int rounds = 1000;
            CountDownLatch done = new CountDownLatch(groups * lanes);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicInteger executions = new AtomicInteger();
            for (int i = 0; i < groups; i++) {
                FairTaskExecutor.Group group = pool.newGroup(1);
                class Lane implements Runnable {
                    int remaining = rounds;
                    final Runnable retry = this::submit;

                    void submit() {
                        group.tryExecute(this, failure::set, retry);
                    }

                    @Override
                    public void run() {
                        executions.incrementAndGet();
                        if (--remaining == 0) {
                            done.countDown();
                        } else {
                            group.tryExecuteContinuation(this, failure::set, retry);
                        }
                    }
                }

                for (int lane = 0; lane < lanes; lane++) {
                    Lane task = new Lane();
                    group.tryExecute(task, failure::set, task.retry);
                }
            }
            await(done);
            Assertions.assertNull(failure.get());
            Assertions.assertEquals(groups * lanes * rounds, executions.get());
        }
    }

    private static final class AsyncTask extends CompletableFuture<Integer> implements Runnable {
        private final CompletableFuture<Integer> shared;
        private final AtomicInteger cancellations = new AtomicInteger();

        private AsyncTask(CompletableFuture<Integer> shared) {
            this.shared = shared;
        }

        @Override
        public void run() {
            shared.whenComplete((value, failure) -> {
                if (failure == null) {
                    complete(value);
                } else {
                    completeExceptionally(failure);
                }
            });
        }

        @Override
        public boolean cancel(boolean interrupt) {
            cancellations.incrementAndGet();
            return super.cancel(interrupt);
        }
    }

    @Test
    void suspendedFutureReleasesWorkerButRemainsOwnedByItsQuery() throws Exception {
        try (FairTaskExecutor pool = executor(1)) {
            FairTaskExecutor.Group first = pool.newGroup(2);
            FairTaskExecutor.Group second = pool.newGroup(2);
            CompletableFuture<Integer> shared = new CompletableFuture<>();
            AsyncTask task = new AsyncTask(shared);
            first.execute(task);
            CompletableFuture<Boolean> marker = new CompletableFuture<>();
            // The sole worker must leave the suspended first query and run another query.
            second.execute(() -> marker.complete(true));
            Assertions.assertTrue(marker.get(5, TimeUnit.SECONDS));
            Assertions.assertFalse(task.isDone());
            first.close();
            Assertions.assertTrue(task.isCancelled(), "returned asynchronous work must still be cancelled");
            Assertions.assertEquals(1, task.cancellations.get());
            Assertions.assertFalse(shared.isDone(), "cancelling a query must not cancel its shared load");
            shared.complete(42);
            Assertions.assertTrue(task.isCancelled());
        }
    }

    @Test
    void completedAsyncTaskIsNotRetainedUntilQueryClose() throws Exception {
        try (FairTaskExecutor pool = executor(1)) {
            FairTaskExecutor.Group group = pool.newGroup(2);
            CompletableFuture<Integer> shared = new CompletableFuture<>();
            AsyncTask task = new AsyncTask(shared);
            group.execute(task);
            CompletableFuture<Boolean> returned = new CompletableFuture<>();
            group.execute(() -> returned.complete(true));
            Assertions.assertTrue(returned.get(5, TimeUnit.SECONDS));
            shared.complete(42);
            Assertions.assertEquals(42, task.get(5, TimeUnit.SECONDS));
            group.close();
            Assertions.assertEquals(0, task.cancellations.get(), "completed tasks must leave the ownership set");
        }
    }

}
