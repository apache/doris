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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.datasource.SplitAssignment;
import org.apache.doris.datasource.hudi.HudiFsViewCacheValue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class HudiBatchFsViewOwnerTest {

    @Test
    void statementCloseReturnsWhileRunningTaskKeepsLeasePinned() throws Exception {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(assignment, lease);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> close = executor.submit(owner::close);
            Mockito.verify(assignment, Mockito.timeout(3000)).stop();
            close.get(3, TimeUnit.SECONDS);
            Mockito.verify(lease, Mockito.never()).close();

            owner.finish();

            Mockito.verify(lease).close();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void normalCompletionDoesNotStopFinishedAssignment() {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(assignment, lease);

        owner.finish();
        owner.close();

        Mockito.verify(assignment, Mockito.never()).stop();
        Mockito.verify(lease).close();
    }

    @Test
    void statementCloseCancelsAcceptedTaskBeforeItStarts() {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(assignment, lease);
        HudiScanNode.TerminalTask task = new HudiScanNode.TerminalTask(
                () -> Assertions.fail("cancelled task must not run"), owner::finish);
        owner.track(task);

        owner.close();

        Assertions.assertTrue(task.isCancelled());
        Mockito.verify(assignment).stop();
        Mockito.verify(lease).close();
    }

    @Test
    void statementCloseDoesNotWaitForAlreadyStartedBlockedTask() throws Exception {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(assignment, lease);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch terminal = new CountDownLatch(1);
        HudiScanNode.TerminalTask task = new HudiScanNode.TerminalTask(() -> {
            started.countDown();
            while (release.getCount() > 0) {
                try {
                    release.await(3, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    interrupted.countDown();
                }
            }
        }, () -> {
            owner.finish();
            terminal.countDown();
        });
        owner.track(task);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.execute(task);
            Assertions.assertTrue(started.await(3, TimeUnit.SECONDS));

            owner.close();

            Mockito.verify(assignment).stop();
            Assertions.assertTrue(interrupted.await(3, TimeUnit.SECONDS));
            Mockito.verify(lease, Mockito.never()).close();
            release.countDown();
            Assertions.assertTrue(terminal.await(3, TimeUnit.SECONDS));
            Mockito.verify(lease).close();
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void assignmentStopInterruptsStartedTaskAndRetainsLeaseUntilTerminal() throws Exception {
        SplitAssignment assignment = new SplitAssignment(
                null, null, null, Collections.emptyMap(), Collections.emptyList(), false);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(assignment, lease);
        assignment.addCloseable(owner);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch terminal = new CountDownLatch(1);
        HudiScanNode.TerminalTask task = new HudiScanNode.TerminalTask(() -> {
            started.countDown();
            while (release.getCount() > 0) {
                try {
                    release.await(3, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    interrupted.countDown();
                }
            }
        }, () -> {
            owner.finish();
            terminal.countDown();
        });
        owner.track(task);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.execute(task);
            Assertions.assertTrue(started.await(3, TimeUnit.SECONDS));

            assignment.stop();

            Assertions.assertTrue(interrupted.await(3, TimeUnit.SECONDS));
            Mockito.verify(lease, Mockito.never()).close();
            release.countDown();
            Assertions.assertTrue(terminal.await(3, TimeUnit.SECONDS));
            Mockito.verify(lease).close();
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void synchronousListingCancellationReturnsBeforeBlockedTaskAndRetainsLease() throws Exception {
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        HudiScanNode.ListingFsViewOwner owner = new HudiScanNode.ListingFsViewOwner(lease, executor);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch terminated = new CountDownLatch(1);
        HudiScanNode.TerminalTask task = new HudiScanNode.TerminalTask(() -> {
            started.countDown();
            try {
                while (release.getCount() > 0) {
                    try {
                        release.await(3, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                        // Model storage code that does not terminate when interrupted.
                    }
                }
            } finally {
                terminated.countDown();
            }
        }, () -> { });
        Assertions.assertTrue(owner.submit(task));
        owner.submissionDone();
        try {
            Assertions.assertTrue(started.await(3, TimeUnit.SECONDS));
            Future<?> waiter = executor.submit(() ->
                    Assertions.assertThrows(CancellationException.class, owner::awaitCompletion));

            owner.close();

            waiter.get(3, TimeUnit.SECONDS);
            Mockito.verify(lease, Mockito.never()).close();
            release.countDown();
            Assertions.assertTrue(terminated.await(3, TimeUnit.SECONDS));
            Mockito.verify(lease, Mockito.timeout(3000)).close();
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void synchronousListingDiscardBeforeSubmissionReleasesLease() {
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.ListingFsViewOwner owner = new HudiScanNode.ListingFsViewOwner(lease, Runnable::run);

        owner.discardBeforeSubmission();

        Mockito.verify(lease).close();
        Assertions.assertDoesNotThrow(owner::awaitCompletion);
    }

    @Test
    void synchronousListingCancellationWinsAfterTerminalTasksCompleteInline() {
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        List<Runnable> submitted = new ArrayList<>();
        HudiScanNode.ListingFsViewOwner owner = new HudiScanNode.ListingFsViewOwner(lease, submitted::add);
        HudiScanNode.TerminalTask completed = new HudiScanNode.TerminalTask(() -> { }, () -> { });
        HudiScanNode.TerminalTask queued = new HudiScanNode.TerminalTask(
                () -> Assertions.fail("cancelled task must not run"), () -> { });
        Assertions.assertTrue(owner.submit(completed));
        Assertions.assertTrue(owner.submit(queued));
        completed.run();
        owner.submissionDone();

        owner.close();

        Assertions.assertEquals(2, submitted.size());
        Assertions.assertTrue(queued.isCancelled());
        Mockito.verify(lease).close();
        Assertions.assertThrows(CancellationException.class, owner::awaitCompletion);
    }

    @Test
    void synchronousListingCancellationRemovesQueuedTask() throws Exception {
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        executor.execute(() -> {
            workerStarted.countDown();
            try {
                releaseWorker.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            Assertions.assertTrue(workerStarted.await(3, TimeUnit.SECONDS));
            HudiScanNode.ListingFsViewOwner owner = new HudiScanNode.ListingFsViewOwner(lease, executor);
            HudiScanNode.TerminalTask queued = new HudiScanNode.TerminalTask(
                    () -> Assertions.fail("cancelled task must not run"), () -> { });
            Assertions.assertTrue(owner.submit(queued));
            Assertions.assertEquals(1, executor.getQueue().size());

            owner.close();

            Assertions.assertTrue(queued.isCancelled());
            Assertions.assertTrue(executor.getQueue().isEmpty());
            owner.submissionDone();
            Mockito.verify(lease).close();
            Assertions.assertThrows(CancellationException.class, owner::awaitCompletion);
        } finally {
            releaseWorker.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void synchronousListingCloseStopsBlockedAndLaterSubmissions() throws Exception {
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        CountDownLatch rejected = new CountDownLatch(1);
        ThreadPoolManager.BlockedPolicy blockedPolicy = new ThreadPoolManager.BlockedPolicy("test", 10);
        RejectedExecutionHandler handler = (task, executor) -> {
            rejected.countDown();
            blockedPolicy.rejectedExecution(task, executor);
        };
        ThreadPoolExecutor listingExecutor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1), handler);
        Runnable queued = () -> { };
        listingExecutor.execute(() -> {
            workerStarted.countDown();
            try {
                releaseWorker.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Assertions.assertTrue(workerStarted.await(3, TimeUnit.SECONDS));
        listingExecutor.execute(queued);
        HudiScanNode.ListingFsViewOwner owner = new HudiScanNode.ListingFsViewOwner(lease, listingExecutor);
        HudiScanNode.TerminalTask blocked = new HudiScanNode.TerminalTask(() -> { }, () -> { });
        HudiScanNode.TerminalTask later = new HudiScanNode.TerminalTask(
                () -> Assertions.fail("cancelled task must not run"), () -> { });
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> firstSubmission = submitter.submit(() -> owner.submit(blocked));
            Assertions.assertTrue(rejected.await(3, TimeUnit.SECONDS));

            owner.close();

            Assertions.assertFalse(firstSubmission.get(3, TimeUnit.SECONDS));
            Assertions.assertFalse(owner.submit(later));
            Assertions.assertTrue(blocked.isCancelled());
            Assertions.assertTrue(later.isCancelled());
            Assertions.assertEquals(1, listingExecutor.getQueue().size());
            Assertions.assertSame(queued, listingExecutor.getQueue().peek());
            owner.submissionDone();
            Mockito.verify(lease).close();
            Assertions.assertThrows(CancellationException.class, owner::awaitCompletion);
        } finally {
            submitter.shutdownNow();
            releaseWorker.countDown();
            listingExecutor.shutdownNow();
        }
    }

    @Test
    void synchronousListingCloseDuringImmediateSubmissionStopsTask() {
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        AtomicReference<HudiScanNode.ListingFsViewOwner> ownerRef = new AtomicReference<>();
        Executor immediateExecutor = task -> ownerRef.get().close();
        HudiScanNode.ListingFsViewOwner owner = new HudiScanNode.ListingFsViewOwner(lease, immediateExecutor);
        ownerRef.set(owner);
        HudiScanNode.TerminalTask task = new HudiScanNode.TerminalTask(
                () -> Assertions.fail("cancelled task must not run"), () -> { });

        Assertions.assertFalse(owner.submit(task));

        Assertions.assertTrue(task.isCancelled());
        owner.submissionDone();
        Mockito.verify(lease).close();
        Assertions.assertThrows(CancellationException.class, owner::awaitCompletion);
    }
}
