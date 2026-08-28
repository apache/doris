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

import org.apache.doris.datasource.SplitAssignment;
import org.apache.doris.datasource.hudi.HudiFsViewCacheValue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
        HudiScanNode.ListingFsViewOwner owner = new HudiScanNode.ListingFsViewOwner(lease);
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
        owner.track(task);
        owner.submissionDone();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            executor.execute(task);
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
        HudiScanNode.ListingFsViewOwner owner = new HudiScanNode.ListingFsViewOwner(lease);

        owner.discardBeforeSubmission();

        Mockito.verify(lease).close();
        Assertions.assertDoesNotThrow(owner::awaitCompletion);
    }

    @Test
    void synchronousListingCancellationWinsAfterTerminalTasksCompleteInline() {
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.ListingFsViewOwner owner = new HudiScanNode.ListingFsViewOwner(lease);
        HudiScanNode.TerminalTask completed = new HudiScanNode.TerminalTask(() -> { }, () -> { });
        HudiScanNode.TerminalTask queued = new HudiScanNode.TerminalTask(
                () -> Assertions.fail("cancelled task must not run"), () -> { });
        owner.track(completed);
        owner.track(queued);
        completed.run();
        owner.submissionDone();

        owner.close();

        Assertions.assertTrue(queued.isCancelled());
        Mockito.verify(lease).close();
        Assertions.assertThrows(CancellationException.class, owner::awaitCompletion);
    }
}
