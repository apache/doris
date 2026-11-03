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

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.SplitAssignment;
import org.apache.doris.datasource.SplitSource;
import org.apache.doris.datasource.SplitSourceManager;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.datasource.hudi.HudiExternalMetaCache;
import org.apache.doris.datasource.hudi.HudiFsViewCacheValue;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

class HudiBatchFsViewOwnerTest {

    @Test
    void deferredBatchSourceSurvivesStatementCloseUntilCoordinatorStopsIt() throws Exception {
        try (BatchScanFixture f = new BatchScanFixture()) {
            f.node.startSplit(1);
            Assertions.assertEquals(1, f.submitted.size());
            SplitSource source = new SplitSource(new Backend(1, "localhost", 0), f.assignment, 1000);
            f.manager.registerSplitSource(source);
            // Deferral is selected after planning. Processor cleanup can also clear the context's executor.
            Mockito.when(f.executor.isDeferredForArrowFlight()).thenReturn(true);
            f.context.setExecutor(null);
            f.statement.close();

            Assertions.assertFalse(f.assignment.isStop(), "DoGet still needs the deferred batch source");
            Assertions.assertSame(source, f.manager.getSplitSource(source.getUniqueId()));
            Assertions.assertFalse(f.producer().isCancelled());
            Mockito.verify(f.lease, Mockito.never()).close();

            f.node.stop();

            Assertions.assertTrue(f.assignment.isStop());
            Assertions.assertNull(f.manager.getSplitSource(source.getUniqueId()));
            Assertions.assertTrue(f.producer().isCancelled());
            Mockito.verify(f.lease).close();
        }
    }

    @Test
    void cancelledBatchAdmissionDoesNotAcquireFsView() throws Exception {
        try (BatchScanFixture f = new BatchScanFixture()) {
            f.rejectAdmission.set(true);
            Assertions.assertThrows(CancellationException.class, () -> f.node.startSplit(1));
            Mockito.verify(f.generation, Mockito.never()).acquireFsView(Mockito.any());
            Mockito.verify(f.lease, Mockito.never()).close();
        }
    }

    @Test
    void partitionAdmissionIsHeldUntilQueuedDeliveryTerminates() throws Exception {
        try (BatchScanFixture f = new BatchScanFixture()) {
            Semaphore admission = new Semaphore(1);
            HivePartition partition = Mockito.mock(HivePartition.class);
            Mockito.when(partition.isDummyPartition()).thenReturn(true);
            f.variables.setForceJniScanner(false);
            f.variables.setEnableRuntimeFilterPartitionPrune(false);
            org.apache.hudi.common.table.view.HoodieTableFileSystemView view =
                    Mockito.mock(org.apache.hudi.common.table.view.HoodieTableFileSystemView.class);
            Mockito.when(view.getLatestBaseFilesBeforeOrOn(Mockito.any(), Mockito.any()))
                    .thenAnswer(call -> Stream.empty());
            Mockito.when(f.lease.get()).thenReturn(view);
            BatchScanFixture.setField(f.node, HudiScanNode.class, "isCowTable", true);
            BatchScanFixture.setField(f.node, HudiScanNode.class, "splittersOnFlight", admission);
            BatchScanFixture.setField(f.node, HudiScanNode.class, "prunedPartitions",
                    Collections.singletonList(partition));
            f.node.startSplit(1);
            f.producer().run();
            Assertions.assertEquals(2, f.submitted.size());
            f.submitted.get(1).run();
            Assertions.assertEquals(3, f.submitted.size());
            Assertions.assertEquals(0, admission.availablePermits());
            Mockito.verify(f.lease, Mockito.never()).close();
            f.node.stop();
            Assertions.assertEquals(1, admission.availablePermits());
            Mockito.verify(f.lease).close();
        }
    }

    @Test
    void ordinaryBatchStatementCloseStillCancelsQueuedPlanning() throws Exception {
        try (BatchScanFixture f = new BatchScanFixture()) {
            f.node.startSplit(1);
            f.statement.close();

            Assertions.assertTrue(f.assignment.isStop());
            Assertions.assertTrue(f.producer().isCancelled());
            Mockito.verify(f.lease).close();
        }
    }

    @Test
    void abandonedBatchWithoutExecutorStillStopsAtStatementClose() throws Exception {
        try (BatchScanFixture f = new BatchScanFixture()) {
            f.context.setExecutor(null);
            f.node.startSplit(1);
            f.statement.close();

            Assertions.assertTrue(f.assignment.isStop());
            Assertions.assertTrue(f.producer().isCancelled());
            Mockito.verify(f.lease).close();
        }
    }

    private static class BatchScanFixture implements AutoCloseable {
        private final ConnectContext previous = ConnectContext.get();
        private final ConnectContext context = new ConnectContext();
        private final SessionVariable variables = new SessionVariable();
        private final StatementContext statement = new StatementContext(context, null);
        private final StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        private final SplitAssignment assignment = new SplitAssignment(
                null, null, null, Collections.emptyMap(), Collections.emptyList(), false);
        private final SplitSourceManager manager = new SplitSourceManager();
        private final HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        private final AtomicBoolean rejectAdmission = new AtomicBoolean();
        private final HudiExternalMetaCache.FsViewGeneration generation =
                Mockito.mock(HudiExternalMetaCache.FsViewGeneration.class);
        private final List<Runnable> submitted = new ArrayList<>();
        private final MockedStatic<Env> mockedEnv;
        private final HudiScanNode node;

        BatchScanFixture() throws Exception {
            context.setStatementContext(statement);
            context.setExecutor(executor);
            context.setThreadLocalInfo();
            Env env = Mockito.mock(Env.class);
            ExternalMetaCacheMgr cache = Mockito.mock(ExternalMetaCacheMgr.class);
            ExecutorService worker = Mockito.mock(ExecutorService.class);
            Mockito.doAnswer(call -> {
                submitted.add(call.getArgument(0));
                return null;
            }).when(worker).execute(Mockito.any(Runnable.class));
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cache);
            Mockito.when(env.getSplitSourceManager()).thenReturn(manager);
            Mockito.when(cache.getScheduleExecutor()).thenReturn(worker);
            Mockito.when(cache.getFileListingExecutor()).thenReturn(worker);
            mockedEnv = Mockito.mockStatic(Env.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
            HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
            HMSExternalCatalog.HudiScanRuntimeContext runtime =
                    Mockito.mock(HMSExternalCatalog.HudiScanRuntimeContext.class);
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(catalog.getHudiScanRuntimeContext()).thenReturn(runtime);
            Mockito.when(runtime.getAuthenticator()).thenReturn(new ExecutionAuthenticator() {});
            Mockito.when(runtime.getFsViewGeneration()).thenReturn(generation);
            Mockito.when(generation.acquireFsView(Mockito.any())).thenReturn(lease);
            TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
            desc.setTable(table);
            node = new HudiScanNode(new PlanNodeId(0), desc, false, Optional.empty(), Optional.empty(),
                    variables, null, ScanContext.EMPTY) {
                @Override
                protected Executor getSplitTaskExecutor() {
                    if (rejectAdmission.get()) {
                        throw new CancellationException("Scan task lease is closed");
                    }
                    return super.getSplitTaskExecutor();
                }
            };
            setField(node, HudiScanNode.class, "prunedPartitions",
                    Collections.singletonList(Mockito.mock(HivePartition.class)));
            setField(node, ScanNode.class, "splitAssignment", assignment);
        }

        private static void setField(Object target, Class<?> owner, String name, Object value) throws Exception {
            Field field = owner.getDeclaredField(name);
            field.setAccessible(true);
            field.set(target, value);
        }

        HudiScanNode.TerminalTask producer() {
            return (HudiScanNode.TerminalTask) submitted.get(0);
        }

        @Override
        public void close() {
            try {
                node.stop();
                statement.close();
            } finally {
                mockedEnv.close();
                ConnectContext.remove();
                if (previous != null) {
                    previous.setThreadLocalInfo();
                }
            }
        }
    }

    @Test
    void statementCloseReturnsWhileRunningTaskKeepsLeasePinned() throws Exception {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(
                assignment, lease, Runnable::run, Runnable::run);
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
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(
                assignment, lease, Runnable::run, Runnable::run);

        owner.finish();
        owner.close();

        Mockito.verify(assignment, Mockito.never()).stop();
        Mockito.verify(lease).close();
    }

    @Test
    void statementCloseCancelsAcceptedTaskBeforeItStarts() {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        List<Runnable> submitted = new ArrayList<>();
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(
                assignment, lease, submitted::add, submitted::add);
        HudiScanNode.TerminalTask task = new HudiScanNode.TerminalTask(
                () -> Assertions.fail("cancelled task must not run"), owner::finish);
        Assertions.assertTrue(owner.submitPartition(task));

        owner.close();

        Assertions.assertTrue(task.isCancelled());
        Mockito.verify(assignment).stop();
        Mockito.verify(lease).close();
    }

    @Test
    void statementCloseRemovesQueuedBatchTasksFromBothExecutors() throws Exception {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        CountDownLatch workersStarted = new CountDownLatch(2);
        CountDownLatch releaseWorkers = new CountDownLatch(1);
        ThreadPoolExecutor scheduleExecutor = blockedExecutor(workersStarted, releaseWorkers);
        ThreadPoolExecutor producerExecutor = blockedExecutor(workersStarted, releaseWorkers);
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(
                assignment, lease, scheduleExecutor, producerExecutor);
        HudiScanNode.TerminalTask partitionTask = new HudiScanNode.TerminalTask(
                () -> Assertions.fail("cancelled partition task must not run"), () -> { });
        HudiScanNode.TerminalTask producerTask = new HudiScanNode.TerminalTask(
                () -> Assertions.fail("cancelled producer task must not run"), () -> { });
        try {
            Assertions.assertTrue(workersStarted.await(3, TimeUnit.SECONDS));
            Assertions.assertTrue(owner.submitPartition(partitionTask));
            Assertions.assertTrue(owner.submitProducer(producerTask));
            Assertions.assertEquals(1, scheduleExecutor.getQueue().size());
            Assertions.assertEquals(1, producerExecutor.getQueue().size());

            owner.close();

            Assertions.assertTrue(partitionTask.isCancelled());
            Assertions.assertTrue(producerTask.isCancelled());
            Assertions.assertTrue(scheduleExecutor.getQueue().isEmpty());
            Assertions.assertTrue(producerExecutor.getQueue().isEmpty());
            Mockito.verify(assignment).stop();
        } finally {
            owner.finish();
            releaseWorkers.countDown();
            scheduleExecutor.shutdownNow();
            producerExecutor.shutdownNow();
        }
        Mockito.verify(lease).close();
    }

    @Test
    void statementCloseDoesNotWaitForAlreadyStartedBlockedTask() throws Exception {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(
                assignment, lease, executor, executor);
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
        Assertions.assertTrue(owner.submitPartition(task));
        try {
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
        ExecutorService executor = Executors.newSingleThreadExecutor();
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(
                assignment, lease, executor, executor);
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
        Assertions.assertTrue(owner.submitPartition(task));
        try {
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
        HudiScanNode.TerminalTask task = new HudiScanNode.TerminalTask(() -> {
            started.countDown();
            while (release.getCount() > 0) {
                try {
                    release.await(3, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                    // Model storage code that does not terminate when interrupted.
                }
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
            executor.shutdown();
            Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
            Mockito.verify(lease).close();
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

    @Test
    void stoppedAssignmentRejectsBatchSyncBeforeThreadPublication() {
        SplitAssignment assignment = new SplitAssignment(null, null, null,
                Collections.emptyMap(), Collections.emptyList(), true);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(
                assignment, lease, Runnable::run, Runnable::run);
        assignment.stop();
        // Mirrors startSplit adding its owner after statement cleanup stopped the assignment.
        assignment.addCloseable(owner);
        try {
            Assertions.assertThrows(CancellationException.class, () -> owner.beginSync(Thread.currentThread()));
            Mockito.verify(lease, Mockito.never()).close();
        } finally {
            owner.endSync();
            owner.finish();
        }
        Mockito.verify(lease).close();
    }

    @Test
    void closedListingOwnerRejectsSyncBeforeThreadPublication() {
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.ListingFsViewOwner owner = new HudiScanNode.ListingFsViewOwner(lease, Runnable::run);
        owner.close();
        try {
            Assertions.assertThrows(CancellationException.class, () -> owner.beginSync(Thread.currentThread()));
            Mockito.verify(lease, Mockito.never()).close();
        } finally {
            owner.endSync();
            owner.discardBeforeSubmission();
        }
        Mockito.verify(lease).close();
    }

    @Test
    void closeInterruptsBlockedBatchSyncAndKeepsLeaseUntilTerminal() throws Exception {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(
                assignment, lease, Runnable::run, Runnable::run);
        CountDownLatch syncStarted = new CountDownLatch(1);
        CountDownLatch syncInterrupted = new CountDownLatch(1);
        Thread syncThread = new Thread(() -> {
            owner.beginSync(Thread.currentThread());
            syncStarted.countDown();
            while (true) {
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException e) {
                    syncInterrupted.countDown();
                    return;
                }
            }
        });
        try {
            syncThread.start();
            Assertions.assertTrue(syncStarted.await(3, TimeUnit.SECONDS));

            owner.close();

            Assertions.assertTrue(syncInterrupted.await(3, TimeUnit.SECONDS));
            // A blocked remote sync keeps owning the lease; only its terminal path releases it.
            Mockito.verify(lease, Mockito.never()).close();
            owner.finish();
            Mockito.verify(lease).close();
        } finally {
            syncThread.interrupt();
            syncThread.join(3000);
            owner.finish();
        }
    }

    @Test
    void closeInterruptsBlockedListingSyncAndKeepsLeaseUntilTerminal() throws Exception {
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.ListingFsViewOwner owner = new HudiScanNode.ListingFsViewOwner(lease, Runnable::run);
        CountDownLatch syncStarted = new CountDownLatch(1);
        CountDownLatch syncInterrupted = new CountDownLatch(1);
        Thread syncThread = new Thread(() -> {
            owner.beginSync(Thread.currentThread());
            syncStarted.countDown();
            while (true) {
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException e) {
                    syncInterrupted.countDown();
                    return;
                }
            }
        });
        try {
            syncThread.start();
            Assertions.assertTrue(syncStarted.await(3, TimeUnit.SECONDS));

            owner.close();

            Assertions.assertTrue(syncInterrupted.await(3, TimeUnit.SECONDS));
            // A blocked remote sync keeps owning the lease; only its terminal path releases it.
            Mockito.verify(lease, Mockito.never()).close();
            owner.discardBeforeSubmission();
            Mockito.verify(lease).close();
        } finally {
            syncThread.interrupt();
            syncThread.join(3000);
            owner.discardBeforeSubmission();
        }
    }

    private static ThreadPoolExecutor blockedExecutor(
            CountDownLatch workersStarted, CountDownLatch releaseWorkers) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        executor.execute(() -> {
            workersStarted.countDown();
            try {
                releaseWorkers.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        return executor;
    }
}
