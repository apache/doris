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

package org.apache.doris.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.EditLog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class ExportHistoryRetentionTest {
    private static final long NOW = 10_000_000L;

    @Test
    public void testFinishTimeBoundaryInvalidLongAndLongRunningJob() {
        ExportJob job = job(1, 1, "a", ExportJobState.FINISHED, NOW - 299_999);
        Deencapsulation.setField(job, "createTimeMs", 1L);
        Assertions.assertFalse(job.isRemovableAt(NOW, 0));

        Deencapsulation.setField(job, "finishTimeMs", NOW - 300_000);
        Assertions.assertTrue(job.isRemovableAt(NOW, 0));
        Deencapsulation.setField(job, "finishTimeMs", NOW - 300_001);
        Assertions.assertTrue(job.isRemovableAt(NOW, -1));
        Assertions.assertFalse(job.isRemovableAt(NOW, 301));

        for (long invalid : new long[] {-1, 0, NOW + 1}) {
            Deencapsulation.setField(job, "finishTimeMs", invalid);
            Assertions.assertFalse(job.isRemovableAt(NOW, 0));
        }
        Deencapsulation.setField(job, "finishTimeMs", NOW - 1_000_000);
        for (ExportJobState active : Arrays.asList(
                ExportJobState.PENDING, ExportJobState.EXPORTING, ExportJobState.IN_QUEUE)) {
            Deencapsulation.setField(job, "state", active);
            Assertions.assertFalse(job.isRemovableAt(NOW, 0));
        }
        Deencapsulation.setField(job, "state", ExportJobState.FINISHED);
        Assertions.assertFalse(job.isRemovableAt(NOW, Long.MAX_VALUE));

        long nearMaxNow = Long.MAX_VALUE - 1;
        Deencapsulation.setField(job, "finishTimeMs", nearMaxNow - 300_000);
        Assertions.assertTrue(job.isRemovableAt(nearMaxNow, 0));
    }

    @Test
    public void testTtlThenFinishAndIdOrderingIgnoresTerminalState() {
        ExportJob expiredFinished = job(9, 1, "ttl", ExportJobState.FINISHED, NOW - 700_000);
        ExportJob cancelledLater = job(8, 1, "c2", ExportJobState.CANCELLED, NOW - 400_000);
        ExportJob cancelledHighId = job(7, 1, "c1b", ExportJobState.CANCELLED, NOW - 500_000);
        ExportJob finishedLowId = job(6, 1, "f1a", ExportJobState.FINISHED, NOW - 500_000);
        ExportJob finishedOld = job(5, 1, "f", ExportJobState.FINISHED, NOW - 600_000);
        List<ExportJob> jobs = Arrays.asList(
                finishedOld, cancelledLater, expiredFinished, cancelledHighId, finishedLowId);

        Assertions.assertEquals(Collections.singletonList(9L),
                ids(ExportMgr.selectJobsToRemove(jobs, NOW, 650, 10)));
        Assertions.assertEquals(Arrays.asList(9L, 5L, 6L),
                ids(ExportMgr.selectJobsToRemove(jobs, NOW, 650, 2)));
        Assertions.assertEquals(Arrays.asList(9L, 5L, 6L, 7L),
                ids(ExportMgr.selectJobsToRemove(jobs, NOW, 650, 1)));
    }

    @Test
    public void testCandidateShortageZeroNegativeAndTenThousandJobs() {
        ExportJob active = job(1, 1, "active", ExportJobState.PENDING, -1);
        ExportJob young = job(2, 1, "young", ExportJobState.CANCELLED, NOW - 299_999);
        ExportJob old = job(3, 1, "old", ExportJobState.FINISHED, NOW - 300_000);
        List<ExportJob> jobs = Arrays.asList(active, young, old);

        Assertions.assertEquals(Collections.singletonList(3L),
                ids(ExportMgr.selectJobsToRemove(jobs, NOW, 3600, 0)));
        Assertions.assertTrue(ExportMgr.selectJobsToRemove(jobs, NOW, 3600, -1).isEmpty());
        Assertions.assertTrue(ExportMgr.selectJobsToRemove(Arrays.asList(active, young), NOW, 0, 0).isEmpty());

        List<ExportJob> synthetic = new ArrayList<>(10_000);
        for (int i = 0; i < 10_000; i++) {
            ExportJobState state = i % 2 == 0 ? ExportJobState.CANCELLED : ExportJobState.FINISHED;
            synthetic.add(job(i + 10, 2, "bulk-" + i, state, NOW - 300_000 - i));
        }
        List<ExportJob> removed = ExportMgr.selectJobsToRemove(synthetic, NOW, 3600, 1000);
        Assertions.assertEquals(9000, removed.size());
        Assertions.assertEquals(4500,
                removed.stream().filter(j -> j.getState() == ExportJobState.CANCELLED).count());
        Assertions.assertEquals(4500,
                removed.stream().filter(j -> j.getState() == ExportJobState.FINISHED).count());
        for (int i = 0; i < removed.size(); i++) {
            Assertions.assertEquals(10_009L - i, removed.get(i).getId());
        }
    }

    @Test
    public void testOnlineCompareRemovePreservesRealRegistrationAcrossDatabases() throws Exception {
        ExportMgr mgr = new ExportMgr();
        ExportJob oldCancelled = job(1, 10, "same", ExportJobState.CANCELLED, NOW - 400_000);
        ExportJob newFinished = job(2, 10, "same", ExportJobState.FINISHED, NOW - 299_999);
        ExportJob otherDb = job(3, 11, "same", ExportJobState.FINISHED, NOW - 299_999);
        mgr.unprotectAddJob(oldCancelled);
        mgr.unprotectAddJob(newFinished);
        mgr.unprotectAddJob(otherDb);

        mgr.removeOldExportJobs(NOW, 3600, 2);
        Assertions.assertNull(mgr.getJob(1));
        Assertions.assertNotNull(mgr.getJob(2));
        Assertions.assertNotNull(mgr.getJob(3));

        try {
            mgr.addExportJobAndRegisterTask(job(4, 10, "same", ExportJobState.PENDING, -1));
            Assertions.fail("new registration must conflict with the surviving non-cancelled job");
        } catch (LabelAlreadyUsedException expected) {
            Assertions.assertTrue(expected.getMessage().contains("Label [same] has already been used."));
        }
        Map<Long, Map<String, Long>> labels = labels(mgr);
        Assertions.assertEquals(Long.valueOf(2), labels.get(10L).get("same"));
        Assertions.assertEquals(Long.valueOf(3), labels.get(11L).get("same"));
    }

    @Test
    public void testCleanupAndRegistrationAreSerializedByManagerLock() throws Exception {
        ExportMgr mgr = new ExportMgr();
        CountDownLatch selectionEntered = new CountDownLatch(1);
        CountDownLatch allowSelection = new CountDownLatch(1);
        ExportJob old = new BlockingExportJob(1, selectionEntered, allowSelection);
        setJobFields(old, 1, "barrier", ExportJobState.CANCELLED, NOW - 400_000);
        mgr.unprotectAddJob(old);
        ReentrantReadWriteLock managerLock = Deencapsulation.getField(mgr, "lock");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch registrationStarted = new CountDownLatch(1);
        AtomicReference<Thread> registrationThread = new AtomicReference<>();
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Future<?> cleanup = null;
        Future<?> registration = null;
        Throwable failure = null;
        try {
            cleanup = executor.submit(() -> mgr.removeOldExportJobs(NOW, 3600, 0));
            Assertions.assertTrue(selectionEntered.await(10, TimeUnit.SECONDS));

            ExportJob replacement = job(2, 1, "barrier", ExportJobState.PENDING, -1);
            registration = executor.submit(() -> {
                registrationThread.set(Thread.currentThread());
                registrationStarted.countDown();
                try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
                    envStatic.when(Env::getCurrentEnv).thenReturn(env);
                    mgr.addExportJobAndRegisterTask(replacement);
                }
                return null;
            });
            Assertions.assertTrue(registrationStarted.await(10, TimeUnit.SECONDS));
            awaitManagerLockQueue(managerLock, registrationThread.get());
            Mockito.verify(editLog, Mockito.never()).logExportCreate(Mockito.any());

            allowSelection.countDown();
            cleanup.get(10, TimeUnit.SECONDS);
            registration.get(10, TimeUnit.SECONDS);
            Assertions.assertNull(mgr.getJob(1));
            Assertions.assertSame(replacement, mgr.getJob(2));
            Assertions.assertEquals(ExportJobState.PENDING, replacement.getState());
            Assertions.assertEquals(Long.valueOf(2), labels(mgr).get(1L).get("barrier"));
            Mockito.verify(editLog).logExportCreate(replacement);

            try {
                mgr.addExportJobAndRegisterTask(job(3, 1, "barrier", ExportJobState.PENDING, -1));
                Assertions.fail("a later real registration must conflict with the surviving job");
            } catch (LabelAlreadyUsedException expected) {
                Assertions.assertTrue(expected.getMessage().contains("Label [barrier] has already been used."));
            }
        } catch (Throwable t) {
            failure = t;
        } finally {
            ConcurrentTestCleanup.finish(
                    allowSelection, executor, failure, cleanup, registration);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testConcurrentCleanupAggregatesFailuresAndTerminatesExecutor() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch failedTaskRan = new CountDownLatch(1);
            Future<?> completedFailure = executor.submit(() -> {
                failedTaskRan.countDown();
                throw new IllegalStateException("completed future failure");
            });
            Assertions.assertTrue(failedTaskRan.await(10, TimeUnit.SECONDS));
            awaitDone(completedFailure);
            Assertions.assertTrue(completedFailure.isDone(), "the regression requires an already completed future");

            Future<?> timedOut = Mockito.mock(Future.class);
            Mockito.when(timedOut.get(10, TimeUnit.SECONDS))
                    .thenThrow(new TimeoutException("second future timeout"));
            AssertionError primary = new AssertionError("primary assertion");
            try {
                ConcurrentTestCleanup.finish(
                        new CountDownLatch(1), executor, primary, completedFailure, timedOut);
                Assertions.fail("the primary failure must be rethrown after cleanup");
            } catch (AssertionError actual) {
                Assertions.assertSame(primary, actual);
                Assertions.assertEquals(2, actual.getSuppressed().length);
                Assertions.assertTrue(actual.getSuppressed()[0] instanceof java.util.concurrent.ExecutionException);
                Assertions.assertEquals("completed future failure",
                        actual.getSuppressed()[0].getCause().getMessage());
                Assertions.assertTrue(actual.getSuppressed()[1] instanceof TimeoutException);
                Assertions.assertEquals("second future timeout", actual.getSuppressed()[1].getMessage());
                Mockito.verify(timedOut).get(10, TimeUnit.SECONDS);
                Assertions.assertTrue(executor.isTerminated(),
                        "cleanup must terminate the executor before rethrowing");
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testImageSelectionAndLabelReplayRules() {
        ExportMgr mgr = new ExportMgr();
        Arrays.asList(
                job(1, 1, "a", ExportJobState.FINISHED, NOW - 600_000),
                job(2, 1, "b", ExportJobState.CANCELLED, NOW - 400_000),
                job(3, 1, "c", ExportJobState.PENDING, -1),
                job(4, 1, "ttl", ExportJobState.FINISHED, NOW - 700_000)).forEach(mgr::unprotectAddJob);
        int oldTtl = Config.history_job_keep_max_second;
        int oldMax = Config.max_export_history_job_num;
        try {
            Config.history_job_keep_max_second = 650;
            Config.max_export_history_job_num = 2;
            Set<Long> imageIds = mgr.getJobsForImage(NOW).stream()
                    .map(ExportJob::getId).collect(Collectors.toSet());
            Assertions.assertEquals(new HashSet<>(Arrays.asList(2L, 3L)), imageIds);
            Assertions.assertEquals(4, mgr.getJobs().size());
            mgr.removeOldExportJobs(NOW, Config.history_job_keep_max_second, Config.max_export_history_job_num);
            Assertions.assertEquals(imageIds, mgr.getJobs().stream()
                    .map(ExportJob::getId).collect(Collectors.toSet()));
        } finally {
            Config.history_job_keep_max_second = oldTtl;
            Config.max_export_history_job_num = oldMax;
        }

        assertLabelRepresentative(Arrays.asList(
                job(11, 1, "same", ExportJobState.CANCELLED, NOW - 400_000),
                job(12, 1, "same", ExportJobState.FINISHED, NOW - 300_000)), 12L);
        assertLabelRepresentative(Arrays.asList(
                job(12, 1, "same", ExportJobState.FINISHED, NOW - 300_000),
                job(11, 1, "same", ExportJobState.CANCELLED, NOW - 400_000)), 12L);

        ExportMgr replayMgr = new ExportMgr();
        ExportJob replayTarget = job(22, 1, "replay", ExportJobState.CANCELLED, -1);
        replayMgr.unprotectAddJob(replayTarget);
        replayMgr.unprotectAddJob(job(21, 1, "replay", ExportJobState.CANCELLED, NOW - 400_000));
        ExportJob source = job(22, 1, "replay", ExportJobState.FINISHED, NOW - 300_000);
        replayMgr.replayUpdateJobState(new ExportJobStateTransfer(source, ExportJobState.FINISHED));
        Assertions.assertSame(replayTarget, replayMgr.getJob(22));
        Assertions.assertEquals(ExportJobState.FINISHED, replayTarget.getState());
        Assertions.assertEquals(Long.valueOf(22), labels(replayMgr).get(1L).get("replay"));
    }

    private static ExportJob job(
            long id, long dbId, String label, ExportJobState state, long finishTimeMs) {
        ExportJob job = new ExportJob(id);
        setJobFields(job, dbId, label, state, finishTimeMs);
        return job;
    }

    private static void setJobFields(
            ExportJob job, long dbId, String label, ExportJobState state, long finishTimeMs) {
        Deencapsulation.setField(job, "dbId", dbId);
        Deencapsulation.setField(job, "label", label);
        Deencapsulation.setField(job, "state", state);
        Deencapsulation.setField(job, "finishTimeMs", finishTimeMs);
    }

    private static List<Long> ids(List<ExportJob> jobs) {
        return jobs.stream().map(ExportJob::getId).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private static Map<Long, Map<String, Long>> labels(ExportMgr mgr) {
        return Deencapsulation.getField(mgr, "dbTolabelToExportJobId");
    }

    private static void assertLabelRepresentative(List<ExportJob> jobs, long expectedId) {
        ExportMgr mgr = new ExportMgr();
        jobs.forEach(mgr::unprotectAddJob);
        Assertions.assertEquals(Long.valueOf(expectedId), labels(mgr).get(1L).get("same"));
    }

    private static void awaitManagerLockQueue(
            ReentrantReadWriteLock managerLock, Thread contender) throws InterruptedException {
        Assertions.assertNotNull(contender);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!managerLock.hasQueuedThread(contender) && System.nanoTime() < deadline) {
            Thread.yield();
        }
        Assertions.assertTrue(managerLock.hasQueuedThread(contender),
                "registration thread never queued for the manager lock");
    }

    private static void awaitDone(Future<?> future) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!future.isDone() && System.nanoTime() < deadline) {
            Thread.yield();
        }
    }

    private static class BlockingExportJob extends ExportJob {
        private final CountDownLatch entered;
        private final CountDownLatch proceed;

        BlockingExportJob(long id, CountDownLatch entered, CountDownLatch proceed) {
            super(id);
            this.entered = entered;
            this.proceed = proceed;
        }

        @Override
        public synchronized boolean isRemovableAt(long currentTimeMs, long ttlSeconds) {
            entered.countDown();
            try {
                Assertions.assertTrue(proceed.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            return super.isRemovableAt(currentTimeMs, ttlSeconds);
        }
    }

    static final class ConcurrentTestCleanup {
        private static final long TIMEOUT_SECONDS = 10;

        private ConcurrentTestCleanup() {
        }

        static void finish(
                CountDownLatch holderBarrier, ExecutorService executor, Throwable primary,
                Future<?>... futures) throws Exception {
            Throwable failure = primary;
            boolean interrupted = primary instanceof InterruptedException;
            holderBarrier.countDown();
            try {
                for (Future<?> future : futures) {
                    if (future == null) {
                        continue;
                    }
                    try {
                        future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        interrupted = true;
                        failure = append(failure, e);
                    } catch (Throwable t) {
                        failure = append(failure, t);
                    }
                }
            } finally {
                executor.shutdownNow();
                try {
                    if (!executor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                        failure = append(failure,
                                new AssertionError("executor did not terminate within timeout"));
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                    failure = append(failure, e);
                } finally {
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            rethrow(failure);
        }

        private static Throwable append(Throwable primary, Throwable additional) {
            if (primary == null) {
                return additional;
            }
            if (primary != additional) {
                primary.addSuppressed(additional);
            }
            return primary;
        }

        private static void rethrow(Throwable failure) throws Exception {
            if (failure == null) {
                return;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            throw new AssertionError(failure);
        }
    }
}
