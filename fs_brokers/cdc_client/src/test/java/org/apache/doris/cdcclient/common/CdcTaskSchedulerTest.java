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

package org.apache.doris.cdcclient.common;

import static org.apache.doris.cdcclient.common.CdcResourceManager.ReaderGroup.JOB_INCREMENTAL;
import static org.apache.doris.cdcclient.common.CdcResourceManager.ReaderGroup.JOB_SNAPSHOT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.doris.cdcclient.service.CdcTaskScheduler;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class CdcTaskSchedulerTest {
    @Test
    void incrementalRunsWhileSnapshotWorkerIsOccupied() throws Exception {
        CountDownLatch snapshotStarted = new CountDownLatch(1);
        CountDownLatch releaseSnapshot = new CountDownLatch(1);
        try (CdcTaskScheduler scheduler = new CdcTaskScheduler(resources(), 1, 8)) {
            CompletableFuture<Void> snapshot =
                    scheduler.submit(
                            "snapshot-job",
                            "snapshot",
                            JOB_SNAPSHOT,
                            () -> {
                                snapshotStarted.countDown();
                                await(releaseSnapshot);
                            });
            try {
                await(snapshotStarted);
                scheduler
                        .submit("incremental-job", "incremental", JOB_INCREMENTAL, () -> {})
                        .get(5, TimeUnit.SECONDS);
                assertFalse(snapshot.isDone());
            } finally {
                releaseSnapshot.countDown();
            }
            snapshot.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void quotaRetryFreesWorkerAndGivesAvailableQuotaToWaitingIncremental() throws Exception {
        CdcResourceManager resources = new CdcResourceManager(288L * 1024 * 1024, new Properties());
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch readerPrepared = new CountDownLatch(1);
        CountDownLatch finishIncremental = new CountDownLatch(1);
        try (CdcResourceManager.SnapshotQuota occupying =
                        resources.newSnapshotQuota("holder", JOB_SNAPSHOT);
                CdcTaskScheduler scheduler = new CdcTaskScheduler(resources, 1, 8)) {
            assertEquals(1, occupying.tryReserveSlots(1));
            CompletableFuture<Void> waiting =
                    scheduler.submit(
                            "incremental",
                            "waiting",
                            JOB_INCREMENTAL,
                            () -> {
                                attempts.incrementAndGet();
                                try (ManagedReader<Object> reader =
                                        resources.createIncrementalReader(
                                                "incremental",
                                                JOB_INCREMENTAL,
                                                Object::new,
                                                ignored -> terminatedWorker())) {
                                    if (reader == null) {
                                        throw new ReaderQuotaUnavailableException(
                                                "incremental",
                                                resources.getReaderReservationBytes(),
                                                0);
                                    }
                                    scheduler.onReaderPrepared("waiting");
                                    readerPrepared.countDown();
                                    await(finishIncremental);
                                }
                            });
            // This action can run on the same worker only after the first task enters quota
            // waiting.
            scheduler
                    .submit("existing-job", "existing", JOB_INCREMENTAL, () -> {})
                    .get(5, TimeUnit.SECONDS);
            assertTrue(scheduler.getResourceWaitReason("waiting").contains("reader memory quota"));
            assertFalse(waiting.isDone());
            assertEquals(1, attempts.get());

            try (CdcResourceManager.SnapshotQuota snapshot =
                    resources.newSnapshotQuota("new", JOB_SNAPSHOT)) {
                occupying.close();
                assertEquals(0, snapshot.tryReserveSlots(1));
                await(readerPrepared);
                assertEquals("", scheduler.getResourceWaitReason("waiting"));
            } finally {
                finishIncremental.countDown();
            }
            waiting.get(5, TimeUnit.SECONDS);
            assertEquals(2, attempts.get());
            assertEquals(0, resources.getReaderBytes(JOB_INCREMENTAL));
        }
    }

    @Test
    void cancellingQuotaWaitFreesQueueCapacityAndIncrementalPriority() throws Exception {
        CdcResourceManager resources = new CdcResourceManager(288L * 1024 * 1024, new Properties());
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch attempted = new CountDownLatch(1);
        try (CdcResourceManager.SnapshotQuota occupying =
                        resources.newSnapshotQuota("holder", JOB_SNAPSHOT);
                CdcTaskScheduler scheduler = new CdcTaskScheduler(resources, 1, 1)) {
            assertEquals(1, occupying.tryReserveSlots(1));
            CompletableFuture<Void> waiting =
                    scheduler.submit(
                            "incremental",
                            "waiting",
                            JOB_INCREMENTAL,
                            () -> {
                                attempts.incrementAndGet();
                                attempted.countDown();
                                throw new ReaderQuotaUnavailableException(
                                        "incremental", resources.getReaderReservationBytes(), 0);
                            });
            await(attempted);
            // Wait for the quota failure to be recorded before exercising cancellation.
            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .until(
                            () ->
                                    scheduler
                                            .getResourceWaitReason("waiting")
                                            .contains("reader memory quota"));
            scheduler.cancel("incremental", "waiting");
            assertTrue(waiting.isCancelled());
            assertEquals("", scheduler.getResourceWaitReason("waiting"));
            occupying.close();
            try (CdcResourceManager.SnapshotQuota next =
                    resources.newSnapshotQuota("next", JOB_SNAPSHOT)) {
                assertEquals(1, next.tryReserveSlots(1));
            }
            scheduler.submit("next", "next", JOB_INCREMENTAL, () -> {}).get(5, TimeUnit.SECONDS);
            assertEquals(1, attempts.get());
        }
    }

    @Test
    void duplicateRunningTaskSharesResultWithoutExecutingAgain() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger executions = new AtomicInteger();
        try (CdcTaskScheduler scheduler = new CdcTaskScheduler(resources(), 2, 8)) {
            CompletableFuture<Void> first =
                    scheduler.submit(
                            "job",
                            "task",
                            JOB_SNAPSHOT,
                            () -> {
                                executions.incrementAndGet();
                                started.countDown();
                                await(release);
                            });
            try {
                await(started);
                CompletableFuture<Void> duplicate =
                        scheduler.submit("job", "task", JOB_SNAPSHOT, executions::incrementAndGet);
                assertSame(first, duplicate);
            } finally {
                release.countDown();
            }
            first.get(5, TimeUnit.SECONDS);
            assertEquals(1, executions.get());
        }
    }

    @Test
    void cancelledQueuedTaskNeverRunsAndFreesQueueCapacity() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger cancelledExecutions = new AtomicInteger();
        try (CdcTaskScheduler scheduler = new CdcTaskScheduler(resources(), 1, 1)) {
            CompletableFuture<Void> running =
                    scheduler.submit(
                            "running-job",
                            "running",
                            JOB_SNAPSHOT,
                            () -> {
                                started.countDown();
                                await(release);
                            });
            CompletableFuture<Void> afterCancellation;
            try {
                await(started);
                CompletableFuture<Void> queued =
                        scheduler.submit(
                                "queued-job",
                                "queued",
                                JOB_SNAPSHOT,
                                cancelledExecutions::incrementAndGet);
                assertSame(
                        queued,
                        scheduler.submit(
                                "queued-job",
                                "queued",
                                JOB_SNAPSHOT,
                                cancelledExecutions::incrementAndGet));
                assertThrows(
                        RejectedExecutionException.class,
                        () -> scheduler.submit("full-job", "full", JOB_SNAPSHOT, () -> {}));
                scheduler.cancel("queued-job", "queued");
                assertTrue(queued.isCancelled());
                afterCancellation = scheduler.submit("next-job", "next", JOB_SNAPSHOT, () -> {});
            } finally {
                release.countDown();
            }
            running.get(5, TimeUnit.SECONDS);
            afterCancellation.get(5, TimeUnit.SECONDS);
            assertEquals(0, cancelledExecutions.get());
        }
    }

    @Test
    void closeCancelsQueuedTasksInterruptsRunningTaskAndRejectsSubmissions() throws Exception {
        CdcResourceManager resources = resources();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger queuedExecutions = new AtomicInteger();
        try (CdcTaskScheduler scheduler = new CdcTaskScheduler(resources, 1, 8)) {
            CompletableFuture<Void> running =
                    scheduler.submit(
                            "running-job",
                            "running",
                            JOB_INCREMENTAL,
                            () -> {
                                started.countDown();
                                try {
                                    assertTrue(release.await(5, TimeUnit.SECONDS));
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    interrupted.countDown();
                                }
                            });
            try {
                await(started);
                CompletableFuture<Void> queued =
                        scheduler.submit(
                                "queued-job",
                                "queued",
                                JOB_INCREMENTAL,
                                queuedExecutions::incrementAndGet);
                scheduler.close();
                await(interrupted);
                assertTrue(running.isCancelled());
                assertTrue(queued.isCancelled());
                assertEquals(0, queuedExecutions.get());
                assertThrows(
                        RejectedExecutionException.class,
                        () -> scheduler.submit("new-job", "new", JOB_SNAPSHOT, () -> {}));
            } finally {
                release.countDown();
            }
        }
    }

    private static CdcResourceManager resources() {
        return new CdcResourceManager(4096L * 1024 * 1024, new Properties());
    }

    private static ExecutorService terminatedWorker() {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        worker.shutdown();
        return worker;
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
