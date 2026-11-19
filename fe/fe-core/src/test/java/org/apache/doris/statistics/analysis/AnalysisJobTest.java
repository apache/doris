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

package org.apache.doris.statistics.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.analysis.AnalysisInfo.JobType;
import org.apache.doris.statistics.repository.ColStatsData;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class AnalysisJobTest {
    private AnalysisManager manager;
    private AnalysisInfo info;
    private AnalysisJob job;
    private List<BaseAnalysisTask> tasks;
    private ExecutorService threads;
    private Consumer<StmtExecutor> insert;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() {
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            env.when(Env::isCheckpointThread).thenReturn(true);
            manager = Mockito.spy(new AnalysisManager());
        }
        Mockito.doNothing().when(manager).logCreateAnalysisTask(Mockito.any());
        Mockito.doNothing().when(manager).logCreateAnalysisJob(Mockito.any());
        Mockito.doNothing().when(manager).updateTableStats(Mockito.any());
        info = new AnalysisInfoBuilder().setJobId(1).setState(AnalysisState.RUNNING)
                .setJobType(JobType.MANUAL).setJobColumns(new HashSet<>())
                .setPartitionUpdateRows(new ConcurrentHashMap<>()).build();
        info.partitionUpdateRows.put(10L, 100L);
        tasks = new ArrayList<>();
        Map<Long, BaseAnalysisTask> taskMap = new HashMap<>();
        for (long id = 2; id <= 3; id++) {
            BaseAnalysisTask task = Mockito.mock(BaseAnalysisTask.class);
            task.info = new AnalysisInfoBuilder(info).setTaskId(id).setColName("col" + id).build();
            Mockito.doAnswer(call -> {
                task.killed = true;
                return null;
            }).when(task).cancel();
            tasks.add(task);
            taskMap.put(id, task);
        }
        insert = Mockito.mock(Consumer.class);
        job = new AnalysisJob(info, tasks, manager) {
            @Override
            void executeInsert(StmtExecutor executor) {
                insert.accept(executor);
            }
        };
        manager.registerJob(job);
        Assertions.assertSame(job, tasks.get(0).job);
        manager.registerSysJob(info, taskMap);
        threads = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    public void tearDown() {
        threads.shutdownNow();
    }

    private static void awaitRelease(CountDownLatch release) {
        try {
            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private void assertReleased() {
        Assertions.assertTrue(manager.analysisJobIdToTaskMap.isEmpty());
        Assertions.assertNull(manager.findRunningJob(info.jobId));
        Assertions.assertNull(job.stmtExecutor);
    }

    @Test
    public void testSuccessfulWriteReleasesExecutorsAndRegistries() throws Exception {
        job.appendBuf(tasks.get(0), List.of(new ColStatsData()));
        job.appendBuf(tasks.get(1), List.of(new ColStatsData()));
        Mockito.verify(insert).accept(Mockito.any());
        Assertions.assertEquals(AnalysisState.FINISHED, info.state);
        tasks.forEach(task -> Assertions.assertEquals(AnalysisState.FINISHED, task.info.state));
        assertReleased();
    }

    @Test
    public void testFailedFinalInsertTerminatesPreviouslyCollectedTasks() throws Exception {
        Mockito.doThrow(new RuntimeException("write failed")).when(insert).accept(Mockito.any());
        job.appendBuf(tasks.get(0), List.of(new ColStatsData()));
        Assertions.assertThrows(RuntimeException.class,
                () -> job.appendBuf(tasks.get(1), List.of(new ColStatsData())));
        Assertions.assertEquals(AnalysisState.FAILED, info.state);
        tasks.forEach(task -> {
            Assertions.assertEquals(AnalysisState.FAILED, task.info.state);
            Assertions.assertTrue(task.killed);
        });
        Assertions.assertTrue(info.partitionUpdateRows.isEmpty());
        assertReleased();
        job.appendBuf(tasks.get(0), List.of(new ColStatsData()));
        job.taskDoneWithoutData(tasks.get(1));
        job.cancel();
        Mockito.verify(insert, Mockito.times(1)).accept(Mockito.any());
        Mockito.verify(manager, Mockito.times(1)).removeJob(1L);
        assertReleased();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    @Timeout(20)
    public void testNonPositiveMergeCountAllowsQueuedTasksToProgress(int mergeCount) throws Exception {
        threads.shutdownNow();
        threads = Executors.newSingleThreadExecutor();
        Future<?> completion = threads.submit(() -> {
            try (MockedStatic<StatisticsUtil> stats = Mockito.mockStatic(
                    StatisticsUtil.class, Mockito.CALLS_REAL_METHODS)) {
                stats.when(StatisticsUtil::getInsertMergeCount).thenReturn(mergeCount);
                for (BaseAnalysisTask task : tasks) {
                    job.appendBuf(task, List.of(new ColStatsData()));
                }
            }
        });
        try {
            completion.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(AnalysisState.FINISHED, info.state);
            Mockito.verify(insert, Mockito.times(2)).accept(Mockito.any());
            assertReleased();
        } finally {
            // A regression must not leave a busy worker behind after the progress assertion fails.
            job.cancel();
        }
    }

    @Test
    public void testBufferedInsertOOMRetainsTuningHintThroughWrapper() throws Exception {
        Mockito.doThrow(new RuntimeException("[MEM_LIMIT_EXCEEDED] statistics insert failed"))
                .when(insert).accept(Mockito.any());
        job.appendBuf(tasks.get(0), List.of(new ColStatsData()));
        BaseAnalysisTask lastTask = tasks.get(1);
        lastTask.info = new AnalysisInfoBuilder(lastTask.info)
                .setScheduleType(AnalysisInfo.ScheduleType.ONCE).build();
        Mockito.doAnswer(call -> {
            job.appendBuf(lastTask, List.of(new ColStatsData()));
            return null;
        }).when(lastTask).execute();
        new AnalysisTaskWrapper(Mockito.mock(AnalysisTaskExecutor.class), lastTask).run();
        Assertions.assertEquals(AnalysisState.FAILED, info.state);
        for (BaseAnalysisTask task : tasks) {
            Assertions.assertTrue(task.info.message.contains("[MEM_LIMIT_EXCEEDED] statistics insert failed"));
            Assertions.assertTrue(task.info.message.contains("statistics_sql_mem_limit_in_bytes"));
            Assertions.assertTrue(task.info.message.contains("huge_table_default_sample_rows"));
            Assertions.assertTrue(task.info.message.contains("statistics_max_string_column_length"));
        }
        Assertions.assertEquals(tasks.get(0).info.message, info.message);
        assertReleased();
    }

    @Test
    public void testFailedInsertAfterJobHistoryEviction() throws Exception {
        manager.analysisJobInfoMap.clear();
        Mockito.doThrow(new RuntimeException("write failed")).when(insert).accept(Mockito.any());
        job.appendBuf(tasks.get(0), List.of(new ColStatsData()));
        Assertions.assertThrows(RuntimeException.class,
                () -> job.appendBuf(tasks.get(1), List.of(new ColStatsData())));
        tasks.forEach(task -> Assertions.assertEquals(AnalysisState.FAILED, task.info.state));
        assertReleased();
    }

    @Test
    public void testHistoryWriteFailureStillUnregistersJob() throws Exception {
        Mockito.doThrow(new RuntimeException("journal failed")).when(manager).logCreateAnalysisTask(Mockito.any());
        Assertions.assertThrows(RuntimeException.class, job::cancel);
        tasks.forEach(task -> Assertions.assertEquals(AnalysisState.FAILED, task.info.state));
        assertReleased();
    }

    @Test
    public void testCancelWithoutDataRejectsLateCompletions() throws Exception {
        job.cancel();
        job.taskDoneWithoutData(tasks.get(0), "late skip");
        job.appendBuf(tasks.get(1), List.of(new ColStatsData()));
        tasks.forEach(task -> {
            Assertions.assertEquals(AnalysisState.FAILED, task.info.state);
            Assertions.assertEquals("analysis job cancelled", task.info.message);
        });
        Mockito.verify(insert, Mockito.never()).accept(Mockito.any());
        assertReleased();
    }

    @Test
    public void testSyncCompletionKeepsPartitionDataForTableStatsUpdate() throws Exception {
        manager.analysisJobIdToTaskMap.clear();
        info.jobColumns.add(org.apache.doris.common.Pair.of("table", "column"));
        job.taskDoneWithoutData(tasks.get(0));
        job.taskDoneWithoutData(tasks.get(1));
        Assertions.assertEquals(100L, info.partitionUpdateRows.get(10L));
        Assertions.assertFalse(info.jobColumns.isEmpty());
        assertReleased();
    }

    @Test
    public void testPartitionUpdatesCannotRepopulateCancelledJob() throws Exception {
        job.putPartitionUpdateRowsIfAbsent(20L, 200L);
        Assertions.assertEquals(200L, info.partitionUpdateRows.get(20L));
        job.cancel();
        job.putPartitionUpdateRowsIfAbsent(30L, 300L);
        Assertions.assertTrue(info.partitionUpdateRows.isEmpty());
        assertReleased();
    }

    @Test
    public void testCancellationAfterFinalStatusKeepsSuccessfulResult() throws Exception {
        tasks.forEach(job::taskDoneWithoutData);
        job.cancel();
        Assertions.assertEquals(AnalysisState.FINISHED, info.state);
        tasks.forEach(task -> Mockito.verify(task, Mockito.never()).cancel());
        assertReleased();
    }

    @Test
    public void testOneCancelFailureStillCancelsOtherTasks() throws Exception {
        Mockito.doThrow(new RuntimeException("cancel failed")).when(tasks.get(0)).cancel();
        Assertions.assertThrows(RuntimeException.class, job::cancel);
        Assertions.assertTrue(tasks.get(1).killed);
        tasks.forEach(task -> Assertions.assertEquals(AnalysisState.FAILED, task.info.state));
        assertReleased();
    }

    @Test
    public void testAllSkippedTasksCompleteWithoutInsert() throws Exception {
        job.taskDoneWithoutData(tasks.get(0), "skipped long column");
        job.taskDoneWithoutData(tasks.get(1));
        Assertions.assertEquals(AnalysisState.FINISHED, info.state);
        Assertions.assertTrue(info.message.contains("skipped long column"));
        Mockito.verify(insert, Mockito.never()).accept(Mockito.any());
        assertReleased();
    }

    @Test
    @Timeout(20)
    public void testCancellationDoesNotWaitForInsertMonitor() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Mockito.doAnswer(call -> {
            entered.countDown();
            awaitRelease(release);
            return null;
        }).when(insert).accept(Mockito.any());
        job.taskDoneWithoutData(tasks.get(0));
        Future<?> write = threads.submit(() -> job.appendBuf(tasks.get(1), List.of(new ColStatsData())));
        try {
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            Future<?> cancel = threads.submit(job::cancel);
            cancel.get(5, TimeUnit.SECONDS);
            Assertions.assertNotNull(job.stmtExecutor,
                    "the executing SQL retains its cancellation handle until it returns");
            job.appendBuf(tasks.get(0), List.of(new ColStatsData()));
        } finally {
            release.countDown();
        }
        write.get(5, TimeUnit.SECONDS);
        tasks.forEach(task -> Assertions.assertEquals(AnalysisState.FAILED, task.info.state));
        assertReleased();
    }

    @Test
    @Timeout(20)
    public void testLateSuccessfulStatusCannotWinAgainstCancellation() throws Exception {
        CountDownLatch insertEntered = new CountDownLatch(1);
        CountDownLatch releaseInsert = new CountDownLatch(1);
        CountDownLatch cancelEntered = new CountDownLatch(1);
        CountDownLatch releaseCancel = new CountDownLatch(1);
        Mockito.doAnswer(call -> {
            insertEntered.countDown();
            awaitRelease(releaseInsert);
            return null;
        }).when(insert).accept(Mockito.any());
        Mockito.doAnswer(call -> {
            tasks.get(0).killed = true;
            cancelEntered.countDown();
            awaitRelease(releaseCancel);
            return null;
        }).when(tasks.get(0)).cancel();
        job.appendBuf(tasks.get(0), List.of(new ColStatsData()));
        Future<?> write = threads.submit(() -> job.appendBuf(tasks.get(1), List.of(new ColStatsData())));
        Future<?> cancel = null;
        try {
            Assertions.assertTrue(insertEntered.await(10, TimeUnit.SECONDS));
            cancel = threads.submit(job::cancel);
            Assertions.assertTrue(cancelEntered.await(10, TimeUnit.SECONDS));
            releaseInsert.countDown();
            write.get(5, TimeUnit.SECONDS);
        } finally {
            releaseInsert.countDown();
            releaseCancel.countDown();
        }
        cancel.get(5, TimeUnit.SECONDS);
        tasks.forEach(task -> Assertions.assertEquals(AnalysisState.FAILED, task.info.state));
        assertReleased();
    }

    @Test
    @Timeout(20)
    public void testFinalSmallBatchArrivingDuringFlushIsNotLost() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger writes = new AtomicInteger();
        Mockito.doAnswer(call -> {
            if (writes.incrementAndGet() == 1) {
                entered.countDown();
                awaitRelease(release);
            }
            return null;
        }).when(insert).accept(Mockito.any());
        List<ColStatsData> firstBatch = Collections.nCopies(StatisticsUtil.getInsertMergeCount(), new ColStatsData());
        Future<?> write = threads.submit(() -> job.appendBuf(tasks.get(0), firstBatch));
        try {
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            job.appendBuf(tasks.get(1), List.of(new ColStatsData()));
        } finally {
            release.countDown();
        }
        write.get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(2, writes.get());
        Assertions.assertEquals(AnalysisState.FINISHED, info.state);
        assertReleased();
    }
}
