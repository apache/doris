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

import org.apache.doris.analysis.TableSample;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Status;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.thrift.TStatusCode;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class ColumnMinMaxBatchCollectorTest {
    private static final Pattern MIN = Pattern.compile("MIN\\(`([^`]+)`\\)");
    private final InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private final Database db = Mockito.mock(Database.class);
    private final OlapTable table = Mockito.mock(OlapTable.class);
    private final MaterializedIndexMeta base = Mockito.mock(MaterializedIndexMeta.class);
    private final MaterializedIndexMeta rollup = Mockito.mock(MaterializedIndexMeta.class);

    @BeforeEach
    void setUp() {
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(table.getBaseIndexId()).thenReturn(10L);
        Mockito.when(table.getIndexMetaByIndexId(10L)).thenReturn(base);
        Mockito.when(table.getIndexMetaByIndexId(20L)).thenReturn(rollup);
        Mockito.when(table.getIndexNameById(20L)).thenReturn("idx");
        Mockito.when(table.getKeysType()).thenReturn(KeysType.AGG_KEYS);
        Mockito.when(base.getColumnByName(Mockito.anyString())).thenAnswer(i -> new Column(i.getArgument(0), Type.INT));
        Mockito.when(rollup.getColumnByName(Mockito.anyString()))
                .thenAnswer(i -> new Column(i.getArgument(0), Type.INT));
    }

    private OlapAnalysisTask task(String name, long index) {
        OlapAnalysisTask task = Mockito.spy(new OlapAnalysisTask());
        task.info = new AnalysisInfoBuilder().setColName(name).setIndexId(index).setRowCount(10)
                .setJobColumns(new HashSet<>()).setPartitionNames(new HashSet<>())
                .setPartitionUpdateRows(new ConcurrentHashMap<>()).setJobType(AnalysisInfo.JobType.SYSTEM).build();
        task.tableSample = new TableSample(false, 100L);
        task.col = new Column(name, Type.INT);
        task.tbl = table;
        task.catalog = catalog;
        task.db = db;
        return task;
    }

    private static List<String> projectedColumns(String sql) {
        List<String> names = new ArrayList<>();
        Matcher m = MIN.matcher(sql);
        while (m.find()) {
            names.add(m.group(1));
        }
        return names;
    }

    private static List<ResultRow> bounds(String sql) {
        List<String> fields = new ArrayList<>();
        for (String name : projectedColumns(sql)) {
            fields.add(name + "-min");
            fields.add(name + "-max");
        }
        return List.of(new ResultRow(fields));
    }

    private static MockedConstruction<StmtExecutor> recordQueries(List<String> queries) {
        return Mockito.mockConstruction(StmtExecutor.class, (executor, context) -> {
            String sql = (String) context.arguments().get(1);
            Mockito.when(executor.getContext()).thenReturn((ConnectContext) context.arguments().get(0));
            Mockito.when(executor.executeInternalQuery()).thenAnswer(i -> {
                queries.add(sql);
                return bounds(sql);
            });
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 64, 65, 100})
    void splitInStableOrderAndReturnEachColumnsBounds(int count) {
        List<String> names = IntStream.range(0, count).mapToObj(i -> String.format("c%03d", i))
                .collect(Collectors.toList());
        List<OlapAnalysisTask> tasks = names.stream().map(name -> task(name, -1)).collect(Collectors.toList());
        Collections.reverse(tasks);
        tasks.add(task(names.get(0), -1)); // Duplicate task in the same index.
        new ColumnMinMaxBatchCollector(tasks, 64);
        List<String> queries = new ArrayList<>();
        try (MockedConstruction<StmtExecutor> ignored = recordQueries(queries)) {
            for (OlapAnalysisTask task : tasks) {
                String name = task.info.colName;
                Assertions.assertEquals(List.of(name + "-min", name + "-max"), task.getSampleMinMax().getValues());
            }
        }
        Assertions.assertEquals((count + 63) / 64, queries.size());
        for (int i = 0; i < queries.size(); i++) {
            int start = 64 * (queries.size() - 1 - i);
            Assertions.assertEquals(names.subList(start, Math.min(count, start + 64)),
                    projectedColumns(queries.get(i)));
        }
    }

    @Test
    void duplicateIdentifierSpellingsShareTheSameProjection() {
        Mockito.when(base.getColumnByName("A")).thenReturn(new Column("a", Type.INT));
        List<OlapAnalysisTask> tasks = List.of(task("A", -1), task("a", -1));
        new ColumnMinMaxBatchCollector(tasks, 64);
        List<String> queries = new ArrayList<>();
        try (MockedConstruction<StmtExecutor> ignored = recordQueries(queries)) {
            for (OlapAnalysisTask task : tasks) {
                Assertions.assertEquals(List.of("a-min", "a-max"), task.getSampleMinMax().getValues());
            }
        }
        Assertions.assertEquals(1, queries.size());
        Assertions.assertEquals(List.of("a"), projectedColumns(queries.get(0)));
    }

    @Test
    void separateIndexAndAggregationUsingTargetIndexSchema() {
        // The same name is a key in the base index and a SUM column in the rollup.
        Mockito.when(rollup.getColumnByName("a"))
                .thenReturn(new Column("a", Type.INT, false, AggregateType.SUM, null, ""));
        List<OlapAnalysisTask> tasks = List.of(task("a", -1), task("b", -1), task("a", 20), task("b", 20));
        new ColumnMinMaxBatchCollector(tasks, 64);
        // The job captured the table and index names along with the column identifiers.
        Mockito.when(table.getName()).thenReturn("renamed");
        Mockito.when(table.getIndexNameById(20L)).thenReturn("renamed_idx");
        List<String> queries = new ArrayList<>();
        try (MockedConstruction<StmtExecutor> ignored = recordQueries(queries)) {
            tasks.forEach(OlapAnalysisTask::getSampleMinMax);
        }
        Assertions.assertEquals(3, queries.size());
        String a = "SUBSTRING(CAST(MIN(`a`) AS STRING), 1, 1024), SUBSTRING(CAST(MAX(`a`) AS STRING), 1, 1024)";
        String b = a.replace("`a`", "`b`");
        String from = " FROM `internal`.`db`.`tbl`";
        Assertions.assertEquals("SELECT " + a + ", " + b + from, queries.get(0).trim());
        Assertions.assertEquals("SELECT " + a + from + " index `idx`", queries.get(1));
        Assertions.assertEquals("SELECT " + b + from + " index `idx`", queries.get(2));
    }

    @Test
    void fullSampleCollectsBoundsInTheStatisticsScan() {
        OlapAnalysisTask a = task("a", -1);
        OlapAnalysisTask b = task("b", -1);
        Mockito.when(table.getRowCountForIndex(10, false)).thenReturn(10L);
        new AnalysisJob(a.info, List.of(a, b));
        for (OlapAnalysisTask task : List.of(a, b)) {
            Mockito.doNothing().when(task).runQuery(Mockito.anyString());
            task.doSample();
            Mockito.verify(task, Mockito.never()).getSampleMinMax();
            ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
            Mockito.verify(task).runQuery(sql.capture());
            String id = "`" + task.col.getName() + "`";
            Assertions.assertTrue(sql.getValue().contains("SUBSTRING(CAST(MIN(" + id + ") AS STRING)"));
            Assertions.assertTrue(sql.getValue().contains("SUBSTRING(CAST(MAX(" + id + ") AS STRING)"));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 1})
    void disabledUsesIndividualQueries(int size) {
        List<OlapAnalysisTask> tasks = List.of(task("a", -1), task("b", -1));
        SessionVariable global = new SessionVariable();
        Assertions.assertEquals(64, global.statisticsMinMaxBatchColumns);
        global.statisticsMinMaxBatchColumns = size;
        try (MockedStatic<VariableMgr> vars = Mockito.mockStatic(VariableMgr.class)) {
            vars.when(VariableMgr::getDefaultSessionVariable).thenReturn(global);
            new AnalysisJob(tasks.get(0).info, tasks);
        }
        List<String> sql = new ArrayList<>();
        try (MockedConstruction<StmtExecutor> ignored = recordQueries(sql)) {
            tasks.forEach(OlapAnalysisTask::getSampleMinMax);
        }
        Assertions.assertEquals(2, sql.size());
        tasks.forEach(t -> Mockito.verify(t).collectMinMax());
    }

    @Test
    void emptyTableWritesEmptyStatsWithoutReadingBounds() throws Exception {
        OlapAnalysisTask task = task("a", -1);
        task.info = new AnalysisInfoBuilder(task.info).setRowCount(0).build();
        new ColumnMinMaxBatchCollector(List.of(task), 64);
        task.job = Mockito.mock(AnalysisJob.class);
        Mockito.when(task.job.getJobInfo()).thenReturn(task.info);
        try (MockedConstruction<StmtExecutor> sql = Mockito.mockConstruction(StmtExecutor.class)) {
            task.doExecute();
            Mockito.verify(task.job).appendBuf(Mockito.eq(task), Mockito.argThat(rows -> rows.size() == 1));
            Assertions.assertTrue(sql.constructed().isEmpty());
            Assertions.assertNull(task.minMaxBatch);
        }
    }

    @Test
    void hugePartitionSwitchesToIndividualBounds() throws Exception {
        OlapAnalysisTask task = task("a", -1);
        task.tableSample = null;
        new AnalysisJob(task.info, List.of(task));
        Partition partition = Mockito.mock(Partition.class);
        MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
        Mockito.when(table.getPartitionNames()).thenReturn(Set.of("p"));
        Mockito.when(table.getPartition("p")).thenReturn(partition);
        Mockito.when(partition.getBaseIndex()).thenReturn(index);
        Mockito.when(index.getRowCount()).thenReturn(Long.MAX_VALUE);
        Mockito.when(table.getRowCountForIndex(10, false)).thenReturn(Long.MAX_VALUE);
        Mockito.doReturn(List.of(partition)).when(table).getPartitions();
        Mockito.when(partition.getName()).thenReturn("p");
        Mockito.when(partition.getVisibleVersion()).thenReturn(2L);
        Mockito.when(partition.getIndex(10L)).thenReturn(index);
        LocalTablet tablet = Mockito.mock(LocalTablet.class);
        Mockito.when(tablet.getId()).thenReturn(12L);
        Mockito.when(tablet.getMinReplicaRowCount(Mockito.anyLong())).thenReturn(500_000_000L);
        Mockito.when(index.getTablets()).thenReturn(List.of(tablet));
        Mockito.when(index.getTabletIdsInOrder()).thenReturn(List.of(12L));
        Mockito.when(index.getTablet(12L)).thenReturn(tablet);
        Mockito.doReturn(new ResultRow(List.of("1", "2"))).when(task).collectMinMax();
        Mockito.doNothing().when(task).runQuery(Mockito.anyString());
        task.doPartitionTable();
        Assertions.assertTrue(task.tableSample.getSampleValue() > 0);
        Mockito.verify(task).collectMinMax();
        Mockito.verify(task).runQuery(Mockito.anyString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"query error", "no rows", "two rows", "short row", "long row"})
    void failedBatchFallsBackOncePerColumn(String failure) {
        List<OlapAnalysisTask> tasks = List.of(task("a", -1), task("b", -1), task("c", -1));
        new ColumnMinMaxBatchCollector(tasks, 2);
        AtomicInteger attempts = new AtomicInteger();
        try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class, (e, c) -> {
            String sql = (String) c.arguments().get(1);
            Mockito.when(e.getContext()).thenReturn((ConnectContext) c.arguments().get(0));
            Mockito.when(e.executeInternalQuery()).thenAnswer(i -> {
                attempts.incrementAndGet();
                if (projectedColumns(sql).size() == 1) {
                    return bounds(sql);
                }
                switch (failure) {
                    case "no rows":
                        return List.of();
                    case "two rows":
                        return List.of(bounds(sql).get(0), bounds(sql).get(0));
                    case "short row":
                        return List.of(new ResultRow(List.of("1", "2")));
                    case "long row":
                        return List.of(new ResultRow(Collections.nCopies(6, "x")));
                    default:
                        throw new IllegalStateException("batch failed");
                }
            });
        })) {
            for (OlapAnalysisTask task : tasks) {
                Assertions.assertEquals(task.info.colName + "-min", task.getSampleMinMax().get(0));
            }
        }
        Assertions.assertEquals(4, attempts.get());
        Mockito.verify(tasks.get(0)).collectMinMax();
        Mockito.verify(tasks.get(1)).collectMinMax();
        Mockito.verify(tasks.get(2), Mockito.never()).collectMinMax();
    }

    @Test
    @Timeout(30)
    void syncExecuteWithSingleWorker() throws Exception {
        List<OlapAnalysisTask> tasks = List.of(task("a", -1), task("b", -1));
        new AnalysisJob(tasks.get(0).info, tasks);
        List<String> queries = Collections.synchronizedList(new ArrayList<>());
        for (OlapAnalysisTask task : tasks) {
            Mockito.doAnswer(i -> {
                try (MockedConstruction<StmtExecutor> ignored = recordQueries(queries)) {
                    Assertions.assertEquals(task.info.colName + "-min", task.getSampleMinMax().get(0));
                }
                return null;
            }).when(task).execute();
        }
        try (AutoCloseConnectContext session = new AutoCloseConnectContext(new ConnectContext())) {
            session.connectContext.getSessionVariable().parallelSyncAnalyzeTaskNum = 1;
            Env.getCurrentEnv().getAnalysisManager().syncExecute(new ArrayList<>(tasks));
        }
        Assertions.assertEquals(1, queries.size());
    }

    @Test
    void concurrentWorkersShareOneQuery() throws Exception {
        List<OlapAnalysisTask> tasks = List.of(task("a", -1), task("b", -1), task("c", -1));
        new ColumnMinMaxBatchCollector(tasks, 64);
        CyclicBarrier start = new CyclicBarrier(tasks.size());
        List<String> queries = Collections.synchronizedList(new ArrayList<>());
        ExecutorService workers = Executors.newFixedThreadPool(tasks.size());
        try {
            List<Future<String>> results = new ArrayList<>();
            for (OlapAnalysisTask task : tasks) {
                results.add(workers.submit(() -> {
                    try (MockedConstruction<StmtExecutor> ignored = recordQueries(queries)) {
                        start.await(10, TimeUnit.SECONDS);
                        return task.getSampleMinMax().get(0);
                    }
                }));
            }
            for (int i = 0; i < tasks.size(); i++) {
                Assertions.assertEquals(tasks.get(i).info.colName + "-min", results.get(i).get(10, TimeUnit.SECONDS));
            }
            Assertions.assertEquals(1, queries.size());
        } finally {
            workers.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"before registration", "job cancel", "task failed", "all members", "failed state"})
    void cancelWakesWaitersBeforeOwnerReturns(String trigger) throws Exception {
        OlapAnalysisTask owner = task("a", -1);
        OlapAnalysisTask waiter = task("b", -1);
        List<OlapAnalysisTask> batch = new ArrayList<>(List.of(owner, waiter));
        IntStream.range(2, 64).forEach(i -> batch.add(task("c" + i, -1)));
        AnalysisJob job = new AnalysisJob(owner.info, batch);
        CountDownLatch ready = new CountDownLatch(1);
        AtomicReference<Thread> waitingWorker = new AtomicReference<>();
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> running = workers.submit(() -> {
                try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class,
                        (executor, c) -> {
                            if (trigger.equals("before registration")) {
                                ready.countDown();
                                Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                            }
                            Mockito.doAnswer(i -> cancellations.incrementAndGet()).when(executor).cancel(Mockito.any());
                            Mockito.when(executor.executeInternalQuery()).thenAnswer(i -> {
                                executions.incrementAndGet();
                                ready.countDown();
                                Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                                throw new IllegalStateException("cancelled by executor");
                            });
                        })) {
                    return owner.getSampleMinMax();
                }
            });
            Assertions.assertTrue(ready.await(10, TimeUnit.SECONDS));
            Future<?> waiting = workers.submit(() -> {
                waitingWorker.set(Thread.currentThread());
                return waiter.getSampleMinMax();
            });
            awaitBlocked(waitingWorker, waiting);
            if (trigger.equals("task failed")) {
                job.taskFailed(owner, "injected failure");
            } else if (trigger.equals("all members")) {
                for (int i = 0; i < batch.size() - 1; i++) {
                    batch.get(i).cancel();
                    Assertions.assertEquals(0, cancellations.get());
                }
                batch.get(batch.size() - 1).cancel();
            } else if (trigger.equals("failed state")) {
                job.updateTaskState(AnalysisState.FAILED, "failed");
            } else {
                job.cancel();
            }
            Assertions.assertEquals(!trigger.equals("all members"), job.isKilled());
            assertCancelled(waiting);
            Assertions.assertFalse(running.isDone());
            release.countDown();
            assertCancelled(running);
            int expected = trigger.equals("before registration") ? 0 : 1;
            Assertions.assertEquals(expected, executions.get());
            Assertions.assertEquals(expected, cancellations.get());
            for (OlapAnalysisTask task : batch) {
                Assertions.assertThrows(CancellationException.class, task::getSampleMinMax);
                Mockito.verify(task, Mockito.never()).collectMinMax();
            }
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"queued", "waiter", "owner", "owner query aborted", "owner interrupted"})
    void cancellingOneMemberPreservesSiblings(String trigger) throws Exception {
        OlapAnalysisTask owner = task("a", -1);
        OlapAnalysisTask waiter = task("b", -1);
        OlapAnalysisTask queued = task("c", -1);
        List<OlapAnalysisTask> tasks = List.of(owner, waiter, queued);
        AnalysisJob job = new AnalysisJob(owner.info, tasks);
        CountDownLatch scanning = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Thread> waitingThread = new AtomicReference<>();
        AtomicReference<Thread> ownerThread = new AtomicReference<>();
        AtomicInteger cancellations = new AtomicInteger();
        List<String> queries = Collections.synchronizedList(new ArrayList<>());
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<ResultRow> running = workers.submit(() -> {
                ownerThread.set(Thread.currentThread());
                try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class, (e, c) -> {
                    String sql = (String) c.arguments().get(1);
                    Mockito.doAnswer(i -> cancellations.incrementAndGet()).when(e).cancel(Mockito.any());
                    Mockito.when(e.executeInternalQuery()).thenAnswer(i -> {
                        queries.add(sql);
                        scanning.countDown();
                        try {
                            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                        } catch (InterruptedException interrupted) {
                            throw new IllegalStateException("internal query interrupted", interrupted);
                        }
                        if (trigger.equals("owner query aborted")) {
                            throw new IllegalStateException("owner query aborted");
                        }
                        return bounds(sql);
                    });
                })) {
                    return owner.getSampleMinMax();
                }
            });
            Assertions.assertTrue(scanning.await(10, TimeUnit.SECONDS));
            Future<ResultRow> waiting = workers.submit(() -> {
                try (MockedConstruction<StmtExecutor> ignored = recordQueries(queries)) {
                    waitingThread.set(Thread.currentThread());
                    return waiter.getSampleMinMax();
                }
            });
            awaitBlocked(waitingThread, waiting);
            OlapAnalysisTask target = trigger.equals("queued") ? queued : trigger.equals("waiter") ? waiter : owner;
            if (trigger.equals("owner interrupted")) {
                ownerThread.get().interrupt();
                assertCancelled(running);
            } else {
                AnalysisTaskWrapper wrapper = new AnalysisTaskWrapper(null, target);
                wrapper.cancel("timeout");
                if (target == queued) {
                    wrapper.run();
                    Mockito.verify(queued, Mockito.never()).execute();
                }
            }
            if (target == waiter) {
                assertCancelled(waiting);
            }
            release.countDown();
            if (target == owner) {
                assertCancelled(running);
            } else {
                Assertions.assertEquals("a-min", running.get(10, TimeUnit.SECONDS).get(0));
            }
            if (target != waiter) {
                Assertions.assertEquals("b-min", waiting.get(10, TimeUnit.SECONDS).get(0));
            }
            if (target != queued) {
                Assertions.assertEquals("c-min", queued.getSampleMinMax().get(0));
            }
            Assertions.assertTrue(target.killed);
            Assertions.assertThrows(CancellationException.class, target::getSampleMinMax);
            Assertions.assertEquals(0, cancellations.get());
            Assertions.assertEquals(trigger.equals("owner query aborted") || trigger.equals("owner interrupted")
                    ? 2 : 1, queries.size());
            Assertions.assertFalse(job.isKilled());
            tasks.forEach(t -> Mockito.verify(t, Mockito.never()).collectMinMax());
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"empty result", "short row", "two rows", "cancelled empty", "cancelled exception",
            "killed task", "killed job"})
    void individualBoundsCheckShapeAndCancellation(String outcome) {
        OlapAnalysisTask a = task("a", -1);
        a.job = Mockito.mock(AnalysisJob.class);
        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class, (e, c) ->
                Mockito.when(e.executeInternalQuery()).thenAnswer(i -> {
                    switch (outcome) {
                        case "cancelled empty":
                        case "cancelled exception":
                            Mockito.when(e.getPendingCancelReason())
                                    .thenReturn(new Status(TStatusCode.CANCELLED, "cancelled by user"));
                            if (outcome.equals("cancelled exception")) {
                                throw new IllegalStateException("executor failed");
                            }
                            break;
                        case "killed task":
                            a.cancel();
                            throw new IllegalStateException("executor failed");
                        case "killed job":
                            Mockito.when(a.job.isKilled()).thenReturn(true);
                            break;
                        case "short row":
                            return List.of(new ResultRow(List.of("min")));
                        case "two rows":
                            return List.of(new ResultRow(List.of("1", "2")), new ResultRow(List.of("3", "4")));
                        default:
                            break;
                    }
                    return List.of();
                }))) {
            if (outcome.startsWith("cancelled")) {
                Assertions.assertEquals("cancelled by user",
                        Assertions.assertThrows(CancellationException.class, a::collectMinMax).getMessage());
            } else if (outcome.startsWith("killed")) {
                Assertions.assertThrows(CancellationException.class, a::collectMinMax);
            } else {
                Assertions.assertThrows(IllegalStateException.class, a::collectMinMax);
            }
            Assertions.assertEquals(1, executors.constructed().size());
            Assertions.assertNull(a.stmtExecutor);
        }
    }

    @Test
    void jobCancellationPreventsIndividualBoundsBeforeTasksAreMarkedKilled() {
        OlapAnalysisTask task = task("a", -1);
        AnalysisJob job = new AnalysisJob(task.info, List.of(task));
        Mockito.doNothing().when(task).cancel();
        job.cancel();
        Assertions.assertTrue(job.isKilled());
        Assertions.assertFalse(task.killed);
        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class)) {
            Assertions.assertThrows(CancellationException.class, task::collectMinMax);
            Assertions.assertThrows(CancellationException.class, task::getSampleMinMax);
            Assertions.assertTrue(executors.constructed().isEmpty());
        }
    }

    @Test
    void individualFailurePropagatesWhenBatchFails() {
        OlapAnalysisTask task = task("a", -1);
        new ColumnMinMaxBatchCollector(List.of(task), 64);
        RuntimeException failure = new IllegalStateException("individual query failed");
        Mockito.doThrow(failure).when(task).collectMinMax();
        try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class, (e, c) ->
                Mockito.when(e.executeInternalQuery()).thenThrow(new IllegalStateException("batch query failed")))) {
            Assertions.assertEquals(failure,
                    Assertions.assertThrows(IllegalStateException.class, task::getSampleMinMax));
            Mockito.verify(task).collectMinMax();
        }
    }

    @Test
    void taskTimeoutAfterBoundsDoesNotInvalidatePublishedResults() {
        OlapAnalysisTask a = task("a", -1);
        OlapAnalysisTask b = task("b", -1);
        new ColumnMinMaxBatchCollector(List.of(a, b), 64);
        List<String> queries = new ArrayList<>();
        try (MockedConstruction<StmtExecutor> ignored = recordQueries(queries)) {
            Assertions.assertEquals("a-min", a.getSampleMinMax().get(0));
            a.cancel();
            Assertions.assertEquals("b-min", b.getSampleMinMax().get(0));
            Assertions.assertEquals(1, queries.size());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"kill query", "statement timeout", "session already killed", "statement already cancelled"})
    void syncCancellationReachesColumnWorkers(String trigger) throws Exception {
        OlapAnalysisTask a = task("a", -1);
        OlapAnalysisTask b = task("b", -1);
        new AnalysisJob(a.info, List.of(a, b));
        ConnectContext session = new ConnectContext();
        session.setExecutor(new StmtExecutor(session, "ANALYZE TABLE tbl WITH SYNC"));
        session.getSessionVariable().parallelSyncAnalyzeTaskNum = 1;
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        Mockito.doAnswer(i -> {
            try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class, (e, c) -> {
                Mockito.doAnswer(cancel -> {
                    cancelled.countDown();
                    return null;
                }).when(e).cancel(Mockito.any());
                Mockito.when(e.executeInternalQuery()).thenAnswer(query -> {
                    running.countDown();
                    Assertions.assertTrue(cancelled.await(10, TimeUnit.SECONDS));
                    throw new IllegalStateException("cancelled by parent");
                });
            })) {
                return a.getSampleMinMax();
            }
        }).when(a).execute();
        boolean alreadyCancelled = trigger.startsWith("session already") || trigger.startsWith("statement already");
        if (trigger.equals("session already killed")) {
            session.setKilled();
        } else if (trigger.equals("statement already cancelled")) {
            session.cancelQuery(Status.CANCELLED);
        }
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try {
            Future<?> done = submitter.submit(() -> {
                try (AutoCloseConnectContext ctx = new AutoCloseConnectContext(session)) {
                    ctx.call();
                    Env.getCurrentEnv().getAnalysisManager().syncExecute(List.of(a, b));
                }
            });
            if (!alreadyCancelled) {
                Assertions.assertTrue(running.await(10, TimeUnit.SECONDS));
                if (trigger.equals("kill query")) {
                    session.kill(false);
                } else {
                    session.cancelQuery(new Status(TStatusCode.TIMEOUT, "statement timeout"));
                }
            }
            assertCancelled(done);
            Assertions.assertEquals(alreadyCancelled ? 1 : 0, running.getCount());
            Assertions.assertTrue(a.killed && b.killed);
            Mockito.verify(b, Mockito.never()).execute();
            Mockito.verify(a, Mockito.never()).collectMinMax();
            Mockito.verify(b, Mockito.never()).collectMinMax();
        } finally {
            cancelled.countDown();
            submitter.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void interruptingAWorkerWaitingForAHeavyScanSlotOnlyStopsThatTask(boolean interrupt) throws Exception {
        for (String name : List.of("a", "b", "c", "d")) {
            Mockito.when(base.getColumnByName(name))
                    .thenReturn(new Column(name, Type.INT, false, AggregateType.SUM, null, ""));
        }
        OlapAnalysisTask a = task("a", -1);
        OlapAnalysisTask c = task("c", -1);
        OlapAnalysisTask d = task("d", -1);
        OlapAnalysisTask key = task("k", -1);
        c.info = new AnalysisInfoBuilder(c.info).setScheduleType(AnalysisInfo.ScheduleType.ONCE).build();
        List<OlapAnalysisTask> tasks = List.of(a, task("b", -1), c, d, key);
        new ColumnMinMaxBatchCollector(tasks, 2);
        AnalysisJob job = Mockito.mock(AnalysisJob.class);
        tasks.forEach(t -> t.job = job);
        Mockito.doAnswer(i -> c.getSampleMinMax()).when(c).execute();
        AnalysisTaskWrapper wrapper = new AnalysisTaskWrapper(Mockito.mock(AnalysisTaskExecutor.class), c);
        CountDownLatch firstScanning = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Thread> secondWorker = new AtomicReference<>();
        List<String> queries = Collections.synchronizedList(new ArrayList<>());
        ExecutorService workers = Executors.newFixedThreadPool(3);
        try {
            Future<ResultRow> first = workers.submit(() -> {
                try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class, (e, ctx) ->
                        Mockito.when(e.executeInternalQuery()).thenAnswer(i -> {
                            firstScanning.countDown();
                            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                            return bounds((String) ctx.arguments().get(1));
                        }))) {
                    return a.getSampleMinMax();
                }
            });
            Assertions.assertTrue(firstScanning.await(10, TimeUnit.SECONDS));
            Future<?> second = workers.submit(() -> {
                try (MockedConstruction<StmtExecutor> ignored = recordQueries(queries)) {
                    secondWorker.set(Thread.currentThread());
                    wrapper.run();
                }
            });
            awaitBlocked(secondWorker, second);
            AtomicReference<Thread> siblingWorker = new AtomicReference<>();
            Future<ResultRow> sibling = workers.submit(() -> {
                try (MockedConstruction<StmtExecutor> ignored = recordQueries(queries)) {
                    siblingWorker.set(Thread.currentThread());
                    return d.getSampleMinMax();
                }
            });
            awaitBlocked(siblingWorker, sibling);
            if (interrupt) {
                secondWorker.get().interrupt();
                second.get(10, TimeUnit.SECONDS);
                assertCancelled(wrapper);
                Assertions.assertTrue(c.killed);
                Mockito.verify(job, Mockito.never()).taskFailed(Mockito.any(), Mockito.anyString());
                Assertions.assertFalse(sibling.isDone());
            }
            try (MockedConstruction<StmtExecutor> ignored = recordQueries(queries)) {
                Assertions.assertEquals("k-min", key.getSampleMinMax().get(0));
            }
            Assertions.assertEquals(1, queries.size());
            release.countDown();
            Assertions.assertEquals("a-min", first.get(10, TimeUnit.SECONDS).get(0));
            Assertions.assertEquals("d-min", sibling.get(10, TimeUnit.SECONDS).get(0));
            second.get(10, TimeUnit.SECONDS);
            if (!interrupt) {
                wrapper.get(10, TimeUnit.SECONDS);
                Assertions.assertEquals("c-min", c.getSampleMinMax().get(0));
            }
            Assertions.assertEquals(List.of("c", "d"), projectedColumns(queries.get(1)));
            tasks.forEach(t -> Mockito.verify(t, Mockito.never()).collectMinMax());
            Assertions.assertEquals(2, queries.size());
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void skipsFullScanBatchingForMergedSampledValueColumnsWhileBatchingKeys() {
        Column keyCol = new Column("k", Type.INT, true, null, "0", "");
        Column valCol = new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", "");
        Mockito.when(base.getKeysType()).thenReturn(KeysType.AGG_KEYS);
        Mockito.when(base.getSchema()).thenReturn(List.of(keyCol, valCol));
        Mockito.when(base.getColumnByName("k")).thenReturn(keyCol);
        Mockito.when(base.getColumnByName("v")).thenReturn(valCol);
        Mockito.when(table.getDefaultDistributionInfo()).thenReturn(new HashDistributionInfo(1, List.of(keyCol)));
        Mockito.doReturn(List.of()).when(table).getPartitions();
        OlapAnalysisTask k1 = task("k", -1);
        OlapAnalysisTask k2 = task("k", -1);
        OlapAnalysisTask v = task("v", -1);
        boolean prev = VariableMgr.getDefaultSessionVariable().enableFixedBlockStatsSampling;
        try {
            VariableMgr.getDefaultSessionVariable().enableFixedBlockStatsSampling = true;
            new ColumnMinMaxBatchCollector(List.of(k1, k2, v), 64);
            Assertions.assertNotNull(k1.minMaxBatch);
            Assertions.assertNull(v.minMaxBatch);
        } finally {
            VariableMgr.getDefaultSessionVariable().enableFixedBlockStatsSampling = prev;
        }
    }

    private static void awaitBlocked(AtomicReference<Thread> worker, Future<?> future) {
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> future.isDone() || (worker.get() != null
                && worker.get().getState() == Thread.State.WAITING));
        Assertions.assertFalse(future.isDone());
    }

    private static void assertCancelled(Future<?> future) {
        ExecutionException e = Assertions.assertThrows(ExecutionException.class,
                () -> future.get(10, TimeUnit.SECONDS));
        Assertions.assertInstanceOf(CancellationException.class, e.getCause());
    }
}
