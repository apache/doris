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

package org.apache.doris.statistics;

import org.apache.doris.analysis.AnalyzeProperties;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.commands.AnalyzeTableCommand;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.thrift.TQueryColumn;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

// CHECKSTYLE OFF
public class AnalysisManagerTest {
    @Test
    public void testBuildAnalysisJobInfoCollectHotValueDefault() {
        AnalysisManager manager = new AnalysisManager();
        Env env = Mockito.mock(Env.class);
        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getNextId()).thenReturn(1L, 2L, 3L, 4L);

            AnalysisInfo fullOnce = manager.buildAnalysisJobInfo(
                    mockAnalyzeCommand(AnalysisMethod.FULL, ScheduleType.ONCE, false, false));
            Assertions.assertFalse(fullOnce.collectHotValue);

            AnalysisInfo samplePeriod = manager.buildAnalysisJobInfo(
                    mockAnalyzeCommand(AnalysisMethod.SAMPLE, ScheduleType.PERIOD, false, true));
            Assertions.assertTrue(samplePeriod.collectHotValue);

            AnalysisInfo automatic = manager.buildAnalysisJobInfo(
                    mockAnalyzeCommand(AnalysisMethod.FULL, ScheduleType.AUTOMATIC, false, true));
            Assertions.assertFalse(automatic.collectHotValue);

            AnalysisInfo automaticSample = manager.buildAnalysisJobInfo(
                    mockAnalyzeCommand(AnalysisMethod.SAMPLE, ScheduleType.AUTOMATIC, false, true));
            Assertions.assertTrue(automaticSample.collectHotValue);

            AnalysisInfo explicitAutomatic = manager.buildAnalysisJobInfo(
                    mockAnalyzeCommand(AnalysisMethod.FULL, ScheduleType.AUTOMATIC, true, false));
            Assertions.assertFalse(explicitAutomatic.collectHotValue);
        }
    }

    @Test
    public void testBuildAnalysisJobInfoAutoSampleCommandCollectsHotValue() {
        AnalysisManager manager = new AnalysisManager();
        Env env = Mockito.mock(Env.class);
        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getNextId()).thenReturn(1L);

            Map<String, String> properties = new HashMap<>();
            properties.put(AnalyzeProperties.PROPERTY_SYNC, "false");
            properties.put(AnalyzeProperties.PROPERTY_ANALYSIS_TYPE, AnalysisType.FUNDAMENTALS.toString());
            properties.put(AnalyzeProperties.PROPERTY_AUTOMATIC, "true");
            properties.put(AnalyzeProperties.PROPERTY_SAMPLE_ROWS, "100");
            AnalyzeTableCommand command = Mockito.spy(new AnalyzeTableCommand(
                    new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, "testDb", "testTbl"),
                    null, ImmutableList.of("testCol"), new AnalyzeProperties(properties)));
            TableIf table = Mockito.mock(TableIf.class);
            Mockito.when(table.getId()).thenReturn(30001L);
            Mockito.when(table.getColumnIndexPairs(Mockito.any()))
                    .thenReturn(Collections.singleton(Pair.of("testTbl", "testCol")));
            Mockito.doReturn(table).when(command).getTable();
            Mockito.doReturn(10001L).when(command).getCatalogId();
            Mockito.doReturn(20001L).when(command).getDbId();

            AnalysisInfo analysisInfo = manager.buildAnalysisJobInfo(command);
            Assertions.assertEquals(ScheduleType.AUTOMATIC, analysisInfo.scheduleType);
            Assertions.assertEquals(AnalysisMethod.SAMPLE, analysisInfo.analysisMethod);
            Assertions.assertTrue(analysisInfo.collectHotValue);
        }
    }

    @Test
    public void testUpdateTaskStatus() {
        BaseAnalysisTask task1 = Mockito.mock(BaseAnalysisTask.class);
        BaseAnalysisTask task2 = Mockito.mock(BaseAnalysisTask.class);

        AnalysisManager manager = Mockito.spy(new AnalysisManager());
        Mockito.doNothing().when(manager).logCreateAnalysisTask(Mockito.any());
        Mockito.doNothing().when(manager).logCreateAnalysisJob(Mockito.any());
        Mockito.doNothing().when(manager).updateTableStats(Mockito.any());

        AnalysisInfo job = new AnalysisInfoBuilder().setJobId(1)
                .setState(AnalysisState.PENDING).setAnalysisType(AnalysisType.FUNDAMENTALS)
                .setJobType(AnalysisInfo.JobType.MANUAL).build();
        AnalysisInfo taskInfo1 = new AnalysisInfoBuilder().setJobId(1)
                .setTaskId(2).setJobType(JobType.MANUAL).setAnalysisType(AnalysisType.FUNDAMENTALS)
                .setState(AnalysisState.PENDING).build();
        AnalysisInfo taskInfo2 = new AnalysisInfoBuilder().setJobId(1)
                .setTaskId(3).setAnalysisType(AnalysisType.FUNDAMENTALS).setJobType(JobType.MANUAL)
                .setState(AnalysisState.PENDING).build();
        manager.replayCreateAnalysisJob(job);
        manager.replayCreateAnalysisTask(taskInfo1);
        manager.replayCreateAnalysisTask(taskInfo2);
        Map<Long, BaseAnalysisTask> tasks = new HashMap<>();

        task1.info = taskInfo1;
        task2.info = taskInfo2;
        tasks.put(2L, task1);
        tasks.put(3L, task2);
        manager.addToJobIdTasksMap(1, tasks);

        Assertions.assertEquals(job.state, AnalysisState.PENDING);
        manager.updateTaskStatus(taskInfo1, AnalysisState.RUNNING, "", 0);
        Assertions.assertEquals(job.state, AnalysisState.RUNNING);
        manager.updateTaskStatus(taskInfo2, AnalysisState.RUNNING, "", 0);
        Assertions.assertEquals(job.state, AnalysisState.RUNNING);
        manager.updateTaskStatus(taskInfo1, AnalysisState.FINISHED, "", 0);
        Assertions.assertEquals(job.state, AnalysisState.RUNNING);
        manager.updateTaskStatus(taskInfo2, AnalysisState.FINISHED, "", 0);
        Assertions.assertEquals(job.state, AnalysisState.FINISHED);
    }

    @Test
    public void testUpdateTaskStatusPreservesSkipMessage() {
        // Verify that a subsequent updateTaskStatus(FINISHED, "") call (e.g. from
        // flushBuffer) does NOT wipe a previously-set skip message on info.message,
        // and that job.message only accumulates the skip reason once.
        BaseAnalysisTask task1 = Mockito.mock(BaseAnalysisTask.class);

        AnalysisManager manager = Mockito.spy(new AnalysisManager());
        Mockito.doNothing().when(manager).logCreateAnalysisTask(Mockito.any());
        Mockito.doNothing().when(manager).logCreateAnalysisJob(Mockito.any());
        Mockito.doNothing().when(manager).updateTableStats(Mockito.any());

        AnalysisInfo job = new AnalysisInfoBuilder().setJobId(10)
                .setState(AnalysisState.PENDING).setAnalysisType(AnalysisType.FUNDAMENTALS)
                .setJobType(AnalysisInfo.JobType.MANUAL).build();
        AnalysisInfo taskInfo = new AnalysisInfoBuilder().setJobId(10)
                .setTaskId(11).setJobType(JobType.MANUAL).setAnalysisType(AnalysisType.FUNDAMENTALS)
                .setColName("big_str").setState(AnalysisState.PENDING).build();
        manager.replayCreateAnalysisJob(job);
        manager.replayCreateAnalysisTask(taskInfo);

        task1.info = taskInfo;
        Map<Long, BaseAnalysisTask> tasks = new HashMap<>();
        tasks.put(11L, task1);
        manager.addToJobIdTasksMap(10, tasks);

        String skipMsg = "Column [big_str] has row(s) whose byte length exceeds 1024"
                + " (Config.statistics_max_string_column_length), skip collecting statistics for this column.";
        manager.updateTaskStatus(taskInfo, AnalysisState.FINISHED, skipMsg, 0);
        Assertions.assertEquals(skipMsg, taskInfo.message);
        Assertions.assertTrue(job.message != null && job.message.contains(skipMsg),
                "expected skip msg in job.message, got: " + job.message);
        String firstJobMessage = job.message;

        // Simulate flushBuffer replay: subsequent FINISHED with empty message should
        // NOT wipe info.message NOR re-append skip reason.
        manager.updateTaskStatus(taskInfo, AnalysisState.FINISHED, "", 0);
        Assertions.assertEquals(skipMsg, taskInfo.message);
        Assertions.assertEquals(firstJobMessage, job.message,
                "job.message should not accumulate again on empty-message update");
    }

    @Test
    public void testUpdateTaskStatusAccumulatesMultipleSkipMessages() {
        // Two string columns get skipped -> job.message must contain both entries keyed
        // by their respective colName, and repeated flushBuffer (FINISHED,"") replays
        // must NOT duplicate them.
        BaseAnalysisTask task1 = Mockito.mock(BaseAnalysisTask.class);
        BaseAnalysisTask task2 = Mockito.mock(BaseAnalysisTask.class);

        AnalysisManager manager = Mockito.spy(new AnalysisManager());
        Mockito.doNothing().when(manager).logCreateAnalysisTask(Mockito.any());
        Mockito.doNothing().when(manager).logCreateAnalysisJob(Mockito.any());
        Mockito.doNothing().when(manager).updateTableStats(Mockito.any());

        AnalysisInfo job = new AnalysisInfoBuilder().setJobId(20)
                .setState(AnalysisState.PENDING).setAnalysisType(AnalysisType.FUNDAMENTALS)
                .setJobType(AnalysisInfo.JobType.MANUAL).build();
        AnalysisInfo ti1 = new AnalysisInfoBuilder().setJobId(20).setTaskId(21)
                .setColName("s1").setJobType(JobType.MANUAL)
                .setAnalysisType(AnalysisType.FUNDAMENTALS).setState(AnalysisState.PENDING).build();
        AnalysisInfo ti2 = new AnalysisInfoBuilder().setJobId(20).setTaskId(22)
                .setColName("s2").setJobType(JobType.MANUAL)
                .setAnalysisType(AnalysisType.FUNDAMENTALS).setState(AnalysisState.PENDING).build();
        manager.replayCreateAnalysisJob(job);
        manager.replayCreateAnalysisTask(ti1);
        manager.replayCreateAnalysisTask(ti2);
        task1.info = ti1;
        task2.info = ti2;
        Map<Long, BaseAnalysisTask> tasks = new HashMap<>();
        tasks.put(21L, task1);
        tasks.put(22L, task2);
        manager.addToJobIdTasksMap(20, tasks);

        String skip1 = "Column [s1] has row(s) whose byte length exceeds 1024 ...";
        String skip2 = "Column [s2] has row(s) whose byte length exceeds 1024 ...";
        manager.updateTaskStatus(ti1, AnalysisState.FINISHED, skip1, 0);
        manager.updateTaskStatus(ti2, AnalysisState.FINISHED, skip2, 0);
        Assertions.assertNotNull(job.message);
        Assertions.assertTrue(job.message.contains("s1:[" + skip1 + "]"),
                "expected s1 skip in job.message, got: " + job.message);
        Assertions.assertTrue(job.message.contains("s2:[" + skip2 + "]"),
                "expected s2 skip in job.message, got: " + job.message);
        String afterFirstRound = job.message;

        // Simulate flushBuffer replay with empty message for both tasks. Neither entry
        // should be duplicated.
        manager.updateTaskStatus(ti1, AnalysisState.FINISHED, "", 0);
        manager.updateTaskStatus(ti2, AnalysisState.FINISHED, "", 0);
        Assertions.assertEquals(afterFirstRound, job.message,
                "job.message must remain stable across flushBuffer replays");
        Assertions.assertEquals(skip1, ti1.message);
        Assertions.assertEquals(skip2, ti2.message);
    }

    @Test
    public void testRecordLimit1() {
        Config.analyze_record_limit = 2;
        AnalysisManager analysisManager = new AnalysisManager();
        analysisManager.replayCreateAnalysisJob(new AnalysisInfoBuilder().setJobId(1).build());
        analysisManager.replayCreateAnalysisJob(new AnalysisInfoBuilder().setJobId(2).build());
        analysisManager.replayCreateAnalysisJob(new AnalysisInfoBuilder().setJobId(3).build());
        Assertions.assertEquals(2, analysisManager.analysisJobInfoMap.size());
        Assertions.assertTrue(analysisManager.analysisJobInfoMap.containsKey(2L));
        Assertions.assertTrue(analysisManager.analysisJobInfoMap.containsKey(3L));
    }

    @Test
    public void testRecordLimit2() {
        Config.analyze_record_limit = 2;
        AnalysisManager analysisManager = new AnalysisManager();
        analysisManager.replayCreateAnalysisTask(new AnalysisInfoBuilder().setTaskId(1).build());
        analysisManager.replayCreateAnalysisTask(new AnalysisInfoBuilder().setTaskId(2).build());
        analysisManager.replayCreateAnalysisTask(new AnalysisInfoBuilder().setTaskId(3).build());
        Assertions.assertEquals(2, analysisManager.analysisTaskInfoMap.size());
        Assertions.assertTrue(analysisManager.analysisTaskInfoMap.containsKey(2L));
        Assertions.assertTrue(analysisManager.analysisTaskInfoMap.containsKey(3L));
    }

    @Test
    public void testAddQuerySlotToQueue() throws DdlException {
        AnalysisManager analysisManager = new AnalysisManager();
        InternalCatalog testCatalog = new InternalCatalog();
        Database db = new Database(100, "testDb");
        testCatalog.unprotectCreateDb(db);
        Column column1 = new Column("placeholder", PrimitiveType.INT);
        Column column2 = new Column("placeholder", PrimitiveType.INT);
        Column column3 = new Column("test", PrimitiveType.INT);
        List<Column> schema = new ArrayList<>();
        schema.add(column1);
        OlapTable table = new OlapTable(200, "testTable", schema, null, null, null);
        db.createTableWithLock(table, true, false);

        OlapTable spyTable = Mockito.spy(table);
        Database spyDb = Mockito.spy(db);
        Mockito.doReturn(spyDb).when(spyTable).getDatabase();
        Mockito.doReturn(testCatalog).when(spyDb).getCatalog();

        SlotReference slot1 = new SlotReference(new ExprId(1), "slot1", IntegerType.INSTANCE, true,
                new ArrayList<>(), spyTable, column1, spyTable, column1, ImmutableList.of());
        SlotReference slot2 = new SlotReference(new ExprId(2), "slot2", IntegerType.INSTANCE, true,
                new ArrayList<>(), spyTable, column2, spyTable, column2, ImmutableList.of());
        SlotReference slot3 = new SlotReference(new ExprId(3), "slot3", IntegerType.INSTANCE, true,
                new ArrayList<>(), spyTable, column3, spyTable, column3, ImmutableList.of());
        Set<Slot> set1 = new HashSet<>();
        set1.add(slot1);
        set1.add(slot2);
        analysisManager.updateHighPriorityColumn(set1);
        Assertions.assertEquals(2, analysisManager.highPriorityColumns.size());
        QueryColumn result = analysisManager.highPriorityColumns.poll();
        Assertions.assertEquals("placeholder", result.colName);
        Assertions.assertEquals(testCatalog.getId(), result.catalogId);
        Assertions.assertEquals(db.getId(), result.dbId);
        Assertions.assertEquals(table.getId(), result.tblId);

        result = analysisManager.highPriorityColumns.poll();
        Assertions.assertEquals("placeholder", result.colName);
        Assertions.assertEquals(testCatalog.getId(), result.catalogId);
        Assertions.assertEquals(db.getId(), result.dbId);
        Assertions.assertEquals(table.getId(), result.tblId);
        Assertions.assertEquals(0, analysisManager.highPriorityColumns.size());
        Set<Slot> set2 = new HashSet<>();
        set2.add(slot3);
        for (int i = 0; i < AnalysisManager.COLUMN_QUEUE_SIZE / 2 - 1; i++) {
            analysisManager.updateHighPriorityColumn(set1);
        }
        Assertions.assertEquals(AnalysisManager.COLUMN_QUEUE_SIZE - 2, analysisManager.highPriorityColumns.size());
        analysisManager.updateHighPriorityColumn(set2);
        Assertions.assertEquals(AnalysisManager.COLUMN_QUEUE_SIZE - 1, analysisManager.highPriorityColumns.size());
        analysisManager.updateHighPriorityColumn(set2);
        Assertions.assertEquals(AnalysisManager.COLUMN_QUEUE_SIZE, analysisManager.highPriorityColumns.size());
        analysisManager.updateHighPriorityColumn(set2);
        Assertions.assertEquals(AnalysisManager.COLUMN_QUEUE_SIZE, analysisManager.highPriorityColumns.size());

        for (int i = 0; i < AnalysisManager.COLUMN_QUEUE_SIZE - 2; i++) {
            result = analysisManager.highPriorityColumns.poll();
            Assertions.assertEquals("placeholder", result.colName);
            Assertions.assertEquals(testCatalog.getId(), result.catalogId);
            Assertions.assertEquals(db.getId(), result.dbId);
            Assertions.assertEquals(table.getId(), result.tblId);
        }
        Assertions.assertEquals(2, analysisManager.highPriorityColumns.size());
        result = analysisManager.highPriorityColumns.poll();
        Assertions.assertEquals("test", result.colName);
        Assertions.assertEquals(testCatalog.getId(), result.catalogId);
        Assertions.assertEquals(db.getId(), result.dbId);
        Assertions.assertEquals(table.getId(), result.tblId);

        Assertions.assertEquals(1, analysisManager.highPriorityColumns.size());
        result = analysisManager.highPriorityColumns.poll();
        Assertions.assertEquals("test", result.colName);
        Assertions.assertEquals(testCatalog.getId(), result.catalogId);
        Assertions.assertEquals(db.getId(), result.dbId);
        Assertions.assertEquals(table.getId(), result.tblId);

        result = analysisManager.highPriorityColumns.poll();
        Assertions.assertNull(result);
    }

    @Test
    public void testMergeFollowerColumn() throws DdlException {
        AnalysisManager analysisManager = new AnalysisManager();
        QueryColumn placeholder = new QueryColumn(1, 2, 3, "placeholder");
        QueryColumn high1 = new QueryColumn(10, 20, 30, "high1");
        QueryColumn high2 = new QueryColumn(11, 21, 31, "high2");
        QueryColumn mid1 = new QueryColumn(100, 200, 300, "mid1");
        QueryColumn mid2 = new QueryColumn(101, 201, 301, "mid2");
        List<TQueryColumn> highColumns = new ArrayList<>();
        highColumns.add(high1.toThrift());
        highColumns.add(high2.toThrift());
        List<TQueryColumn> midColumns = new ArrayList<>();
        midColumns.add(mid1.toThrift());
        midColumns.add(mid2.toThrift());
        for (int i = 0; i < AnalysisManager.COLUMN_QUEUE_SIZE - 1; i++) {
            analysisManager.highPriorityColumns.offer(placeholder);
        }
        for (int i = 0; i < AnalysisManager.COLUMN_QUEUE_SIZE - 2; i++) {
            analysisManager.midPriorityColumns.offer(placeholder);
        }
        Assertions.assertEquals(AnalysisManager.COLUMN_QUEUE_SIZE - 1, analysisManager.highPriorityColumns.size());
        Assertions.assertEquals(AnalysisManager.COLUMN_QUEUE_SIZE - 2, analysisManager.midPriorityColumns.size());
        analysisManager.mergeFollowerQueryColumns(highColumns, midColumns);
        Assertions.assertEquals(AnalysisManager.COLUMN_QUEUE_SIZE, analysisManager.highPriorityColumns.size());
        Assertions.assertEquals(AnalysisManager.COLUMN_QUEUE_SIZE, analysisManager.midPriorityColumns.size());
        for (int i = 0; i < AnalysisManager.COLUMN_QUEUE_SIZE - 1; i++) {
            QueryColumn poll = analysisManager.highPriorityColumns.poll();
            Assertions.assertEquals("placeholder", poll.colName);
            Assertions.assertEquals(1, poll.catalogId);
            Assertions.assertEquals(2, poll.dbId);
            Assertions.assertEquals(3, poll.tblId);
        }
        QueryColumn poll = analysisManager.highPriorityColumns.poll();
        Assertions.assertEquals("high1", poll.colName);
        Assertions.assertEquals(10, poll.catalogId);
        Assertions.assertEquals(20, poll.dbId);
        Assertions.assertEquals(30, poll.tblId);
        Assertions.assertEquals(0, analysisManager.highPriorityColumns.size());

        for (int i = 0; i < AnalysisManager.COLUMN_QUEUE_SIZE - 2; i++) {
            QueryColumn pol2 = analysisManager.midPriorityColumns.poll();
            Assertions.assertEquals("placeholder", pol2.colName);
            Assertions.assertEquals(1, pol2.catalogId);
            Assertions.assertEquals(2, pol2.dbId);
            Assertions.assertEquals(3, pol2.tblId);
        }
        QueryColumn pol2 = analysisManager.midPriorityColumns.poll();
        Assertions.assertEquals("mid1", pol2.colName);
        Assertions.assertEquals(100, pol2.catalogId);
        Assertions.assertEquals(200, pol2.dbId);
        Assertions.assertEquals(300, pol2.tblId);

        pol2 = analysisManager.midPriorityColumns.poll();
        Assertions.assertEquals("mid2", pol2.colName);
        Assertions.assertEquals(101, pol2.catalogId);
        Assertions.assertEquals(201, pol2.dbId);
        Assertions.assertEquals(301, pol2.tblId);
        Assertions.assertEquals(0, analysisManager.midPriorityColumns.size());
    }

    @Test
    public void testAsyncDropStats() throws InterruptedException {
        AtomicInteger count = new AtomicInteger(0);
        AnalysisManager analysisManager = Mockito.spy(new AnalysisManager());
        Mockito.doAnswer(invocation -> {
            try {
                Thread.sleep(1000);
                count.incrementAndGet();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        }).when(analysisManager).invalidateLocalStats(
                Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong(),
                Mockito.any(), Mockito.any(), Mockito.any());
        for (int i = 0; i < 20; i++) {
            System.out.println("Submit " + i);
            analysisManager.submitAsyncDropStatsTask(0, 0, 0, null, false);
        }
        Thread.sleep(10000);
        System.out.println(count.get());
        Assertions.assertTrue(count.get() > 0);
        Assertions.assertTrue(count.get() <= 20);
    }

    private AnalyzeTableCommand mockAnalyzeCommand(AnalysisMethod analysisMethod, ScheduleType scheduleType,
            boolean hasCollectHotValue, boolean collectHotValue) {
        AnalyzeTableCommand command = Mockito.mock(AnalyzeTableCommand.class);
        TableIf table = Mockito.mock(TableIf.class);
        Map<String, String> properties = new HashMap<>();
        if (hasCollectHotValue) {
            properties.put(AnalyzeProperties.PROPERTY_COLLECT_HOT_VALUE, String.valueOf(collectHotValue));
        }
        AnalyzeProperties analyzeProperties = new AnalyzeProperties(properties);
        Mockito.when(table.getId()).thenReturn(30001L);
        Mockito.when(command.getTable()).thenReturn(table);
        Mockito.when(command.getColumnNames()).thenReturn(Collections.emptySet());
        Mockito.when(command.isPartitionOnly()).thenReturn(false);
        Mockito.when(command.isSamplingPartition()).thenReturn(false);
        Mockito.when(command.isStarPartition()).thenReturn(false);
        Mockito.when(command.getPartitionCount()).thenReturn(0L);
        Mockito.when(command.getSamplePercent()).thenReturn(0);
        Mockito.when(command.getSampleRows()).thenReturn(100);
        Mockito.when(command.getAnalysisType()).thenReturn(AnalysisType.FUNDAMENTALS);
        Mockito.when(command.getAnalysisMethod()).thenReturn(analysisMethod);
        Mockito.when(command.getScheduleType()).thenReturn(scheduleType);
        Mockito.when(command.getCron()).thenReturn(null);
        Mockito.when(command.getCatalogId()).thenReturn(10001L);
        Mockito.when(command.getDbId()).thenReturn(20001L);
        Mockito.when(command.getPartitionNames()).thenReturn(Collections.emptySet());
        Mockito.when(command.forceFull()).thenReturn(false);
        Mockito.when(command.usingSqlForExternalTable()).thenReturn(false);
        Mockito.when(command.getAnalyzeProperties()).thenReturn(analyzeProperties);
        return command;
    }

    @Test
    void testBootstrapTableStatsIfAbsentWithZeroLoadedRows() {
        AnalysisManager manager = Mockito.spy(new AnalysisManager());
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(1000L);

        // loadedRows <= 0 → should return immediately without creating stats.
        manager.bootstrapTableStatsIfAbsent(table, 0);
        Assertions.assertNull(manager.findTableStatsStatus(1000L));

        manager.bootstrapTableStatsIfAbsent(table, -1);
        Assertions.assertNull(manager.findTableStatsStatus(1000L));
    }

    @Test
    void testBootstrapTableStatsIfAbsentWhenStatsAlreadyExist() {
        AnalysisManager manager = Mockito.spy(new AnalysisManager());
        OlapTable table = mockTable(1000L, "test_tbl");

        // Seed existing TableStatsMeta so bootstrap should short-circuit.
        // Use replayUpdateTableStatsStatus to avoid touching edit log.
        TableStatsMeta existing = TableStatsMeta.newBootstrapStats(table, 100L, 100L);
        manager.replayUpdateTableStatsStatus(existing);

        manager.bootstrapTableStatsIfAbsent(table, 200L);
        TableStatsMeta result = manager.findTableStatsStatus(1000L);
        Assertions.assertNotNull(result);
        // Existing row count should be unchanged.
        Assertions.assertEquals(100L, result.rowCount);
    }

    @Test
    void testBootstrapTableStatsIfAbsentCreatesStats() {
        AnalysisManager manager = Mockito.spy(new AnalysisManager());
        // Avoid touching edit log in unit tests.
        Mockito.doNothing().when(manager).logCreateTableStats(Mockito.any(TableStatsMeta.class));

        OlapTable table = mockTable(1000L, "test_tbl");

        manager.bootstrapTableStatsIfAbsent(table, 128L);

        TableStatsMeta result = manager.findTableStatsStatus(1000L);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(128L, result.rowCount);
        Assertions.assertEquals(128L, result.updatedRows.get());
        // Bootstrap stats should not be marked as user-injected.
        Assertions.assertFalse(result.userInjected);
        // Bootstrap stats should not have a job type.
        Assertions.assertNull(result.jobType);
        // Bootstrap should set updatedTime but leave lastAnalyzeTime as 0.
        Assertions.assertTrue(result.updatedTime > 0);
        Assertions.assertEquals(0L, result.lastAnalyzeTime);
    }

    private static OlapTable mockTable(long tableId, String tableName) {
        org.apache.doris.datasource.CatalogIf catalog = Mockito.mock(org.apache.doris.datasource.CatalogIf.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(catalog.getName()).thenReturn("internal");

        org.apache.doris.catalog.Database database = Mockito.mock(org.apache.doris.catalog.Database.class);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getId()).thenReturn(100L);
        Mockito.when(database.getFullName()).thenReturn("default_cluster:test_db");

        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(tableId);
        Mockito.when(table.getName()).thenReturn(tableName);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getBaseIndexId()).thenReturn(200L);
        Mockito.when(table.getRowCountForIndex(Mockito.anyLong(), Mockito.anyBoolean())).thenReturn(-1L);
        Mockito.when(table.getRowCount()).thenReturn(-1L);
        return table;
    }
}

