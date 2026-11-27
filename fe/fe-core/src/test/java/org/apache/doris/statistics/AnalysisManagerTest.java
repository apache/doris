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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonUtils;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.commands.AnalyzeTableCommand;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.TableStatsDeletionLog;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TQueryColumn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    public void testColdHiveAnalyzeRecordsEstimatedRowCountInJob() {
        AnalysisManager manager = new AnalysisManager();
        Env env = Mockito.mock(Env.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(table.getId()).thenReturn(30001L);
        Mockito.when(table.getRowCount()).thenReturn(-1L);
        Mockito.when(table.getRowCountForAnalyze()).thenReturn(32L);
        AnalyzeTableCommand command = mockAnalyzeCommand(AnalysisMethod.FULL, ScheduleType.ONCE, false, false);
        Mockito.when(command.getTable()).thenReturn(table);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            AnalysisInfo info = manager.buildAnalysisJobInfo(command);
            Assertions.assertEquals(32L, info.rowCount);
            Mockito.verify(table).getRowCountForAnalyze();
            Mockito.verify(table, Mockito.never()).fetchRowCount();
        }
    }

    @Test
    public void testUpdateTaskStatus(@Mocked BaseAnalysisTask task1,
            @Mocked BaseAnalysisTask task2) {

        new MockUp<AnalysisManager>() {
            @Mock
            public void logCreateAnalysisTask(AnalysisInfo job) {}

            @Mock
            public void logCreateAnalysisJob(AnalysisInfo job) {}

            @Mock
            public void updateTableStats(AnalysisInfo jobInfo) {}

        };

        new MockUp<AnalysisInfo>() {
            @Mock
            public String toString() {
                return "";
            }
        };

        AnalysisInfo job = new AnalysisInfoBuilder().setJobId(1)
                .setState(AnalysisState.PENDING).setAnalysisType(AnalysisType.FUNDAMENTALS)
                .setJobType(AnalysisInfo.JobType.MANUAL).build();
        AnalysisInfo taskInfo1 = new AnalysisInfoBuilder().setJobId(1)
                .setTaskId(2).setJobType(JobType.MANUAL).setAnalysisType(AnalysisType.FUNDAMENTALS)
                .setState(AnalysisState.PENDING).build();
        AnalysisInfo taskInfo2 = new AnalysisInfoBuilder().setJobId(1)
                .setTaskId(3).setAnalysisType(AnalysisType.FUNDAMENTALS).setJobType(JobType.MANUAL)
                .setState(AnalysisState.PENDING).build();
        AnalysisManager manager = new AnalysisManager();
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

        new MockUp<Table>() {
            @Mock
            public DatabaseIf getDatabase() {
                return db;
            }
        };

        new MockUp<Database>() {
            @Mock
            public CatalogIf getCatalog() {
                return testCatalog;
            }
        };

        SlotReference slot1 = new SlotReference(new ExprId(1), "slot1", IntegerType.INSTANCE, true,
                new ArrayList<>(), table, column1, table, column1, ImmutableList.of());
        SlotReference slot2 = new SlotReference(new ExprId(2), "slot2", IntegerType.INSTANCE, true,
                new ArrayList<>(), table, column2, table, column2, ImmutableList.of());
        SlotReference slot3 = new SlotReference(new ExprId(3), "slot3", IntegerType.INSTANCE, true,
                new ArrayList<>(), table, column3, table, column3, ImmutableList.of());
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
    public void testManualAnalysisValidatesPaimonRowCountBeforePlanning() {
        AtomicBoolean planningStarted = new AtomicBoolean(false);
        FileStoreTable unsafeTable = new AppendOnlyFileStoreTable(
                Mockito.mock(FileIO.class),
                new Path("memory://manual_analysis_guard"),
                new TableSchema(
                        0,
                        Collections.singletonList(new DataField(0, "id", new IntType())),
                        0,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        ImmutableMap.of("scan.manifest.parallelism", "0"),
                        null),
                CatalogEnvironment.empty()) {
            @Override
            public ReadBuilder newReadBuilder() {
                planningStarted.set(true);
                return super.newReadBuilder();
            }
        };
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        PaimonExternalDatabase database = Mockito.mock(PaimonExternalDatabase.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getRemoteName()).thenReturn("db");
        PaimonExternalTable table = Mockito.spy(new TestPaimonExternalTable(catalog, database));
        Mockito.doReturn(-1L).when(table).getRowCount();
        Mockito.doReturn(Collections.emptySet()).when(table).getColumnIndexPairs(Mockito.any());
        AnalyzeTableCommand command = mockAnalyzeCommand(AnalysisMethod.FULL, ScheduleType.ONCE, false, false);
        Mockito.when(command.getTable()).thenReturn(table);
        AnalysisManager manager = new AnalysisManager();
        Env env = Mockito.mock(Env.class);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class);
                MockedStatic<PaimonUtils> paimonUtils = Mockito.mockStatic(PaimonUtils.class);
                MockedStatic<StatisticsUtil> statisticsUtil = Mockito.mockStatic(
                        StatisticsUtil.class, Mockito.CALLS_REAL_METHODS)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getNextId()).thenReturn(1L);
            paimonUtils.when(() -> PaimonUtils.getPaimonTable(table)).thenReturn(unsafeTable);
            statisticsUtil.when(() -> StatisticsUtil.isEmptyTable(table, AnalysisMethod.FULL)).thenReturn(false);

            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> manager.buildAnalysisJobInfo(command));
        }
        Assertions.assertFalse(planningStarted.get());
    }

    @Test
    public void testAsyncDropStats() throws InterruptedException {
        AtomicInteger count = new AtomicInteger(0);
        new MockUp<AnalysisManager>() {
            @Mock
            public void invalidateLocalStats(long catalogId, long dbId, long tableId, Set<String> columns,
                                             TableStatsMeta tableStats, PartitionNamesInfo partitionNames) {
                try {
                    Thread.sleep(1000);
                    count.incrementAndGet();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        AnalysisManager analysisManager = new AnalysisManager();
        for (int i = 0; i < 20; i++) {
            System.out.println("Submit " + i);
            analysisManager.submitAsyncDropStatsTask(0, 0, 0, null, false);
        }
        Thread.sleep(10000);
        System.out.println(count.get());
        Assertions.assertTrue(count.get() > 0);
        Assertions.assertTrue(count.get() <= 20);
    }

    private static class TestPaimonExternalTable extends PaimonExternalTable {
        private TestPaimonExternalTable(PaimonExternalCatalog catalog, PaimonExternalDatabase database) {
            super(30001L, "table", "table", catalog, database);
        }

        @Override
        protected synchronized void makeSureInitialized() {
        }
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

    // A table of the given database which reports one replica for the rows a load reports.
    private OlapTable mockOlapTableInReplica(long tableId, String name, Database db) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(tableId);
        Mockito.when(table.getBaseIndexId()).thenReturn(10001L);
        Mockito.when(table.getIndexIdList()).thenReturn(ImmutableList.of(10001L));
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.getDatabase()).thenReturn(db);
        TableProperty tableProperty = Mockito.mock(TableProperty.class);
        ReplicaAllocation replicaAllocation = Mockito.mock(ReplicaAllocation.class);
        Mockito.when(tableProperty.getReplicaAllocation()).thenReturn(replicaAllocation);
        Mockito.when(replicaAllocation.getTotalReplicaNum()).thenReturn((short) 1);
        Mockito.when(table.getTableProperty()).thenReturn(tableProperty);
        db.registerTable(table);
        return table;
    }

    /**
     * A load recreates the statistics record of a table whose record was removed, e.g. by TRUNCATE TABLE, and
     * accumulates the loaded rows into it. The record is created on the transaction visibility of the load,
     * which the master applies in {@code DatabaseTransactionMgr.updateCatalogAfterVisible} and a follower
     * applies while it replays the transaction state, so the row count is reported on every frontend without
     * an edit log entry of its own.
     */
    @Test
    public void testLoadCreatesTheStatisticsRecordOfATableWithoutOne() throws Exception {
        AnalysisManager manager = Mockito.spy(new AnalysisManager());
        InternalCatalog catalog = new InternalCatalog();
        Database db = new Database(20001L, "test_load_creates_stats");
        catalog.unprotectCreateDb(db);
        OlapTable table = mockOlapTableInReplica(30001L, "t_load", db);

        Map<Long, Map<Long, Long>> tabletRecords = Maps.newHashMap();
        tabletRecords.put(30001L, ImmutableMap.of(1L, 3L, 2L, 2L));

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            // What the master does when the loading transaction becomes visible.
            manager.updateUpdatedRows(tabletRecords, db.getId(), 0L);

            // The record exists with the rows of the load, and the planner reads them as the row count of the
            // table while the backends have not reported the new tablets yet.
            TableStatsMeta record = manager.findTableStatsStatus(30001L);
            Assertions.assertNotNull(record);
            Assertions.assertEquals(5, record.updatedRows.get());
            Assertions.assertEquals(5, record.getRowCountWithDeltaRows(table, 10001L));
            Assertions.assertTrue(record.isColumnsStatsEmpty());

            // A record which exists is accumulated, not replaced.
            manager.updateUpdatedRows(tabletRecords, db.getId(), 0L);
            Assertions.assertSame(record, manager.findTableStatsStatus(30001L));
            Assertions.assertEquals(10, record.updatedRows.get());
        }
    }

    @Test
    public void testReplayOfALoadCreatesTheSameStatisticsRecord() throws Exception {
        // A follower applies the row counts of a load while it replays the transaction state, through the very
        // same method. A table whose record the truncation removed gets the same record there.
        AnalysisManager manager = Mockito.spy(new AnalysisManager());
        InternalCatalog catalog = new InternalCatalog();
        Database db = new Database(20001L, "test_load_replay_creates_stats");
        catalog.unprotectCreateDb(db);
        OlapTable table = mockOlapTableInReplica(30002L, "t_replay", db);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            Map<Long, Map<Long, Long>> tabletRecords = Maps.newHashMap();
            tabletRecords.put(30002L, ImmutableMap.of(1L, 4L));
            manager.replayUpdateRowsRecord(new UpdateRowsEvent(tabletRecords, db.getId()));
        }

        TableStatsMeta record = manager.findTableStatsStatus(30002L);
        Assertions.assertNotNull(record);
        Assertions.assertEquals(4, record.updatedRows.get());
        Assertions.assertEquals(4, record.getRowCountWithDeltaRows(table, 10001L));
    }

    @Test
    public void testLoadDoesNotTouchTheCollectedRowCountOfAnExistingRecord() throws Exception {
        AnalysisManager manager = Mockito.spy(new AnalysisManager());
        InternalCatalog catalog = new InternalCatalog();
        Database db = new Database(20001L, "test_load_keeps_stats");
        catalog.unprotectCreateDb(db);
        OlapTable table = mockOlapTableInReplica(30003L, "t_keeps", db);

        // The record an analysis left: 100 collected rows, no delta.
        TableStatsMeta analyzed = new TableStatsMeta(table);
        analyzed.addIndexRowForTest(10001L, 100);
        analyzed.rowCount = 100;
        analyzed.updatedRows.set(100);
        analyzed.setUpdatedRowsBaseForTest(100);
        manager.replayUpdateTableStatsStatus(analyzed);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            Map<Long, Map<Long, Long>> tabletRecords = Maps.newHashMap();
            tabletRecords.put(30003L, ImmutableMap.of(1L, 7L));
            manager.updateUpdatedRows(tabletRecords, db.getId(), 0L);
        }

        TableStatsMeta record = manager.findTableStatsStatus(30003L);
        Assertions.assertSame(analyzed, record);
        Assertions.assertEquals(100, record.getRowCount(10001L));
        Assertions.assertEquals(107, record.updatedRows.get());
        Assertions.assertEquals(107, record.getRowCountWithDeltaRows(table, 10001L));
    }

    @Test
    public void testALoadWhichReportsRowsByTableIdCreatesTheRecordAsWell() throws Exception {
        // A cloud commit reports the rows of a transaction by table id instead of by tablet, and journals the
        // event, so the follower replays it through this branch as well.
        AnalysisManager manager = Mockito.spy(new AnalysisManager());
        InternalCatalog catalog = new InternalCatalog();
        Database db = new Database(20001L, "test_load_by_table_id");
        catalog.unprotectCreateDb(db);
        OlapTable table = mockOlapTableInReplica(30004L, "t_by_table", db);

        Map<Long, Long> records = Maps.newHashMap();
        records.put(30004L, 6L);
        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            manager.replayUpdateRowsRecord(new UpdateRowsEvent(db.getId(), records));
        }

        TableStatsMeta record = manager.findTableStatsStatus(30004L);
        Assertions.assertNotNull(record);
        Assertions.assertEquals(6, record.updatedRows.get());
        Assertions.assertEquals(6, record.getRowCountWithDeltaRows(table, 10001L));
    }

    @Test
    public void testLoadOfAnUnresolvableTableDoesNotCreateARecord() throws Exception {
        AnalysisManager manager = Mockito.spy(new AnalysisManager());
        InternalCatalog catalog = new InternalCatalog();
        Database db = new Database(20001L, "test_load_missing_table");
        catalog.unprotectCreateDb(db);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            Map<Long, Map<Long, Long>> tabletRecords = Maps.newHashMap();
            tabletRecords.put(39999L, ImmutableMap.of(1L, 1L));
            manager.updateUpdatedRows(tabletRecords, db.getId(), 0L);
        }
        Assertions.assertNull(manager.findTableStatsStatus(39999L));
    }

    private OlapTable mockOlapTableForRemoval(long tableId, String name, Database db, List<String> calls) {
        OlapTable table = mockOlapTableInReplica(tableId, name, db);
        // The lock is not taken by the mock, record the calls instead: the deletion has to be journaled
        // between them.
        Mockito.doAnswer(invocation -> {
            calls.add("writeLock");
            return null;
        }).when(table).writeLock();
        Mockito.doAnswer(invocation -> {
            calls.add("writeUnlock");
            return null;
        }).when(table).writeUnlock();
        return table;
    }

    /**
     * The record of a table which has none is recreated by the load that makes a transaction visible, in
     * {@code DatabaseTransactionMgr.updateCatalogAfterVisible}, which runs under the table write lock until it
     * returns. A deletion which is applied between the journaling of that transaction and the recreation is
     * replayed after the creation on every follower while the master applied it before the creation, so the
     * master would keep a record which no follower has. The deletion is therefore applied and journaled under
     * the same lock, and the journal entry is written before the lock is released.
     */
    @Test
    public void testRemoveTableStatsAndLogHoldsTheTableLock() {
        AnalysisManager manager = new AnalysisManager();
        InternalCatalog catalog = new InternalCatalog();
        Database db = new Database(20001L, "test_remove_table_stats_lock");
        catalog.unprotectCreateDb(db);
        List<String> calls = new ArrayList<>();
        OlapTable table = mockOlapTableForRemoval(30005L, "t_remove_stats", db, calls);
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.doAnswer(invocation -> {
                calls.add("logDeleteTableStats");
                // The record is already removed, and the lock is still held.
                Assertions.assertNull(manager.findTableStatsStatus(30005L));
                return null;
            }).when(editLog).logDeleteTableStats(Mockito.any(TableStatsDeletionLog.class));

            manager.replayUpdateTableStatsStatus(new TableStatsMeta(table));
            manager.removeTableStatsAndLog(table);

            Assertions.assertEquals(ImmutableList.of("writeLock", "logDeleteTableStats", "writeUnlock"), calls);
            Assertions.assertNull(manager.findTableStatsStatus(30005L));
        }
    }

    @Test
    public void testRemoveTableStatsAndLogDoesNotLockANonOlapTable() {
        AnalysisManager manager = new AnalysisManager();
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);

            TableIf table = Mockito.mock(TableIf.class);
            Mockito.when(table.getId()).thenReturn(30006L);

            manager.removeTableStatsAndLog(table);

            // A load only recreates the record of an OLAP table, there is no creation to be ordered with.
            Mockito.verify(table, Mockito.never()).writeLock();
            Mockito.verify(editLog).logDeleteTableStats(Mockito.any(TableStatsDeletionLog.class));
        }
    }
    // A record whose serialization waits, so that a test can publish another record while the image of the
    // statistics map is written. The latches are transient: they are not part of the image.
    private static class BlockingTableStatsMeta extends TableStatsMeta {
        private final transient CountDownLatch imageWriting = new CountDownLatch(1);
        private final transient CountDownLatch resumeImage = new CountDownLatch(1);

        @Override
        public void write(DataOutput out) throws IOException {
            imageWriting.countDown();
            try {
                resumeImage.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            super.write(out);
        }
    }

    /**
     * The image writes the number of records of the statistics map and then the records, and the reader trusts
     * that number. A record which a load publishes while the image is written, for instance from the commit
     * report of a cloud backend, must therefore not be inserted between the two: it would be written although
     * the declared number excludes it, and the reader would take the extra record as the header of the next
     * module of the image.
     */
    @Test
    public void testRecordInsertedWhileTheImageIsWrittenWaitsForTheStatsMap() throws Exception {
        AnalysisManager manager = new AnalysisManager();
        InternalCatalog catalog = new InternalCatalog();
        Database db = new Database(20002L, "test_stats_map_lock");
        catalog.unprotectCreateDb(db);
        OlapTable table = mockOlapTableInReplica(30008L, "t_stats_map_lock", db);
        BlockingTableStatsMeta record = new BlockingTableStatsMeta();
        manager.replayUpdateTableStatsStatus(record);

        ByteArrayOutputStream image = new ByteArrayOutputStream();
        Thread dump = new Thread(() -> {
            try {
                manager.write(new DataOutputStream(image));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        dump.start();
        Assertions.assertTrue(record.imageWriting.await(30, TimeUnit.SECONDS), "the image is not being written");

        // The load of another table publishes its record from its own thread, exactly like the commit report
        // of a cloud backend, which takes no table lock.
        CountDownLatch loadReady = new CountDownLatch(1);
        CountDownLatch insertNow = new CountDownLatch(1);
        CountDownLatch inserted = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread load = new Thread(() -> {
            try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
                envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
                loadReady.countDown();
                insertNow.await();
                manager.replayUpdateRowsRecord(new UpdateRowsEvent(db.getId(),
                        ImmutableMap.of(table.getId(), 1L)));
                inserted.countDown();
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        load.start();
        Assertions.assertTrue(loadReady.await(30, TimeUnit.SECONDS), "the load is not ready");
        insertNow.countDown();
        // The insertion has to wait for the monitor which the image holds, otherwise the record can be written
        // although the number of records was already written.
        boolean insertedWhileWriting = inserted.await(500, TimeUnit.MILLISECONDS);

        record.resumeImage.countDown();
        dump.join(30000);
        load.join(30000);

        Assertions.assertFalse(insertedWhileWriting, "the record was inserted while the image was written");
        Assertions.assertNull(failure.get());
        Assertions.assertFalse(dump.isAlive(), "the image was not written");
        Assertions.assertFalse(load.isAlive());

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(image.toByteArray()));
        AnalysisManager loaded = AnalysisManager.readFields(in);
        // The reader consumes exactly the records which the image declares.
        Assertions.assertEquals(0, in.available(), "the image contains more records than it declares");
        Assertions.assertNotNull(loaded.findTableStatsStatus(0L));
        Assertions.assertNull(loaded.findTableStatsStatus(table.getId()),
                "the record which was created after the image was written is in the image");
    }
    /**
     * A cloud load reports the rows of its transaction from the backend, and the report applies them and
     * journals them afterwards. A whole table DROP STATS removes the record and journals the deletion, so
     * without a common lock the deletion can be applied between the application of the report and its journal
     * entry: a follower then replays "remove, then add" and keeps a record which the master no longer has.
     */
    @Test
    public void testCloudRowUpdateIsSerializedWithTheTableLock() throws Exception {
        AnalysisManager manager = new AnalysisManager();
        InternalCatalog catalog = new InternalCatalog();
        Database db = new Database(20003L, "test_cloud_row_update_lock");
        catalog.unprotectCreateDb(db);
        OlapTable table = mockOlapTableInReplica(30009L, "t_cloud_row_update", db);
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        Mockito.when(table.tryWriteLockIfExist(Mockito.anyLong(), Mockito.any(TimeUnit.class)))
                .thenAnswer(invocation -> lock.writeLock().tryLock(invocation.getArgument(0),
                        invocation.getArgument(1)));
        Mockito.doAnswer(invocation -> {
            lock.writeLock().unlock();
            return null;
        }).when(table).writeUnlock();

        CountDownLatch reportReady = new CountDownLatch(1);
        CountDownLatch reported = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread report = new Thread(() -> {
            try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
                envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
                envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
                Mockito.when(env.getEditLog()).thenReturn(editLog);
                Mockito.when(env.isMaster()).thenReturn(true);
                reportReady.countDown();
                manager.updateUpdatedRows(ImmutableMap.of(table.getId(), 5L), db.getId());
                reported.countDown();
            } catch (Throwable t) {
                failure.set(t);
            }
        });

        // The deletion holds the table write lock while it removes the record and journals it.
        lock.writeLock().lock();
        report.start();
        Assertions.assertTrue(reportReady.await(30, TimeUnit.SECONDS), "the report is not ready");
        boolean appliedWhileTheDeletionRan = reported.await(500, TimeUnit.MILLISECONDS);
        lock.writeLock().unlock();
        report.join(30000);

        Assertions.assertFalse(appliedWhileTheDeletionRan, "the report was applied while the table was locked");
        Assertions.assertNull(failure.get());
        Assertions.assertFalse(report.isAlive());
        Mockito.verify(env, Mockito.atLeast(1)).isMaster();
        TableStatsMeta record = manager.findTableStatsStatus(table.getId());
        Assertions.assertNotNull(record);
        Assertions.assertEquals(5, record.updatedRows.get());
        Mockito.verify(editLog, Mockito.times(1)).logUpdateRowsRecord(Mockito.any(UpdateRowsEvent.class));
    }
    /**
     * A cloud stream load queues the report of the rows it loaded and can return before the frontend received
     * it. A whole table TRUNCATE replaces the partitions and keeps the table id, so a report which arrives
     * afterwards counts the rows the truncation removed: they must neither be applied to the record which the
     * next load recreates nor be journaled.
     */
    @Test
    public void testReportOfAPartitionRemovedByTruncateIsNotApplied() throws Exception {
        AnalysisManager manager = new AnalysisManager();
        InternalCatalog catalog = new InternalCatalog();
        Database db = new Database(20004L, "test_stale_cloud_report");
        catalog.unprotectCreateDb(db);
        OlapTable table = mockOlapTableInReplica(30010L, "t_stale_report", db);
        long replacedPartitionId = 40001L;
        long partitionId = 40002L;
        Mockito.when(table.getPartition(partitionId)).thenReturn(Mockito.mock(Partition.class));
        Mockito.when(table.tryWriteLockIfExist(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(true);
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(env.isMaster()).thenReturn(true);

            // The report of the 100 rows which the truncation removed arrives after the replacement partitions
            // were created.
            manager.updateUpdatedRows(ImmutableMap.of(table.getId(), 100L), db.getId(),
                    ImmutableMap.of(table.getId(), ImmutableList.of(replacedPartitionId)));
            Assertions.assertNull(manager.findTableStatsStatus(table.getId()));
            Mockito.verify(editLog, Mockito.never()).logUpdateRowsRecord(Mockito.any(UpdateRowsEvent.class));

            // The 3 rows which the next load reports belong to the current partition and recreate the record.
            manager.updateUpdatedRows(ImmutableMap.of(table.getId(), 3L), db.getId(),
                    ImmutableMap.of(table.getId(), ImmutableList.of(partitionId)));
            TableStatsMeta record = manager.findTableStatsStatus(table.getId());
            Assertions.assertNotNull(record);
            Assertions.assertEquals(3, record.updatedRows.get());
            Mockito.verify(editLog).logUpdateRowsRecord(Mockito.any(UpdateRowsEvent.class));
        }
    }

    @Test
    public void testReportOfRemovedPartitionsIsRecognized() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getPartition(1L)).thenReturn(Mockito.mock(Partition.class));

        Assertions.assertTrue(AnalysisManager.reportCountsCurrentPartitions(table, ImmutableList.of()));
        Assertions.assertTrue(AnalysisManager.reportCountsCurrentPartitions(table, ImmutableList.of(1L)));
        Assertions.assertFalse(AnalysisManager.reportCountsCurrentPartitions(table, ImmutableList.of(2L)));
        // A report which counts rows of a removed partition is stale as a whole.
        Assertions.assertFalse(AnalysisManager.reportCountsCurrentPartitions(table, ImmutableList.of(1L, 2L)));
    }
}
