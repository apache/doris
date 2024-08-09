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
import org.apache.doris.analysis.AnalyzeTblStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.ShowAnalyzeStmt;
import org.apache.doris.analysis.ShowAutoAnalyzeJobsStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TQueryColumn;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

// CHECKSTYLE OFF
public class AnalysisManagerTest {
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

    // test build sync job
    @Test
    public void testBuildAndAssignJob1() throws Exception {
        AnalysisInfo analysisInfo = new AnalysisInfoBuilder().setJobColumns(new HashSet<>()).build();
        new MockUp<StatisticsUtil>() {

            @Mock
            public boolean statsTblAvailable() {
                return true;
            }
        };
        new MockUp<AnalysisManager>() {

            @Mock
            public AnalysisInfo buildAnalysisJobInfo(AnalyzeTblStmt stmt) throws DdlException {
                return analysisInfo;
            }

            @Mock
            @VisibleForTesting
            public void createTaskForExternalTable(AnalysisInfo jobInfo,
                    Map<Long, BaseAnalysisTask> analysisTasks,
                    boolean isSync) throws DdlException {
                // DO NOTHING
            }

            @Mock
            public void createTaskForEachColumns(AnalysisInfo jobInfo, Map<Long, BaseAnalysisTask> analysisTasks,
                    boolean isSync) throws DdlException {
                // DO NOTHING
            }

            @Mock
            public void syncExecute(Collection<BaseAnalysisTask> tasks) {
                // DO NOTHING
            }

            @Mock
            public void updateTableStats(AnalysisInfo jobInfo) {
                // DO NOTHING
            }
        };
        AnalyzeTblStmt analyzeTblStmt = new AnalyzeTblStmt(new TableName("test"),
                new PartitionNames(false, new ArrayList<String>() {
                    {
                        add("p1");
                        add("p2");
                    }
                }), new ArrayList<String>() {
                    {
                        add("c1");
                        add("c2");
                    }
        }, new AnalyzeProperties(new HashMap<String, String>() {
            {
                put(AnalyzeProperties.PROPERTY_SYNC, "true");
            }
        }));

        AnalysisManager analysisManager = new AnalysisManager();
        Assertions.assertNull(analysisManager.buildAndAssignJob(analyzeTblStmt));
        analysisInfo.jobColumns.add(Pair.of("index1", "c1"));
        analysisManager.buildAndAssignJob(analyzeTblStmt);
        new Expectations() {
            {
                analysisManager.syncExecute((Collection<BaseAnalysisTask>) any);
                times = 1;
                analysisManager.updateTableStats((AnalysisInfo) any);
                times = 1;
                // Jmockit would try to invoke this method with `null` when initiate instance of Expectations
                // and cause NPE, comment these lines until find other way to test behavior that don't invoke something.
                // analysisManager.persistAnalysisJob((AnalysisInfo) any);
                // times = 0;
            }
        };
    }

    // test build async job
    @Test
    public void testBuildAndAssignJob2(@Injectable OlapAnalysisTask analysisTask) throws Exception {
        AnalysisInfo analysisInfo = new AnalysisInfoBuilder().setJobColumns(new HashSet<>())
                .setScheduleType(ScheduleType.PERIOD)
                .build();
        new MockUp<StatisticsUtil>() {

            @Mock
            public boolean statsTblAvailable() {
                return true;
            }
        };
        new MockUp<AnalysisManager>() {

            @Mock
            public AnalysisInfo buildAnalysisJobInfo(AnalyzeTblStmt stmt) throws DdlException {
                return analysisInfo;
            }

            @Mock
            @VisibleForTesting
            public void createTaskForExternalTable(AnalysisInfo jobInfo,
                    Map<Long, BaseAnalysisTask> analysisTasks,
                    boolean isSync) throws DdlException {
                // DO NOTHING
            }

            @Mock
            public void createTaskForEachColumns(AnalysisInfo jobInfo, Map<Long, BaseAnalysisTask> analysisTasks,
                    boolean isSync) throws DdlException {
                analysisTasks.put(1L, analysisTask);
            }

            @Mock
            public void syncExecute(Collection<BaseAnalysisTask> tasks) {
                // DO NOTHING
            }

            @Mock
            public void updateTableStats(AnalysisInfo jobInfo) {
                // DO NOTHING
            }

            @Mock
            public void logCreateAnalysisJob(AnalysisInfo analysisJob) {

            }
        };
        AnalyzeTblStmt analyzeTblStmt = new AnalyzeTblStmt(new TableName("test"),
                new PartitionNames(false, new ArrayList<String>() {
                    {
                        add("p1");
                        add("p2");
                    }
                }), new ArrayList<String>() {
            {
                add("c1");
                add("c2");
            }
        }, new AnalyzeProperties(new HashMap<String, String>() {
            {
                put(AnalyzeProperties.PROPERTY_SYNC, "false");
                put(AnalyzeProperties.PROPERTY_PERIOD_SECONDS, "100");
            }
        }));
        AnalysisManager analysisManager = new AnalysisManager();
        analysisInfo.jobColumns.add(Pair.of("index1", "c1"));
        analysisManager.buildAndAssignJob(analyzeTblStmt);
        new Expectations() {
            {
                analysisManager.recordAnalysisJob(analysisInfo);
                times = 1;
            }
        };
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
    public void testShowAutoJobs(@Injectable ShowAnalyzeStmt stmt) {
        new MockUp<ShowAnalyzeStmt>() {
            @Mock
            public String getStateValue() {
                return null;
            }

            @Mock
            public TableName getDbTableName() {
                return null;
            }

            @Mock
            public boolean isAuto() {
                return true;
            }
        };
        AnalysisManager analysisManager = new AnalysisManager();
        analysisManager.analysisJobInfoMap.put(
            1L, new AnalysisInfoBuilder().setJobId(1).setJobType(JobType.MANUAL).build());
        analysisManager.analysisJobInfoMap.put(
            2L, new AnalysisInfoBuilder().setJobId(2).setJobType(JobType.SYSTEM).setState(AnalysisState.RUNNING).build());
        analysisManager.analysisJobInfoMap.put(
            3L, new AnalysisInfoBuilder().setJobId(3).setJobType(JobType.SYSTEM).setState(AnalysisState.FINISHED).build());
        analysisManager.analysisJobInfoMap.put(
            4L, new AnalysisInfoBuilder().setJobId(4).setJobType(JobType.SYSTEM).setState(AnalysisState.FAILED).build());
        List<AnalysisInfo> analysisInfos = analysisManager.findAnalysisJobs(stmt);
        Assertions.assertEquals(3, analysisInfos.size());
        Assertions.assertEquals(AnalysisState.RUNNING, analysisInfos.get(0).getState());
        Assertions.assertEquals(AnalysisState.FINISHED, analysisInfos.get(1).getState());
        Assertions.assertEquals(AnalysisState.FAILED, analysisInfos.get(2).getState());
    }

    @Test
    public void testShowAutoTasks(@Injectable ShowAnalyzeStmt stmt) {
        AnalysisManager analysisManager = new AnalysisManager();
        analysisManager.analysisTaskInfoMap.put(
            1L, new AnalysisInfoBuilder().setJobId(2).setJobType(JobType.MANUAL).build());
        analysisManager.analysisTaskInfoMap.put(
            2L, new AnalysisInfoBuilder().setJobId(1).setJobType(JobType.SYSTEM).setState(AnalysisState.RUNNING).build());
        analysisManager.analysisTaskInfoMap.put(
            3L, new AnalysisInfoBuilder().setJobId(1).setJobType(JobType.SYSTEM).setState(AnalysisState.FINISHED).build());
        analysisManager.analysisTaskInfoMap.put(
            4L, new AnalysisInfoBuilder().setJobId(1).setJobType(JobType.SYSTEM).setState(AnalysisState.FAILED).build());
        List<AnalysisInfo> analysisInfos = analysisManager.findTasks(1);
        Assertions.assertEquals(3, analysisInfos.size());
        Assertions.assertEquals(AnalysisState.RUNNING, analysisInfos.get(0).getState());
        Assertions.assertEquals(AnalysisState.FINISHED, analysisInfos.get(1).getState());
        Assertions.assertEquals(AnalysisState.FAILED, analysisInfos.get(2).getState());
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
                new ArrayList<>(), table, column1, Optional.empty(), ImmutableList.of());
        SlotReference slot2 = new SlotReference(new ExprId(2), "slot2", IntegerType.INSTANCE, true,
                new ArrayList<>(), table, column2, Optional.empty(), ImmutableList.of());
        SlotReference slot3 = new SlotReference(new ExprId(3), "slot3", IntegerType.INSTANCE, true,
                new ArrayList<>(), table, column3, Optional.empty(), ImmutableList.of());
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
    public void testShowAutoJobs() {
        AnalysisManager manager = new AnalysisManager();
        TableName high1 = new TableName("catalog1", "db1", "high1");
        TableName high2 = new TableName("catalog2", "db2", "high2");
        TableName mid1 = new TableName("catalog3", "db3", "mid1");
        TableName mid2 = new TableName("catalog4", "db4", "mid2");
        TableName low1 = new TableName("catalog5", "db5", "low1");

        manager.highPriorityJobs.put(high1, new HashSet<>());
        manager.highPriorityJobs.get(high1).add(Pair.of("index1", "col1"));
        manager.highPriorityJobs.get(high1).add(Pair.of("index2", "col2"));
        manager.highPriorityJobs.put(high2, new HashSet<>());
        manager.highPriorityJobs.get(high2).add(Pair.of("index1", "col3"));
        manager.midPriorityJobs.put(mid1, new HashSet<>());
        manager.midPriorityJobs.get(mid1).add(Pair.of("index1", "col4"));
        manager.midPriorityJobs.put(mid2, new HashSet<>());
        manager.midPriorityJobs.get(mid2).add(Pair.of("index1", "col5"));
        manager.lowPriorityJobs.put(low1, new HashSet<>());
        manager.lowPriorityJobs.get(low1).add(Pair.of("index1", "col6"));
        manager.lowPriorityJobs.get(low1).add(Pair.of("index1", "col7"));

        new MockUp<StatementBase>() {
            @Mock
            public boolean isAnalyzed() {
                return true;
            }
        };
        ShowAutoAnalyzeJobsStmt stmt = new ShowAutoAnalyzeJobsStmt(null, null);
        List<AutoAnalysisPendingJob> autoAnalysisPendingJobs = manager.showAutoPendingJobs(stmt);
        Assertions.assertEquals(5, autoAnalysisPendingJobs.size());
        AutoAnalysisPendingJob job = autoAnalysisPendingJobs.get(0);
        Assertions.assertEquals("catalog1", job.catalogName);
        Assertions.assertEquals("db1", job.dbName);
        Assertions.assertEquals("high1", job.tableName);
        Assertions.assertEquals(2, job.columns.size());
        Assertions.assertTrue(job.columns.contains(Pair.of("index1", "col1")));
        Assertions.assertTrue(job.columns.contains(Pair.of("index2", "col2")));
        Assertions.assertEquals(JobPriority.HIGH, job.priority);

        job = autoAnalysisPendingJobs.get(1);
        Assertions.assertEquals("catalog2", job.catalogName);
        Assertions.assertEquals("db2", job.dbName);
        Assertions.assertEquals("high2", job.tableName);
        Assertions.assertEquals(1, job.columns.size());
        Assertions.assertTrue(job.columns.contains(Pair.of("index1", "col3")));
        Assertions.assertEquals(JobPriority.HIGH, job.priority);

        job = autoAnalysisPendingJobs.get(2);
        Assertions.assertEquals("catalog3", job.catalogName);
        Assertions.assertEquals("db3", job.dbName);
        Assertions.assertEquals("mid1", job.tableName);
        Assertions.assertEquals(1, job.columns.size());
        Assertions.assertTrue(job.columns.contains(Pair.of("index1", "col4")));
        Assertions.assertEquals(JobPriority.MID, job.priority);

        job = autoAnalysisPendingJobs.get(3);
        Assertions.assertEquals("catalog4", job.catalogName);
        Assertions.assertEquals("db4", job.dbName);
        Assertions.assertEquals("mid2", job.tableName);
        Assertions.assertEquals(1, job.columns.size());
        Assertions.assertTrue(job.columns.contains(Pair.of("index1", "col5")));
        Assertions.assertEquals(JobPriority.MID, job.priority);

        job = autoAnalysisPendingJobs.get(4);
        Assertions.assertEquals("catalog5", job.catalogName);
        Assertions.assertEquals("db5", job.dbName);
        Assertions.assertEquals("low1", job.tableName);
        Assertions.assertEquals(2, job.columns.size());
        Assertions.assertTrue(job.columns.contains(Pair.of("index1", "col6")));
        Assertions.assertTrue(job.columns.contains(Pair.of("index1", "col7")));
        Assertions.assertEquals(JobPriority.LOW, job.priority);

        new MockUp<ShowAutoAnalyzeJobsStmt>() {
            @Mock
            public String getPriority() {
                return JobPriority.HIGH.name().toUpperCase();
            }
        };
        List<AutoAnalysisPendingJob> highJobs = manager.showAutoPendingJobs(stmt);
        Assertions.assertEquals(2, highJobs.size());
        job = highJobs.get(0);
        Assertions.assertEquals("catalog1", job.catalogName);
        Assertions.assertEquals("db1", job.dbName);
        Assertions.assertEquals("high1", job.tableName);
        Assertions.assertEquals(2, job.columns.size());
        Assertions.assertTrue(job.columns.contains(Pair.of("index1", "col1")));
        Assertions.assertTrue(job.columns.contains(Pair.of("index2", "col2")));
        Assertions.assertEquals(JobPriority.HIGH, job.priority);

        job = highJobs.get(1);
        Assertions.assertEquals("catalog2", job.catalogName);
        Assertions.assertEquals("db2", job.dbName);
        Assertions.assertEquals("high2", job.tableName);
        Assertions.assertEquals(1, job.columns.size());
        Assertions.assertTrue(job.columns.contains(Pair.of("index1", "col3")));
        Assertions.assertEquals(JobPriority.HIGH, job.priority);

        new MockUp<ShowAutoAnalyzeJobsStmt>() {
            @Mock
            public String getPriority() {
                return JobPriority.MID.name().toUpperCase();
            }
        };
        List<AutoAnalysisPendingJob> midJobs = manager.showAutoPendingJobs(stmt);
        Assertions.assertEquals(2, midJobs.size());
        job = midJobs.get(0);
        Assertions.assertEquals("catalog3", job.catalogName);
        Assertions.assertEquals("db3", job.dbName);
        Assertions.assertEquals("mid1", job.tableName);
        Assertions.assertEquals(1, job.columns.size());
        Assertions.assertTrue(job.columns.contains(Pair.of("index1", "col4")));
        Assertions.assertEquals(JobPriority.MID, job.priority);

        job = midJobs.get(1);
        Assertions.assertEquals("catalog4", job.catalogName);
        Assertions.assertEquals("db4", job.dbName);
        Assertions.assertEquals("mid2", job.tableName);
        Assertions.assertEquals(1, job.columns.size());
        Assertions.assertTrue(job.columns.contains(Pair.of("index1", "col5")));
        Assertions.assertEquals(JobPriority.MID, job.priority);

        new MockUp<ShowAutoAnalyzeJobsStmt>() {
            @Mock
            public String getPriority() {
                return JobPriority.LOW.name().toUpperCase();
            }
        };
        List<AutoAnalysisPendingJob> lowJobs = manager.showAutoPendingJobs(stmt);
        Assertions.assertEquals(1, lowJobs.size());
        job = lowJobs.get(0);
        Assertions.assertEquals("catalog5", job.catalogName);
        Assertions.assertEquals("db5", job.dbName);
        Assertions.assertEquals("low1", job.tableName);
        Assertions.assertEquals(2, job.columns.size());
        Assertions.assertTrue(job.columns.contains(Pair.of("index1", "col6")));
        Assertions.assertTrue(job.columns.contains(Pair.of("index1", "col7")));
        Assertions.assertEquals(JobPriority.LOW, job.priority);
    }

    @Test
    public void testAsyncDropStats() throws InterruptedException {
        AtomicInteger count = new AtomicInteger(0);
        new MockUp<AnalysisManager>() {
            @Mock
            public void invalidateLocalStats(long catalogId, long dbId, long tableId, Set<String> columns,
                                             TableStatsMeta tableStats, PartitionNames partitionNames) {
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
            analysisManager.submitAsyncDropStatsTask(0, 0, 0, null, null);
        }
        Thread.sleep(25000);
        System.out.println(count.get());
        Assertions.assertTrue(count.get() > 10);
        Assertions.assertTrue(count.get() < 20);
    }
}
