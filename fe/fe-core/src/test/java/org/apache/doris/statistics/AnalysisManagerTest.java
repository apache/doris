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
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        AnalysisInfo analysisInfo = new AnalysisInfoBuilder().setJobColumns(new ArrayList<>()).build();
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
        AnalysisInfo analysisInfo = new AnalysisInfoBuilder().setJobColumns(new ArrayList<>())
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
    public void testReAnalyze() {
        new MockUp<OlapTable>() {

            final Column c = new Column("col1", PrimitiveType.INT);
            @Mock
            public List<Column> getBaseSchema() {
                return Lists.newArrayList(c);
            }

            @Mock
            public List<Column> getColumns() { return Lists.newArrayList(c); }

            @Mock
            public List<Pair<String, String>> getColumnIndexPairs(Set<String> columns) {
                List<Pair<String, String>> jobList = Lists.newArrayList();
                jobList.add(Pair.of("1", "1"));
                jobList.add(Pair.of("2", "2"));
                jobList.add(Pair.of("3", "3"));
                return jobList;
            }
        };
        OlapTable olapTable = new OlapTable();
        List<Pair<String, String>> jobList = Lists.newArrayList();
        jobList.add(Pair.of("1", "1"));
        jobList.add(Pair.of("2", "2"));
        TableStatsMeta stats0 = new TableStatsMeta(
            0, new AnalysisInfoBuilder().setJobColumns(jobList)
            .setColName("col1").build(), olapTable);
        Assertions.assertTrue(olapTable.needReAnalyzeTable(stats0));

        new MockUp<OlapTable>() {
            int count = 0;
            int[] rowCount = new int[]{100, 100, 200, 200, 1, 1};

            @Mock
            public long getRowCount() {
                return rowCount[count++];
            }
            @Mock
            public List<Pair<String, String>> getColumnIndexPairs(Set<String> columns) {
                List<Pair<String, String>> jobList = Lists.newArrayList();
                return jobList;
            }
        };
        TableStatsMeta stats1 = new TableStatsMeta(
                50, new AnalysisInfoBuilder().setJobColumns(new ArrayList<>())
                .setColName("col1").build(), olapTable);
        stats1.updatedRows.addAndGet(50);

        Assertions.assertTrue(olapTable.needReAnalyzeTable(stats1));
        TableStatsMeta stats2 = new TableStatsMeta(
                190, new AnalysisInfoBuilder()
                .setJobColumns(new ArrayList<>()).setColName("col1").build(), olapTable);
        stats2.updatedRows.addAndGet(20);
        Assertions.assertFalse(olapTable.needReAnalyzeTable(stats2));

        TableStatsMeta stats3 = new TableStatsMeta(0, new AnalysisInfoBuilder()
                .setJobColumns(new ArrayList<>()).setEmptyJob(true).setColName("col1").build(), olapTable);
        Assertions.assertTrue(olapTable.needReAnalyzeTable(stats3));

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
        List<AnalysisInfo> analysisInfos = analysisManager.showAnalysisJob(stmt);
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
}
