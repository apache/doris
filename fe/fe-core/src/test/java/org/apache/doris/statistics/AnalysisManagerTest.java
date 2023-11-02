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
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

// CHECKSTYLE OFF
public class AnalysisManagerTest {
    @Test
    public void testUpdateTaskStatus(@Mocked BaseAnalysisTask task1,
            @Mocked BaseAnalysisTask task2) {

        new MockUp<AnalysisManager>() {
            @Mock
            public void logCreateAnalysisTask(AnalysisInfo job) {
            }

            @Mock
            public void logCreateAnalysisJob(AnalysisInfo job) {
            }

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
        AnalysisInfo analysisInfo = new AnalysisInfoBuilder().setColToPartitions(new HashMap<>()).build();
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
        analysisInfo.colToPartitions.put("c1", new HashSet<String>() {
            {
                add("p1");
                add("p2");
            }
        });
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
        AnalysisInfo analysisInfo = new AnalysisInfoBuilder().setColToPartitions(new HashMap<>())
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
        analysisInfo.colToPartitions.put("c1", new HashSet<String>() {
            {
                add("p1");
                add("p2");
            }
        });
        analysisManager.buildAndAssignJob(analyzeTblStmt);
        new Expectations() {
            {
                analysisManager.recordAnalysisJob(analysisInfo);
                times = 1;
            }
        };
    }

    @Test
    public void testSystemJobStatusUpdater() {
        new MockUp<BaseAnalysisTask>() {

            @Mock
            protected void init(AnalysisInfo info) {

            }
        };

        new MockUp<AnalysisManager>() {
            @Mock
            public void updateTableStats(AnalysisInfo jobInfo) {}

            @Mock
            protected void logAutoJob(AnalysisInfo autoJob) {

            }
        };

        AnalysisManager analysisManager = new AnalysisManager();
        AnalysisInfo job = new AnalysisInfoBuilder()
                .setJobId(0)
                .setColName("col1, col2").build();
        analysisManager.systemJobInfoMap.put(job.jobId, job);
        AnalysisInfo task1 = new AnalysisInfoBuilder()
                .setJobId(0)
                .setTaskId(1)
                .setState(AnalysisState.RUNNING)
                .setColName("col1").build();
        AnalysisInfo task2 = new AnalysisInfoBuilder()
                .setJobId(0)
                .setTaskId(1)
                .setState(AnalysisState.FINISHED)
                .setColName("col2").build();
        OlapAnalysisTask ot1 = new OlapAnalysisTask(task1);
        OlapAnalysisTask ot2 = new OlapAnalysisTask(task2);
        Map<Long, BaseAnalysisTask> taskMap = new HashMap<>();
        taskMap.put(ot1.info.taskId, ot1);
        taskMap.put(ot2.info.taskId, ot2);
        analysisManager.analysisJobIdToTaskMap.put(job.jobId, taskMap);

        // test invalid job
        AnalysisInfo invalidJob = new AnalysisInfoBuilder().setJobId(-1).build();
        analysisManager.systemJobStatusUpdater.apply(new TaskStatusWrapper(invalidJob,
                AnalysisState.FAILED, "", 0));

        // test finished
        analysisManager.systemJobStatusUpdater.apply(new TaskStatusWrapper(task1, AnalysisState.FAILED, "", 0));
        analysisManager.systemJobStatusUpdater.apply(new TaskStatusWrapper(task1, AnalysisState.FINISHED, "", 0));
        Assertions.assertEquals(1, analysisManager.autoJobs.size());
        Assertions.assertTrue(analysisManager.systemJobInfoMap.isEmpty());
    }

    @Test
    public void testReAnalyze() {
        new MockUp<OlapTable>() {

            int count = 0;
            int[] rowCount = new int[]{100, 200};
            @Mock
            public long getRowCount() {
                return rowCount[count++];
            }

            @Mock
            public List<Column> getBaseSchema() {
                return Lists.newArrayList(new Column("col1", PrimitiveType.INT));
            }

        };
        OlapTable olapTable = new OlapTable();
        TableStatsMeta stats1 = new TableStatsMeta(0, 50, new AnalysisInfoBuilder().setColName("col1").build());
        stats1.updatedRows.addAndGet(30);

        Assertions.assertTrue(olapTable.needReAnalyzeTable(stats1));
        TableStatsMeta stats2 = new TableStatsMeta(0, 190, new AnalysisInfoBuilder().setColName("col1").build());
        stats2.updatedRows.addAndGet(20);
        Assertions.assertFalse(olapTable.needReAnalyzeTable(stats2));

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
    public void testRecordLimit3() {
        Config.analyze_record_limit = 2;
        AnalysisManager analysisManager = new AnalysisManager();
        analysisManager.autoJobs.offer(new AnalysisInfoBuilder().setJobId(1).build());
        analysisManager.autoJobs.offer(new AnalysisInfoBuilder().setJobId(2).build());
        analysisManager.autoJobs.offer(new AnalysisInfoBuilder().setJobId(3).build());
        Assertions.assertEquals(2, analysisManager.autoJobs.size());
    }

}
