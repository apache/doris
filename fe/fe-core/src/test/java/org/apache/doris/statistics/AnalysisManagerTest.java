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
import org.apache.doris.common.DdlException;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
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
                analysisManager.persistAnalysisJob(analysisInfo);
                times = 1;
            }
        };
    }

}
