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

import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMode;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class AnalysisTaskExecutorTest extends TestWithFeService {

    @Mocked
    AnalysisTaskScheduler analysisTaskScheduler;

    @Override
    protected void runBeforeAll() throws Exception {
        try {
            InternalSchemaInitializer.createDB();
            createDatabase("analysis_job_test");
            connectContext.setDatabase("default_cluster:analysis_job_test");
            createTable("CREATE TABLE t1 (col1 int not null, col2 int not null, col3 int not null)\n"

                    + "DISTRIBUTED BY HASH(col3)\n" + "BUCKETS 1\n"
                    + "PROPERTIES(\n" + "    \"replication_num\"=\"1\"\n"
                    + ");");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testExpiredJobCancellation() throws Exception {
        AnalysisInfo analysisJobInfo = new AnalysisInfoBuilder().setJobId(0).setTaskId(0)
                .setCatalogName("internal").setDbName("default_cluster:analysis_job_test").setTblName("t1")
                .setColName("col1").setJobType(JobType.MANUAL)
                .setAnalysisMode(AnalysisMode.FULL)
                .setAnalysisMethod(AnalysisMethod.FULL)
                .setAnalysisType(AnalysisType.FUNDAMENTALS)
                .build();
        OlapAnalysisTask analysisJob = new OlapAnalysisTask(analysisJobInfo);

        new MockUp<AnalysisTaskScheduler>() {
            public synchronized BaseAnalysisTask getPendingTasks() {
                return analysisJob;
            }
        };

        AnalysisTaskExecutor analysisTaskExecutor = new AnalysisTaskExecutor(analysisTaskScheduler);
        BlockingQueue<AnalysisTaskWrapper> b = Deencapsulation.getField(analysisTaskExecutor, "taskQueue");
        AnalysisTaskWrapper analysisTaskWrapper = new AnalysisTaskWrapper(analysisTaskExecutor, analysisJob);
        Deencapsulation.setField(analysisTaskWrapper, "startTime", 5);
        b.put(analysisTaskWrapper);
        analysisTaskExecutor.start();
    }

    @Test
    public void testTaskExecution() throws Exception {

        new MockUp<StmtExecutor>() {
            @Mock
            public List<ResultRow> executeInternalQuery() {
                return Collections.emptyList();
            }
        };

        new MockUp<OlapAnalysisTask>() {
            @Mock
            public void execSQL(String sql) throws Exception {
            }
        };

        new MockUp<StatisticsCache>() {

            @Mock
            public void syncLoadColStats(long tableId, long idxId, String colName) {
            }
        };

        AnalysisTaskExecutor analysisTaskExecutor = new AnalysisTaskExecutor(analysisTaskScheduler);
        HashMap<String, Set<String>> colToPartitions = Maps.newHashMap();
        colToPartitions.put("col1", Collections.singleton("t1"));
        AnalysisInfo analysisInfo = new AnalysisInfoBuilder().setJobId(0).setTaskId(0)
                .setCatalogName("internal").setDbName("default_cluster:analysis_job_test").setTblName("t1")
                .setColName("col1").setJobType(JobType.MANUAL)
                .setAnalysisMode(AnalysisMode.FULL)
                .setAnalysisMethod(AnalysisMethod.FULL)
                .setAnalysisType(AnalysisType.FUNDAMENTALS)
                .setColToPartitions(colToPartitions)
                .build();
        OlapAnalysisTask task = new OlapAnalysisTask(analysisInfo);
        new MockUp<AnalysisTaskScheduler>() {
            @Mock
            public synchronized BaseAnalysisTask getPendingTasks() {
                return task;
            }
        };
        new MockUp<AnalysisManager>() {
            @Mock
            public void updateTaskStatus(AnalysisInfo info, AnalysisState jobState, String message, long time) {}
        };
        Deencapsulation.invoke(analysisTaskExecutor, "doFetchAndExecute");
    }
}
