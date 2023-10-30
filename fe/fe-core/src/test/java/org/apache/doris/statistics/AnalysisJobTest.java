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

import org.apache.doris.catalog.Env;
import org.apache.doris.qe.StmtExecutor;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class AnalysisJobTest {

    @Test
    public void initTest(@Mocked AnalysisInfo jobInfo, @Mocked OlapAnalysisTask task) {
        AnalysisJob analysisJob = new AnalysisJob(jobInfo, Arrays.asList(task));
        Assertions.assertSame(task.job, analysisJob);
    }

    @Test
    public void testAppendBufTest1(@Mocked AnalysisInfo analysisInfo, @Mocked OlapAnalysisTask olapAnalysisTask) {
        new MockUp<AnalysisJob>() {
            @Mock
            protected void writeBuf() {
            }

            @Mock
            public void updateTaskState(AnalysisState state, String msg) {
            }

            @Mock
            public void deregisterJob() {
            }
        };
        AnalysisJob job = new AnalysisJob(analysisInfo, Arrays.asList(olapAnalysisTask));
        job.queryingTask = new HashSet<>();
        job.queryingTask.add(olapAnalysisTask);
        job.queryFinished = new HashSet<>();
        job.buf = new ArrayList<>();
        job.total = 20;

        job.appendBuf(olapAnalysisTask, Arrays.asList(new ColStatsData()));
        new Expectations() {
            {
                job.writeBuf();
                times = 0;
            }
        };
    }

    @Test
    public void testAppendBufTest2(@Mocked AnalysisInfo analysisInfo, @Mocked OlapAnalysisTask olapAnalysisTask) {
        new MockUp<AnalysisJob>() {
            @Mock
            protected void writeBuf() {
            }

            @Mock
            public void updateTaskState(AnalysisState state, String msg) {
            }

            @Mock
            public void deregisterJob() {
            }
        };
        AnalysisJob job = new AnalysisJob(analysisInfo, Arrays.asList(olapAnalysisTask));
        job.queryingTask = new HashSet<>();
        job.queryingTask.add(olapAnalysisTask);
        job.queryFinished = new HashSet<>();
        job.buf = new ArrayList<>();
        job.total = 1;

        job.appendBuf(olapAnalysisTask, Arrays.asList(new ColStatsData()));
        new Expectations() {
            {
                job.writeBuf();
                times = 1;
                job.deregisterJob();
                times = 1;
            }
        };
    }

    @Test
    public void testAppendBufTest3(@Mocked AnalysisInfo analysisInfo, @Mocked OlapAnalysisTask olapAnalysisTask) {
        new MockUp<AnalysisJob>() {
            @Mock
            protected void writeBuf() {
            }

            @Mock
            public void updateTaskState(AnalysisState state, String msg) {
            }

            @Mock
            public void deregisterJob() {
            }
        };
        AnalysisJob job = new AnalysisJob(analysisInfo, Arrays.asList(olapAnalysisTask));
        job.queryingTask = new HashSet<>();
        job.queryingTask.add(olapAnalysisTask);
        job.queryFinished = new HashSet<>();
        job.buf = new ArrayList<>();
        ColStatsData colStatsData = new ColStatsData();
        for (int i = 0; i < StatisticConstants.ANALYZE_JOB_BUF_SIZE; i++) {
            job.buf.add(colStatsData);
        }
        job.total = 100;

        job.appendBuf(olapAnalysisTask, Arrays.asList(new ColStatsData()));
        new Expectations() {
            {
                job.writeBuf();
                times = 1;
            }
        };
    }

    @Test
    public void testUpdateTaskState(
            @Mocked AnalysisInfo info,
            @Mocked OlapAnalysisTask task1,
            @Mocked OlapAnalysisTask task2) {
        new MockUp<AnalysisManager>() {
            @Mock
            public void updateTaskStatus(AnalysisInfo info, AnalysisState taskState, String message, long time) {
            }
        };
        AnalysisManager analysisManager = new AnalysisManager();
        new MockUp<Env>() {
            @Mock
            public AnalysisManager getAnalysisManager() {
                return analysisManager;
            }
        };
        AnalysisJob job = new AnalysisJob(info, Collections.singletonList(task1));
        job.queryFinished = new HashSet<>();
        job.queryFinished.add(task2);
        job.updateTaskState(AnalysisState.FAILED, "");
        new Expectations() {
            {
                analysisManager.updateTaskStatus((AnalysisInfo) any, (AnalysisState) any, anyString, anyLong);
                times = 2;
            }
        };
    }

    @Test
    public void testWriteBuf1(@Mocked AnalysisInfo info,
            @Mocked OlapAnalysisTask task1, @Mocked OlapAnalysisTask task2) {
        AnalysisJob job = new AnalysisJob(info, Collections.singletonList(task1));
        job.queryFinished = new HashSet<>();
        job.queryFinished.add(task2);
        new MockUp<AnalysisJob>() {
            @Mock
            public void updateTaskState(AnalysisState state, String msg) {
            }

            @Mock
            protected void executeWithExceptionOnFail(StmtExecutor stmtExecutor) throws Exception {

            }

            @Mock
            protected void syncLoadStats() {
            }
        };
        new Expectations() {
            {
                job.syncLoadStats();
                times = 1;
            }
        };
        job.writeBuf();

        Assertions.assertEquals(0, job.queryFinished.size());
    }

    @Test
    public void testWriteBuf2(@Mocked AnalysisInfo info,
            @Mocked OlapAnalysisTask task1, @Mocked OlapAnalysisTask task2) {
        AnalysisJob job = new AnalysisJob(info, Collections.singletonList(task1));
        job.queryFinished = new HashSet<>();
        job.queryFinished.add(task2);
        new MockUp<AnalysisJob>() {
            @Mock
            public void updateTaskState(AnalysisState state, String msg) {
            }

            @Mock
            protected void executeWithExceptionOnFail(StmtExecutor stmtExecutor) throws Exception {
                throw new RuntimeException();
            }

            @Mock
            protected void syncLoadStats() {
            }
        };
        job.writeBuf();
        Assertions.assertEquals(1, job.queryFinished.size());
    }

}
