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
import org.apache.doris.statistics.util.StatisticsUtil;

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
import java.util.concurrent.atomic.AtomicInteger;

public class AnalysisJobTest {

    // make user task has been set corresponding job
    @Test
    public void initTest(@Mocked AnalysisInfo jobInfo, @Mocked OlapAnalysisTask task) {
        AnalysisJob analysisJob = new AnalysisJob(jobInfo, Arrays.asList(task));
        Assertions.assertSame(task.job, analysisJob);
    }

    @Test
    public void testAppendBufTest1(@Mocked AnalysisInfo analysisInfo, @Mocked OlapAnalysisTask olapAnalysisTask) {
        AtomicInteger writeBufInvokeTimes = new AtomicInteger();
        new MockUp<AnalysisJob>() {
            @Mock
            protected void writeBuf() {
                writeBufInvokeTimes.incrementAndGet();
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
        job.totalTaskCount = 20;

        // not all task finished nor cached limit exceed, shouldn't  write
        job.appendBuf(olapAnalysisTask, Arrays.asList(new ColStatsData()));
        Assertions.assertEquals(0, writeBufInvokeTimes.get());
    }

    @Test
    public void testAppendBufTest2(@Mocked AnalysisInfo analysisInfo, @Mocked OlapAnalysisTask olapAnalysisTask) {
        AtomicInteger writeBufInvokeTimes = new AtomicInteger();
        AtomicInteger deregisterTimes = new AtomicInteger();

        new MockUp<AnalysisJob>() {
            @Mock
            protected void writeBuf() {
                writeBufInvokeTimes.incrementAndGet();
            }

            @Mock
            public void updateTaskState(AnalysisState state, String msg) {
            }

            @Mock
            public void deregisterJob() {
                deregisterTimes.getAndIncrement();
            }
        };
        AnalysisJob job = new AnalysisJob(analysisInfo, Arrays.asList(olapAnalysisTask));
        job.queryingTask = new HashSet<>();
        job.queryingTask.add(olapAnalysisTask);
        job.queryFinished = new HashSet<>();
        job.buf = new ArrayList<>();
        job.totalTaskCount = 1;

        job.appendBuf(olapAnalysisTask, Arrays.asList(new ColStatsData()));
        // all task finished, should write and deregister this job
        Assertions.assertEquals(1, writeBufInvokeTimes.get());
        Assertions.assertEquals(1, deregisterTimes.get());
    }

    @Test
    public void testAppendBufTest3(@Mocked AnalysisInfo analysisInfo, @Mocked OlapAnalysisTask olapAnalysisTask) {
        AtomicInteger writeBufInvokeTimes = new AtomicInteger();

        new MockUp<AnalysisJob>() {
            @Mock
            protected void writeBuf() {
                writeBufInvokeTimes.incrementAndGet();
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
        for (int i = 0; i < StatisticsUtil.getInsertMergeCount(); i++) {
            job.buf.add(colStatsData);
        }
        job.totalTaskCount = 100;

        job.appendBuf(olapAnalysisTask, Arrays.asList(new ColStatsData()));
        // cache limit exceed, should write them
        Assertions.assertEquals(1, writeBufInvokeTimes.get());
    }

    @Test
    public void testUpdateTaskState(
            @Mocked AnalysisInfo info,
            @Mocked OlapAnalysisTask task1,
            @Mocked OlapAnalysisTask task2) {
        AtomicInteger updateTaskStatusInvokeTimes = new AtomicInteger();
        new MockUp<AnalysisManager>() {
            @Mock
            public void updateTaskStatus(AnalysisInfo info, AnalysisState taskState, String message, long time) {
                updateTaskStatusInvokeTimes.getAndIncrement();
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
        Assertions.assertEquals(2, updateTaskStatusInvokeTimes.get());
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
        AnalysisJob job = new AnalysisJob(info, Collections.singletonList(task1));
        job.buf.add(new ColStatsData());
        job.queryFinished = new HashSet<>();
        job.queryFinished.add(task2);
        job.writeBuf();
        Assertions.assertEquals(1, job.queryFinished.size());
    }

}
