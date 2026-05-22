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

import org.apache.doris.statistics.util.StatisticsUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

public class AnalysisJobTest {

    // make user task has been set corresponding job
    @Test
    public void initTest() {
        AnalysisInfo jobInfo = Mockito.mock(AnalysisInfo.class);
        OlapAnalysisTask task = Mockito.mock(OlapAnalysisTask.class);
        AnalysisJob analysisJob = new AnalysisJob(jobInfo, Arrays.asList(task));
        Assertions.assertSame(task.job, analysisJob);
    }

    @Test
    public void testAppendBufTest1() {
        AnalysisInfo analysisInfo = Mockito.mock(AnalysisInfo.class);
        OlapAnalysisTask olapAnalysisTask = Mockito.mock(OlapAnalysisTask.class);
        OlapAnalysisTask olapAnalysisTask2 = Mockito.mock(OlapAnalysisTask.class);
        AtomicInteger flushBufferInvokeTimes = new AtomicInteger();

        AnalysisJob job = Mockito.spy(new AnalysisJob(analysisInfo, Arrays.asList(olapAnalysisTask)));
        Mockito.doAnswer(inv -> {
            flushBufferInvokeTimes.incrementAndGet();
            return null;
        }).when(job).flushBuffer();
        Mockito.doNothing().when(job).updateTaskState(ArgumentMatchers.any(), ArgumentMatchers.anyString());
        Mockito.doNothing().when(job).deregisterJob();

        job.queryingTask = new HashSet<>();
        job.queryingTask.add(olapAnalysisTask);
        job.queryingTask.add(olapAnalysisTask2);
        job.queryFinished = new HashSet<>();
        job.buf = new ArrayList<>();

        // not all task finished nor cached limit exceed, shouldn't  write
        job.appendBuf(olapAnalysisTask, Arrays.asList(new ColStatsData()));
        Assertions.assertEquals(0, flushBufferInvokeTimes.get());
    }

    @Test
    public void testAppendBufTest2() {
        AnalysisInfo analysisInfo = Mockito.mock(AnalysisInfo.class);
        OlapAnalysisTask olapAnalysisTask = Mockito.mock(OlapAnalysisTask.class);
        AtomicInteger writeBufInvokeTimes = new AtomicInteger();
        AtomicInteger deregisterTimes = new AtomicInteger();

        AnalysisJob job = Mockito.spy(new AnalysisJob(analysisInfo, Arrays.asList(olapAnalysisTask)));
        Mockito.doAnswer(inv -> {
            writeBufInvokeTimes.incrementAndGet();
            return null;
        }).when(job).flushBuffer();
        Mockito.doNothing().when(job).updateTaskState(ArgumentMatchers.any(), ArgumentMatchers.anyString());
        Mockito.doAnswer(inv -> {
            deregisterTimes.getAndIncrement();
            return null;
        }).when(job).deregisterJob();

        job.queryingTask = new HashSet<>();
        job.queryingTask.add(olapAnalysisTask);
        job.queryFinished = new HashSet<>();
        job.buf = new ArrayList<>();

        job.appendBuf(olapAnalysisTask, Arrays.asList(new ColStatsData()));
        // all task finished, should write and deregister this job
        Assertions.assertEquals(1, writeBufInvokeTimes.get());
        Assertions.assertEquals(1, deregisterTimes.get());
    }

    @Test
    public void testAppendBufTest3() {
        AnalysisInfo analysisInfo = Mockito.mock(AnalysisInfo.class);
        OlapAnalysisTask olapAnalysisTask = Mockito.mock(OlapAnalysisTask.class);
        AtomicInteger writeBufInvokeTimes = new AtomicInteger();

        AnalysisJob job = Mockito.spy(new AnalysisJob(analysisInfo, Arrays.asList(olapAnalysisTask)));
        Mockito.doAnswer(inv -> {
            writeBufInvokeTimes.incrementAndGet();
            return null;
        }).when(job).flushBuffer();
        Mockito.doNothing().when(job).updateTaskState(ArgumentMatchers.any(), ArgumentMatchers.anyString());
        Mockito.doNothing().when(job).deregisterJob();

        job.queryingTask = new HashSet<>();
        job.queryingTask.add(olapAnalysisTask);
        job.queryFinished = new HashSet<>();
        job.buf = new ArrayList<>();
        ColStatsData colStatsData = new ColStatsData();
        for (int i = 0; i < StatisticsUtil.getInsertMergeCount(); i++) {
            job.buf.add(colStatsData);
        }

        job.appendBuf(olapAnalysisTask, Arrays.asList(new ColStatsData()));
        // cache limit exceed, should write them
        Assertions.assertEquals(1, writeBufInvokeTimes.get());
    }

    @Test
    public void testUpdateTaskState() {
        AnalysisInfo info = Mockito.mock(AnalysisInfo.class);
        OlapAnalysisTask task1 = Mockito.mock(OlapAnalysisTask.class);
        OlapAnalysisTask task2 = Mockito.mock(OlapAnalysisTask.class);
        AtomicInteger updateTaskStatusInvokeTimes = new AtomicInteger();

        AnalysisManager analysisManager = Mockito.mock(AnalysisManager.class);
        Mockito.doAnswer(inv -> {
            updateTaskStatusInvokeTimes.getAndIncrement();
            return null;
        })
                .when(analysisManager).updateTaskStatus(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyLong());

        AnalysisJob job = new AnalysisJob(info, Collections.singletonList(task1));
        job.analysisManager = analysisManager;
        job.queryFinished = new HashSet<>();
        job.queryFinished.add(task2);
        job.updateTaskState(AnalysisState.FAILED, "");
        Assertions.assertEquals(2, updateTaskStatusInvokeTimes.get());
    }

    @Test
    public void testWriteBuf1() throws Exception {
        AnalysisInfo info = Mockito.mock(AnalysisInfo.class);
        OlapAnalysisTask task1 = Mockito.mock(OlapAnalysisTask.class);
        OlapAnalysisTask task2 = Mockito.mock(OlapAnalysisTask.class);

        AnalysisJob job = Mockito.spy(new AnalysisJob(info, Collections.singletonList(task1)));
        Mockito.doNothing().when(job).updateTaskState(ArgumentMatchers.any(), ArgumentMatchers.anyString());
        Mockito.doNothing().when(job).executeWithExceptionOnFail(ArgumentMatchers.any());

        job.queryFinished = new HashSet<>();
        job.queryFinished.add(task2);
        job.flushBuffer();

        Assertions.assertEquals(0, job.queryFinished.size());
    }

    @Test
    public void testWriteBuf2() throws Exception {
        AnalysisInfo info = Mockito.mock(AnalysisInfo.class);
        OlapAnalysisTask task1 = Mockito.mock(OlapAnalysisTask.class);
        OlapAnalysisTask task2 = Mockito.mock(OlapAnalysisTask.class);

        AnalysisJob job = Mockito.spy(new AnalysisJob(info, Collections.singletonList(task1)));
        Mockito.doNothing().when(job).updateTaskState(ArgumentMatchers.any(), ArgumentMatchers.anyString());
        Mockito.doNothing().when(job).executeWithExceptionOnFail(ArgumentMatchers.any());

        job.buf.add(new ColStatsData());
        job.queryFinished = new HashSet<>();
        job.queryFinished.add(task2);
        job.flushBuffer();
        Assertions.assertEquals(0, job.queryFinished.size());
    }

    @Test
    public void testSetSqlHash() throws Exception {
        AnalysisInfo info = Mockito.mock(AnalysisInfo.class);
        OlapAnalysisTask task1 = Mockito.mock(OlapAnalysisTask.class);
        OlapAnalysisTask task2 = Mockito.mock(OlapAnalysisTask.class);

        AnalysisJob job = Mockito.spy(new AnalysisJob(info, Collections.singletonList(task1)));
        Mockito.doNothing().when(job).updateTaskState(ArgumentMatchers.any(), ArgumentMatchers.anyString());
        Mockito.doNothing().when(job).executeWithExceptionOnFail(ArgumentMatchers.any());

        job.queryFinished = new HashSet<>();
        job.queryFinished.add(task2);
        job.buf.add(new ColStatsData());
        job.flushBuffer();
        Assertions.assertEquals(0, job.queryFinished.size());
        Assertions.assertEquals(0, job.buf.size());
        Assertions.assertEquals("a48a05f7d2f079b74481e2909a7fa79b", job.stmtExecutor.getContext().getSqlHash());
    }

}
