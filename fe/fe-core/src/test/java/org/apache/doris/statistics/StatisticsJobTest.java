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

import org.apache.doris.common.DdlException;
import org.apache.doris.statistics.StatisticsJob.JobState;
import org.apache.doris.statistics.StatisticsTask.TaskState;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class StatisticsJobTest {
    private StatisticsJob statisticsJobUnderTest;

    private StatisticsTask statisticsTaskUnderTest;

    @Before
    public void setUp() throws Exception {
        HashSet<Long> tblIds = new HashSet<>(Collections.singletonList(0L));
        Map<Long, List<String>> tblIdToPartitionName = Maps.newHashMap();
        Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();
        statisticsJobUnderTest = new StatisticsJob(0L, tblIds, tblIdToPartitionName,
                tableIdToColumnName, new HashMap<>());

        StatsCategory statsCategory = new StatsCategory();
        StatsGranularity statsGranularity = new StatsGranularity();
        List<StatsType> statsTypes = Collections.singletonList(StatsType.ROW_COUNT);
        statisticsTaskUnderTest = new SQLStatisticsTask(0L,
                Collections.singletonList(new StatisticsDesc(statsCategory, statsGranularity, statsTypes)));

        List<StatisticsTask> tasks = statisticsJobUnderTest.getTasks();
        tasks.add(statisticsTaskUnderTest);
    }

    @Test
    public void testUpdateJobState() throws Exception {
        // Run the test
        statisticsJobUnderTest.updateJobState(JobState.SCHEDULING);

        // Verify the results
        JobState jobState = statisticsJobUnderTest.getJobState();
        Assert.assertEquals(JobState.SCHEDULING, jobState);
    }

    @Test
    public void testUpdateJobState_ThrowsDdlException() {
        // Run the test
        Assert.assertThrows(DdlException.class,
                () -> statisticsJobUnderTest.updateJobState(JobState.RUNNING));
    }

    @Test
    public void testUpdateJobInfoByTaskId() throws Exception {
        // Setup
        statisticsJobUnderTest.updateJobState(JobState.SCHEDULING);
        statisticsJobUnderTest.updateJobState(JobState.RUNNING);
        statisticsTaskUnderTest.updateTaskState(TaskState.RUNNING);

        // Run the test
        long taskId = statisticsTaskUnderTest.getId();
        statisticsJobUnderTest.updateJobInfoByTaskId(taskId, "");

        // Verify the results
        JobState jobState = statisticsJobUnderTest.getJobState();
        Assert.assertEquals(JobState.FINISHED, jobState);

        TaskState taskState = statisticsTaskUnderTest.getTaskState();
        Assert.assertEquals(TaskState.FINISHED, taskState);
    }

    @Test
    public void testUpdateJobInfoByTaskIdFailed() throws Exception {
        // Setup
        statisticsJobUnderTest.updateJobState(JobState.SCHEDULING);
        statisticsJobUnderTest.updateJobState(JobState.RUNNING);
        statisticsTaskUnderTest.updateTaskState(TaskState.RUNNING);

        // Run the test
        long taskId = statisticsTaskUnderTest.getId();
        statisticsJobUnderTest.updateJobInfoByTaskId(taskId, "errorMsg");

        // Verify the results
        JobState jobState = statisticsJobUnderTest.getJobState();
        Assert.assertEquals(JobState.FAILED, jobState);

        TaskState taskState = statisticsTaskUnderTest.getTaskState();
        Assert.assertEquals(TaskState.FAILED, taskState);
    }

    @Test
    public void testUpdateJobInfoByTaskId_ThrowsDdlException() {
        // Run the test
        long taskId = statisticsTaskUnderTest.getId();
        Assert.assertThrows(DdlException.class,
                () -> statisticsJobUnderTest.updateJobInfoByTaskId(taskId, ""));
    }
}
