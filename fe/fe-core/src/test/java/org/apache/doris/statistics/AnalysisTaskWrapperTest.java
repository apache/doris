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
import org.apache.doris.common.DdlException;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class AnalysisTaskWrapperTest {

    @Test
    public void testOOMErrorMessageReplacement() throws Exception {
        AnalysisInfo analysisInfo = new AnalysisInfoBuilder().setJobId(1).setTaskId(2)
                .setScheduleType(ScheduleType.ONCE).build();
        AnalysisJob mockJob = Mockito.mock(AnalysisJob.class);

        BaseAnalysisTask task = new BaseAnalysisTask() {
            @Override
            public void execute() throws Exception {
                doExecute();
            }

            @Override
            public void doExecute() throws Exception {
                throw new Exception("(172.16.0.90)[MEM_LIMIT_EXCEEDED]PreCatch error code:11, "
                        + "[E11] Allocator mem tracker check failed... can `set exec_mem_limit` to change limit, "
                        + "details see be.INFO.");
            }

            @Override
            protected void doSample() {
            }

            @Override
            protected void deleteNotExistPartitionStats(AnalysisInfo jobInfo) throws DdlException {
            }

            @Override
            public String toString() {
                return "test-analysis-task";
            }
        };
        task.info = analysisInfo;
        task.job = mockJob;

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::isCheckpointThread).thenReturn(true);
            AnalysisTaskExecutor executor = new AnalysisTaskExecutor(1);
            AnalysisTaskWrapper wrapper = new AnalysisTaskWrapper(executor, task);

            wrapper.run();
        }

        ArgumentCaptor<String> msgCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockJob).taskFailed(Mockito.eq(task), msgCaptor.capture());
        String errorMsg = msgCaptor.getValue();
        Mockito.verifyNoMoreInteractions(mockJob);
        org.junit.jupiter.api.Assertions.assertTrue(errorMsg.contains("can `set exec_mem_limit` to change limit"));
        org.junit.jupiter.api.Assertions.assertTrue(
                errorMsg.contains("For statistics analyze, you can increase `statistics_sql_mem_limit_in_bytes`, "
                        + "decrease `huge_table_default_sample_rows`, "
                        + "or use `statistics_max_string_column_length` to skip large string columns."));
    }
}
