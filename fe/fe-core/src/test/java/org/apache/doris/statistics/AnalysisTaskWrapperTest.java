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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class AnalysisTaskWrapperTest {

    @Test
    public void testOOMErrorMessageReplacement() throws Exception {
        AnalysisTaskExecutor executor = new AnalysisTaskExecutor(1);
        AnalysisInfo info = new AnalysisInfoBuilder().setScheduleType(ScheduleType.ONCE).build();

        BaseAnalysisTask task = new BaseAnalysisTask(info) {
            @Override
            public void doExecute() throws Exception {
                throw new Exception("(172.16.0.90)[MEM_LIMIT_EXCEEDED]PreCatch error code:11, "
                        + "[E11] Allocator mem tracker check failed... can `set exec_mem_limit` to change limit, "
                        + "details see be.INFO.");
            }
        };

        AnalysisJob mockJob = Mockito.mock(AnalysisJob.class);
        task.job = mockJob;

        AnalysisTaskWrapper wrapper = new AnalysisTaskWrapper(executor, task);

        new MockUp<MetricRepo>() {
            @Mock
            public void init() {
                // Mock init to prevent NPE
            }
        };
        Deencapsulation.setField(MetricRepo.class, "isInit", true);
        Deencapsulation.setField(MetricRepo.class, "COUNTER_STATISTICS_FAILED_ANALYZE_TASK", new LongCounterMetric("test", "", ""));
        
        wrapper.run();

        Mockito.verify(mockJob, Mockito.times(1)).taskFailed(Mockito.eq(task),
                Mockito.contains("can modify `statistics_sql_mem_limit_in_bytes` to increase limit, "
                        + "or decrease `huge_table_default_sample_rows` to reduce memory usage, "
                        + "or use `statistics_max_string_column_length` to skip large string columns"));
    }
}